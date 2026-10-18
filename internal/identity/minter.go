package identity

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base32"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"golang.org/x/net/idna"
	"golang.org/x/text/unicode/norm"

	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// maxHandleAttempts bounds collision suffixing: base handle plus -2..-N.
const maxHandleAttempts = 50

// maxPLCErrorBody caps how much of a PLC directory error response is read
// back for the error message.
const maxPLCErrorBody = 4 << 10

// MintRequest describes the fediverse actor whose handle is minted as
// <user label>.<instance host>.<handle zone root>.
type MintRequest struct {
	// ActorType selects person or group. A group's user label gets the "c-"
	// prefix; a person's label that would start with "c-" gets "u-" instead.
	ActorType store.ActorType
	// PreferredUsername is the AP preferredUsername (e.g. "alice",
	// "technology").
	PreferredUsername string
	// Instance is the actor's home DNS hostname without a port (e.g.
	// "lemmy.world"); a port or an IP address is a validation error.
	Instance string
}

// Identity is a freshly minted did:plc plus its escrowed key material. The
// caller (tasks 05/06) persists it via store.BridgedActors.UpsertActor —
// SigningKeyEncrypted goes into the signing_key column as-is.
type Identity struct {
	DID    string
	Handle string
	// DIDKey is the did:key encoding of the actor's verification
	// (signing) public key, as registered in the PLC operation.
	DIDKey string
	// SigningKeyEncrypted is the actor's secp256k1 private key sealed with
	// the bridge KEK, bound to DID.
	SigningKeyEncrypted []byte
}

// Minter creates did:plc identities on a PLC directory for bridged actors.
//
// Per PLAN.md: each actor gets its own secp256k1 signing key (the
// verification key in the DID document); the bridge's single escrow
// rotation key is the only rotation key, which is what makes later
// claiming/migration possible (the bridge can sign a PLC op handing the
// identity over). The PDS endpoint in every DID doc is the bridge itself.
type Minter struct {
	plcURL         string
	bridgeHostname string
	handleZoneRoot string
	// bridgeScheme is the URL scheme of the advertised PDS endpoint
	// ("https" everywhere real; "http" only under the local e2e harness).
	bridgeScheme string
	rotationKey  *atcrypto.PrivateKeyK256
	custodian    *Custodian
	actors       interface {
		HandleTaken(context.Context, string) (bool, error)
	}
	httpClient   *http.Client
	userAgent    string
	logger       *slog.Logger
	plcRetryWait func(context.Context, time.Duration) error
}

// MinterOptions configures NewMinter. All fields except HTTPClient are
// required.
type MinterOptions struct {
	// HandleZoneRoot anchors newly minted handles (e.g. ap.tdpl.io).
	HandleZoneRoot string
	// PLCDirectoryURL is the directory ops are POSTed to
	// (config.PLCDirectoryURL, e.g. https://plc.directory).
	PLCDirectoryURL string
	// BridgeHostname anchors the handle space and the PDS endpoint
	// (config.BridgeHostname).
	BridgeHostname string
	// BridgeScheme is the URL scheme of the PDS endpoint advertised in
	// minted DID documents (config.BridgeScheme). "https" everywhere real;
	// "http" exists for the local e2e harness. Empty means "https" — the
	// same defensive default as ap.ServiceActor.BaseURL.
	BridgeScheme string
	// RotationKey is the bridge escrow rotation key
	// (LoadOrCreateRotationKey).
	RotationKey *atcrypto.PrivateKeyK256
	// Custodian seals the per-actor signing keys.
	Custodian *Custodian
	// PLCRetryWait waits between PLC retry attempts; nil uses a timer.
	PLCRetryWait func(ctx context.Context, delay time.Duration) error
	// Actors is consulted for handle-collision suffixing.
	Actors interface {
		HandleTaken(context.Context, string) (bool, error)
	}
	// HTTPClient makes the directory requests. Production wires
	// ap.NewGuardedHTTPClient(cfg.AllowPrivateAddresses, 0) so PLC egress
	// shares the AP client's SSRF guard; tests hitting 127.0.0.1 pass a
	// guard-disabled client the same way.
	HTTPClient *http.Client
	// UserAgent is sent on every directory request (config.UserAgent —
	// same identification convention as the AP client). Defaults to the AP
	// client's fallback when empty.
	UserAgent string
	Logger    *slog.Logger
}

// NewMinter validates the options and builds a Minter.
func NewMinter(opts MinterOptions) (*Minter, error) {
	u, err := url.Parse(opts.PLCDirectoryURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, errors.NewValidationError("plc_directory_url",
			fmt.Sprintf("%q is not an absolute http(s) URL", opts.PLCDirectoryURL))
	}
	if opts.BridgeHostname == "" {
		return nil, errors.NewValidationError("bridge_hostname", "must not be empty")
	}
	if opts.HandleZoneRoot == "" {
		return nil, errors.NewValidationError("handle_zone_root", "must not be empty")
	}
	scheme := opts.BridgeScheme
	switch scheme {
	case "":
		scheme = "https"
	case "http", "https":
	default:
		return nil, errors.NewValidationError("bridge_scheme",
			fmt.Sprintf("must be http or https, got %q", opts.BridgeScheme))
	}
	if opts.RotationKey == nil {
		return nil, errors.NewValidationError("rotation_key", "must not be nil")
	}
	if opts.Custodian == nil {
		return nil, errors.NewValidationError("custodian", "must not be nil")
	}
	if opts.Actors == nil {
		return nil, errors.NewValidationError("actors", "must not be nil")
	}
	if opts.HTTPClient == nil {
		return nil, errors.NewValidationError("http_client",
			"must be provided (use ap.NewGuardedHTTPClient so PLC egress is SSRF-guarded)")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	userAgent := opts.UserAgent
	if userAgent == "" {
		userAgent = "tidepool/0.1" // matches internal/ap's client fallback
	}
	return &Minter{
		plcURL:         strings.TrimRight(opts.PLCDirectoryURL, "/"),
		bridgeHostname: strings.ToLower(opts.BridgeHostname),
		handleZoneRoot: strings.ToLower(opts.HandleZoneRoot),
		bridgeScheme:   scheme,
		rotationKey:    opts.RotationKey,
		custodian:      opts.Custodian,
		actors:         opts.Actors,
		httpClient:     opts.HTTPClient,
		userAgent:      userAgent,
		logger:         logger,
		plcRetryWait:   opts.PLCRetryWait,
	}, nil
}

// MintActor generates a signing keypair, picks a free bridged handle, signs
// the genesis PLC operation with the escrow rotation key, registers it with
// the directory, and returns the identity with the sealed signing key.
//
// It does NOT write bridged_actors: callers upsert the returned Identity
// themselves (they own consent state). A concurrent mint racing the same
// handle is caught by the store's transactional handle claim at that point.
//
// Failure semantics: the DID is derived locally and the signing key is
// sealed BEFORE the operation is submitted, so a key-custody failure can
// never orphan a registered DID. A failure during submission, however, can
// still leave a live DID on the directory (registration is irreversible and
// e.g. a timeout may fire after the directory processed the op) with no
// bridged_actors row. Such orphans are logged at error level with did and
// handle so they can be found; making a retry after the resulting handle
// collision reuse the registered DID is deferred to tasks 05/06.
func (m *Minter) MintActor(ctx context.Context, req MintRequest) (*Identity, error) {
	handle, err := m.availableHandle(ctx, req.ActorType, req.PreferredUsername, req.Instance)
	if err != nil {
		return nil, err
	}

	signingKey, err := atcrypto.GeneratePrivateKeyK256()
	if err != nil {
		return nil, fmt.Errorf("identity: generate signing key: %w", err)
	}
	signingPub, err := signingKey.PublicKey()
	if err != nil {
		return nil, fmt.Errorf("identity: derive signing public key: %w", err)
	}
	rotationPub, err := m.rotationKey.PublicKey()
	if err != nil {
		return nil, fmt.Errorf("identity: derive rotation public key: %w", err)
	}

	op, err := m.signOperation(genesisOperation(
		rotationPub.DIDKey(), signingPub.DIDKey(), handle, m.pdsEndpoint()))
	if err != nil {
		return nil, err
	}
	did, err := didForOperation(op)
	if err != nil {
		return nil, err
	}

	// Seal the key BEFORE submitting the irreversible PLC op: a custody
	// failure here aborts the mint with nothing registered anywhere.
	sealed, err := m.custodian.EncryptActorKey(did, signingKey)
	if err != nil {
		return nil, err
	}

	if err := m.submitOperation(ctx, did, op); err != nil {
		// The directory may have processed the op even though the call
		// failed (e.g. timeout after registration): treat this as a
		// potential orphaned DID and make it findable in the logs.
		m.logger.Error("PLC mint failed at submission; DID may be orphaned on the directory",
			"did", did, "handle", handle, "error", err)
		return nil, fmt.Errorf("identity: mint %s (handle %q): %w", did, handle, err)
	}

	m.logger.Info("minted did:plc for bridged actor",
		"did", did, "handle", handle, "actor_type", string(req.ActorType), "instance", req.Instance)

	return &Identity{
		DID:                 did,
		Handle:              handle,
		DIDKey:              signingPub.DIDKey(),
		SigningKeyEncrypted: sealed,
	}, nil
}

// pdsEndpoint is the PDS service endpoint advertised in every minted DID
// document: the bridge itself, under the configured scheme.
func (m *Minter) pdsEndpoint() string {
	return m.bridgeScheme + "://" + m.bridgeHostname
}

// availableHandle builds the bridged handle and suffixes it (-2, -3, ...)
// until it does not collide with a claim or an already-assigned handle.
// It rejects an unknown actor type before any lookup.
// The user label is <prefix><base label><suffix>: the prefix is "c-" for a
// group, "u-" for a person whose label would otherwise start with "c-" once
// suffixed and truncated (so no person can hold a community-shaped label,
// e.g. "c" suffixed to "c-2" becomes "u-c-2"), and empty otherwise. The
// prefix counts toward the 63-byte label limit and the 253-byte handle
// limit; truncation trims the base label, never the prefix or the suffix.
// A punycode (xn--) base label is never suffixed or truncated: punycode
// reads the label's last '-' as its delimiter, so either edit yields a label
// that no longer decodes or decodes to other text. When an xn-- label needs
// a suffix or does not fit after the prefix, the username's hash label is
// used instead.
func (m *Minter) availableHandle(ctx context.Context, actorType store.ActorType, username, instance string) (string, error) {
	if !actorType.Valid() {
		return "", errors.NewValidationError("actor_type", fmt.Sprintf("unknown actor type %q", actorType))
	}
	base, host, err := handleLabels(username, instance)
	if err != nil {
		return "", err
	}
	maxUserLength := min(63, 253-len(host)-len(m.handleZoneRoot)-2)
	if maxUserLength < len(labelPrefix(actorType, base))+1 {
		return "", errors.NewValidationError("instance", "host and handle zone leave no room for a user label")
	}
	for attempt := 1; attempt <= maxHandleAttempts; attempt++ {
		suffix := ""
		if attempt > 1 {
			suffix = fmt.Sprintf("-%d", attempt)
		}
		name, fits := fitUserLabel("", base, username, suffix, maxUserLength)
		if prefix := labelPrefix(actorType, name); fits && prefix != "" {
			name, fits = fitUserLabel(prefix, base, username, suffix, maxUserLength)
		}
		if !fits {
			return "", errors.NewValidationError("handle", "no room for a collision suffix")
		}
		candidate := fmt.Sprintf("%s.%s.%s", name, host, m.handleZoneRoot)
		if _, err := syntax.ParseHandle(candidate); err != nil {
			return "", errors.NewValidationError("handle",
				fmt.Sprintf("derived handle %q is not a valid atproto handle: %v", candidate, err))
		}
		taken, err := m.actors.HandleTaken(ctx, candidate)
		if err != nil {
			return "", fmt.Errorf("identity: check handle availability for %q: %w", candidate, err)
		}
		if !taken {
			return candidate, nil
		}
		// Taken; try the next suffix.
	}
	return "", errors.NewValidationError("handle",
		fmt.Sprintf("no free handle for %s@%s after %d attempts", username, instance, maxHandleAttempts))
}

// fitUserLabel builds <prefix><base label><suffix> within maxUserLength
// bytes, swapping an xn-- base label for the username's hash label when it
// needs a suffix or does not fit, and truncating the base label otherwise.
// It reports false when the prefix and suffix alone leave no room.
func fitUserLabel(prefix, base, username, suffix string, maxUserLength int) (string, bool) {
	if maxUserLength <= len(prefix)+len(suffix) {
		return "", false
	}
	label := base
	if strings.HasPrefix(label, "xn--") && (suffix != "" || len(prefix)+len(label) > maxUserLength) {
		label = fallbackLabel(username)
	}
	name := prefix + suffixLabel(label, suffix)
	if len(name) > maxUserLength {
		name = prefix + strings.TrimRight(label[:maxUserLength-len(prefix)-len(suffix)], "-") + suffix
	}
	return name, true
}

// labelPrefix returns the user-label prefix for an actor: "c-" for a group;
// "u-" for a person whose label starts with "c-", which reserves the "c-"
// label space for communities; otherwise none.
func labelPrefix(actorType store.ActorType, base string) string {
	if actorType == store.ActorTypeGroup {
		return "c-"
	}
	if strings.HasPrefix(base, "c-") {
		return "u-"
	}
	return ""
}

// handleLabels forms the base user label and host for
// <user label>.<instance host>.<handle zone root>. The host is lowercased
// and keeps its dots; it must be a bare hostname (callers pass
// url.Hostname()), so a host with a ':' (a port or an IPv6 literal) or an IP
// address is a validation error. The username is NFC-normalized before ASCII
// label normalization; names without ASCII letters or digits use an IDNA2008
// punycode label when valid, or a hash of the original bytes otherwise.
// availableHandle swaps a punycode label for the hash when it collides or
// does not fit.
func handleLabels(username, instance string) (name string, host string, err error) {
	name = normalizeLabel(norm.NFC.String(username))
	if name == "" && username != "" {
		name = idnaLabel(username)
		if name == "" {
			name = fallbackLabel(username)
		}
	}
	if name == "" {
		return "", "", errors.NewValidationError("preferred_username",
			fmt.Sprintf("%q normalizes to an empty handle label", username))
	}
	host = strings.ToLower(instance)
	if host == "" {
		return "", "", errors.NewValidationError("instance",
			fmt.Sprintf("%q has an empty host", instance))
	}
	if strings.Contains(host, ":") || net.ParseIP(host) != nil {
		return "", "", errors.NewValidationError("instance",
			fmt.Sprintf("%q is not a bare DNS hostname", instance))
	}
	return name, host, nil
}

var usernameIDNAProfile = idna.New(
	idna.MapForLookup(), idna.Transitional(false), idna.BidiRule(),
	idna.StrictDomainName(true), idna.ValidateLabels(true), idna.CheckJoiners(true),
)

// idnaLabel accepts only a single punycode label whose mapped code points
// are PVALID under RFC 5892. The lookup profile alone also accepts symbols
// that IDNA2008 disallows, so its mapping must be checked separately.
func idnaLabel(username string) string {
	mapped, err := usernameIDNAProfile.ToUnicode(norm.NFC.String(username))
	if err != nil || strings.Contains(mapped, ".") {
		return ""
	}
	for _, character := range mapped {
		if !idnaPVALID(character) {
			return ""
		}
	}
	label, err := usernameIDNAProfile.ToASCII(mapped)
	if err != nil || len(label) > 63 || !strings.HasPrefix(label, "xn--") {
		return ""
	}
	return label
}

// oldHangulJamo is the RFC 5892 §2.9 OldHangulJamo category: conjoining
// Jamo that are Lo but DISALLOWED under IDNA2008.
var oldHangulJamo = &unicode.RangeTable{R16: []unicode.Range16{
	{Lo: 0x1100, Hi: 0x11ff, Stride: 1},
	{Lo: 0xa960, Hi: 0xa97f, Stride: 1},
	{Lo: 0xd7b0, Hi: 0xd7ff, Stride: 1},
}}

// idnaPVALID approximates RFC 5892 PVALID: the LetterDigits categories
// (§2.1) plus '-', minus the Arabic-Indic digits (CONTEXTO), the
// OldHangulJamo ranges (§2.9), and the §2.6 Exceptions listed in the switch
// (first case PVALID, second case DISALLOWED or CONTEXTO).
func idnaPVALID(character rune) bool {
	// Arabic-Indic digit ranges are CONTEXTO, not PVALID, despite being Nd.
	if character >= 0x0660 && character <= 0x0669 ||
		character >= 0x06f0 && character <= 0x06f9 {
		return false
	}
	if unicode.Is(oldHangulJamo, character) {
		return false
	}
	switch character {
	case 0x00df, 0x03c2, 0x06fd, 0x06fe, 0x0f0b, 0x3007:
		return true
	case 0x0640, 0x07fa, 0x302e, 0x302f, 0x3031, 0x3032, 0x3033, 0x3034, 0x3035, 0x303b:
		return false
	}
	return character == '-' || unicode.Is(unicode.Ll, character) ||
		unicode.Is(unicode.Lu, character) || unicode.Is(unicode.Lo, character) ||
		unicode.Is(unicode.Lm, character) || unicode.Is(unicode.Lt, character) ||
		unicode.Is(unicode.Mn, character) || unicode.Is(unicode.Mc, character) ||
		unicode.Is(unicode.Nd, character)
}

// fallbackLabel derives a stable DNS label for a string with no characters
// representable in [a-z0-9]: "u" + the first 10 hex chars of its sha256.
func fallbackLabel(s string) string {
	sum := sha256.Sum256([]byte(s))
	return "u" + hex.EncodeToString(sum[:])[:10]
}

// suffixLabel appends suffix to base, truncating base first so the result
// still fits the 63-char DNS label limit (normalizeLabel truncates to 63
// BEFORE suffixing, so "-2".."-50" would otherwise overflow the label).
func suffixLabel(base, suffix string) string {
	if max := 63 - len(suffix); len(base) > max {
		base = strings.TrimRight(base[:max], "-")
	}
	return base + suffix
}

// normalizeLabel lowercases and maps a string into the DNS-label alphabet
// [a-z0-9-], collapsing runs of unrepresentable characters into single
// dashes and trimming dashes from the ends. Truncated to 63 chars (DNS
// label limit; atproto handles inherit it).
func normalizeLabel(s string) string {
	var b strings.Builder
	lastDash := true // suppress leading dash
	for _, r := range strings.ToLower(s) {
		switch {
		case r >= 'a' && r <= 'z' || r >= '0' && r <= '9':
			b.WriteRune(r)
			lastDash = false
		default:
			if !lastDash {
				b.WriteByte('-')
				lastDash = true
			}
		}
	}
	label := strings.TrimRight(b.String(), "-")
	if len(label) > 63 {
		label = strings.TrimRight(label[:63], "-")
	}
	return label
}

// genesisOperation builds the (unsigned) did:plc genesis operation. This is
// a PLC *operation*, not a DID document — same shape arroba/bridgy-fed
// publish: one escrow rotation key, the actor's own verification key, the
// bridged handle in alsoKnownAs, and the bridge as the PDS endpoint.
// https://github.com/did-method-plc/did-method-plc#operation-serialization-signing-and-validation
func genesisOperation(rotationDIDKey, signingDIDKey, handle, pdsEndpoint string) map[string]any {
	return map[string]any{
		"type":         "plc_operation",
		"rotationKeys": []any{rotationDIDKey},
		"verificationMethods": map[string]any{
			"atproto": signingDIDKey,
		},
		"alsoKnownAs": []any{"at://" + handle},
		"services": map[string]any{
			"atproto_pds": map[string]any{
				"type":     "AtprotoPersonalDataServer",
				"endpoint": pdsEndpoint,
			},
		},
		"prev": nil,
	}
}

// signOperation signs an unsigned operation with the escrow rotation key: the
// signature is over the DAG-CBOR encoding of the op without its sig field,
// base64url (no padding) encoded, per the PLC spec.
func (m *Minter) signOperation(op map[string]any) (map[string]any, error) {
	unsigned, err := atdata.MarshalCBOR(op)
	if err != nil {
		return nil, fmt.Errorf("identity: encode PLC op: %w", err)
	}
	sig, err := m.rotationKey.HashAndSign(unsigned)
	if err != nil {
		return nil, fmt.Errorf("identity: sign PLC op: %w", err)
	}
	op["sig"] = base64.RawURLEncoding.EncodeToString(sig)
	return op, nil
}

// didForOperation derives the did:plc from the signed genesis operation:
// base32(sha256(dag-cbor(signed op))) truncated to 24 chars, lowercased.
func didForOperation(signedOp map[string]any) (string, error) {
	encoded, err := atdata.MarshalCBOR(signedOp)
	if err != nil {
		return "", fmt.Errorf("identity: encode signed PLC op: %w", err)
	}
	sum := sha256.Sum256(encoded)
	hash := strings.ToLower(base32.StdEncoding.EncodeToString(sum[:]))
	return "did:plc:" + hash[:24], nil
}

// submitOperation POSTs the signed operation to the PLC directory.
func (m *Minter) submitOperation(ctx context.Context, did string, signedOp map[string]any) error {
	body, err := json.Marshal(signedOp)
	if err != nil {
		return fmt.Errorf("identity: marshal PLC op: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		m.plcURL+"/"+url.PathEscape(did), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("identity: build PLC request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", m.userAgent)

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("identity: POST PLC op for %s: %w", did, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, maxPLCErrorBody))
		return fmt.Errorf("identity: PLC directory rejected op for %s: status %d: %s",
			did, resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	return nil
}
