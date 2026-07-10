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
	"net/http"
	"net/url"
	"strings"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/bluesky-social/indigo/atproto/syntax"

	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// maxHandleAttempts bounds collision suffixing: base handle plus -2..-N.
const maxHandleAttempts = 50

// maxPLCErrorBody caps how much of a PLC directory error response is read
// back for the error message.
const maxPLCErrorBody = 4 << 10

// MintRequest describes the fediverse actor an identity is minted for.
type MintRequest struct {
	// ActorType selects person or group; the handle scheme is the same for
	// both (name.instance-with-dashes.bridge-hostname), the type only
	// drives logging.
	ActorType store.ActorType
	// PreferredUsername is the AP preferredUsername (e.g. "alice",
	// "technology").
	PreferredUsername string
	// Instance is the actor's home host (e.g. "lemmy.world").
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
	// bridgeScheme is the URL scheme of the advertised PDS endpoint
	// ("https" everywhere real; "http" only under the local e2e harness).
	bridgeScheme string
	rotationKey  *atcrypto.PrivateKeyK256
	custodian    *Custodian
	actors       store.BridgedActors
	httpClient   *http.Client
	userAgent    string
	logger       *slog.Logger
}

// MinterOptions configures NewMinter. All fields except HTTPClient are
// required.
type MinterOptions struct {
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
	// Actors is consulted for handle-collision suffixing.
	Actors store.BridgedActors
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
		bridgeScheme:   scheme,
		rotationKey:    opts.RotationKey,
		custodian:      opts.Custodian,
		actors:         opts.Actors,
		httpClient:     opts.HTTPClient,
		userAgent:      userAgent,
		logger:         logger,
	}, nil
}

// MintActor generates a signing keypair, picks a free bridged handle, signs
// the genesis PLC operation with the escrow rotation key, registers it with
// the directory, and returns the identity with the sealed signing key.
//
// It does NOT write bridged_actors: callers upsert the returned Identity
// themselves (they own consent state). A concurrent mint racing the same
// handle is caught by the bridged_actors handle unique index at that point.
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
	if !req.ActorType.Valid() {
		return nil, errors.NewValidationError("actor_type", fmt.Sprintf("unknown actor type %q", req.ActorType))
	}
	handle, err := m.availableHandle(ctx, req.PreferredUsername, req.Instance)
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

	op, err := m.signGenesisOp(genesisOperation(
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
// until it does not collide with an already-assigned handle.
func (m *Minter) availableHandle(ctx context.Context, username, instance string) (string, error) {
	base, instanceLabel, err := handleLabels(username, instance)
	if err != nil {
		return "", err
	}
	for attempt := 1; attempt <= maxHandleAttempts; attempt++ {
		name := base
		if attempt > 1 {
			name = suffixLabel(base, fmt.Sprintf("-%d", attempt))
		}
		candidate := fmt.Sprintf("%s.%s.%s", name, instanceLabel, m.bridgeHostname)
		if _, err := syntax.ParseHandle(candidate); err != nil {
			return "", errors.NewValidationError("handle",
				fmt.Sprintf("derived handle %q is not a valid atproto handle: %v", candidate, err))
		}
		_, err := m.actors.GetByHandle(ctx, candidate)
		if errors.IsNotFound(err) {
			return candidate, nil
		}
		if err != nil {
			return "", fmt.Errorf("identity: check handle availability for %q: %w", candidate, err)
		}
		// Taken; try the next suffix.
	}
	return "", errors.NewValidationError("handle",
		fmt.Sprintf("no free handle for %s@%s after %d attempts", username, instance, maxHandleAttempts))
}

// handleLabels normalizes the AP username and instance host into DNS labels
// per the locked handle scheme: dots separate what @/! separated on the
// fediverse side, and the instance's own dots become dashes
// (alice@lemmy.world → alice.lemmy-world.<bridge>). Characters that cannot
// appear in a handle label (Lemmy allows underscores in usernames) map to
// dashes too.
func handleLabels(username, instance string) (name string, instanceLabel string, err error) {
	name = normalizeLabel(username)
	if name == "" && username != "" {
		// All-CJK/emoji usernames normalize to nothing; fall back to a
		// deterministic hash-derived label so those actors still bridge.
		name = fallbackLabel(username)
	}
	if name == "" {
		return "", "", errors.NewValidationError("preferred_username",
			fmt.Sprintf("%q normalizes to an empty handle label", username))
	}
	instanceLabel = normalizeLabel(strings.ReplaceAll(instance, ".", "-"))
	if instanceLabel == "" {
		return "", "", errors.NewValidationError("instance",
			fmt.Sprintf("%q normalizes to an empty handle label", instance))
	}
	return name, instanceLabel, nil
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

// signGenesisOp signs the operation with the escrow rotation key: the
// signature is over the DAG-CBOR encoding of the op without its sig field,
// base64url (no padding) encoded, per the PLC spec.
func (m *Minter) signGenesisOp(op map[string]any) (map[string]any, error) {
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
