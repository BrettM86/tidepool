package consume

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/bluesky-social/indigo/atproto/syntax"

	"tidepool/internal/errors"
)

// The handle resolver exists because of one gap: a Jetstream commit carries a
// DID and no handle, but personas.CreateActorForDID FREEZES the local part at
// creation. Whatever name the first federating interaction supplies is the
// name that user wears on the fediverse forever, so it cannot be guessed, and
// it cannot be taken on trust either.
//
// Hence BIDIRECTIONAL verification. The DID document's alsoKnownAs is a claim
// made BY the DID about a handle; on its own it is unverified, and a DID can
// claim any handle it likes — including one that already belongs to somebody
// else. The handle has to claim the DID back before the pair is believed. A
// one-way check would let an attacker mint @alice@coves.social by publishing
// alsoKnownAs: at://alice.coves.social in their own DID doc, and because the
// local part is frozen, that theft would be permanent.

// DIDResolver answers "what handle does this DID verifiably own?".
type DIDResolver interface {
	// ResolveDIDHandle returns the DID's bidirectionally verified handle.
	//
	// Failure taxonomy matters here, because the caller is a Jetstream
	// handler: an error wrapping ErrPermanentEvent means the pairing can
	// never be believed (no handle in the document, an unsupported DID
	// method, a handle that names a different DID), while a bare error means
	// "ask again later" (directory 5xx, timeouts) and stays redrivable.
	ResolveDIDHandle(ctx context.Context, did string) (handle string, err error)
}

// LookupTXTFunc resolves DNS TXT records. It matches net.Resolver.LookupTXT.
type LookupTXTFunc func(ctx context.Context, name string) ([]string, error)

// DefaultLookupTXT is the production DNS resolver. Wire it into
// ResolverOptions.LookupTXT; it is a named function rather than an implicit
// default so that no test can silently reach the network through it.
func DefaultLookupTXT(ctx context.Context, name string) ([]string, error) {
	return net.DefaultResolver.LookupTXT(ctx, name)
}

// ResolverOptions configures a HandleResolver.
type ResolverOptions struct {
	// PLCDirectoryURL is the did:plc directory DID documents are fetched from
	// (config.PLCDirectoryURL, e.g. https://plc.directory).
	PLCDirectoryURL string
	// HTTPClient makes both the directory and the well-known requests.
	// Production wires ap.NewGuardedHTTPClient(cfg.AllowPrivateAddresses, 0)
	// so this egress shares the AP client's SSRF guard — which matters more
	// here than almost anywhere else, because the well-known host comes from
	// a DID document a stranger controls.
	HTTPClient *http.Client
	// UserAgent identifies the bridge on every request (config.UserAgent).
	UserAgent string
	// LookupTXT enables the DNS half of the handle-verification convention
	// (_atproto.{handle} TXT carrying "did=..."), which is tried BEFORE the
	// HTTPS well-known. Wire DefaultLookupTXT in production.
	//
	// Optional, and deliberately not defaulted: a resolver that silently fell
	// back to the system resolver would make any test that forgot to inject
	// one issue real DNS queries for the handles in its fixtures. Nil means
	// verification is well-known-only, which is announced at construction
	// rather than discovered from a TXT-only handle failing to verify.
	LookupTXT LookupTXTFunc
	Logger    *slog.Logger
}

// HandleResolver resolves and verifies a did:plc's handle.
type HandleResolver struct {
	plcURL     string
	httpClient *http.Client
	userAgent  string
	lookupTXT  LookupTXTFunc
	logger     *slog.Logger
}

var _ DIDResolver = (*HandleResolver)(nil)

// didPLCPrefix is the only DID method this task resolves.
const didPLCPrefix = "did:plc:"

// wellKnownDIDPath is the HTTPS half of atproto handle verification: the
// handle's own server serves the DID that owns it.
const wellKnownDIDPath = "/.well-known/atproto-did"

// Response body caps. A DID document is a few hundred bytes and a well-known
// response is one DID; both come from hosts a stranger controls, so neither is
// read unbounded.
const (
	maxDIDDocumentBytes = 1 << 20
	maxWellKnownBytes   = 1 << 10
)

// NewHandleResolver validates the options and builds a resolver.
func NewHandleResolver(opts ResolverOptions) (*HandleResolver, error) {
	if opts.HTTPClient == nil {
		// No default client. The well-known host is read out of a DID
		// document a stranger controls, which makes this the most
		// SSRF-exposed egress in the bridge; an unguarded http.DefaultClient
		// here would happily fetch http://169.254.169.254/.
		return nil, errors.NewValidationError("HTTPClient",
			"must not be nil: wrap the egress with ap.NewGuardedHTTPClient")
	}
	parsed, err := url.Parse(opts.PLCDirectoryURL)
	if err != nil {
		return nil, errors.NewValidationError("PLCDirectoryURL", err.Error())
	}
	if parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return nil, errors.NewValidationError("PLCDirectoryURL",
			"must be an absolute http(s) URL, got "+opts.PLCDirectoryURL)
	}

	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	if opts.LookupTXT == nil {
		logger.Warn("handle resolver has no DNS resolver: handles that publish only a " +
			"_atproto TXT record cannot be verified (pass consume.DefaultLookupTXT)")
	}
	return &HandleResolver{
		plcURL:     strings.TrimSuffix(opts.PLCDirectoryURL, "/"),
		httpClient: opts.HTTPClient,
		userAgent:  opts.UserAgent,
		lookupTXT:  opts.LookupTXT,
		logger:     logger,
	}, nil
}

// ResolveDIDHandle fetches the DID document, reads the handle it claims via
// alsoKnownAs, and confirms the handle claims the DID back before returning
// it.
//
// The reverse check tries the DNS convention first (_atproto.{handle} TXT
// carrying "did=...", when a LookupTXT is wired) and falls back to the HTTPS
// well-known (GET https://{handle}/.well-known/atproto-did, whose body is the
// DID) — the same order the atproto spec gives for handle resolution.
func (r *HandleResolver) ResolveDIDHandle(ctx context.Context, did string) (string, error) {
	if err := validatePLCDID(did); err != nil {
		// Rejected before any network call: a did:web sent to a PLC directory
		// is a wasted request at best.
		return "", err
	}

	document, err := r.fetchDIDDocument(ctx, did)
	if err != nil {
		return "", err
	}

	handle, err := handleFromAlsoKnownAs(document.AlsoKnownAs)
	if err != nil {
		return "", fmt.Errorf("%s: %w", did, err)
	}

	if err := r.verifyHandleClaimsDID(ctx, handle, did); err != nil {
		return "", err
	}
	return handle, nil
}

// didDocument is the sliver of a DID document this resolver reads.
type didDocument struct {
	AlsoKnownAs []string `json:"alsoKnownAs"`
}

// fetchDIDDocument reads the DID document from the PLC directory. Every
// non-200 is TRANSIENT, 404 included: this event exists because the repo
// committed, so the DID does exist, and a directory that has not caught up yet
// is propagation lag rather than a nonexistent identity.
func (r *HandleResolver) fetchDIDDocument(ctx context.Context, did string) (*didDocument, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, r.plcURL+"/"+did, nil)
	if err != nil {
		return nil, fmt.Errorf("build DID document request for %s: %w", did, err)
	}
	request.Header.Set("Accept", "application/did+ld+json, application/json")
	if r.userAgent != "" {
		request.Header.Set("User-Agent", r.userAgent)
	}

	response, err := r.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("fetch DID document for %s: %w", did, err)
	}
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch DID document for %s: directory returned %d", did, response.StatusCode)
	}

	var document didDocument
	if err := json.NewDecoder(io.LimitReader(response.Body, maxDIDDocumentBytes)).Decode(&document); err != nil {
		// A directory serving a body that is not a DID document is
		// transient: it is a fact about the directory, not about the DID.
		return nil, fmt.Errorf("decode DID document for %s: %w", did, err)
	}
	return &document, nil
}

// handleFromAlsoKnownAs picks the first entry that is actually an atproto
// handle. alsoKnownAs is a general-purpose field — https:// profile links and
// mailto: addresses live there too — so only at:// entries are considered, and
// each is parsed as a handle before it is believed: the value ends up in a URL
// HOST, so "evil.example/path" must never reach a request.
func handleFromAlsoKnownAs(alsoKnownAs []string) (string, error) {
	for _, entry := range alsoKnownAs {
		candidate, found := strings.CutPrefix(entry, "at://")
		if !found {
			continue
		}
		candidate = strings.ToLower(strings.TrimSpace(candidate))
		if _, err := syntax.ParseHandle(candidate); err != nil {
			continue
		}
		return candidate, nil
	}
	// RULED PERMANENT: the document is a complete answer, and it says this DID
	// claims no handle. Nothing about retrying changes what the document says,
	// so it is dead-lettered exhausted rather than redriven — the recovery
	// path is manual, by design.
	return "", fmt.Errorf("%w: DID document claims no atproto handle", ErrPermanentEvent)
}

// verifyHandleClaimsDID is the reverse direction: the handle must name the DID
// back. DNS is authoritative when it answers; the well-known is the fallback
// the spec allows, and is what most PDS-hosted handles use.
func (r *HandleResolver) verifyHandleClaimsDID(ctx context.Context, handle, did string) error {
	if r.lookupTXT != nil {
		claimed, found := r.lookupTXTDID(ctx, handle)
		if found {
			if claimed == did {
				return nil
			}
			return fmt.Errorf("%w: handle %s claims %s, not %s", ErrPermanentEvent, handle, claimed, did)
		}
	}
	return r.verifyWellKnown(ctx, handle, did)
}

// atprotoTXTPrefix is the subdomain the handle's DID claim is published under.
const atprotoTXTPrefix = "_atproto."

// lookupTXTDID reads the DID a handle publishes over DNS. A lookup error or a
// missing record is reported as "not found" rather than as a failure: the
// well-known fallback is the answer for every handle that does not use DNS,
// and DNS being unreachable must not condemn one that does.
func (r *HandleResolver) lookupTXTDID(ctx context.Context, handle string) (did string, found bool) {
	records, err := r.lookupTXT(ctx, atprotoTXTPrefix+handle)
	if err != nil {
		r.logger.Debug("no _atproto TXT record; falling back to the well-known",
			slog.String("handle", handle), slog.String("error", err.Error()))
		return "", false
	}
	for _, record := range records {
		if claimed, ok := strings.CutPrefix(strings.TrimSpace(record), "did="); ok {
			return strings.TrimSpace(claimed), true
		}
	}
	return "", false
}

// verifyWellKnown fetches https://{handle}/.well-known/atproto-did and
// compares it to the DID.
//
// A 200 naming a DIFFERENT DID is the impersonation case and is permanent: the
// handle has answered, and the answer is no. Everything else — a 5xx, a
// network error, or a 404 — is transient. A 404 in particular is NOT a
// disavowal: the handle may publish its claim over DNS only, or its owner may
// not have finished setting it up, and both become true later.
func (r *HandleResolver) verifyWellKnown(ctx context.Context, handle, did string) error {
	endpoint := "https://" + handle + wellKnownDIDPath
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return fmt.Errorf("build well-known request for %s: %w", handle, err)
	}
	request.Header.Set("Accept", "text/plain")
	if r.userAgent != "" {
		request.Header.Set("User-Agent", r.userAgent)
	}

	response, err := r.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("verify handle %s: %w", handle, err)
	}
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("verify handle %s: well-known returned %d", handle, response.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(response.Body, maxWellKnownBytes))
	if err != nil {
		return fmt.Errorf("verify handle %s: read well-known: %w", handle, err)
	}
	// Real PDSes serve the DID with a trailing newline; a byte-exact
	// comparison would reject every genuine handle on the network.
	if claimed := strings.TrimSpace(string(body)); claimed != did {
		return fmt.Errorf("%w: handle %s claims %s, not %s", ErrPermanentEvent, handle, claimed, did)
	}
	return nil
}

// validatePLCDID rejects everything this task cannot resolve, and does it
// before any network call. The identifier is also charset-checked because it
// is interpolated into the directory URL's PATH: a DID carrying a slash would
// address a different endpoint entirely.
func validatePLCDID(did string) error {
	identifier, isPLC := strings.CutPrefix(did, didPLCPrefix)
	if !isPLC || identifier == "" {
		return fmt.Errorf("%w: %q is not a did:plc, which is the only method this consumer resolves",
			ErrPermanentEvent, did)
	}
	for _, char := range identifier {
		if (char < 'a' || char > 'z') && (char < '0' || char > '9') {
			return fmt.Errorf("%w: did:plc identifier %q is not base32", ErrPermanentEvent, identifier)
		}
	}
	return nil
}
