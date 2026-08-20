package consume

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
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
	// Production wires ap.NewGuardedHTTPClient(cfg.AllowPrivateAddresses, 30s)
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

// didDocument is the sliver of a DID document this resolver reads: the handle
// claims, and the services that name where the repo is hosted (see
// account_status.go, which confirms a deletion against that PDS).
type didDocument struct {
	AlsoKnownAs []string     `json:"alsoKnownAs"`
	Service     []didService `json:"service"`
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
	if len(alsoKnownAs) == 0 {
		// TRANSIENT, deliberately. An empty alsoKnownAs is a complete answer
		// today, but it is an answer about a mutable external world: a user
		// who publishes their handle minutes after their first comment must
		// still get that comment federated. Under this consumer's semantics a
		// permanent failure is dead-lettered with its redrive budget already
		// spent and never retried, so "permanent" here would mean "lost until
		// a human intervenes", while transient costs ten cheap retries and
		// reaches the same terminal state if the handle never appears.
		return "", fmt.Errorf("DID document publishes no alsoKnownAs yet")
	}
	// PERMANENT. The DID has published identity claims and not one of them is
	// an atproto handle — https:// profile links and mailto: addresses are
	// perfectly valid alsoKnownAs entries and are simply not handles. Retrying
	// re-reads the same list; the recovery path is the user publishing a
	// handle, which arrives as a new event rather than a redrive of this one.
	return "", fmt.Errorf("%w: DID document claims no atproto handle", ErrPermanentEvent)
}

// verifyHandleClaimsDID is the reverse direction: the handle must name the DID
// back. DNS is tried first; the well-known is the fallback the spec allows,
// and is what most PDS-hosted handles use.
//
// The DNS RESULT taxonomy is load-bearing (second-opinion C5). A well-known
// naming a different DID is impersonation and permanent ONLY when DNS
// authoritatively said the handle has no record — because then the well-known
// is the whole answer. When DNS was UNREACHABLE (SERVFAIL, a timeout) we never
// learned the owner's authoritative claim, so a mismatched well-known cannot
// be trusted as impersonation: an attacker who controls the handle's web
// server but not its DNS would otherwise win a permanent verdict during a DNS
// blip, stranding a legitimate mint. That case is TRANSIENT so the redrive
// re-checks once DNS recovers.
func (r *HandleResolver) verifyHandleClaimsDID(ctx context.Context, handle, did string) error {
	dnsAuthoritative := true
	if r.lookupTXT != nil {
		claimed, found, authoritative := r.lookupTXTDID(ctx, handle)
		if found {
			if claimed == did {
				return nil
			}
			// DNS itself named a different DID: authoritative impersonation.
			// The TXT value is written by whoever runs the handle's zone, so it
			// is quoted and capped like any other remote claim.
			return fmt.Errorf("%w: handle %s DNS claims %s, not %s",
				ErrPermanentEvent, handle, quoteRemoteClaim(claimed), did)
		}
		dnsAuthoritative = authoritative
	}
	return r.verifyWellKnown(ctx, handle, did, dnsAuthoritative)
}

// atprotoTXTPrefix is the subdomain the handle's DID claim is published under.
const atprotoTXTPrefix = "_atproto."

// lookupTXTDID reads the DID a handle publishes over DNS.
//
// The third return distinguishes an AUTHORITATIVE answer from an outage.
// authoritative is true when DNS gave a definitive result — a record was
// found, OR the name resolved to NXDOMAIN (net.DNSError.IsNotFound), which is
// an authoritative "this handle publishes no DNS claim". It is false only when
// DNS was UNREACHABLE (SERVFAIL, timeout), where the absence of a record tells
// us nothing about the real owner's claim.
func (r *HandleResolver) lookupTXTDID(ctx context.Context, handle string) (did string, found, authoritative bool) {
	records, err := r.lookupTXT(ctx, atprotoTXTPrefix+handle)
	if err != nil {
		var dnsErr *net.DNSError
		if stderrors.As(err, &dnsErr) && dnsErr.IsNotFound {
			// NXDOMAIN: DNS authoritatively has no record for this handle, so
			// the well-known is the answer.
			return "", false, true
		}
		// Reachable-but-broken DNS: not authoritative. The well-known must not
		// be trusted to condemn the handle on its own.
		r.logger.Debug("DNS lookup for _atproto record failed (not authoritative); falling back to the well-known",
			slog.String("handle", handle), slog.String("error", err.Error()))
		return "", false, false
	}
	for _, record := range records {
		if claimed, ok := strings.CutPrefix(strings.TrimSpace(record), "did="); ok {
			return strings.TrimSpace(claimed), true, true
		}
	}
	// DNS answered but published no atproto claim: authoritative "no record".
	return "", false, true
}

// verifyWellKnown fetches https://{handle}/.well-known/atproto-did and
// compares it to the DID.
//
// A 200 naming a DIFFERENT DID is impersonation and permanent ONLY when DNS
// was authoritative (dnsAuthoritative); during a DNS outage the same mismatch
// is transient (see verifyHandleClaimsDID). Everything else — a 5xx, a network
// error, or a 404 — is transient regardless: a 404 is NOT a disavowal, since
// the handle may publish its claim over DNS only or not have finished setting
// it up, and both become true later.
func (r *HandleResolver) verifyWellKnown(ctx context.Context, handle, did string, dnsAuthoritative bool) error {
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
	claimed := strings.TrimSpace(string(body))
	if claimed == did {
		return nil
	}
	if !dnsAuthoritative {
		// DNS was unreachable, so we never learned the owner's authoritative
		// claim; a mismatched well-known cannot be trusted as impersonation.
		// Transient, so the redrive re-checks once DNS recovers.
		return fmt.Errorf("handle %s well-known claims %s, not %s, but DNS was unreachable",
			handle, quoteRemoteClaim(claimed), did)
	}
	return fmt.Errorf("%w: handle %s claims %s, not %s",
		ErrPermanentEvent, handle, quoteRemoteClaim(claimed), did)
}

// maxQuotedClaimBytes bounds how much of a remote claim an error message
// repeats. A did:plc is 32 characters; anything past this is not a claim being
// reported, it is a body being transcribed.
const maxQuotedClaimBytes = 128

// quoteRemoteClaim renders a DID claimed by a REMOTE party — a well-known body
// from a host named in a stranger's DID document, or a TXT record from that
// handle's zone — as operator-facing evidence rather than a transcript.
//
// Two properties, both load-bearing rather than cosmetic. It QUOTES
// (strconv.Quote escapes a NUL to the four printable characters `\x00` and
// coerces invalid UTF-8 to escapes), because these errors become a dead
// letter's last_error, a postgres TEXT column that rejects a NUL outright — an
// echoed raw body would let a stranger's server fail the write meant to capture
// its own failure. And it CAPS the length, because the read is bounded at 1 KiB
// but the error is rewritten into that column on every redrive pass.
func quoteRemoteClaim(claimed string) string {
	if len(claimed) > maxQuotedClaimBytes {
		// Cut on the byte, then let Quote escape whatever partial rune the cut
		// left behind; the marker is outside the quotes so it cannot be read as
		// part of what the remote actually said.
		return strconv.Quote(claimed[:maxQuotedClaimBytes]) + "…"
	}
	return strconv.Quote(claimed)
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
