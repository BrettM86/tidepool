package consume

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// Account-status confirmation for decision 19's terminal tier.
//
// A #account event says the repo's status CHANGED; it does not prove what the
// status is now. The event may be stale (a reconnect rewind replays it), it may
// be a status the user has since reversed, and the action it triggers — asking
// every peer to delete the user's content — is one no peer undoes. So the
// terminal tier confirms against the identity's own sources before sending
// anything, and this is the read that does it.

// atprotoPDSServiceID is the DID document service entry that names a repo's
// hosting PDS.
const atprotoPDSServiceID = "atproto_pds"

// maxRepoStatusBytes caps the PDS response. getRepoStatus answers a few dozen
// bytes; the host is named by a document a stranger controls, so it is not read
// unbounded.
const maxRepoStatusBytes = 1 << 12

// AccountStatus reports whether the DID's repo is DELETED, confirmed against
// the identity's own sources: the PLC directory for the DID document, and the
// PDS that document names for the repo's status.
//
// THREE OUTCOMES, AND THEY MUST STAY THREE:
//
//	(true,  nil) — confirmed deleted. The destructive tier may run.
//	(false, nil) — confirmed live. Nothing destructive; the event is handled.
//	(_,     err) — WE DO NOT KNOW. Nothing destructive, nothing recorded, retry.
//
// "We could not confirm" must never collapse into "confirmed not deleted", and
// it must never collapse into "confirmed deleted" either. Both directions are
// unrecoverable in opposite ways: one asks every peer to erase a user who never
// left, the other silently drops a real deletion and leaves their content
// federated forever.
//
// That collapse is 17c-3's P1-c one layer up. There, a ban's `expires` was read
// through a helper that returned nil for BOTH "absent" and "present but
// unparseable"; nil meant permanent; and an author was excluded forever because
// a timestamp did not parse. The shape of the bug was not the timestamp — it
// was two different facts arriving as one value on the field where the default
// was irreversible. This is that field, one layer up, so the error stays an
// error the whole way out.
//
// Every failure here is therefore an ERROR rather than a verdict: a directory
// that will not answer, a document with no PDS, a PDS that returns anything but
// a status we can read. Retrying costs a request; guessing costs a user's
// content.
func (r *HandleResolver) AccountStatus(ctx context.Context, did string) (bool, error) {
	if err := validatePLCDID(did); err != nil {
		return false, err
	}
	document, err := r.fetchDIDDocument(ctx, did)
	if err != nil {
		return false, fmt.Errorf("confirm account status for %s: %w", did, err)
	}
	endpoint, err := pdsEndpoint(document)
	if err != nil {
		// A document with no PDS is NOT read as "deleted", tempting as that is:
		// the same shape appears while a document is mid-rewrite, and acting on
		// it would erase a user whose hosting simply moved.
		return false, fmt.Errorf("confirm account status for %s: %w", did, err)
	}
	return r.repoDeleted(ctx, endpoint, did)
}

// didService is the sliver of a DID document's service list this reads.
type didService struct {
	ID              string `json:"id"`
	Type            string `json:"type"`
	ServiceEndpoint string `json:"serviceEndpoint"`
}

// pdsEndpoint finds the repo's hosting PDS. The id suffix is what identifies it
// (documents spell it "#atproto_pds" or the full "did:plc:xxx#atproto_pds").
//
// THE ENDPOINT IS UNTRUSTED TEXT IN THE ORDINARY CASE, not merely under attack:
// it is written by the DID's own controller, which is the party a deletion
// verdict is about. So it is validated to exhaustion BEFORE the first packet —
// a guard that fires on the RESPONSE has already sent the bridge somewhere a
// stranger chose, and has already leaked which DID it is about to act on.
func pdsEndpoint(document *didDocument) (*url.URL, error) {
	for _, service := range document.Service {
		// EXACT fragment match, not a suffix test: "#not_atproto_pds" ends with
		// the same characters, and a document its own subject writes is where
		// that shows up. Both spellings are legal — the bare "#atproto_pds" and
		// the fully-qualified "did:plc:xxx#atproto_pds" — so what is compared is
		// the fragment itself.
		if _, fragment, found := strings.Cut(service.ID, "#"); !found || fragment != atprotoPDSServiceID {
			continue
		}
		return checkedPDSEndpoint(service.ServiceEndpoint)
	}
	return nil, fmt.Errorf("DID document names no atproto PDS")
}

// checkedPDSEndpoint accepts only a plain, absolute HTTPS origin.
//
// HTTPS IS NOT A PREFERENCE HERE. This response decides whether a user's content
// is erased from every instance that holds it, and no peer un-deletes. Over
// cleartext, anyone on the path can WRITE {"active":false,"status":"deleted"} —
// the irreversible verdict, for free, with no credential and no compromise of
// either endpoint. There is no allow-insecure option on purpose: a deployment
// whose PDS is reachable only over http cannot confirm deletions, and it fails
// CLOSED (a retryable error an operator can see) rather than acting on an answer
// nobody can vouch for.
//
// Userinfo is refused too: credentials in a URL a stranger wrote are not ours to
// send, and Go would put them on the wire.
func checkedPDSEndpoint(endpoint string) (*url.URL, error) {
	parsed, err := url.Parse(endpoint)
	switch {
	case err != nil:
		return nil, fmt.Errorf("DID document names an unparseable PDS endpoint")
	case parsed.Scheme != "https":
		return nil, fmt.Errorf(
			"DID document names a PDS endpoint that is not https; a confirmation over a channel "+
				"anyone can rewrite cannot decide an irreversible erasure (%q)", parsed.Scheme)
	case parsed.Host == "":
		return nil, fmt.Errorf("DID document names a PDS endpoint with no host")
	case parsed.User != nil:
		return nil, fmt.Errorf("DID document names a PDS endpoint carrying userinfo")
	case parsed.RawQuery != "" || parsed.Fragment != "":
		// An XRPC base is an origin with an optional path prefix. A query or
		// fragment on it means the value is not that, and appending to it would
		// silently produce a URL nobody wrote.
		return nil, fmt.Errorf("DID document names a PDS endpoint carrying a query or fragment")
	}
	parsed.Path = strings.TrimSuffix(parsed.Path, "/")
	return parsed, nil
}

// repoStatus is the sliver of com.atproto.sync.getRepoStatus this reads.
type repoStatus struct {
	Active bool   `json:"active"`
	Status string `json:"status"`
}

// repoStatusDeleted is the one status value that means the repo is gone. The
// others — takendown, suspended, deactivated — are states a user comes back
// from, which is the whole distinction decision 19 draws between the transient
// tier and this one.
const repoStatusDeleted = "deleted"

// repoDeleted asks the PDS what it holds for this repo.
//
// ONLY an explicit, readable answer decides. A non-200 is unknown — a PDS that
// 400s an unrecognised repo looks identical to one that is misconfigured, and
// "the host said something we did not understand" is not evidence a user
// deleted their account.
func (r *HandleResolver) repoDeleted(ctx context.Context, endpoint *url.URL, did string) (bool, error) {
	// BUILT FROM THE PARSED URL, never by concatenation: JoinPath escapes the
	// segments and preserves a path prefix (https://host/pds), and setting the
	// query as a value keeps the did from being pasted into a string that may
	// already contain one.
	target := endpoint.JoinPath("/xrpc/com.atproto.sync.getRepoStatus")
	target.RawQuery = url.Values{"did": []string{did}}.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return false, fmt.Errorf("build repo status request for %s: %w", did, err)
	}
	request.Header.Set("Accept", "application/json")
	if r.userAgent != "" {
		request.Header.Set("User-Agent", r.userAgent)
	}

	// EVERY HOP STAYS ON THE NAMED PDS, over https. The DID document naming that
	// PDS is the ENTIRE authorization for this answer — it is what makes the
	// response evidence about this repo rather than an opinion from a stranger —
	// and a redirect is written by the very server whose answer we are trying to
	// verify, so "it told us to" is exactly as trustworthy as the answer itself.
	//
	// The client is COPIED rather than mutated: this policy belongs to this
	// request, and the same client also fetches DID documents and well-knowns,
	// where redirects are ordinary. Copying shares the transport (and its SSRF
	// guard) while giving this call its own rules.
	client := *r.httpClient
	client.CheckRedirect = func(hop *http.Request, _ []*http.Request) error {
		if hop.URL.Scheme != "https" || !strings.EqualFold(hop.URL.Host, request.URL.Host) {
			return fmt.Errorf(
				"refusing redirect to %s: a deletion verdict may come only from the PDS the DID "+
					"document names, over https", hop.URL.Redacted())
		}
		return nil
	}

	response, err := client.Do(request)
	if err != nil {
		return false, fmt.Errorf("fetch repo status for %s: %w", did, err)
	}
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode != http.StatusOK {
		return false, fmt.Errorf("fetch repo status for %s: pds returned %d", did, response.StatusCode)
	}
	var status repoStatus
	if err := json.NewDecoder(io.LimitReader(response.Body, maxRepoStatusBytes)).Decode(&status); err != nil {
		return false, fmt.Errorf("decode repo status for %s: %w", did, err)
	}
	// Both halves are required. `active` alone would read a suspension as a
	// deletion, and `status` alone would believe a field the PDS may omit for a
	// live repo.
	return !status.Active && status.Status == repoStatusDeleted, nil
}
