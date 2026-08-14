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
const atprotoPDSServiceID = "#atproto_pds"

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
// (documents spell it "#atproto_pds" or the full "did:plc:xxx#atproto_pds"),
// and the endpoint must be an absolute http(s) URL before it reaches a request:
// it comes from a document its own subject controls.
func pdsEndpoint(document *didDocument) (string, error) {
	for _, service := range document.Service {
		if !strings.HasSuffix(service.ID, atprotoPDSServiceID) {
			continue
		}
		parsed, err := url.Parse(service.ServiceEndpoint)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return "", fmt.Errorf("DID document names a PDS endpoint that is not an absolute http(s) URL")
		}
		return strings.TrimSuffix(service.ServiceEndpoint, "/"), nil
	}
	return "", fmt.Errorf("DID document names no atproto PDS")
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
func (r *HandleResolver) repoDeleted(ctx context.Context, endpoint, did string) (bool, error) {
	target := endpoint + "/xrpc/com.atproto.sync.getRepoStatus?did=" + url.QueryEscape(did)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return false, fmt.Errorf("build repo status request for %s: %w", did, err)
	}
	request.Header.Set("Accept", "application/json")
	if r.userAgent != "" {
		request.Header.Set("User-Agent", r.userAgent)
	}

	response, err := r.httpClient.Do(request)
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
