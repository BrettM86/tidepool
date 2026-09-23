//go:build e2e

package e2e

// Task 18 deliverable 1: the harness gains an atproto repo host we did NOT
// write. Every other scenario in this suite drives OUR PDS implementation —
// the bridge's virtual repos — through OUR reading of the atproto spec, at
// both ends. A shared misreading between the thing that emits commits and
// the thing that consumes them is therefore invisible: both halves agree,
// and the suite goes green on the agreement rather than on the spec. The
// reference PDS (ghcr.io/bluesky-social/pds) is the third party that cannot
// share our misreadings. When a record written by an implementation nobody
// here controls crosses the same relay, the same firehose, and the same
// Jetstream that the bridge's records cross, the pipeline has been proven
// against the protocol instead of against itself.
//
// This scenario is deliberately a smoke test, not a feature test: it writes
// one postv2 into a native repo and watches for it downstream. What it
// buys is the wire, not the record.
//
// TRIPWIRE for anyone extending this file, in both directions. vetEvent
// (helpers.go) checks collections against expectedCollections globally, with
// no notion of which repo a record came from, so it fails CLOSED on a
// collection outside the whitelist — a scenario writing, say, a vote record
// into the native repo takes every other scenario down with it — and fails
// OPEN on one inside it: a social.coves.actor.profile written into the
// native repo is indistinguishable, to the whitelist, from the bridge
// writing the same collection into a repo it owns. That second half is why
// the whitelist wants rescoping by repo CLASS rather than by collection
// alone; it is task 18's sweep item and out of scope here.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
)

// ── Bootstrap contract ─────────────────────────────────────────────────────

// The reference PDS's bootstrap account. These three constants ARE the
// contract between this test and the compose stack: the `pds-bootstrap`
// one-shot in docker-compose.e2e.yml must create exactly this handle with
// exactly this password (com.atproto.server.createAccount, invites off),
// and the handle's domain must be one of the PDS's configured
// PDS_SERVICE_HANDLE_DOMAINS. Change either side without the other and the
// scenario fails at createSession with an authentication error rather than
// anything about federation.
const (
	nativeHandle   = "native-alice.pds.test"
	nativePassword = "native-alice-e2e-pass"
)

// nativeCommunityDID is a syntactically valid did:plc that nothing in this
// stack resolves — the smoke record needs the postv2 lexicon's required
// `community` field to PARSE as a DID, and nothing more. No acceptance
// record is ever written for this post: the bridge's Jetstream consumer
// (CONSUMER_ENABLED=1 in the e2e stack) does see it, but the dispatcher's
// pre-engine check (handlePostV2 → isBridgedCommunity) finds no
// communities row for this DID and skips the post unclaimed, so it never
// reaches the acceptance engine. The community is a name on a record, not a
// participant — never create a bridged community with this DID.
const nativeCommunityDID = "did:plc:e2enativepdssmokecommun2"

// pdsURL is the reference PDS's host endpoint. It is published on the RELAY
// service's ports (the pds container shares the relay's network namespace,
// so it cannot publish its own), and on 3081 rather than the PDS's natural
// 3001 because the Coves dev stack already owns 3001 on this machine.
func pdsURL() string { return envOr("PDS_E2E_URL", "http://localhost:3081") }

// ── Reference-PDS client ───────────────────────────────────────────────────

// pdsClient speaks the two XRPC endpoints this scenario needs against the
// reference PDS. Test-only, and deliberately not routed through any of
// tidepool's own atproto client code: a client that shared production's
// request-building would inherit production's assumptions about the server,
// which is the exact thing an outside implementation is here to test.
type pdsClient struct {
	http      *http.Client
	accessJWT string
	did       string
	handle    string
}

// do issues one JSON XRPC call, decoding into out when non-nil. Errors name
// the endpoint and carry the server's body: a reference PDS reports lexicon
// and auth failures in prose that is worth reading verbatim.
func (c *pdsClient) do(method, nsid string, body, out any) error {
	var reqBody io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reqBody = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, pdsURL()+"/xrpc/"+nsid, reqBody)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.accessJWT != "" {
		req.Header.Set("Authorization", "Bearer "+c.accessJWT)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("pds %s: status %d: %s", nsid, resp.StatusCode, truncate(raw, 300))
	}
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("pds %s: decode: %w (%s)", nsid, err, truncate(raw, 200))
		}
	}
	return nil
}

// createSession authenticates as the bootstrap account and remembers the
// DID the PDS minted for it — the DID this scenario then follows through
// the relay. We never hardcode that DID: it is whatever the local PLC
// issued when the bootstrap ran, and only the PDS can tell us.
func (c *pdsClient) createSession(handle, password string) error {
	var out struct {
		DID       string `json:"did"`
		Handle    string `json:"handle"`
		AccessJWT string `json:"accessJwt"`
	}
	err := c.do(http.MethodPost, "com.atproto.server.createSession",
		map[string]any{"identifier": handle, "password": password}, &out)
	if err != nil {
		return err
	}
	if out.DID == "" || out.AccessJWT == "" {
		return fmt.Errorf("pds createSession(%s): session missing did/accessJwt (did=%q)", handle, out.DID)
	}
	c.did, c.handle, c.accessJWT = out.DID, out.Handle, out.AccessJWT
	return nil
}

// createRecord writes into the session's OWN repo. Returns the at-uri the
// PDS assigned, which the caller checks against the rkey it asked for.
func (c *pdsClient) createRecord(collection, rkey string, record map[string]any) (uri, cid string, err error) {
	var out struct {
		URI string `json:"uri"`
		CID string `json:"cid"`
	}
	err = c.do(http.MethodPost, "com.atproto.repo.createRecord", map[string]any{
		"repo":       c.did,
		"collection": collection,
		"rkey":       rkey,
		"record":     record,
	}, &out)
	if err != nil {
		return "", "", err
	}
	return out.URI, out.CID, nil
}

// ── Scenario ───────────────────────────────────────────────────────────────

// TestNativePDS_RecordTransitsRelayToJetstream is the acceptance test for a
// reference PDS in the stack: a record written by @atproto/pds — an
// implementation this repo did not write and cannot quietly agree with —
// reaches the suite's Jetstream through the same relay every bridged commit
// crosses.
//
// The chain each assertion pins, in causal order — which is also the order
// they run in, so a break is reported by the hop that broke rather than by
// the last hop to time out. The reference PDS is reachable and the bootstrap
// account exists (createSession); the relay has crawled the PDS and built
// repo state for the account (a pre-write getLatestCommit — the
// account-creation commit is already a head, so this isolates "peering
// works" from "our record transits"); the PDS accepts and commits our record
// (createRecord); the relay validated the commit's signature against the DID
// it resolved from the local PLC and Jetstream decoded the relay's firehose
// (the awaited event — Jetstream's upstream IS the relay, so arrival there
// is the transit proven at once), carrying the exact CID the PDS assigned;
// and the relay advanced its own repo state to the commit it emitted.
func TestNativePDS_RecordTransitsRelayToJetstream(t *testing.T) {
	h := newHarness(t)

	pds := &pdsClient{http: h.http}
	if err := pds.createSession(nativeHandle, nativePassword); err != nil {
		t.Fatalf("createSession as the native bootstrap account %q at %s: %v\n"+
			"the e2e stack has no reference PDS (or its bootstrap did not run) — see docker-compose.e2e.yml: "+
			"without a repo host we did not implement, every commit this suite validates was produced and "+
			"consumed by our own reading of the spec, and a shared misreading stays invisible",
			nativeHandle, pdsURL(), err)
	}
	t.Logf("native account: handle=%s did=%s (minted by the reference PDS against the local PLC)", pds.handle, pds.did)

	// Peering first, BEFORE anything is written. The account-creation commit
	// already gave this repo a head, so a head served here means the relay
	// crawled the PDS, resolved the DID at the local PLC, and verified a
	// commit signature — all the machinery our record will need, checked
	// while nothing about our record is in question yet. Without this hop a
	// broken requestCrawl surfaces only as a generic Jetstream timeout two
	// steps later, pointing at the wrong service.
	preRev := h.awaitRelayHead(t, pds.did, "",
		"the reference PDS was never crawled — check that pds-bootstrap's requestCrawl reached the relay "+
			"(`docker compose -f docker-compose.e2e.yml logs pds-bootstrap`) and that it announced the same "+
			"host string the PDS's DID document names; that match IS the peering mechanism")
	t.Logf("relay already tracks the native repo at rev %s (the account-creation commit) — peering is live", preRev)

	// One rkey per run, time-derived: the PDS's repo outlives a single
	// `make e2e-test` (the stack is brought up separately and re-tested),
	// and a fixed rkey would 400 as already-existing on the second run —
	// which would look like a transit failure and is not one.
	rkey := syntax.NewTIDNow(0).String()

	// Cursor BEFORE the write, so a listener that finishes dialing after the
	// commit still replays it.
	cursor := cursorNow()
	l := h.newListener(t, cursor, colPostV2)

	record := map[string]any{
		"$type":     colPostV2,
		"community": nativeCommunityDID,
		"title":     "native pds smoke " + rkey,
		"createdAt": time.Now().UTC().Format(time.RFC3339),
	}
	uri, cid, err := pds.createRecord(colPostV2, rkey, record)
	if err != nil {
		t.Fatalf("createRecord %s/%s in the native repo %s: %v", colPostV2, rkey, pds.did, err)
	}
	if want := fmt.Sprintf("at://%s/%s/%s", pds.did, colPostV2, rkey); uri != want {
		t.Fatalf("the PDS committed the record at %q, not the %q we asked for — the rest of this scenario follows the rkey we chose", uri, want)
	}
	t.Logf("wrote %s (cid %s)", uri, cid)

	ev := l.await("native postv2 through the relay", func(e *jsEvent) bool {
		return e.Did == pds.did && e.Commit.Collection == colPostV2 &&
			e.Commit.RKey == rkey && e.Commit.Operation == opCreate
	})

	// Content addressing checks the whole record at once, and checks it the
	// way atproto itself does: the CID is the hash of the DAG-CBOR the PDS
	// committed, so an identical CID at the far end means the bytes Jetstream
	// re-encoded to JSON are the bytes the PDS signed. A field-by-field
	// comparison would only cover the fields someone thought to list.
	if ev.Commit.CID != cid {
		t.Errorf("jetstream delivered %s at cid %q, but the PDS committed cid %q — "+
			"the record did not survive the PDS → relay → jetstream re-encoding intact",
			uri, ev.Commit.CID, cid)
	}

	// The relay must also ADVANCE its own repo state to the commit it just
	// emitted, not merely forward the frame. Its indexing is asynchronous, so
	// a rev still behind the emitted one is lag to wait out, never an
	// immediate failure — the deadline is the only verdict.
	postRev := h.awaitRelayHead(t, pds.did, ev.Commit.Rev,
		"the commit reached Jetstream, so the relay forwarded it without ever building repo state from it")
	t.Logf("relay head advanced %s → %s across the write", preRev, postRev)
}

// awaitRelayHead polls the relay's view of a repo head until it serves a
// non-empty cid/rev at or after minRev ("" for any head), returning that
// rev. Every non-answer — a transport error, a 200 carrying an empty head,
// a rev still behind minRev — is transient by default and recorded as the
// reason to report if the deadline arrives; nothing here fails early,
// because every one of those states is a normal moment in the relay's
// asynchronous indexing. hint says what a real timeout would MEAN at this
// call site, which is the part a reader chasing the failure needs.
func (h *harness) awaitRelayHead(t *testing.T, did, minRev, hint string) string {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	lastErr := "the poll never completed an attempt"
	for {
		headCID, rev, err := h.relayGetLatestCommit(did)
		switch {
		case err != nil:
			lastErr = err.Error()
			t.Logf("relay getLatestCommit(%s): %v (retrying)", did, err)
		case headCID == "" || rev == "":
			lastErr = fmt.Sprintf("getLatestCommit returned 200 with empty cid/rev (%q/%q)", headCID, rev)
		case rev < minRev:
			lastErr = fmt.Sprintf("relay head rev %q is still older than rev %q, which it had already emitted for this repo", rev, minRev)
		default:
			return rev
		}
		if time.Now().After(deadline) {
			t.Fatalf("the relay never served a usable head for repo %s within %s: %s — %s",
				did, eventTimeout, lastErr, hint)
		}
		time.Sleep(time.Second)
	}
}
