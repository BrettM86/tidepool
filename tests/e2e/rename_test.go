//go:build e2e

package e2e

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/require"
)

func TestRename_OldFormatUserAndCommunityAcrossLiveIdentityStreams(t *testing.T) {
	h := newHarness(t)
	// Open all three connections before either actor is minted. Both #identity
	// observations before the POST must be the mint-time event, not a replay
	// accidentally satisfying the rename assertion.
	bridge := h.dialBridgeFirehoseCursor(t, "")
	relay := h.dialRelayFirehoseCursor(t, "")
	jetstream := h.newListener(t, cursorNow())
	community, sub := setupSubscribedCommunity(t, h, "ren")
	username := h.uniqueName(t, "renusr")
	user := h.registerUser(t, username)
	user.createPost(t, community.ID, "Rename "+username, "mint user repo")
	newCommunity := bridgedCommunityHandle(community.Name)
	newUser := bridgedHandle(username)
	// Resolve the user DID through the bridge only after mint has committed.
	var userDID string
	require.Eventually(t, func() bool {
		result, err := h.bridgeXRPC("/xrpc/com.atproto.identity.resolveHandle", url.Values{"handle": {newUser}})
		if err != nil || result.status != http.StatusOK {
			return false
		}
		var identity struct {
			Did string `json:"did"`
		}
		if json.Unmarshal(result.body, &identity) != nil {
			return false
		}
		userDID = identity.Did
		return userDID != ""
	}, eventTimeout, 250*time.Millisecond)
	handles := map[string]string{sub.DID: newCommunity, userDID: newUser}
	bridgeMint := awaitIdentityFrames(t, bridge, handles, nil)
	relayMint := awaitIdentityFrames(t, relay, handles, nil)
	jetstreamMint := make(map[string]int64)
	for did, handle := range handles {
		jetstreamMint[did] = jetstream.awaitIdentity("mint identity "+did, did, handle, 0).TimeUs
	}
	oldUser := h.prepareOldFormatActor(t, userDID)
	oldCommunity := h.prepareOldFormatActor(t, sub.DID)
	require.Equal(t, strings.ReplaceAll(username, "_", "-")+".lemmy.tidepool", oldUser)
	require.Equal(t, strings.ReplaceAll(community.Name, "_", "-")+".lemmy.tidepool", oldCommunity)
	fixtureCID := map[string]string{
		userDID: plcOperationCID(t, h.plcLastOperation(t, userDID)),
		sub.DID: plcOperationCID(t, h.plcLastOperation(t, sub.DID)),
	}
	body, err := json.Marshal(map[string]any{"dids": []string{userDID, sub.DID}})
	require.NoError(t, err)
	request, err := http.NewRequest(http.MethodPost, tidepoolURL()+"/admin/handles/migrate", bytes.NewReader(body))
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer "+adminToken())
	request.Header.Set("Content-Type", "application/json")
	response, err := h.http.Do(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusAccepted, response.StatusCode)
	require.NoError(t, response.Body.Close())
	bridgeRenamed := awaitIdentityFrames(t, bridge, handles, bridgeMint)
	relayRenamed := awaitIdentityFrames(t, relay, handles, relayMint)
	for did, handle := range handles {
		require.Greater(t, bridgeRenamed[did], bridgeMint[did])
		require.Greater(t, relayRenamed[did], relayMint[did])
		renamed := jetstream.awaitIdentity("rename identity "+did, did, handle, jetstreamMint[did])
		require.Greater(t, renamed.TimeUs, jetstreamMint[did])
		last := h.plcLastOperation(t, did)
		require.Equal(t, []any{"at://" + handle}, last["alsoKnownAs"])
		require.Equal(t, fixtureCID[did], last["prev"])
		response := queryHandleTXT(t, handle)
		require.Equal(t, dns.RcodeSuccess, response.Rcode)
		require.Len(t, response.Answer, 1)
		txt, ok := response.Answer[0].(*dns.TXT)
		require.True(t, ok)
		require.Equal(t, []string{"did=" + did}, txt.Txt)
		status, body := h.wellKnownDID(t, handle)
		require.Equal(t, http.StatusOK, status)
		require.Equal(t, did, body)
	}
	for _, old := range []string{oldUser, oldCommunity} {
		response := queryHandleTXT(t, old)
		require.Equal(t, dns.RcodeSuccess, response.Rcode)
		require.Empty(t, response.Answer, "old handle must stop resolving")
	}
}

func TestRename_RollbackRestoresOldHandle(t *testing.T) {
	h := newHarness(t)
	community, _ := setupSubscribedCommunity(t, h, "rback")
	username := h.uniqueName(t, "rbusr")
	user := h.registerUser(t, username)
	user.createPost(t, community.ID, "Rollback "+username, "mint user repo")
	newHandle := bridgedHandle(username)
	var did string
	require.Eventually(t, func() bool {
		result, err := h.bridgeXRPC("/xrpc/com.atproto.identity.resolveHandle", url.Values{"handle": {newHandle}})
		if err != nil || result.status != http.StatusOK {
			return false
		}
		var resolved struct {
			DID string `json:"did"`
		}
		if json.Unmarshal(result.body, &resolved) != nil {
			return false
		}
		did = resolved.DID
		return did != ""
	}, eventTimeout, 250*time.Millisecond)
	oldHandle := h.prepareOldFormatActor(t, did)
	bridge := h.dialBridgeFirehose(t)
	postMigration := func(rollback bool) {
		t.Helper()
		body, err := json.Marshal(map[string]any{"rollback": rollback, "dids": []string{did}})
		require.NoError(t, err)
		request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, tidepoolURL()+"/admin/handles/migrate", bytes.NewReader(body))
		require.NoError(t, err)
		request.Header.Set("Authorization", "Bearer "+adminToken())
		request.Header.Set("Content-Type", "application/json")
		response, err := h.http.Do(request)
		require.NoError(t, err)
		defer response.Body.Close()
		require.Equal(t, http.StatusAccepted, response.StatusCode)
	}
	postMigration(false)
	forward := awaitIdentityFrames(t, bridge, map[string]string{did: newHandle}, nil)
	require.Eventually(t, func() bool {
		var progress struct {
			Running bool `json:"running"`
		}
		return h.adminDo(http.MethodGet, "/admin/handles/migrate", nil, &progress) == nil && !progress.Running
	}, eventTimeout, 250*time.Millisecond, "forward run must finalize before rollback POST")
	renameOperation := h.plcLastOperation(t, did)
	require.Equal(t, []any{"at://" + newHandle}, renameOperation["alsoKnownAs"])
	renameCID := plcOperationCID(t, renameOperation)
	postMigration(true)
	rollback := awaitIdentityFrames(t, bridge, map[string]string{did: oldHandle}, forward)
	require.Greater(t, rollback[did], forward[did])
	last := h.plcLastOperation(t, did)
	require.Equal(t, []any{"at://" + oldHandle}, last["alsoKnownAs"])
	require.Equal(t, renameCID, last["prev"])
	answer := queryHandleTXT(t, oldHandle)
	require.Equal(t, dns.RcodeSuccess, answer.Rcode)
	require.Len(t, answer.Answer, 1)
	txt, ok := answer.Answer[0].(*dns.TXT)
	require.True(t, ok)
	require.Equal(t, []string{"did=" + did}, txt.Txt)
	status, resolved := h.wellKnownDID(t, oldHandle)
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, did, resolved)
	answer = queryHandleTXT(t, newHandle)
	require.Equal(t, dns.RcodeSuccess, answer.Rcode)
	require.Empty(t, answer.Answer)
	status, _ = h.wellKnownDID(t, newHandle)
	require.Equal(t, http.StatusNotFound, status)
}

func TestRename_RecoversAfterPLCAcceptedBeforeFinalization(t *testing.T) {
	h := newHarness(t)
	community, _ := setupSubscribedCommunity(t, h, "crash")
	username := h.uniqueName(t, "crusr")
	user := h.registerUser(t, username)
	user.createPost(t, community.ID, "PLC crash recovery "+username, "mint user repo")
	pending := bridgedHandle(username)
	var did string
	require.Eventually(t, func() bool {
		result, err := h.bridgeXRPC("/xrpc/com.atproto.identity.resolveHandle", url.Values{"handle": {pending}})
		if err != nil || result.status != http.StatusOK {
			return false
		}
		var resolved struct {
			DID string `json:"did"`
		}
		if json.Unmarshal(result.body, &resolved) != nil {
			return false
		}
		did = resolved.DID
		return did != ""
	}, eventTimeout, 250*time.Millisecond)
	old := h.prepareOldFormatActor(t, did)
	db, err := sql.Open("postgres", "postgres://tidepool:tidepool@127.0.0.1:"+envOr("TIDEPOOL_E2E_PG_PORT", "5453")+"/tidepool?sslmode=disable")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	require.NoError(t, db.PingContext(t.Context()))
	result, err := db.ExecContext(t.Context(), `UPDATE bridged_actors SET pending_handle = $1 WHERE did = $2 AND handle = $3`, pending, did, old)
	require.NoError(t, err)
	updated, err := result.RowsAffected()
	require.NoError(t, err)
	require.EqualValues(t, 1, updated)
	_, err = db.ExecContext(t.Context(), `INSERT INTO handle_claims (handle, actor_id)
		SELECT $1, id FROM bridged_actors WHERE did = $2`, pending, did)
	require.NoError(t, err)
	h.submitPLCHandleUpdate(t, db, did, pending)
	require.Equal(t, []any{"at://" + pending}, h.plcLastOperation(t, did)["alsoKnownAs"])
	countPLCOps := func() int {
		response, err := h.http.Get(plcE2EURL() + "/" + did + "/log/audit")
		require.NoError(t, err)
		defer response.Body.Close()
		require.Equal(t, http.StatusOK, response.StatusCode)
		var operations []json.RawMessage
		require.NoError(t, json.NewDecoder(response.Body).Decode(&operations))
		return len(operations)
	}
	operationsBefore := countPLCOps()
	var cursor int64
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT COALESCE(max(seq), 0) FROM firehose_events`).Scan(&cursor))
	for _, handle := range []string{old, pending} {
		answer := queryHandleTXT(t, handle)
		require.Equal(t, dns.RcodeSuccess, answer.Rcode)
		require.Len(t, answer.Answer, 1)
		txt, ok := answer.Answer[0].(*dns.TXT)
		require.True(t, ok)
		require.Equal(t, []string{"did=" + did}, txt.Txt)
		status, resolved := h.wellKnownDID(t, handle)
		require.Equal(t, http.StatusOK, status)
		require.Equal(t, did, resolved)
	}
	body, err := json.Marshal(map[string]any{"dids": []string{did}})
	require.NoError(t, err)
	request, err := http.NewRequest(http.MethodPost, tidepoolURL()+"/admin/handles/migrate", bytes.NewReader(body))
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer "+adminToken())
	request.Header.Set("Content-Type", "application/json")
	response, err := h.http.Do(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusAccepted, response.StatusCode)
	require.NoError(t, response.Body.Close())
	var finalHandle string
	require.Eventually(t, func() bool {
		return db.QueryRowContext(t.Context(), `SELECT handle FROM bridged_actors WHERE did = $1`, did).Scan(&finalHandle) == nil && finalHandle == pending
	}, eventTimeout, 250*time.Millisecond, "Tidepool must finalize the PLC-accepted pending handle")
	require.Equal(t, operationsBefore, countPLCOps(), "an already-accepted PLC update must not be resubmitted")
	bridge := h.dialBridgeFirehoseCursor(t, fmt.Sprint(cursor))
	identities := awaitIdentityFrames(t, bridge, map[string]string{did: pending}, nil)
	require.Greater(t, identities[did], cursor)
	var identityCount int
	var identityHandle string
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT count(*), COALESCE(max(identity_handle), '')
		FROM firehose_events WHERE kind = 'identity' AND did = $1 AND seq > $2`, did, cursor).
		Scan(&identityCount, &identityHandle))
	require.Equal(t, 1, identityCount)
	require.Equal(t, pending, identityHandle)
	answer := queryHandleTXT(t, old)
	require.Equal(t, dns.RcodeSuccess, answer.Rcode)
	require.Empty(t, answer.Answer)
	status, _ := h.wellKnownDID(t, old)
	require.Equal(t, http.StatusNotFound, status)
	answer = queryHandleTXT(t, pending)
	require.Equal(t, dns.RcodeSuccess, answer.Rcode)
	require.Len(t, answer.Answer, 1)
	txt, ok := answer.Answer[0].(*dns.TXT)
	require.True(t, ok)
	require.Equal(t, []string{"did=" + did}, txt.Txt)
	status, resolved := h.wellKnownDID(t, pending)
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, did, resolved)
}

func TestRename_FetchesMissingUsernameFromLiveLemmyActor(t *testing.T) {
	h := newHarness(t)
	community, _ := setupSubscribedCommunity(t, h, "fetch")
	// Lemmy's supported username alphabet is [a-z0-9_]; the underscore
	// exercises handle normalization while retaining the raw AP username.
	username := h.uniqueName(t, "fetchusr")
	user := h.registerUser(t, username)
	user.createPost(t, community.ID, "Fetch rename "+username, "mint user repo")
	newHandle := bridgedHandle(username)
	var did string
	require.Eventually(t, func() bool {
		result, err := h.bridgeXRPC("/xrpc/com.atproto.identity.resolveHandle", url.Values{"handle": {newHandle}})
		if err != nil || result.status != http.StatusOK {
			return false
		}
		var resolved struct {
			Did string `json:"did"`
		}
		if json.Unmarshal(result.body, &resolved) != nil {
			return false
		}
		did = resolved.Did
		return did != ""
	}, eventTimeout, 250*time.Millisecond)
	h.prepareOldFormatActor(t, did)
	db, err := sql.Open("postgres", "postgres://tidepool:tidepool@127.0.0.1:"+envOr("TIDEPOOL_E2E_PG_PORT", "5453")+"/tidepool?sslmode=disable")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	require.NoError(t, db.PingContext(t.Context()))
	result, err := db.ExecContext(t.Context(), `UPDATE bridged_actors SET preferred_username = NULL WHERE did = $1`, did)
	require.NoError(t, err)
	updated, err := result.RowsAffected()
	require.NoError(t, err)
	require.EqualValues(t, 1, updated)
	body, err := json.Marshal(map[string]any{"dids": []string{did}})
	require.NoError(t, err)
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, tidepoolURL()+"/admin/handles/migrate", bytes.NewReader(body))
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer "+adminToken())
	request.Header.Set("Content-Type", "application/json")
	response, err := h.http.Do(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusAccepted, response.StatusCode)
	require.NoError(t, response.Body.Close())
	require.Eventually(t, func() bool {
		var handle, pending, preferred string
		if err := db.QueryRowContext(t.Context(), `SELECT COALESCE(handle, ''), COALESCE(pending_handle, ''), COALESCE(preferred_username, '')
			FROM bridged_actors WHERE did = $1`, did).Scan(&handle, &pending, &preferred); err != nil {
			return false
		}
		return handle == newHandle && pending == "" && preferred == username
	}, eventTimeout, 250*time.Millisecond)
	require.Equal(t, []any{"at://" + newHandle}, h.plcLastOperation(t, did)["alsoKnownAs"])
}

func TestRename_ResumesAfterRestartMidRun(t *testing.T) {
	h := newHarness(t)
	community, _ := setupSubscribedCommunity(t, h, "restart")
	db, err := sql.Open("postgres", "postgres://tidepool:tidepool@127.0.0.1:"+envOr("TIDEPOOL_E2E_PG_PORT", "5453")+"/tidepool?sslmode=disable")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	require.NoError(t, db.PingContext(t.Context()))
	type actor struct {
		did, old, handle string
		baseline         map[string]any
		operationCount   int
	}
	actors := make([]actor, 0, 4)
	audit := func(did string) []map[string]any {
		response, err := h.http.Get(plcE2EURL() + "/" + did + "/log/audit")
		require.NoError(t, err)
		defer response.Body.Close()
		require.Equal(t, http.StatusOK, response.StatusCode)
		var operations []map[string]any
		require.NoError(t, json.NewDecoder(response.Body).Decode(&operations))
		return operations
	}
	for index := 0; index < 4; index++ {
		username := h.uniqueName(t, "rstusr")
		user := h.registerUser(t, username)
		user.createPost(t, community.ID, "Restart rename "+username, "mint user repo")
		handle := bridgedHandle(username)
		var did string
		deadline := time.Now().Add(eventTimeout)
		for did == "" {
			result, err := h.bridgeXRPC("/xrpc/com.atproto.identity.resolveHandle", url.Values{"handle": {handle}})
			if err == nil && result.status == http.StatusOK {
				var resolved struct {
					Did string `json:"did"`
				}
				if json.Unmarshal(result.body, &resolved) == nil {
					did = resolved.Did
				}
			}
			if did != "" {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("minted handle %s did not resolve to a DID", handle)
			}
			select {
			case <-time.After(250 * time.Millisecond):
			case <-t.Context().Done():
				t.Fatal("test context ended while resolving minted handle")
			}
		}
		old := h.prepareOldFormatActor(t, did)
		baseline := h.plcLastOperation(t, did)
		operations := audit(did)
		require.NotEmpty(t, operations)
		require.Equal(t, baseline, operations[len(operations)-1]["operation"])
		actors = append(actors, actor{did: did, old: old, handle: handle, baseline: baseline, operationCount: len(operations)})
	}
	lock, err := db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer lock.Rollback()
	var locked int
	require.NoError(t, lock.QueryRowContext(t.Context(), `SELECT 1 FROM bridged_actors WHERE did = $1 FOR UPDATE`, actors[2].did).Scan(&locked))
	require.Equal(t, 1, locked)
	preferredHandles := make(map[string]string, len(actors))
	var lastSample time.Time
	sampleTXT := func() {
		if delay := time.Until(lastSample.Add(time.Second)); delay > 0 {
			select {
			case <-time.After(delay):
			case <-t.Context().Done():
				t.Fatal("test context ended while waiting to sample TXT")
			}
		}
		lastSample = time.Now()
		for _, actor := range actors {
			first, second := actor.old, actor.handle
			if preferredHandles[actor.did] == actor.handle {
				first, second = second, first
			}
			resolved := false
			for _, handle := range []string{first, second} {
				answer := queryHandleTXT(t, handle)
				for _, record := range answer.Answer {
					if txt, ok := record.(*dns.TXT); answer.Rcode == dns.RcodeSuccess && ok && len(txt.Txt) == 1 && txt.Txt[0] == "did="+actor.did {
						preferredHandles[actor.did] = handle
						resolved = true
						break
					}
				}
				if resolved {
					break
				}
			}
			if !resolved {
				t.Fatalf("neither old nor new handle resolves DID %s", actor.did)
			}
		}
	}
	sampleTXT()
	dids := make([]string, 0, 4)
	for _, actor := range actors {
		dids = append(dids, actor.did)
	}
	body, err := json.Marshal(map[string]any{"dids": dids})
	require.NoError(t, err)
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, tidepoolURL()+"/admin/handles/migrate", bytes.NewReader(body))
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer "+adminToken())
	request.Header.Set("Content-Type", "application/json")
	response, err := h.http.Do(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusAccepted, response.StatusCode)
	require.NoError(t, response.Body.Close())
	deadline := time.Now().Add(eventTimeout)
	for {
		sampleTXT()
		var migrated sql.NullTime
		if err := db.QueryRowContext(t.Context(), `SELECT handle_migrated_at FROM bridged_actors WHERE did = $1`, actors[0].did).Scan(&migrated); err != nil {
			t.Fatalf("read migration state for %s: %v", actors[0].did, err)
		}
		if migrated.Valid {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("DID %s did not finalize before restart", actors[0].did)
		}
		select {
		case <-time.After(250 * time.Millisecond):
		case <-t.Context().Done():
			t.Fatal("test context ended while waiting for first rename")
		}
	}
	h.restartTidepool(t)
	sampleTXT()
	require.NoError(t, lock.Rollback())
	deadline = time.Now().Add(eventTimeout)
	for {
		sampleTXT()
		unfinished := ""
		for _, actor := range actors {
			var handle string
			var migrated sql.NullTime
			if err := db.QueryRowContext(t.Context(), `SELECT handle, handle_migrated_at FROM bridged_actors WHERE did = $1`, actor.did).Scan(&handle, &migrated); err != nil {
				t.Fatalf("read migration state for %s: %v", actor.did, err)
			}
			if (!migrated.Valid || handle != actor.handle) && unfinished == "" {
				unfinished = actor.did
			}
		}
		if unfinished == "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("DID %s did not finalize after restart", unfinished)
		}
		select {
		case <-time.After(250 * time.Millisecond):
		case <-t.Context().Done():
			t.Fatal("test context ended while waiting for resumed rename")
		}
	}
	sampleTXT()
	for _, actor := range actors {
		var handle string
		require.NoError(t, db.QueryRowContext(t.Context(), `SELECT handle FROM bridged_actors WHERE did = $1`, actor.did).Scan(&handle))
		require.Equal(t, actor.handle, handle)
		operations := audit(actor.did)
		require.Len(t, operations, actor.operationCount+1)
		require.Equal(t, actor.baseline, operations[actor.operationCount-1]["operation"])
		renamed, ok := operations[actor.operationCount]["operation"].(map[string]any)
		require.True(t, ok)
		require.Equal(t, []any{"at://" + actor.handle}, renamed["alsoKnownAs"])
		answer := queryHandleTXT(t, actor.handle)
		require.Equal(t, dns.RcodeSuccess, answer.Rcode)
		require.Len(t, answer.Answer, 1)
		txt, ok := answer.Answer[0].(*dns.TXT)
		require.True(t, ok)
		require.Equal(t, []string{"did=" + actor.did}, txt.Txt)
	}
}
