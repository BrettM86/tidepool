package accept

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// Round 3: the admin/debug surface — the last task-16 deliverable. Coves'
// post.getStatus reads a post's admission state from the firehose-visible
// acceptance/removal records; THIS surface is the bridge operator's own window
// on the WHY of every rejection, which those records cannot carry, plus a force
// re-admit (the mod-override seam task 17 reuses for restore).

const adminToken = "admin-secret-token"

// adminRequest issues one admin request and returns the recorder. An empty token
// omits the Authorization header (the 401 path).
func adminRequest(t *testing.T, router chi.Router, method, path, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, "https://bridge.example"+path, r)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// newAdminRouter mounts an Admin over the given engine + this DB's admissions.
func newAdminRouter(t *testing.T, conn *sql.DB, engine *Engine) chi.Router {
	t.Helper()
	admin, err := NewAdmin(AdminOptions{
		Token:      adminToken,
		Admissions: NewAdmissions(conn),
		Engine:     engine,
	})
	require.NoError(t, err)
	router := chi.NewRouter()
	admin.Routes(router)
	return router
}

// listItem is the wire contract this surface pins for one listed admission.
type listItem struct {
	Post         string `json:"post"`
	Community    string `json:"community"`
	Status       string `json:"status"`
	DecisionCode string `json:"decisionCode"`
	EvaluatedCID string `json:"evaluatedCid"`
}

type listResponse struct {
	Admissions []listItem `json:"admissions"`
}

// readmitResponse is the wire contract for a force re-admit.
type readmitResponse struct {
	Post         string `json:"post"`
	Status       string `json:"status"`
	DecisionCode string `json:"decisionCode"`
	Enqueued     bool   `json:"enqueued"`
}

// ---------------------------------------------------------------------------
// A1 — GET /admin/admissions: list decisions + reasons, filterable by status
// ---------------------------------------------------------------------------

func TestAdminList_FiltersByStatusAndReturnsReasons(t *testing.T) {
	conn := acceptanceDB(t)
	ctx := context.Background()
	adm := NewAdmissions(conn)

	// Four decisions in the ledger: one accepted, two rejected (distinct
	// reasons), one removed.
	seed := []Admission{
		{CommunityDID: acCommunityDID, PostURI: acPostURI + "-acc", AuthorDID: acAuthorDID, Status: StatusAccepted, EvaluatedCID: acPostCID, AcceptanceRKey: "rk", AcceptedCID: acPostCID},
		{CommunityDID: acCommunityDID, PostURI: acPostURI + "-rej1", AuthorDID: acAuthorDID, Status: StatusRejected, DecisionCode: DecisionTitleRequired, EvaluatedCID: acPostCID},
		{CommunityDID: acCommunityDID, PostURI: acPostURI + "-rej2", AuthorDID: acAuthorDID, Status: StatusRejected, DecisionCode: DecisionRateLimit, EvaluatedCID: acPostCID},
		{CommunityDID: acCommunityDID, PostURI: acPostURI + "-rem", AuthorDID: acAuthorDID, Status: StatusRemoved, DecisionCode: DecisionTitleRequired, EvaluatedCID: acPostCID},
	}
	for _, a := range seed {
		require.NoError(t, adm.Record(ctx, a))
	}

	engine := engineWith(t, conn, newRepos(t, conn), realEnqueuer(t, conn))
	router := newAdminRouter(t, conn, engine)

	rec := adminRequest(t, router, http.MethodGet, "/admin/admissions?status=rejected", adminToken, "")
	require.Equal(t, http.StatusOK, rec.Code, "listing rejected admissions must succeed")

	var resp listResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp),
		"the list body is {\"admissions\":[{post,community,status,decisionCode,evaluatedCid}]}")

	codes := map[string]string{}
	for _, item := range resp.Admissions {
		assert.Equal(t, StatusRejected, item.Status, "status=rejected must return only rejected rows")
		codes[item.Post] = item.DecisionCode
	}
	require.Len(t, resp.Admissions, 2,
		"status=rejected returns exactly the two rejected admissions, not the accepted or removed ones")
	assert.Equal(t, DecisionTitleRequired, codes[acPostURI+"-rej1"],
		"each rejection carries its distinct machine-readable reason — the whole point of this surface")
	assert.Equal(t, DecisionRateLimit, codes[acPostURI+"-rej2"])
}

// ---------------------------------------------------------------------------
// A2 — POST /admin/admissions/readmit: force re-admit one post
// ---------------------------------------------------------------------------

// A rejected post whose cause has cleared (opted-out → re-enabled) is re-admitted
// from stored state: acceptance written, Create{Page} enqueued, ledger accepted.
func TestAdminReadmit_ReAdmitsAPostWhoseRejectionCauseCleared(t *testing.T) {
	conn := acceptanceDB(t)
	ctx := context.Background()
	seedBridgedCommunity(t, conn)
	repos := newRepos(t, conn)
	enq := realEnqueuer(t, conn)
	engine := engineWith(t, conn, repos, enq)
	dispatcher := wireDispatcher(t, conn, engine, enq)
	router := newAdminRouter(t, conn, engine)

	// Opted out → the post is rejected (and its record snapshot is stored on the
	// ledger row, so readmit can re-run from stored state).
	_, err := store.NewFederationPrefs(conn).Upsert(ctx, store.FederationPref{
		DID: acAuthorDID, Source: store.FederationPrefSourceRecord,
	})
	require.NoError(t, err)
	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("create", acPostRKey, acRevCreate, acPostCID, acPostTimeUS, pv2Record())))
	status, code := admissionOf(t, conn, acCommunityDID, acPostURI)
	require.Equal(t, StatusRejected, status)
	require.Equal(t, DecisionOptedOut, code)

	// The author re-enables federation.
	require.NoError(t, store.NewFederationPrefs(conn).Delete(ctx, acAuthorDID))

	// Force re-admit.
	rec := adminRequest(t, router, http.MethodPost, "/admin/admissions/readmit", adminToken,
		`{"post":"`+acPostURI+`"}`)
	require.Equal(t, http.StatusOK, rec.Code, "readmit of a now-eligible post succeeds")

	var resp readmitResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, StatusAccepted, resp.Status, "the re-admission now passes")
	assert.True(t, resp.Enqueued, "a newly accepted post enqueues its Create{Page}")

	// The acceptance now stands, the Create{Page} is enqueued, and the ledger is
	// accepted.
	_, ok := acceptanceSubjectCID(t, repos, acCommunityDID, acPostURI)
	assert.True(t, ok, "readmit wrote the acceptance record")
	assert.Equal(t, 1, activityKindCount(t, conn, "Create"),
		"readmit enqueued exactly one Create{Page}")
	status, _ = admissionOf(t, conn, acCommunityDID, acPostURI)
	assert.Equal(t, StatusAccepted, status, "the ledger row flips to accepted")
}

// A post that STILL fails admission (opted-out, never re-enabled) returns the
// failure: still rejected, nothing enqueued, no acceptance.
func TestAdminReadmit_StillFailingPostStaysRejected(t *testing.T) {
	conn := acceptanceDB(t)
	ctx := context.Background()
	seedBridgedCommunity(t, conn)
	repos := newRepos(t, conn)
	enq := realEnqueuer(t, conn)
	engine := engineWith(t, conn, repos, enq)
	dispatcher := wireDispatcher(t, conn, engine, enq)
	router := newAdminRouter(t, conn, engine)

	_, err := store.NewFederationPrefs(conn).Upsert(ctx, store.FederationPref{
		DID: acAuthorDID, Source: store.FederationPrefSourceRecord,
	})
	require.NoError(t, err)
	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("create", acPostRKey, acRevCreate, acPostCID, acPostTimeUS, pv2Record())))

	// Readmit WITHOUT clearing the opt-out.
	rec := adminRequest(t, router, http.MethodPost, "/admin/admissions/readmit", adminToken,
		`{"post":"`+acPostURI+`"}`)
	require.Equal(t, http.StatusOK, rec.Code,
		"a readmit that still fails is a reported outcome, not an HTTP error")

	var resp readmitResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, StatusRejected, resp.Status, "the post still fails admission")
	assert.Equal(t, DecisionOptedOut, resp.DecisionCode, "and the response carries why")
	assert.False(t, resp.Enqueued, "nothing is enqueued for a post that still fails")

	_, ok := acceptanceSubjectCID(t, repos, acCommunityDID, acPostURI)
	assert.False(t, ok, "no acceptance is written")
	assert.Zero(t, countRows(t, conn, "outbound_activities"), "and nothing is enqueued")
}

// ---------------------------------------------------------------------------
// A3 — authz: both endpoints require the admin bearer
// ---------------------------------------------------------------------------

func TestAdminAdmissions_RequireBearer(t *testing.T) {
	conn := acceptanceDB(t)
	engine := engineWith(t, conn, newRepos(t, conn), realEnqueuer(t, conn))
	router := newAdminRouter(t, conn, engine)

	for _, tc := range []struct {
		name, method, path, body string
	}{
		{"list", http.MethodGet, "/admin/admissions", ""},
		{"readmit", http.MethodPost, "/admin/admissions/readmit", `{"post":"at://x/y/z"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// No token.
			rec := adminRequest(t, router, tc.method, tc.path, "", tc.body)
			assert.Equal(t, http.StatusUnauthorized, rec.Code, "no bearer → 401")

			// Wrong token.
			rec = adminRequest(t, router, tc.method, tc.path, "wrong-token", tc.body)
			assert.Equal(t, http.StatusUnauthorized, rec.Code, "wrong bearer → 401")

			// Valid token: auth passes (the handler may still be unimplemented,
			// but it is NOT a 401).
			rec = adminRequest(t, router, tc.method, tc.path, adminToken, tc.body)
			assert.NotEqual(t, http.StatusUnauthorized, rec.Code,
				"a valid bearer must pass the guard")
		})
	}
}
