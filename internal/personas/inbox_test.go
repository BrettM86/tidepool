package personas

import (
	"bytes"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/identity"
)

// recordingInbox stands in for ingest.Inbox's delivery handler and records
// exactly what reached it.
type recordingInbox struct {
	calls   int
	method  string
	path    string
	host    string
	headers http.Header
	body    []byte
	status  int
}

func (h *recordingInbox) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.calls++
	h.method = r.Method
	h.path = r.URL.Path
	h.host = r.Host
	h.headers = r.Header.Clone()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r.Body)
	h.body = buf.Bytes()
	status := h.status
	if status == 0 {
		status = http.StatusAccepted
	}
	w.Header().Set("X-Inbox", "reached")
	w.WriteHeader(status)
	_, _ = w.Write([]byte("delivered"))
}

func newInboxService(t *testing.T, database *sql.DB, inbox http.Handler) *Service {
	t.Helper()
	custodian, err := identity.NewCustodian(testKEK)
	require.NoError(t, err)
	svc, err := New(Options{
		DB:           database,
		Custodian:    custodian,
		UserOrigin:   userOrigin,
		InboxHandler: inbox,
	})
	require.NoError(t, err)
	require.NotNil(t, svc)
	return svc
}

func postOnUserOrigin(h http.Handler, target string, body []byte, header http.Header) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "https://"+userHost+target, bytes.NewReader(body))
	req.Host = userHost
	req.Header.Set("Content-Type", ap.ContentTypeActivityJSON)
	for k, values := range header {
		for _, v := range values {
			req.Header.Add(k, v)
		}
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// TestServeInbox_DispatchesVerbatim: the user origin publishes a shared
// inbox, but it must not grow a SECOND verify pipeline. The delivery is
// handed to the existing ingest inbox untouched — signature verification,
// authority binding, dedupe, admission control, and the refusal taxonomy
// all stay in one implementation.
func TestServeInbox_DispatchesVerbatim(t *testing.T) {
	database := personasTestDB(t)
	inbox := &recordingInbox{}
	svc := newInboxService(t, database, inbox)

	body := []byte(`{"@context":"https://www.w3.org/ns/activitystreams",` +
		`"id":"https://lemmy.world/activities/like/1","type":"Like",` +
		`"actor":"https://lemmy.world/u/alice","object":"https://lemmy.world/post/1"}`)
	rec := postOnUserOrigin(svc, "/ap/inbox", body,
		http.Header{"Signature": []string{`keyId="https://lemmy.world/u/alice#main-key"`}})

	require.Equal(t, 1, inbox.calls, "POST /ap/inbox must reach the ingest inbox")
	assert.Equal(t, http.MethodPost, inbox.method)
	assert.Equal(t, "/ap/inbox", inbox.path)
	assert.Equal(t, userHost, inbox.host, "the routed Host must survive the handoff")
	assert.Equal(t, body, inbox.body, "the body must arrive byte-identical: the digest covers it")
	assert.Equal(t, `keyId="https://lemmy.world/u/alice#main-key"`, inbox.headers.Get("Signature"),
		"the signature headers must not be rewritten on the way through")
	assert.Equal(t, ap.ContentTypeActivityJSON, inbox.headers.Get("Content-Type"))

	// The inbox's own response is what the remote sees — including the
	// refusal taxonomy, which Lemmy reads to decide retry vs drop.
	assert.Equal(t, http.StatusAccepted, rec.Code)
	assert.Equal(t, "reached", rec.Header().Get("X-Inbox"))
	assert.Equal(t, "delivered", rec.Body.String())
}

// TestServeInbox_PropagatesRefusals: a 503 (retryable) must not be flattened
// into anything else on the way out, or Lemmy drops the delivery forever.
func TestServeInbox_PropagatesRefusals(t *testing.T) {
	database := personasTestDB(t)
	inbox := &recordingInbox{status: http.StatusServiceUnavailable}
	svc := newInboxService(t, database, inbox)

	rec := postOnUserOrigin(svc, "/ap/inbox", []byte(`{"type":"Like"}`), nil)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code,
		"the ingest inbox owns the status; the user origin only routes")
}

// TestServeInbox_Unconfigured: without an inbox handler the origin would be
// advertising an inbox it cannot serve. 404 is the honest answer.
func TestServeInbox_Unconfigured(t *testing.T) {
	database := personasTestDB(t)
	svc := newInboxService(t, database, nil)

	rec := postOnUserOrigin(svc, "/ap/inbox", []byte(`{"type":"Like"}`), nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestServeInbox_RejectsGET: an inbox is write-only. The method check comes
// first, so the answer does not leak whether an inbox is wired up.
func TestServeInbox_RejectsGET(t *testing.T) {
	database := personasTestDB(t)

	for _, tc := range []struct {
		name  string
		inbox http.Handler
	}{
		{"configured", &recordingInbox{}},
		{"unconfigured", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := newInboxService(t, database, tc.inbox)
			rec := serveOnUserOrigin(svc, http.MethodGet, "/ap/inbox", nil)
			assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
			assert.Equal(t, http.MethodPost, rec.Header().Get("Allow"))
		})
	}
}
