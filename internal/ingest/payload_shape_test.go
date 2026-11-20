package ingest

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
)

// overDeepDocument nests {"object":...} 33 levels deep, one past
// ap.MaxObjectNestingDepth.
func overDeepDocument() []byte {
	return []byte(strings.Repeat(`{"object":`, 33) + `"https://lemmy.world/post/x"` + strings.Repeat(`}`, 33))
}

// TestInboxPayloadShapeRejectionPoisonsWithoutRefetch: an Announce of a bare
// IRI whose origin serves an over-deep document is poisoned on its first
// attempt; driving the queue through as many claim cycles as an ordinary
// failure would be retried downloads the hostile document exactly once.
func TestInboxPayloadShapeRejectionPoisonsWithoutRefetch(t *testing.T) {
	const (
		objectPath = "/post/990033"
		objectIRI  = "https://lemmy.world" + objectPath
	)
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.serveJSON(objectPath, overDeepDocument())
	announce := announceOf(objectIRI)
	announce["id"] = "https://lemmy.world/activities/announce/shape-1"
	activityID := announce["id"].(string)

	require.Equal(t, http.StatusAccepted, h.deliver(group, announce))
	for cycle := 1; cycle <= 3; cycle++ {
		h.drain()
		h.makeEventDue(activityID)
	}
	h.drain()

	assert.Equal(t, 1, h.hitCount(objectPath), "the over-deep document is fetched once, not once per attempt")
	event := h.storedEvent(activityID)
	assert.NotNil(t, event.FailedAt, "the shape rejection poisons the event")
	assert.Equal(t, 1, event.Attempts, "poisoned on the first attempt")
}

// TestInboxPayloadShapeRejectionLogsSigner: a signature-verified delivery
// whose body is over-deep answers 400 and logs one Warn naming the verified
// signer, its host and the reason, and nothing from the body.
func TestInboxPayloadShapeRejectionLogsSigner(t *testing.T) {
	const marker = "SHAPE-MARKER-7f3a9c"
	h := newHarness(t)
	group := h.technologyGroup()

	capture := &captureHandler{}
	inbox, err := NewInbox(InboxOptions{
		Verifier: ap.NewVerifier(h.client),
		Events:   h.events,
		Queue:    h.queue,
		Service:  h.service,
		Fetcher:  h.client,
		Logger:   slog.New(capture),
	})
	require.NoError(t, err)

	// The outer activity is depth 1; 32 nested objects take it to 33.
	var inner any = map[string]any{"content": marker}
	for range 31 {
		inner = map[string]any{"object": inner}
	}
	activity := map[string]any{
		"id":     "https://lemmy.world/activities/announce/" + marker,
		"type":   "Announce",
		"actor":  groupID,
		"object": inner,
	}

	status := h.deliverThrough(inbox.InboxHandler(), "/inbox", group, activity)
	assert.Equal(t, http.StatusBadRequest, status)

	capture.mu.Lock()
	defer capture.mu.Unlock()
	var warns []slog.Record
	for _, r := range capture.records {
		if r.Level == slog.LevelWarn {
			warns = append(warns, r)
		}
	}
	require.Len(t, warns, 1, "exactly one Warn record for the shape rejection")
	attrs := map[string]string{}
	warns[0].Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.String()
		return true
	})
	assert.Equal(t, groupID, attrs["signer"])
	assert.Equal(t, "lemmy.world", attrs["host"])
	assert.Contains(t, attrs["reason"], "nesting depth")
	assert.NotContains(t, warns[0].Message, marker)
	for key, value := range attrs {
		assert.NotContains(t, value, marker, "attribute %s must not carry body content", key)
	}
}

// TestInboxSignerKeyDocumentShapeRejectionLogsLabel: a delivery whose signer's
// actor (key) document is over-deep is a definitive 401, logged under its own
// label with the key and host, not as a bad signature.
func TestInboxSignerKeyDocumentShapeRejectionLogsLabel(t *testing.T) {
	const signerID = "https://lemmy.world/u/deepsigner"
	h := newHarness(t)
	h.serveJSON("/u/deepsigner", overDeepDocument())
	key, err := ap.GenerateRSAKey()
	require.NoError(t, err)
	signer := &remoteActor{id: signerID, key: key}

	capture := &captureHandler{}
	inbox, err := NewInbox(InboxOptions{
		Verifier: ap.NewVerifier(h.client),
		Events:   h.events,
		Queue:    h.queue,
		Service:  h.service,
		Fetcher:  h.client,
		Logger:   slog.New(capture),
	})
	require.NoError(t, err)

	status := h.deliverThrough(inbox.InboxHandler(), "/inbox", signer,
		likeActivity("https://lemmy.world/activities/like/deep-signer", signerID))
	assert.Equal(t, http.StatusUnauthorized, status)

	capture.mu.Lock()
	defer capture.mu.Unlock()
	var warns []slog.Record
	for _, r := range capture.records {
		if r.Level == slog.LevelWarn {
			warns = append(warns, r)
		}
	}
	require.Len(t, warns, 1, "exactly one Warn record for the key-document shape rejection")
	assert.Equal(t, "inbox delivery rejected: signer key document payload shape", warns[0].Message)
	attrs := map[string]string{}
	warns[0].Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.String()
		return true
	})
	assert.Equal(t, "lemmy.world", attrs["host"])
	assert.Contains(t, attrs["reason"], "nesting depth")
}

// TestInboxSignerKeyDocumentShapeRejectionBoundsLog: the unverified keyId is
// attacker-sized (headers allow up to 1 MiB), so the key-document shape Warn
// truncates it and carries a fixed reason instead of the error text, which
// embeds the keyId. The host is attacker data too: url.Parse keeps an IPv6
// zone identifier in Hostname(), so an oversized zone must not reach the log
// unbounded. The record stays small and still names the label.
func TestInboxSignerKeyDocumentShapeRejectionBoundsLog(t *testing.T) {
	cases := []struct {
		name       string
		signerID   string
		keyID      string
		wantHost   string
		hostPrefix string
	}{
		{
			name:     "oversized keyId fragment",
			signerID: "https://lemmy.world/u/deepsigner",
			keyID:    "https://lemmy.world/u/deepsigner#" + strings.Repeat("A", 100<<10),
			wantHost: "lemmy.world",
		},
		{
			name:       "oversized IPv6 zone identifier",
			signerID:   "https://[::ffff:93.184.216.34%25" + strings.Repeat("z", 100<<10) + "]/u/deepsigner",
			keyID:      "https://[::ffff:93.184.216.34%25" + strings.Repeat("z", 100<<10) + "]/u/deepsigner#main-key",
			hostPrefix: "::ffff:93.184.216.34%zzz",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			h.serveJSON("/u/deepsigner", overDeepDocument())
			key, err := ap.GenerateRSAKey()
			require.NoError(t, err)

			capture := &captureHandler{}
			inbox, err := NewInbox(InboxOptions{
				Verifier: ap.NewVerifier(h.client),
				Events:   h.events,
				Queue:    h.queue,
				Service:  h.service,
				Fetcher:  h.client,
				Logger:   slog.New(capture),
			})
			require.NoError(t, err)

			body, err := json.Marshal(likeActivity("https://lemmy.world/activities/like/deep-signer-long", tc.signerID))
			require.NoError(t, err)
			req := httptest.NewRequest(http.MethodPost, "https://"+bridgeHost+"/inbox", bytes.NewReader(body))
			req.Header.Set("Content-Type", ap.ContentTypeActivityJSON)
			require.NoError(t, ap.NewSigner(tc.keyID, key).SignRequest(req, body))
			rec := httptest.NewRecorder()
			inbox.InboxHandler().ServeHTTP(rec, req)
			assert.Equal(t, http.StatusUnauthorized, rec.Code)

			capture.mu.Lock()
			defer capture.mu.Unlock()
			var warns []slog.Record
			for _, r := range capture.records {
				if r.Level == slog.LevelWarn {
					warns = append(warns, r)
				}
			}
			require.Len(t, warns, 1, "exactly one Warn record for the key-document shape rejection")
			assert.Equal(t, "inbox delivery rejected: signer key document payload shape", warns[0].Message)
			size := len(warns[0].Message)
			attrs := map[string]string{}
			warns[0].Attrs(func(a slog.Attr) bool {
				attrs[a.Key] = a.Value.String()
				size += len(a.Key) + len(a.Value.String())
				return true
			})
			assert.Less(t, size, 1024, "the warning record must stay bounded")
			if tc.wantHost != "" {
				assert.Equal(t, tc.wantHost, attrs["host"])
			} else {
				assert.True(t, strings.HasPrefix(attrs["host"], tc.hostPrefix), "host keeps its leading bytes")
			}
			assert.Contains(t, attrs["reason"], "nesting depth")
		})
	}
}

// TestTruncateUTF8 keeps whole runes: a cut inside a multi-byte sequence backs
// up to the rune boundary.
func TestTruncateUTF8(t *testing.T) {
	assert.Equal(t, "short", truncateUTF8("short", 256))
	assert.Equal(t, "ab", truncateUTF8("ab€", 4), "€ is 3 bytes; a 4-byte cut drops it whole")
	assert.Equal(t, "ab€", truncateUTF8("ab€d", 5))
}
