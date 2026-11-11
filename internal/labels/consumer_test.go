package labels_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/labeling"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/labels"
	"tidepool/internal/labels/labelstest"
)

const (
	consumerLabelerDID = "did:web:coves.social"
	consumerSubject    = "at://did:plc:ewvi7nxzyoun6zhxrhs64oiz/social.coves.community.post/3m2abcdefgh2a"
	// waitBound bounds every wait in these tests.
	waitBound = 5 * time.Second
)

// fakeStore is an in-memory labels.Store recording the order of its writes.
type fakeStore struct {
	mu      sync.Mutex
	cursor  labels.Cursor
	events  []string
	commits chan labels.Cursor
}

func newFakeStore(at labels.Cursor) *fakeStore {
	return &fakeStore{cursor: at, commits: make(chan labels.Cursor, 64)}
}

func (s *fakeStore) Cursor(context.Context) (labels.Cursor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cursor, nil
}

func (s *fakeStore) Commit(_ context.Context, at labels.Cursor, _ labeling.Label) error {
	s.mu.Lock()
	s.cursor = at
	s.events = append(s.events, fmt.Sprintf("commit(%d,%d)", at.Epoch, at.Seq))
	s.mu.Unlock()
	s.commits <- at
	return nil
}

func (s *fakeStore) Reset(context.Context) (labels.Cursor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cursor = labels.Cursor{Epoch: s.cursor.Epoch + 1, Seq: 0}
	s.events = append(s.events, "reset")
	return s.cursor, nil
}

func (s *fakeStore) Activate(context.Context) error { return nil }

func (s *fakeStore) ApplyLapsed(context.Context) (int, error) { return 0, nil }

func (s *fakeStore) snapshot() (labels.Cursor, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cursor, append([]string(nil), s.events...)
}

// sequenceResolver answers the n-th Resolve with keys[n], repeating the last.
type sequenceResolver struct {
	mu       sync.Mutex
	endpoint string
	keys     []atcrypto.PublicKey
	calls    int
}

func (r *sequenceResolver) Resolve(_ context.Context, did string) (labels.Labeler, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := r.keys[min(r.calls, len(r.keys)-1)]
	r.calls++
	return labels.Labeler{DID: did, Endpoint: r.endpoint, Key: key}, nil
}

// runConsumer starts the consumer and returns a stop that cancels it and
// waits (bounded) for Run to return.
func runConsumer(t *testing.T, store labels.Store, resolver labels.KeyResolver) (*labels.Consumer, <-chan struct{}, func()) {
	t.Helper()
	consumer, err := labels.NewConsumer(labels.ConsumerOptions{
		LabelerDID: consumerLabelerDID,
		Resolver:   resolver,
		Store:      store,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		MinBackoff: 10 * time.Millisecond,
		MaxBackoff: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	require.NotNil(t, consumer)
	ctx, cancel := context.WithCancel(context.Background())
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		consumer.Run(ctx)
	}()
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case <-returned:
		case <-time.After(waitBound):
			t.Fatalf("Run did not return within %s of cancel", waitBound)
		}
	}
	t.Cleanup(stop)
	return consumer, returned, stop
}

// awaitCommits waits for n commits, Run's early return, or the bound,
// whichever is first; the caller asserts on what was recorded.
func awaitCommits(store *fakeStore, returned <-chan struct{}, n int) {
	deadline := time.After(waitBound)
	for i := 0; i < n; i++ {
		select {
		case <-store.commits:
		case <-returned:
			return
		case <-deadline:
			return
		}
	}
}

// awaitConnections waits for n server connections, Run's early return, or
// the bound.
func awaitConnections(server *labelstest.Server, returned <-chan struct{}, n int) {
	deadline := time.After(waitBound)
	for i := 0; i < n; i++ {
		select {
		case <-server.Connections():
		case <-returned:
			return
		case <-deadline:
			return
		}
	}
}

func TestConsumer_FutureCursorResetsTheEpochAndReplaysFromZero(t *testing.T) {
	key := labelstest.NewKey(t)
	label := func(seq int64) labelstest.Frame {
		return labelstest.Labels(t, seq, labelstest.Sign(t, key, labeling.Label{
			SourceDID: consumerLabelerDID, URI: consumerSubject, Val: "!takedown",
		}))
	}
	server := labelstest.NewServer(t, func(cursor int64) labelstest.Reply {
		switch cursor {
		case 5:
			return labelstest.Reply{Frames: []labelstest.Frame{labelstest.FutureCursor(t)}}
		case 0:
			return labelstest.Reply{Frames: []labelstest.Frame{label(1), label(2), label(3)}, Hold: true}
		default:
			return labelstest.Reply{Hold: true}
		}
	})
	store := newFakeStore(labels.Cursor{Epoch: 0, Seq: 5})
	resolver := &sequenceResolver{endpoint: server.URL, keys: []atcrypto.PublicKey{labelstest.PublicKey(t, key)}}

	consumer, returned, stop := runConsumer(t, store, resolver)
	awaitCommits(store, returned, 3)
	stop()

	cursor, events := store.snapshot()
	assert.Equal(t, []int64{5, 0}, server.Cursors(), "cursors the server saw")
	assert.Equal(t, []string{"reset", "commit(1,1)", "commit(1,2)", "commit(1,3)"}, events)
	assert.Equal(t, labels.Cursor{Epoch: 1, Seq: 3}, cursor)
	assert.Equal(t, uint64(1), consumer.Stats().Resets)
}

func TestConsumer_VerificationFailure(t *testing.T) {
	keyA := labelstest.NewKey(t)
	keyB := labelstest.NewKey(t)
	keyNeverResolved := labelstest.NewKey(t)
	publicA := labelstest.PublicKey(t, keyA)
	publicB := labelstest.PublicKey(t, keyB)

	t.Run("a rotated key is picked up by the re-resolve", func(t *testing.T) {
		rotated := labelstest.Sign(t, keyB, labeling.Label{SourceDID: consumerLabelerDID, URI: consumerSubject, Val: "!takedown"})
		server := labelstest.NewServer(t, func(cursor int64) labelstest.Reply {
			if cursor == 0 {
				return labelstest.Reply{Frames: []labelstest.Frame{labelstest.Labels(t, 1, rotated)}, Hold: true}
			}
			return labelstest.Reply{Hold: true}
		})
		store := newFakeStore(labels.Cursor{})
		resolver := &sequenceResolver{endpoint: server.URL, keys: []atcrypto.PublicKey{publicA, publicB}}

		consumer, returned, stop := runConsumer(t, store, resolver)
		awaitCommits(store, returned, 1)
		stop()

		cursor, events := store.snapshot()
		assert.Equal(t, []string{"commit(0,1)"}, events)
		assert.Equal(t, labels.Cursor{Epoch: 0, Seq: 1}, cursor)
		assert.Equal(t, uint64(0), consumer.Stats().VerificationFailures)
	})

	for _, tc := range []struct {
		name string
		bad  labeling.Label
	}{
		{
			name: "signed by a key never resolved",
			bad:  labelstest.Sign(t, keyNeverResolved, labeling.Label{SourceDID: consumerLabelerDID, URI: consumerSubject, Val: "!takedown"}),
		},
		{
			name: "signed by the resolved key with another source DID",
			bad:  labelstest.Sign(t, keyA, labeling.Label{SourceDID: "did:web:impostor.example", URI: consumerSubject, Val: "!takedown"}),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			good := labelstest.Sign(t, keyA, labeling.Label{SourceDID: consumerLabelerDID, URI: consumerSubject, Val: "!takedown"})
			// The first connection carries a good label then the bad one and
			// stays open, so only the consumer can end it; every later
			// connection stays open with no frames, which bounds the run to
			// exactly the first reconnect.
			server := labelstest.NewServer(t, func(cursor int64) labelstest.Reply {
				if cursor == 0 {
					return labelstest.Reply{Frames: []labelstest.Frame{labelstest.Labels(t, 1, good), labelstest.Labels(t, 2, tc.bad)}, Hold: true}
				}
				return labelstest.Reply{Hold: true}
			})
			store := newFakeStore(labels.Cursor{})
			resolver := &sequenceResolver{endpoint: server.URL, keys: []atcrypto.PublicKey{publicA}}

			consumer, returned, stop := runConsumer(t, store, resolver)
			awaitConnections(server, returned, 2)
			stop()

			cursor, events := store.snapshot()
			assert.Equal(t, []string{"commit(0,1)"}, events, "only the good label is committed")
			assert.Equal(t, labels.Cursor{Epoch: 0, Seq: 1}, cursor, "the cursor stays at the last committed seq")
			assert.Equal(t, []int64{0, 1}, server.Cursors(), "the reconnect resumes from the last committed seq")
			assert.Equal(t, uint64(1), consumer.Stats().VerificationFailures)
		})
	}
}

// failOnceActivateStore is a fakeStore whose first Activate fails; each
// Activate records its outcome in the store's events.
type failOnceActivateStore struct {
	*fakeStore
	activations int
}

func (s *failOnceActivateStore) Activate(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activations++
	if s.activations == 1 {
		s.events = append(s.events, "activate failed")
		return fmt.Errorf("activation unavailable")
	}
	s.events = append(s.events, "activated")
	return nil
}

// TestConsumer_AFailedActivationIsRetriedBeforeTheFirstSubscribe: the
// store's first activation fails. The consumer does not subscribe until an
// activation succeeds, and subscribes once it does.
func TestConsumer_AFailedActivationIsRetriedBeforeTheFirstSubscribe(t *testing.T) {
	key := labelstest.NewKey(t)
	server := labelstest.NewServer(t, func(int64) labelstest.Reply { return labelstest.Reply{Hold: true} })
	store := &failOnceActivateStore{fakeStore: newFakeStore(labels.Cursor{Epoch: 0, Seq: 3})}
	resolver := &sequenceResolver{endpoint: server.URL, keys: []atcrypto.PublicKey{labelstest.PublicKey(t, key)}}

	_, returned, stop := runConsumer(t, store, resolver)
	var atSubscribe []string
	select {
	case <-server.Connections():
		_, atSubscribe = store.snapshot()
	case <-returned:
		t.Fatal("Run returned before subscribing")
	case <-time.After(waitBound):
		t.Fatalf("the consumer did not subscribe within %s", waitBound)
	}
	stop()

	assert.Equal(t, []string{"activate failed", "activated"}, atSubscribe,
		"the first subscribe comes only after the failed activation is retried and succeeds")
	assert.Equal(t, []int64{3}, server.Cursors(), "the consumer subscribes at the stored cursor")
}
