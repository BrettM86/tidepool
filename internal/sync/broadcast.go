package sync

import (
	"context"
	"fmt"
	"log/slog"
	stdsync "sync"
	"time"

	"github.com/lib/pq"

	"tidepool/internal/errors"
	"tidepool/internal/repo"
)

// Broadcaster wakes subscribeRepos connections when new firehose events
// land. It carries no event data itself: each connection tails
// firehose_events with its own cursor (DB-backed, so restarts and slow
// consumers are safe); the broadcaster only says "something new is visible,
// go look".
//
// Wake-up transport is postgres LISTEN/NOTIFY, chosen over seq polling
// because the repo layer already emits pg_notify inside every commit
// transaction (repo.FirehoseNotifyChannel): delivery coincides exactly with
// commit visibility, adds no per-subscriber DB load between events, and —
// because the notify runs while the global commit advisory lock is held —
// arrives in seq order. A low-frequency poll ticker backstops it anyway:
// lib/pq's Listener can drop notifications across reconnects, and a missed
// wake-up must degrade to added latency, never to a stalled stream (task
// 03's global lock guarantees a `seq > cursor` re-scan is always complete).
type Broadcaster struct {
	logger       *slog.Logger
	listener     *pq.Listener
	pollInterval time.Duration

	mu   stdsync.Mutex
	subs map[*Subscription]struct{}
}

const (
	// defaultPollInterval is the fallback wake-up period; it only bounds
	// added latency when a NOTIFY is lost, so it can be lazy.
	defaultPollInterval = 10 * time.Second

	listenerMinReconnect = 250 * time.Millisecond
	listenerMaxReconnect = 30 * time.Second
)

// NewBroadcaster connects a LISTEN session to the database and subscribes to
// the repo layer's firehose channel. Callers must run Run in a goroutine and
// Close the broadcaster when done. pollInterval <= 0 selects the default.
func NewBroadcaster(databaseURL string, pollInterval time.Duration, logger *slog.Logger) (*Broadcaster, error) {
	if databaseURL == "" {
		return nil, errors.NewValidationError("databaseURL", "must not be empty")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if pollInterval <= 0 {
		pollInterval = defaultPollInterval
	}
	b := &Broadcaster{
		logger:       logger,
		pollInterval: pollInterval,
		subs:         make(map[*Subscription]struct{}),
	}
	b.listener = pq.NewListener(databaseURL, listenerMinReconnect, listenerMaxReconnect,
		func(ev pq.ListenerEventType, err error) {
			if err != nil {
				logger.Warn("firehose listener event", "event", int(ev), "error", err)
			}
		})
	if err := b.listener.Listen(repo.FirehoseNotifyChannel); err != nil {
		_ = b.listener.Close()
		// Connectivity failure, not caller input — keep the chain wrapped.
		return nil, fmt.Errorf("sync: LISTEN %s: %w", repo.FirehoseNotifyChannel, err)
	}
	return b, nil
}

// Run fans wake-ups out to subscribers until ctx is canceled. A nil
// notification (lib/pq's reconnect marker) and every poll tick also wake
// everyone, so subscribers re-check the log after any possible gap.
func (b *Broadcaster) Run(ctx context.Context) {
	ticker := time.NewTicker(b.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case n, ok := <-b.listener.Notify:
			if !ok {
				// Close() closed the channel; without this check a closed
				// channel reads as endless nils (indistinguishable from
				// pq's reconnect marker) and Run hot-loops at 100% CPU.
				return
			}
			if n == nil {
				b.logger.Info("firehose listener reconnected; waking all subscribers")
			}
			b.wakeAll()
		case <-ticker.C:
			b.wakeAll()
		}
	}
}

// Close tears down the LISTEN session. Safe to call after Run has returned.
func (b *Broadcaster) Close() error {
	return b.listener.Close()
}

// Subscribe registers a wake-up channel for one connection. The caller must
// Close the subscription when the connection ends.
func (b *Broadcaster) Subscribe() *Subscription {
	s := &Subscription{
		b:    b,
		wake: make(chan struct{}, 1),
	}
	b.mu.Lock()
	b.subs[s] = struct{}{}
	b.mu.Unlock()
	return s
}

// subscriberCount reports the number of live subscriptions (used by tests to
// observe slow-consumer eviction).
func (b *Broadcaster) subscriberCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.subs)
}

func (b *Broadcaster) wakeAll() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for s := range b.subs {
		select {
		case s.wake <- struct{}{}:
		default: // already has a pending wake-up; one is enough
		}
	}
}

// Subscription is one connection's membership in the broadcaster.
type Subscription struct {
	b    *Broadcaster
	wake chan struct{}
}

// Wake returns the channel that receives a token whenever new events may be
// visible. The channel has capacity 1: subscribers that registered before
// reading their cursor position can never miss a wake-up, only coalesce
// several into one.
func (s *Subscription) Wake() <-chan struct{} {
	return s.wake
}

// Close unregisters the subscription.
func (s *Subscription) Close() {
	s.b.mu.Lock()
	delete(s.b.subs, s)
	s.b.mu.Unlock()
}
