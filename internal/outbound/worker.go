package outbound

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"tidepool/internal/store"
)

// DefaultLease bounds a delivery claim: long enough for a POST + retries, short
// enough that a crashed worker's delivery is re-claimable.
const DefaultLease = 2 * time.Minute

// WorkerOptions configures a Worker.
type WorkerOptions struct {
	// DB is the bridge database.
	DB *sql.DB
	// Activities / Deliveries are the split queue. Optional: nil from DB.
	Activities store.OutboundActivities
	Deliveries store.OutboundDeliveries
	// Objects gates causally (a bridge-origin parent must be accepted before
	// its child delivers) and is stamped accepted on delivery success. Optional.
	Objects store.OutboundObjects
	// Actors is the consent recheck at claim time: a disabled/paused actor's
	// create/update is cancelled, not delivered (delete/undo are exempt).
	// Optional: nil from DB.
	Actors store.APActors
	// Signers yields the per-actor Signer each delivery is signed with.
	Signers SignerProvider
	// Inboxes re-resolves a rotated inbox once before poisoning.
	Inboxes InboxResolver
	// Sender POSTs the signed activity. *ap.Client satisfies it.
	Sender ActivitySender
	// Lease overrides DefaultLease.
	Lease time.Duration
	// Logger receives per-delivery outcomes. Nil uses slog.Default().
	Logger *slog.Logger
}

// Worker claims one delivery at a time and carries it to a terminal state. It
// is the at-least-once engine: a crash between POST and MarkDelivered redelivers
// (Lemmy dedupes on our stable activity id, and its duplicate-activity response
// is classified DELIVERED, not poisoned).
type Worker struct {
	db         *sql.DB
	activities store.OutboundActivities
	deliveries store.OutboundDeliveries
	objects    store.OutboundObjects
	actors     store.APActors
	signers    SignerProvider
	inboxes    InboxResolver
	sender     ActivitySender
	lease      time.Duration
	logger     *slog.Logger
}

// NewWorker wires a Worker.
func NewWorker(opts WorkerOptions) (*Worker, error) {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	lease := opts.Lease
	if lease <= 0 {
		lease = DefaultLease
	}
	activities := opts.Activities
	if activities == nil {
		activities = store.NewOutboundActivities(opts.DB)
	}
	deliveries := opts.Deliveries
	if deliveries == nil {
		deliveries = store.NewOutboundDeliveries(opts.DB)
	}
	objects := opts.Objects
	if objects == nil {
		objects = store.NewOutboundObjects(opts.DB)
	}
	return &Worker{
		db:         opts.DB,
		activities: activities,
		deliveries: deliveries,
		objects:    objects,
		actors:     opts.Actors,
		signers:    opts.Signers,
		inboxes:    opts.Inboxes,
		sender:     opts.Sender,
		lease:      lease,
		logger:     logger,
	}, nil
}

// DeliverNext claims one processable delivery and carries it to a terminal
// state (delivered / poisoned / cancelled) or reschedules it. It returns
// worked=true when a delivery was claimed and handled, worked=false (nil error)
// when the queue held nothing claimable.
//
// STUB (task 15 RED): GREEN claims, rechecks consent (exempting delete/undo),
// enforces causal gating, signs and POSTs, then records the fenced outcome.
func (w *Worker) DeliverNext(ctx context.Context) (worked bool, err error) {
	return false, nil
}
