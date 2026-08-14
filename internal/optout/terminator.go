// Package optout implements the TERMINAL tier of the federation lifecycle
// (PLAN.md decision 19): what the bridge does when a native account is gone.
//
// It is a separate package from the consumer that calls it because the two make
// opposite mistakes. The consumer's job is to apply what a firehose event says;
// this tier's job is to DISBELIEVE it until the identity's own sources agree,
// because the action on the other side — asking every peer to delete a user's
// content — is one no peer undoes.
package optout

import (
	"context"
	"fmt"
	"log/slog"

	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// AccountConfirmer answers whether a DID's repo is really deleted, read from
// the identity's own sources rather than from the event that brought us here.
// *consume.HandleResolver implements it.
//
// THE THREE OUTCOMES ARE THE INTERFACE, and collapsing any two is the bug this
// seam exists to prevent:
//
//	(true,  nil) — confirmed deleted; the destructive tier may run.
//	(false, nil) — confirmed live; nothing destructive, and the event is done.
//	(_,     err) — UNKNOWN; nothing destructive, nothing recorded, retry.
//
// "We could not confirm" is not "confirmed not deleted". That collapse is
// 17c-3's P1-c one layer up — a ban's expiry where absent and unparseable both
// became nil, nil meant permanent, and an author was excluded forever because a
// timestamp did not parse — and here it goes wrong in both directions at once:
// read as live it silently drops a real deletion and leaves the user's content
// federated forever, read as deleted it erases a user who never left.
type AccountConfirmer interface {
	AccountStatus(ctx context.Context, did string) (deleted bool, err error)
}

// RemoteContentDeleter asks peers to drop a user's already-federated content —
// the irreversible half. Optional: a deployment without it records the
// preference and leaves the work in the backlog.
type RemoteContentDeleter interface {
	DeleteRemoteContent(ctx context.Context, did string) error
}

// Options configures a Terminator. Confirmer and Prefs are required.
type Options struct {
	// Confirmer re-verifies the deletion against PLC and the PDS.
	Confirmer AccountConfirmer
	// Prefs records the user's terminal state, so the intent survives a crash
	// between deciding and sending.
	Prefs store.FederationPrefs
	// Deleter is the destructive seam. Optional (see RemoteContentDeleter).
	Deleter RemoteContentDeleter
	Logger  *slog.Logger
}

// Terminator applies decision 19's terminal tier: confirm, then act.
type Terminator struct {
	confirmer AccountConfirmer
	prefs     store.FederationPrefs
	deleter   RemoteContentDeleter
	logger    *slog.Logger
}

// NewTerminator validates options and builds a Terminator.
func NewTerminator(opts Options) (*Terminator, error) {
	if opts.Confirmer == nil {
		// REQUIRED. A terminator that cannot confirm would act on the event
		// alone, which is the one thing this tier exists not to do.
		return nil, errors.NewValidationError("confirmer", "must not be nil")
	}
	if opts.Prefs == nil {
		return nil, errors.NewValidationError("prefs", "must not be nil")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	if opts.Deleter == nil {
		// Announced once, at construction, rather than discovered from a
		// confirmed deletion quietly doing nothing.
		logger.Warn("account terminator has no destructive seam wired: confirmed deletions " +
			"will be recorded and left in the backlog")
	}
	return &Terminator{
		confirmer: opts.Confirmer,
		prefs:     opts.Prefs,
		deleter:   opts.Deleter,
		logger:    logger,
	}, nil
}

// TerminateAccount handles a repo whose #account status says deleted.
//
// CONFIRM FIRST, ALWAYS. The event is a claim about a moment that may have
// passed: reconnects replay events, a user can reactivate, and a deletion that
// was true an hour ago may not be true now. Nothing here is recoverable once
// sent, so the sequence is confirm → record → send, and each step only runs
// because the one before it succeeded.
//
// An unconfirmable status returns an ERROR, which is what leaves the consumer's
// per-DID seq un-advanced and the event redrivable. A confirmed LIVE account
// returns nil: there is nothing to do, and the seq must advance or a stale
// deletion event would wedge every later status change for that DID behind it.
func (t *Terminator) TerminateAccount(ctx context.Context, did string) error {
	deleted, err := t.confirmer.AccountStatus(ctx, did)
	if err != nil {
		// UNKNOWN. Not "not deleted" — see AccountConfirmer.
		return fmt.Errorf("confirm account deletion for %s: %w", did, err)
	}
	if !deleted {
		// The event said deleted and the identity says otherwise, which is
		// exactly why the confirm exists: a rewound cursor replaying a
		// deletion the user has since reversed, or a status that never meant
		// what the event implied. Logged at INFO because it is a real
		// divergence between the firehose and PLC, and silence here would make
		// a confirm that is broken indistinguishable from one that is working.
		t.logger.Info("account deletion not confirmed; taking no destructive action",
			slog.String("did", did))
		return nil
	}

	// RECORDED BEFORE ANYTHING IS SENT. Peers that honour a Delete cannot
	// restore what they dropped, so the user's terminal state has to survive a
	// crash between deciding and sending — and it is what stops the bridge
	// federating for them again in the meantime.
	if _, err := t.prefs.Upsert(ctx, store.FederationPref{
		DID:          did,
		Enabled:      false,
		DeleteRemote: true,
		Source:       store.FederationPrefSourceAccount,
	}); err != nil {
		return fmt.Errorf("record account deletion for %s: %w", did, err)
	}

	if t.deleter == nil {
		// The same shape the opt-out path already uses for an unwired
		// destructive tier: the request is recorded, so it can be acted on from
		// the backlog rather than the user's deletion being lost. Deliberately
		// NOT degraded into a pause — a deletion half-handled as a pause looks
		// handled in the database and is not.
		t.logger.Warn("account confirmed deleted but no destructive seam is wired",
			slog.String("did", did))
		return nil
	}
	if err := t.deleter.DeleteRemoteContent(ctx, did); err != nil {
		return fmt.Errorf("delete remote content for %s: %w", did, err)
	}
	t.logger.Info("account confirmed deleted; remote content withdrawal requested",
		slog.String("did", did))
	return nil
}
