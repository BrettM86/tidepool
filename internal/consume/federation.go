package consume

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// The social.coves.bridge.federation handler — the OPT-OUT (decision 11).
//
// Federation is DEFAULT-ON and this record is the only way a user turns it
// down, so the invariant running through this file is that ABSENCE MEANS
// ENABLED. The handler never writes an "enabled" row; it DELETES the row that
// said otherwise.
//
// Two writers, one authority. federation_prefs is the authority — it answers
// for every DID, including the ones that have never federated anything — while
// ap_actors.enabled is a MIRROR that is updated only if the actor already
// exists. Minting an actor in order to disable it would create exactly the
// identity the record asks the bridge not to create, so the mirror write is
// allowed to find nothing.

// handleFederation applies one federation record commit.
//
// A record DELETE and enabled=true are the SAME outcome — default-on restored
// — because absence is what default-on looks like in this table. That also
// clears any stored deleteRemote: a stale destructive flag on a re-enabled
// user is a loaded gun pointed at the task 17 tier.
//
// tx is the REV-GATE's transaction, and every state write this handler makes
// rides it: the preference row, the cancellation of that actor's queued
// deliveries, and the actor mirror are ONE decision that commits with the gate
// advance or not at all. A failure leaves the gate un-advanced and nothing
// committed, so the record replays and re-applies the whole decision.
//
// WITH ONE EXCEPTION, and it is the destructive tier — see applyOptOut. The
// preference has to be durable BEFORE peers are asked to delete anything, and
// the seam that asks them writes the very same row from its own transaction, so
// on that path the preference is committed ahead of the gate on purpose.
func (d *Dispatcher) handleFederation(ctx context.Context, tx *sql.Tx, did string, commit *CommitEvent) error {
	if commit.Operation == operationDelete {
		// A delete commit carries no record body, which costs nothing here:
		// the DID is the whole question.
		return d.restoreDefaultFederation(ctx, tx, did)
	}

	enabled, ok := boolField(commit.Record, "enabled")
	if !ok {
		// The lexicon requires enabled. A record without it can never be
		// applied, no matter how often it is retried — it goes to the DLQ
		// already exhausted, where a lexicon rollout mistake stays visible
		// instead of becoming a silent drop.
		return fmt.Errorf("%w: federation record for %s has no boolean enabled field",
			ErrPermanentEvent, did)
	}
	if enabled {
		return d.restoreDefaultFederation(ctx, tx, did)
	}

	// deleteRemote is optional and defaults to false: the soft tier. Nothing
	// destructive is ever INFERRED — only an explicit true escalates.
	deleteRemote, _ := boolField(commit.Record, "deleteRemote")
	return d.applyOptOut(ctx, tx, did, deleteRemote)
}

// applyOptOut records the opt-out and stops the user's outbound traffic.
//
// THE RESULTING ORDER, and which connection each step runs on, because that is
// the whole of this function:
//
//  1. the PREFERENCE. On the gate tx for the soft tier; on its own connection,
//     committed immediately, for the destructive one (see below).
//  2. the CANCELLATION of everything already queued — on the gate tx.
//  3. the DESTRUCTIVE SEAM, if asked for. Outside any transaction of ours: it
//     opens its own.
//  4. the ACTOR MIRROR — on the gate tx, and LAST, because the seam above
//     updates that same row from its own transaction.
//
// WHY STEP 1 SPLITS. The seam in step 3 reads and writes federation_prefs from
// its own transaction: outbound.Purger marks this exact row purged there, and
// purged_at is the only durable record that peers were really asked to delete.
// Holding the row uncommitted on the gate tx across that call would do both
// halves of the damage at once — the purge would find no preference to mark,
// and its UPDATE of the row would block on a lock this handler cannot release
// without committing while it waits synchronously for the call to return. That
// is precisely the shape rev_gate.go's DEADLOCK NOTE forbids.
//
// The cost of the split is stated rather than hidden: on the destructive path a
// later failure leaves a committed preference under an unadvanced gate, so the
// record replays. That is safe because the opt-out is idempotent (the same
// upsert, the same deterministic activity ids, a delivery insert that returns
// the standing row) and because the preference is the SAFE half to have
// committed early — it says "stop", and a replay re-asserts it.
func (d *Dispatcher) applyOptOut(ctx context.Context, tx *sql.Tx, did string, deleteRemote bool) error {
	// The preference is the AUTHORITY, not a mirror: it answers for every DID,
	// including the ones with no actor to mirror onto.
	pref := store.FederationPref{
		DID:          did,
		Enabled:      false,
		DeleteRemote: deleteRemote,
		Source:       store.FederationPrefSourceRecord,
	}
	var err error
	if deleteRemote {
		_, err = d.prefs.Upsert(ctx, pref)
	} else {
		_, err = d.prefs.UpsertTx(ctx, tx, pref)
	}
	if err != nil {
		return fmt.Errorf("record federation opt-out for %s: %w", did, err)
	}

	// STOP MEANS TWO FACTS AT ONCE: nothing new goes out, and nothing already
	// queued goes out either. The cancellation answers the second — and it
	// cancels only what PUBLISHES, never a retraction.
	//
	// That exemption is what makes this replay-safe. The destructive tier below
	// enqueues its withdrawal (a Delete and some Undos) on its own transaction,
	// which can commit while this one later rolls back; the record then replays
	// and reaches this line again. A sweeping cancel here would cancel the
	// erasure the previous attempt committed, and nothing would repair it — the
	// delivery insert returns the standing row rather than reviving it, and the
	// votes are already retracted, so the re-run finds nothing to enumerate. The
	// user would be tombstoned, the log would say the purge applied, and no peer
	// would ever have been told.
	cancelled, err := d.deliveries.CancelOutwardForActorTx(ctx, tx, did)
	if err != nil {
		return fmt.Errorf("cancel queued deliveries for %s: %w", did, err)
	}
	if cancelled > 0 {
		d.logger.Info("federation opt-out cancelled queued deliveries",
			slog.String("did", did), slog.Int64("cancelled", cancelled))
	}

	if deleteRemote {
		if err := d.deleteRemoteContent(ctx, did); err != nil {
			return err
		}
	}

	// The mirror answers the first fact — the consumer, the admission gate and
	// the delivery claim all read it — and it lands LAST, for a reason that is
	// about locks rather than meaning: it UPDATEs the actor row, and the
	// destructive tier updates that same row from its own transaction. Holding
	// this one across that call deadlocks the two against each other, and the
	// handler cannot release a lock it holds without committing.
	//
	// It still rides the rev-gate transaction, so the cancellation and the flag
	// commit together with the gate advance: nothing retries the missing half,
	// because the record is applied ONCE under a gate that rejects the replay.
	return d.mirrorActorEnabled(ctx, tx, did, false)
}

// deleteRemoteContent runs the DESTRUCTIVE tier: peers are asked to delete what
// they already hold, the votes they still count are retracted, and the identity
// stops resolving.
//
// It runs on its OWN transaction rather than the gate's (the seam takes no tx),
// which is why the caller must not be holding a lock on anything it writes. The
// consequence of that split is stated plainly: if the gate transaction later
// rolls back, the withdrawal has still been enqueued and the record replays —
// which is safe only because every part of the purge is idempotent (a
// deterministic activity id, a delivery insert that returns the standing row,
// a tombstone that keeps its first timestamp).
//
// IRREVERSIBLE. Peers that honour a Delete cannot restore what they dropped,
// and Lemmy's own un-delete on refetch is not something other software promises.
func (d *Dispatcher) deleteRemoteContent(ctx context.Context, did string) error {
	if d.remoteDeleter == nil {
		// A deployment where the destructive tier has not landed yet. The
		// request is already recorded, so it can be acted on from the backlog
		// rather than the user's wish being lost.
		d.logger.Warn("federation deleteRemote requested with no destructive seam wired",
			slog.String("did", did))
		return nil
	}
	// NOT once per record, despite the gate: this call can run again on the
	// replay its own doc describes, so the purge has to be idempotent rather
	// than merely rare (it is — see outbound.Purger).
	if err := d.remoteDeleter.DeleteRemoteContent(ctx, did); err != nil {
		return fmt.Errorf("delete remote content for %s: %w", did, err)
	}
	return nil
}

// restoreDefaultFederation is the re-enable path shared by enabled=true and a
// record delete: the preference row goes away (absence IS default-on) and an
// EXISTING actor is re-enabled under its original identity.
//
// Cancelled deliveries are NOT resurrected. Re-enabling restores the user's
// ability to federate from now on; the work they cancelled by asking us to stop
// was withdrawn at their request, and re-sending it would publish on their
// behalf something they had already taken back.
// A WITHDRAWN IDENTITY IS NOT RESTORED BY EITHER. The destructive tier asked
// every peer to delete this user's content and its actor document answers 410
// forever; re-enabling afterwards would sign new posts, comments and votes as
// somebody peers were explicitly told is gone. Both halves of the restore are
// refused at their own store — SetEnabled cannot re-enable a tombstoned actor,
// Delete cannot remove a purged preference — so this reads the outcome back
// rather than deciding it, and says so once, loudly, where an operator can see
// that a user tried to come back and could not.
//
// BOTH HALVES RIDE THE GATE TRANSACTION. No destructive seam is reachable from
// here, so nothing opens a second transaction against either row and the whole
// restore commits with the gate advance — which matters in this direction most
// of all: absence means default-on, so a cleared preference that outlived a
// failed event would silently re-enable federation for somebody who asked us to
// stop.
func (d *Dispatcher) restoreDefaultFederation(ctx context.Context, tx *sql.Tx, did string) error {
	cleared, err := d.prefs.DeleteTx(ctx, tx, did)
	if err != nil {
		return fmt.Errorf("clear federation preference for %s: %w", did, err)
	}
	if err := d.mirrorActorEnabled(ctx, tx, did, true); err != nil {
		return err
	}
	if cleared {
		return nil // an ordinary re-enable
	}

	// Nothing was cleared, so this is either a DID that never opted out or one
	// whose withdrawal committed and cannot be undone. The read tells them
	// apart, and it is safe on any connection precisely BECAUSE nothing was
	// written: this transaction has changed nothing about the row it is asking
	// about, so an outside snapshot and the transaction's own agree.
	//
	// The read is the report. Nothing here can fail the event: the record was
	// applied exactly as far as it is allowed to go, and retrying would re-ask a
	// question whose answer is terminal.
	pref, err := d.prefs.Get(ctx, did)
	switch {
	case errors.IsNotFound(err):
		return nil // there was nothing to clear
	case err != nil:
		return fmt.Errorf("read federation preference for %s: %w", did, err)
	case pref.PurgedAt != nil:
		d.logger.Warn("re-enable refused: this identity was withdrawn and the withdrawal is irreversible",
			slog.String("did", did), slog.Time("purged_at", *pref.PurgedAt))
	}
	return nil
}

// mirrorActorEnabled updates the ap_actors mirror IF the actor exists. A
// missing actor is a no-op success, not an error: an opt-out (or an enable)
// from a DID that has never federated anything is the ordinary case, and
// actors mint at the first federating interaction — never here.
func (d *Dispatcher) mirrorActorEnabled(ctx context.Context, tx *sql.Tx, did string, enabled bool) error {
	err := d.apActors.SetEnabledTx(ctx, tx, did, enabled)
	if errors.IsNotFound(err) {
		d.logger.Debug("federation preference for a DID with no actor",
			slog.String("did", did), slog.Bool("enabled", enabled))
		return nil
	}
	if err != nil {
		return fmt.Errorf("mirror federation preference onto actor %s: %w", did, err)
	}
	return nil
}

// mayFederate answers the opt-out gate for one author. It is keyed by the
// AUTHOR's DID and reads storage every time: a cached or shared "federation is
// on" flag would let one user's re-enable lift another user's opt-out.
//
// NotFound MEANS default-on. That is the whole reason the store reports an
// absent row as NotFound rather than inventing an enabled one.
func (d *Dispatcher) mayFederate(ctx context.Context, did string) (bool, error) {
	pref, err := d.prefs.Get(ctx, did)
	if errors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("read federation preference for %s: %w", did, err)
	}
	return pref.Enabled, nil
}

// operationDelete is the commit operation that carries no record body.
const operationDelete = "delete"

// boolField reads an optional boolean from a decoded record. It reports
// whether the field was present AND boolean, so a caller can tell "absent"
// (defaults apply) from "false" (the user said so).
func boolField(record map[string]any, name string) (value, ok bool) {
	raw, present := record[name]
	if !present {
		return false, false
	}
	parsed, isBool := raw.(bool)
	return parsed, isBool
}
