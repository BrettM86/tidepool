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
// tx is the REV-GATE's transaction, and the disable path writes on it: the
// actor mirror and the cancellation of that actor's queued deliveries are one
// decision, and they commit with the gate advance or not at all. Atomicity here
// is structural rather than defended — there is no window in which one landed
// and the other did not, and a failure leaves the gate un-advanced so the record
// replays and re-applies both.
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

	// The preference is recorded BEFORE the destructive seam is reached, and
	// deliberately NOT on the transaction below. Peers that honor a Delete
	// cannot restore what they dropped, so the user's intent must be durable
	// before anything is sent — and the gate transaction has not committed by
	// the time the destructive seam runs. It is the authority besides: it
	// answers for every DID, including the ones with no actor to mirror onto.
	if _, err := d.prefs.Upsert(ctx, store.FederationPref{
		DID:          did,
		Enabled:      false,
		DeleteRemote: deleteRemote,
		Source:       store.FederationPrefSourceRecord,
	}); err != nil {
		return fmt.Errorf("record federation opt-out for %s: %w", did, err)
	}

	// STOP MEANS TWO FACTS AT ONCE: nothing new goes out, and nothing already
	// queued goes out either. The cancellation answers the second, and it runs
	// FIRST — before the destructive tier below — because that tier ENQUEUES
	// the withdrawal, and a cancellation of "this actor's pending work" that ran
	// afterwards would cancel the Delete{Person} it just queued. The order is
	// not a preference: the two statements are the same predicate pointed at
	// different moments.
	cancelled, err := d.deliveries.CancelForActorTx(ctx, tx, did)
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
	// Reached exactly once per applied record: the rev gate rejects a replay
	// before this handler runs, and asking peers a second time to delete
	// content the user may have since re-enabled is unrecoverable.
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
func (d *Dispatcher) restoreDefaultFederation(ctx context.Context, tx *sql.Tx, did string) error {
	if err := d.prefs.Delete(ctx, did); err != nil {
		return fmt.Errorf("clear federation preference for %s: %w", did, err)
	}
	return d.mirrorActorEnabled(ctx, tx, did, true)
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
