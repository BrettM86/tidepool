package store

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"time"

	"tidepool/internal/errors"
)

// postgresCommunityBans is the community_bans repository: one community's ban of
// one author (migration 027).
//
// It is its own repository over its own table, and postgresCommunities EMBEDS it
// so a holder of the concrete communities store can be type-asserted to
// CommunityBans — a wiring convenience that does NOT widen the Communities
// interface (see the embed for why that matters).
type postgresCommunityBans struct {
	db *sql.DB
}

// NewCommunityBans creates the postgres-backed community_bans repository.
func NewCommunityBans(db *sql.DB) CommunityBans {
	return &postgresCommunityBans{db: db}
}

func (r *postgresCommunityBans) Ban(ctx context.Context, ban CommunityBan) (cancelled int64, err error) {
	switch {
	case ban.CommunityDID == "":
		return 0, errors.NewValidationError("community_did", "must not be empty")
	case ban.SubjectDID == "":
		return 0, errors.NewValidationError("subject_did", "must not be empty")
	case ban.CommunityAPID == "":
		// The delivery side has no other handle for this community, so a ban
		// stored without it is one the queue can never apply.
		return 0, errors.NewValidationError("community_ap_id", "must not be empty")
	}

	// ONE TRANSACTION, because the two writes are one decision. The row is what
	// stops the author's NEXT post; the cancellation is what stops the posts
	// already queued. If the row committed and the cancellation did not, the
	// queue would keep pushing a banned author's posts at a community that
	// rejects them until each one poisons — and nothing would retry the
	// cancellation, because Lemmy sends the Block exactly once.
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("ban %q in %q: begin: %w", ban.SubjectDID, ban.CommunityDID, err)
	}
	defer func() { _ = tx.Rollback() }()

	// banned_at is preserved on conflict: a re-delivered Block is the same ban
	// arriving twice, not a new one. Everything the moderator can CHANGE by
	// re-issuing (the expiry, the reason, whether content goes) is taken from
	// the new activity — with ONE exception, which is the WHERE clause below.
	//
	// A BAN THAT IS OVER MAY NOT WEAKEN ONE THAT IS IN FORCE. Without that
	// clause the row is last-writer-wins, and the last writer is not the last
	// moderator: a dead-letter redrive, a backfill replay and a delayed
	// redelivery all present an OLD Block again under a NEW activity id, which
	// the inbox's dedup cannot recognize as a repeat. Sequence — a three-day ban
	// lapses, the moderators escalate to permanent, the old Block is redriven —
	// and expires_at goes back to a timestamp in the past. Standing() reads
	// unbanned from that instant, PERMANENTLY: Lemmy sent its Block exactly once
	// and sends nothing at all to say a ban is still on, so no later activity can
	// correct it. The same last-writer-wins rewrites `reason` and `remove_data`,
	// which is why the guard covers the whole SET rather than the expiry alone.
	//
	// It is deliberately NARROW — it refuses only writes that are already dead on
	// arrival. An arriving ban that IS in force is a moderator re-issuing, and
	// shortening a live ban is a decision they are entitled to make; that shape
	// is indistinguishable from a stale replay with the state this table holds,
	// so it is allowed through (residual, and the mild one: the ban stays a ban).
	//
	// BOTH SIDES ARE WEIGHED AGAINST THE DATABASE'S now(), the same clock
	// Standing() reads, so "in force" cannot mean one thing at the write and
	// another at the read.
	//
	// RETURNING answers two questions in the one statement: no row comes back
	// when the guard refused the update (the standing ban was left alone), and
	// the boolean says whether the row that IS there now is in force.
	var inForce bool
	if err := tx.QueryRowContext(ctx, `
		INSERT INTO community_bans (
			community_did, subject_did, community_ap_id, expires_at, reason, remove_data)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (community_did, subject_did) DO UPDATE SET
			community_ap_id = EXCLUDED.community_ap_id,
			expires_at = EXCLUDED.expires_at,
			reason = EXCLUDED.reason,
			remove_data = EXCLUDED.remove_data,
			updated_at = now()
		WHERE EXCLUDED.expires_at IS NULL
		   OR EXCLUDED.expires_at > now()
		   OR NOT (community_bans.expires_at IS NULL OR community_bans.expires_at > now())
		RETURNING expires_at IS NULL OR expires_at > now()`,
		ban.CommunityDID, ban.SubjectDID, ban.CommunityAPID,
		ban.ExpiresAt, ban.Reason, ban.RemoveData).Scan(&inForce); err != nil {
		if !stderrors.Is(err, sql.ErrNoRows) {
			return 0, fmt.Errorf("ban %q in %q: %w", ban.SubjectDID, ban.CommunityDID, err)
		}
		// The guard refused: a lapsed Block over a ban that is still in force.
		// Nothing was written and nothing may be cancelled — the exclusion that
		// stands is not this activity's, and it did its own cancelling when it
		// landed.
		inForce = false
	}

	// THE ROW IS RECORDED EITHER WAY (unless it would weaken a standing ban);
	// THE CANCELLATION IS NOT. A Block whose expiry has already passed when it
	// reaches us is a faithful record of a ban that is over, and storing it keeps
	// the audit trail honest (and idempotent, since a later redelivery finds the
	// same row). But it is not in force, so it must not cancel work by an author
	// nobody is currently excluding: a cancelled delivery is never re-queued.
	//
	// The condition is the SAME predicate Standing() reads — read off the STORED
	// row above, on the database's clock, rather than compared in Go against
	// another one — and it is kept here rather than at the call site so no caller
	// can cancel on a ban that does not apply.
	if inForce {
		cancelled, err = cancelPendingForActorInCommunity(ctx, tx, ban.SubjectDID, ban.CommunityAPID)
		if err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("ban %q in %q: commit: %w", ban.SubjectDID, ban.CommunityDID, err)
	}
	return cancelled, nil
}

func (r *postgresCommunityBans) Lift(ctx context.Context, communityDID, subjectDID string,
	undoneExpiry *time.Time) (lifted, retained bool, err error) {

	if communityDID == "" || subjectDID == "" {
		return false, false, errors.NewValidationError("ban", "community_did and subject_did must not be empty")
	}
	// The row is DELETED rather than marked lifted. A ban is current state, not
	// a log — the admissions ledger already records what happened to each post —
	// and a lifted row that readers had to filter is one more chance to read a
	// ban that no longer exists as one that does.
	//
	// It lifts ONLY the ban. Content removed under removeData stays removed:
	// Lemmy models restoration as a separate restore_data flag, so republishing
	// here would reverse a decision nobody reversed.
	//
	// THE DELETE IS CONDITIONAL, for the reason Ban()'s upsert is guarded: an
	// Undo{Block} can be redriven or replayed under a new activity id long after
	// the ban it reversed stopped being the ban in force, and an unconditional
	// delete then lifts the NEWER one — permanently, since Lemmy will not send a
	// second Block.
	//
	// undoneExpiry is the only description of the reversed ban an Undo carries
	// (the row holds no activity id to match, and Lemmy mints a fresh Block
	// inside the Undo, so ids cannot be compared). The rule it supports: an Undo
	// of a ban that ended at T cannot lift a ban that outlives T — a permanent
	// row (NULL) or one expiring later is a STRONGER ban than the one being
	// undone, so it is not the ban this Undo is about.
	//
	// What is protected is a ban IN FORCE, exactly as in Ban(), and on the same
	// clock: a row that has lapsed excludes nobody, so there is nothing there for
	// a stale Undo to take away and the delete proceeds. Without that third
	// clause the guard would fire on rows whose removal changes nothing, and an
	// operator would be handed a warning about an author who is not banned.
	//
	// RESIDUAL, and it is real: an Undo naming NO expiry — the shape Lemmy sends
	// for a permanent ban, and the common one — is indistinguishable from its own
	// replay, so it still lifts whatever stands. Closing that needs state this
	// table does not hold (the activity id of the ban in force, or the moment the
	// Undo was first seen); it is not inferable from the row.
	if err := r.db.QueryRowContext(ctx, `
		WITH standing AS (
			SELECT 1 FROM community_bans
			 WHERE community_did = $1 AND subject_did = $2
		), gone AS (
			DELETE FROM community_bans
			 WHERE community_did = $1 AND subject_did = $2
			   AND ($3::timestamptz IS NULL
			        OR (expires_at IS NOT NULL AND expires_at <= $3::timestamptz)
			        OR NOT (expires_at IS NULL OR expires_at > now()))
			RETURNING 1
		)
		SELECT EXISTS (SELECT 1 FROM gone), EXISTS (SELECT 1 FROM standing)`,
		communityDID, subjectDID, undoneExpiry).Scan(&lifted, &retained); err != nil {
		return false, false, fmt.Errorf("lift ban on %q in %q: %w", subjectDID, communityDID, err)
	}
	// retained is "a ban was there and is STILL there" — a refusal, not a
	// no-op — and the caller has to be able to say which of the two happened.
	return lifted, retained && !lifted, nil
}

func (r *postgresCommunityBans) Standing(ctx context.Context, communityDID, subjectDID string) (bool, error) {
	return standingBan(ctx, r.db, communityDID, subjectDID)
}

func (r *postgresCommunityBans) StandingTx(ctx context.Context, tx *sql.Tx, communityDID, subjectDID string) (bool, error) {
	if tx == nil {
		return false, errors.NewValidationError("tx", "must not be nil")
	}
	return standingBan(ctx, tx, communityDID, subjectDID)
}

func standingBan(ctx context.Context, q queryRower, communityDID, subjectDID string) (bool, error) {
	if communityDID == "" || subjectDID == "" {
		return false, errors.NewValidationError("ban", "community_did and subject_did must not be empty")
	}
	var standing bool
	// The expiry test is in the STATEMENT, not in Go, so no reader can forget
	// it: a lapsed ban is indistinguishable from no ban, and the only signal
	// that it lapsed is the clock — Lemmy sends nothing.
	if err := q.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM community_bans
			 WHERE community_did = $1 AND subject_did = $2
			   AND (expires_at IS NULL OR expires_at > now()))`,
		communityDID, subjectDID).Scan(&standing); err != nil {
		return false, fmt.Errorf("read ban on %q in %q: %w", subjectDID, communityDID, err)
	}
	return standing, nil
}
