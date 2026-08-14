package store

import (
	"context"
	"database/sql"
	"fmt"

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
	// the new activity.
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO community_bans (
			community_did, subject_did, community_ap_id, expires_at, reason, remove_data)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (community_did, subject_did) DO UPDATE SET
			community_ap_id = EXCLUDED.community_ap_id,
			expires_at = EXCLUDED.expires_at,
			reason = EXCLUDED.reason,
			remove_data = EXCLUDED.remove_data,
			updated_at = now()`,
		ban.CommunityDID, ban.SubjectDID, ban.CommunityAPID,
		ban.ExpiresAt, ban.Reason, ban.RemoveData); err != nil {
		return 0, fmt.Errorf("ban %q in %q: %w", ban.SubjectDID, ban.CommunityDID, err)
	}

	cancelled, err = cancelPendingForActorInCommunity(ctx, tx, ban.SubjectDID, ban.CommunityAPID)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("ban %q in %q: commit: %w", ban.SubjectDID, ban.CommunityDID, err)
	}
	return cancelled, nil
}

func (r *postgresCommunityBans) Lift(ctx context.Context, communityDID, subjectDID string) (lifted bool, err error) {
	if communityDID == "" || subjectDID == "" {
		return false, errors.NewValidationError("ban", "community_did and subject_did must not be empty")
	}
	// The row is DELETED rather than marked lifted. A ban is current state, not
	// a log — the admissions ledger already records what happened to each post —
	// and a lifted row that readers had to filter is one more chance to read a
	// ban that no longer exists as one that does.
	//
	// It lifts ONLY the ban. Content removed under removeData stays removed:
	// Lemmy models restoration as a separate restore_data flag, so republishing
	// here would reverse a decision nobody reversed.
	result, err := r.db.ExecContext(ctx,
		`DELETE FROM community_bans WHERE community_did = $1 AND subject_did = $2`,
		communityDID, subjectDID)
	if err != nil {
		return false, fmt.Errorf("lift ban on %q in %q: %w", subjectDID, communityDID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("lift ban on %q in %q: rows affected: %w", subjectDID, communityDID, err)
	}
	return affected > 0, nil
}

func (r *postgresCommunityBans) Standing(ctx context.Context, communityDID, subjectDID string) (bool, error) {
	if communityDID == "" || subjectDID == "" {
		return false, errors.NewValidationError("ban", "community_did and subject_did must not be empty")
	}
	var standing bool
	// The expiry test is in the STATEMENT, not in Go, so no reader can forget
	// it: a lapsed ban is indistinguishable from no ban, and the only signal
	// that it lapsed is the clock — Lemmy sends nothing.
	if err := r.db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM community_bans
			 WHERE community_did = $1 AND subject_did = $2
			   AND (expires_at IS NULL OR expires_at > now()))`,
		communityDID, subjectDID).Scan(&standing); err != nil {
		return false, fmt.Errorf("read ban on %q in %q: %w", subjectDID, communityDID, err)
	}
	return standing, nil
}
