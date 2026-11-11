package store

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"

	"tidepool/internal/errors"
)

// Bridged community removal (migration 060). A Coves `!takedown` label on a
// bridged community's bare DID removes the community: the label applier
// records an active removal with the community's follow state and provenance
// at that moment and unfollows it, and while the removal stands no follow path
// follows it again. A community counts as removed while it has an active
// removal or while a community `!takedown` stands on its DID, so a label that
// arrived before Tidepool had the community (or before an applier recorded it)
// refuses the follow too.

// FollowProvenance is how a community was last followed.
type FollowProvenance string

const (
	// FollowProvenanceUnknown is a community followed before provenance was
	// recorded; it counts as configured.
	FollowProvenanceUnknown FollowProvenance = ""
	// FollowProvenanceFollowList is an explicit follow-list entry.
	FollowProvenanceFollowList FollowProvenance = "follow-list"
	// FollowProvenanceInstance is an instance entry's community listing.
	FollowProvenanceInstance FollowProvenance = "instance"
	// FollowProvenancePostWalk is an instance entry's recent post walk.
	FollowProvenancePostWalk FollowProvenance = "post-walk"
	// FollowProvenanceManual is POST /admin/communities.
	FollowProvenanceManual FollowProvenance = "manual"
)

// ErrCommunityRemoved refuses a Follow of a removed community.
var ErrCommunityRemoved = stderrors.New("community is removed")

// communityTakedownLabel is the label value that removes a community.
const communityTakedownLabel = "!takedown"

// CommunityRemoval is one recorded removal of a community.
type CommunityRemoval struct {
	CommunityDID string
	LabelerDID   string
	// FollowStateAtRemoval and ProvenanceAtRemoval are the community's
	// follow state and provenance when the removal was recorded.
	FollowStateAtRemoval FollowState
	ProvenanceAtRemoval  FollowProvenance
}

// CommunityRemovals is the removal side of the postgres communities store.
// It is not part of Communities so test doubles of Communities need not
// implement it; holders type-assert the store they were given.
type CommunityRemovals interface {
	// CommunityRemoved reports whether the community with did is removed: it
	// has an active removal, or a community `!takedown` stands on did.
	CommunityRemoved(ctx context.Context, did string) (bool, error)

	// SetFollowPending records a sent Follow as SetFollowState(pending)
	// does, and records provenance when the community was not followed or
	// had none recorded. It returns ErrCommunityRemoved, changing nothing,
	// when the community is removed.
	SetFollowPending(ctx context.Context, apGroupID string, provenance FollowProvenance) error

	// AcceptPendingFollow records an Accept as SetFollowState(accepted)
	// does, only while the community is pending and not removed, and
	// reports whether it did. A removal that committed after the caller
	// read the community pending leaves it unfollowed.
	AcceptPendingFollow(ctx context.Context, apGroupID string) (bool, error)

	// Unfollow records an unsubscribe as SetFollowState(none) does and, in
	// the same write, marks the community's active removal as not followed
	// at the removal, so the removal's negation does not follow it again. An
	// Undo{Follow} the removal still owes stays owed: the unsubscribe's own
	// send is best-effort, and a second Undo{Follow} is harmless.
	Unfollow(ctx context.Context, apGroupID string) error

	// MarkUnfollowOwed records, outside a removal's transaction, that the
	// active removal of the community with did owes Lemmy an Undo{Follow}, as
	// MarkUnfollowOwedTx does: for a Follow sent while the removal committed
	// whose compensating Undo{Follow} did not go out. It returns a not-found
	// error when the community has no active removal.
	MarkUnfollowOwed(ctx context.Context, did string) error

	// ListOwedUnfollows returns every Undo{Follow} a removal owes (see
	// MarkUnfollowOwedTx), oldest first.
	ListOwedUnfollows(ctx context.Context) ([]OwedUnfollow, error)

	// ClearOwedUnfollow records that the Undo{Follow} the removal with
	// removalID owed is no longer owed, for one dropped unsent because
	// Tidepool no longer has the community.
	ClearOwedUnfollow(ctx context.Context, removalID int64) error

	// CompleteOwedUnfollow records the Undo{Follow} the removal with
	// removalID owed as delivered. When the community was followed again
	// while the Undo{Follow} was in flight, the Undo{Follow} may have reached
	// Lemmy after the new Follow and undone it: the community is re-pended
	// with its Follow owed (RestoreFollowTx with sent false), so an Accept of
	// the undone Follow is refused and the follow retrier sends a fresh one.
	// It reports whether it did.
	CompleteOwedUnfollow(ctx context.Context, removalID int64) (refollowOwed bool, err error)
}

// OwedUnfollow is an Undo{Follow} a community removal owes Lemmy.
type OwedUnfollow struct {
	RemovalID int64
	// APGroupID is the community's group IRI, empty when Tidepool no longer
	// has the community.
	APGroupID string
	// FollowState is the community's follow state now. A community followed
	// again since the removal (a negation that restored its follow, or a
	// later subscribe) no longer owes the Undo{Follow} of the old follow:
	// sending it would unfollow the new one.
	FollowState FollowState
}

// communityRemovedCondition is the SQL condition that the community DID
// expression didExpr is removed.
func communityRemovedCondition(didExpr string) string {
	return `(EXISTS (
			SELECT 1 FROM community_removal_suppressions removal
			WHERE removal.community_did = ` + didExpr + ` AND removal.cleared_at IS NULL)
		OR EXISTS (
			SELECT 1 FROM (
				SELECT neg, exp FROM coves_labels
				WHERE uri = ` + didExpr + ` AND val = '` + communityTakedownLabel + `'
				ORDER BY reset_epoch DESC, seq DESC LIMIT 1
			) current_label
			WHERE ` + StandingLabelCondition("current_label") + `))`
}

func (r *postgresCommunities) CommunityRemoved(ctx context.Context, did string) (bool, error) {
	var removed bool
	if err := r.db.QueryRowContext(ctx,
		`SELECT `+communityRemovedCondition("$1::text"), did).Scan(&removed); err != nil {
		return false, fmt.Errorf("read removal of community %s: %w", did, err)
	}
	return removed, nil
}

func (r *postgresCommunities) SetFollowPending(ctx context.Context, apGroupID string, provenance FollowProvenance) error {
	if !provenance.valid() || provenance == FollowProvenanceUnknown {
		return errors.NewValidationError("follow_provenance", fmt.Sprintf("unknown provenance %q", provenance))
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("set follow pending for %q: begin: %w", apGroupID, err)
	}
	defer func() { _ = tx.Rollback() }()
	// The row lock orders this write with a removal, which takes the same
	// lock (LockCommunityByDIDTx) before recording itself: the removal check
	// below runs after any removal holding the row has committed.
	community, err := scanCommunity(tx.QueryRowContext(ctx,
		`SELECT`+communityColumns+` FROM communities WHERE ap_group_id = $1 FOR UPDATE`, apGroupID))
	if stderrors.Is(err, sql.ErrNoRows) {
		return errors.NewNotFoundError("community", apGroupID)
	}
	if err != nil {
		return fmt.Errorf("set follow pending for %q: lock: %w", apGroupID, err)
	}
	var removed bool
	if err := tx.QueryRowContext(ctx,
		`SELECT `+communityRemovedCondition("$1::text"), community.DID).Scan(&removed); err != nil {
		return fmt.Errorf("set follow pending for %q: read removal: %w", apGroupID, err)
	}
	if removed {
		return ErrCommunityRemoved
	}
	if err := setFollowState(ctx, tx, apGroupID, FollowStatePending); err != nil {
		return err
	}
	// A resend to a community already followed keeps the provenance of the
	// path that followed it.
	if _, err := tx.ExecContext(ctx, `
		UPDATE communities SET follow_provenance = $2
		WHERE ap_group_id = $1 AND (follow_provenance IS NULL OR $3)`,
		apGroupID, string(provenance), community.FollowState == FollowStateNone); err != nil {
		return fmt.Errorf("set follow provenance for %q: %w", apGroupID, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("set follow pending for %q: commit: %w", apGroupID, err)
	}
	return nil
}

func (r *postgresCommunities) AcceptPendingFollow(ctx context.Context, apGroupID string) (bool, error) {
	// Under READ COMMITTED an UPDATE that waited on a removal's row lock
	// rechecks follow_state against the committed row, which the removal
	// set to none.
	//
	// A pending community with no follow_requested_at has a Follow owed and
	// not yet sent (RestoreFollowTx with sent false): an Accept then answers
	// a Follow from before the removal, which Lemmy has since seen undone, so
	// it is refused and the follow retrier sends the owed Follow. Every path
	// that sends a Follow stamps follow_requested_at (setFollowState pending,
	// the retrier's claim, RestoreFollowTx with sent true); a pending row
	// from before migration 012 has none either and is accepted once the
	// retrier's first claim resends its Follow.
	result, err := r.db.ExecContext(ctx, `
		UPDATE communities
		SET follow_state = 'accepted', followed_at = CURRENT_TIMESTAMP
		WHERE ap_group_id = $1 AND follow_state = 'pending'
		  AND follow_requested_at IS NOT NULL
		  AND NOT `+communityRemovedCondition("communities.did"), apGroupID)
	if err != nil {
		return false, fmt.Errorf("accept follow of %q: %w", apGroupID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("accept follow of %q: rows affected: %w", apGroupID, err)
	}
	return affected == 1, nil
}

func (r *postgresCommunities) ListOwedUnfollows(ctx context.Context) ([]OwedUnfollow, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT removal.id, COALESCE(community.ap_group_id, ''), COALESCE(community.follow_state, 'none')
		FROM community_removal_suppressions removal
		LEFT JOIN communities community ON community.did = removal.community_did
		WHERE removal.unfollow_owed_at IS NOT NULL
		ORDER BY removal.unfollow_owed_at, removal.id`)
	if err != nil {
		return nil, fmt.Errorf("list owed unfollows: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var owed []OwedUnfollow
	for rows.Next() {
		var unfollow OwedUnfollow
		var state string
		if err := rows.Scan(&unfollow.RemovalID, &unfollow.APGroupID, &state); err != nil {
			return nil, fmt.Errorf("list owed unfollows: scan: %w", err)
		}
		unfollow.FollowState = FollowState(state)
		owed = append(owed, unfollow)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list owed unfollows: rows: %w", err)
	}
	return owed, nil
}

func (r *postgresCommunities) ClearOwedUnfollow(ctx context.Context, removalID int64) error {
	if _, err := r.db.ExecContext(ctx, `
		UPDATE community_removal_suppressions SET unfollow_owed_at = NULL WHERE id = $1`, removalID); err != nil {
		return fmt.Errorf("clear owed unfollow of removal %d: %w", removalID, err)
	}
	return nil
}

func (r *postgresCommunities) CompleteOwedUnfollow(ctx context.Context, removalID int64) (bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("complete owed unfollow of removal %d: begin: %w", removalID, err)
	}
	defer func() { _ = tx.Rollback() }()
	var did string
	err = tx.QueryRowContext(ctx, `
		SELECT community_did FROM community_removal_suppressions WHERE id = $1`, removalID).Scan(&did)
	if stderrors.Is(err, sql.ErrNoRows) {
		return false, errors.NewNotFoundError("community removal", fmt.Sprint(removalID))
	}
	if err != nil {
		return false, fmt.Errorf("complete owed unfollow of removal %d: %w", removalID, err)
	}
	// The community's row lock comes before the removal's row, as a removal
	// and its negation take them, so a negation or subscribe that follows the
	// community again has committed, or waits for this commit, when its follow
	// state is read.
	community, err := LockCommunityByDIDTx(ctx, tx, did)
	if err != nil && !errors.IsNotFound(err) {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE community_removal_suppressions SET unfollow_owed_at = NULL WHERE id = $1`, removalID); err != nil {
		return false, fmt.Errorf("complete owed unfollow of removal %d: clear: %w", removalID, err)
	}
	refollowOwed := community != nil && community.FollowState != FollowStateNone
	if refollowOwed {
		// RestoreFollowTx also clears last_backfill_at, so the fresh Follow's
		// Accept walks the outbox again: that walk covers what the community
		// posted while Lemmy had it unfollowed.
		if err := RestoreFollowTx(ctx, tx, community.APGroupID, false); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("complete owed unfollow of removal %d: commit: %w", removalID, err)
	}
	return refollowOwed, nil
}

// MarkUnfollowOwedTx records that the active removal of the community with did
// owes Lemmy an Undo{Follow}, and returns the removal's id. The removal's
// transaction marks it, so the Undo{Follow} stays owed through a failed send
// or a crash before the send, until CompleteOwedUnfollow records it delivered.
func MarkUnfollowOwedTx(ctx context.Context, tx *sql.Tx, did string) (int64, error) {
	if tx == nil {
		return 0, errors.NewValidationError("tx", "must not be nil")
	}
	var removalID int64
	err := tx.QueryRowContext(ctx, `
		UPDATE community_removal_suppressions SET unfollow_owed_at = now()
		WHERE community_did = $1 AND cleared_at IS NULL
		RETURNING id`, did).Scan(&removalID)
	if stderrors.Is(err, sql.ErrNoRows) {
		return 0, errors.NewNotFoundError("community removal", did)
	}
	if err != nil {
		return 0, fmt.Errorf("mark unfollow owed for community %s: %w", did, err)
	}
	return removalID, nil
}

func (r *postgresCommunities) MarkUnfollowOwed(ctx context.Context, did string) error {
	result, err := r.db.ExecContext(ctx, `
		UPDATE community_removal_suppressions SET unfollow_owed_at = now()
		WHERE community_did = $1 AND cleared_at IS NULL`, did)
	if err != nil {
		return fmt.Errorf("mark unfollow owed for community %s: %w", did, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("mark unfollow owed for community %s: rows affected: %w", did, err)
	}
	if affected == 0 {
		return errors.NewNotFoundError("community removal", did)
	}
	return nil
}

func (r *postgresCommunities) Unfollow(ctx context.Context, apGroupID string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("unfollow %q: begin: %w", apGroupID, err)
	}
	defer func() { _ = tx.Rollback() }()
	// The follow-state update takes the community's row lock first, so a
	// removal recording itself concurrently has committed before the
	// removal update below reads the suppressions.
	if err := setFollowState(ctx, tx, apGroupID, FollowStateNone); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE community_removal_suppressions SET follow_state_at_removal = 'none'
		WHERE cleared_at IS NULL
		  AND community_did = (SELECT did FROM communities WHERE ap_group_id = $1)`, apGroupID); err != nil {
		return fmt.Errorf("unfollow %q: clear follow state at removal: %w", apGroupID, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("unfollow %q: commit: %w", apGroupID, err)
	}
	return nil
}

func (p FollowProvenance) valid() bool {
	switch p {
	case FollowProvenanceUnknown, FollowProvenanceFollowList, FollowProvenanceInstance,
		FollowProvenancePostWalk, FollowProvenanceManual:
		return true
	}
	return false
}

// LockCommunityByDIDTx reads the community with did and locks its row for the
// caller's transaction. It returns a not-found error when Tidepool has no
// community with did.
func LockCommunityByDIDTx(ctx context.Context, tx *sql.Tx, did string) (*Community, error) {
	if tx == nil {
		return nil, errors.NewValidationError("tx", "must not be nil")
	}
	community, err := scanCommunity(tx.QueryRowContext(ctx,
		`SELECT`+communityColumns+` FROM communities WHERE did = $1 FOR UPDATE`, did))
	if stderrors.Is(err, sql.ErrNoRows) {
		return nil, errors.NewNotFoundError("community", did)
	}
	if err != nil {
		return nil, fmt.Errorf("lock community %s: %w", did, err)
	}
	return community, nil
}

// AddCommunityRemovalTx records removal as the community's active removal and
// reports whether it did. A community that already has one keeps it, with the
// follow state and provenance recorded then.
func AddCommunityRemovalTx(ctx context.Context, tx *sql.Tx, removal CommunityRemoval) (bool, error) {
	if tx == nil {
		return false, errors.NewValidationError("tx", "must not be nil")
	}
	if !removal.FollowStateAtRemoval.Valid() {
		return false, errors.NewValidationError("follow_state_at_removal",
			fmt.Sprintf("unknown state %q", removal.FollowStateAtRemoval))
	}
	if !removal.ProvenanceAtRemoval.valid() {
		return false, errors.NewValidationError("follow_provenance_at_removal",
			fmt.Sprintf("unknown provenance %q", removal.ProvenanceAtRemoval))
	}
	result, err := tx.ExecContext(ctx, `
		INSERT INTO community_removal_suppressions (
			community_did, labeler_did, follow_state_at_removal, follow_provenance_at_removal
		) VALUES ($1, $2, $3, NULLIF($4, ''))
		ON CONFLICT (community_did) WHERE cleared_at IS NULL DO NOTHING`,
		removal.CommunityDID, removal.LabelerDID, string(removal.FollowStateAtRemoval),
		string(removal.ProvenanceAtRemoval))
	if err != nil {
		return false, fmt.Errorf("record removal of community %s: %w", removal.CommunityDID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("record removal of community %s: rows affected: %w", removal.CommunityDID, err)
	}
	return affected == 1, nil
}

// ClearCommunityRemovalTx clears the active removal of the community with did
// and returns it, or nil when there was none.
func ClearCommunityRemovalTx(ctx context.Context, tx *sql.Tx, did string) (*CommunityRemoval, error) {
	if tx == nil {
		return nil, errors.NewValidationError("tx", "must not be nil")
	}
	removal := CommunityRemoval{CommunityDID: did}
	var state string
	var provenance sql.NullString
	err := tx.QueryRowContext(ctx, `
		UPDATE community_removal_suppressions SET cleared_at = now()
		WHERE community_did = $1 AND cleared_at IS NULL
		RETURNING labeler_did, follow_state_at_removal, follow_provenance_at_removal`, did).
		Scan(&removal.LabelerDID, &state, &provenance)
	if stderrors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("clear removal of community %s: %w", did, err)
	}
	removal.FollowStateAtRemoval = FollowState(state)
	removal.ProvenanceAtRemoval = FollowProvenance(provenance.String)
	return &removal, nil
}

// RestoreFollowTx records the community pending after a removal's negation,
// and clears last_backfill_at so the Accept walks the outbox although the walk
// before the removal is fresh. sent true records a Follow already sent; sent
// false records one owed, with no send time, so the follow retrier sends it at
// its next sweep and counts it as an attempt then. The restored Follow is a new
// obligation, so it starts a fresh attempt budget, as a fresh subscription
// does: attempts the Follow before it used would otherwise leave the retrier
// no budget to send it. Provenance is kept.
func RestoreFollowTx(ctx context.Context, tx *sql.Tx, apGroupID string, sent bool) error {
	if tx == nil {
		return errors.NewValidationError("tx", "must not be nil")
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE communities
		SET follow_state = 'pending',
		    follow_requested_at = CASE WHEN $2 THEN CURRENT_TIMESTAMP ELSE NULL END,
		    follow_attempts = CASE WHEN $2 THEN 1 ELSE 0 END,
		    last_backfill_at = NULL
		WHERE ap_group_id = $1`, apGroupID, sent)
	if err != nil {
		return fmt.Errorf("restore follow of %q: %w", apGroupID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("restore follow of %q: rows affected: %w", apGroupID, err)
	}
	if affected == 0 {
		return errors.NewNotFoundError("community", apGroupID)
	}
	return nil
}
