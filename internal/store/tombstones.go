package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"tidepool/internal/errors"
)

type postgresTombstones struct {
	db *sql.DB
}

// NewTombstones creates the postgres-backed ap_tombstones repository.
func NewTombstones(db *sql.DB) Tombstones {
	return &postgresTombstones{db: db}
}

func (r *postgresTombstones) Record(ctx context.Context, apID, announcer string) error {
	if apID == "" {
		return errors.NewValidationError("ap_id", "must not be empty")
	}
	// Per (ap_id, announcer): re-recording keeps the FIRST deleted_at, so a
	// re-delivered Delete cannot walk its own marker past the retention
	// horizon. Two communities' markers for the same id are separate rows.
	query := `
		INSERT INTO ap_tombstones (ap_id, announcer)
		VALUES ($1, $2)
		ON CONFLICT (ap_id, announcer) DO NOTHING`
	if _, err := r.db.ExecContext(ctx, query, apID, announcer); err != nil {
		return fmt.Errorf("record tombstone %q (announcer %q): %w", apID, announcer, err)
	}
	return nil
}

func (r *postgresTombstones) ExistsFor(ctx context.Context, apID, communityIRI string) (bool, error) {
	if apID == "" {
		return false, errors.NewValidationError("ap_id", "must not be empty")
	}
	// '' is always visible (the origin-authorized marker); a community sees
	// its own on top of that. A caller with no community context passes ''
	// and therefore sees only global markers — the announcer IN (...) form
	// collapses to announcer = '' for it, which is exactly the rule.
	var exists bool
	query := `SELECT EXISTS (
		SELECT 1 FROM ap_tombstones WHERE ap_id = $1 AND announcer IN ('', $2))`
	if err := r.db.QueryRowContext(ctx, query, apID, communityIRI).Scan(&exists); err != nil {
		return false, fmt.Errorf("check tombstone %q (community %q): %w", apID, communityIRI, err)
	}
	return exists, nil
}

func (r *postgresTombstones) Remove(ctx context.Context, apID, communityIRI string) error {
	if apID == "" {
		return errors.NewValidationError("ap_id", "must not be empty")
	}
	if communityIRI == "" {
		// Origin authority (a bare Undo{Delete}, authorized against the target
		// id's own host): the id's own instance says the content is live, which
		// outranks any community's claim that it is gone. Leaving a
		// community-scoped marker here would suppress content the origin is
		// serving and the bridge is about to materialize — an inconsistency,
		// not a safety margin. Deliberately asymmetric with ExistsFor(''),
		// which stays conservative because a READ carries no such proof.
		if _, err := r.db.ExecContext(ctx,
			`DELETE FROM ap_tombstones WHERE ap_id = $1`, apID); err != nil {
			return fmt.Errorf("remove tombstones %q: %w", apID, err)
		}
		return nil
	}
	// A community's undo clears that community's OWN row and nothing else —
	// not the global marker, which is ORIGIN-authorized and outranks it. The
	// earlier `announcer IN ('', $2)` form inverted the privilege: any followed
	// community could destroy an origin-authorized marker (and the
	// compensation re-Record would then permanently downgrade it to that
	// community's scope), so one community's undo could un-suppress content the
	// object's own instance said was gone.
	if _, err := r.db.ExecContext(ctx,
		`DELETE FROM ap_tombstones WHERE ap_id = $1 AND announcer = $2`,
		apID, communityIRI); err != nil {
		return fmt.Errorf("remove tombstone %q (community %q): %w", apID, communityIRI, err)
	}
	return nil
}

// tombstonePruneBatchSize bounds one DELETE inside Prune (same reasoning as
// the firehose pruner: short statements never stall the write path).
const tombstonePruneBatchSize = 1000

func (r *postgresTombstones) Prune(ctx context.Context, cutoff time.Time) (int64, error) {
	var total int64
	for {
		// Keyed on the whole (ap_id, announcer) pair: pruning by ap_id alone
		// would take a sibling community's still-fresh marker down with an
		// aged one.
		res, err := r.db.ExecContext(ctx, `
			DELETE FROM ap_tombstones WHERE (ap_id, announcer) IN (
				SELECT ap_id, announcer FROM ap_tombstones WHERE deleted_at < $1 LIMIT $2
			)`, cutoff, tombstonePruneBatchSize)
		if err != nil {
			return total, fmt.Errorf("prune tombstones before %s: %w", cutoff.Format(time.RFC3339), err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return total, fmt.Errorf("prune tombstones: rows affected: %w", err)
		}
		total += n
		if n < tombstonePruneBatchSize {
			return total, nil
		}
	}
}
