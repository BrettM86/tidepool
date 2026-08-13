package store

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"

	"tidepool/internal/errors"
)

type postgresOutboundObjects struct {
	db *sql.DB
}

// NewOutboundObjects creates the postgres-backed outbound_objects repository.
func NewOutboundObjects(db *sql.DB) OutboundObjects {
	return &postgresOutboundObjects{db: db}
}

const outboundObjectColumns = `
	at_uri, ap_object_id, last_cid, last_rev,
	community_did, community_ap_id, translated_snapshot,
	last_activity_seq, depth, created_at, updated_at, tombstoned_at, accepted_at`

// execer is the subset of *sql.DB and *sql.Tx these repositories need, so one
// statement runs either standalone or inside a caller's transaction.
type execer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func (r *postgresOutboundObjects) Upsert(ctx context.Context, object OutboundObject) (*OutboundObject, error) {
	return r.upsert(ctx, r.db, object)
}

func (r *postgresOutboundObjects) UpsertTx(ctx context.Context, tx *sql.Tx, object OutboundObject) (*OutboundObject, error) {
	if tx == nil {
		return nil, errors.NewValidationError("tx", "must not be nil")
	}
	return r.upsert(ctx, tx, object)
}

func (r *postgresOutboundObjects) upsert(ctx context.Context, q execer, object OutboundObject) (*OutboundObject, error) {
	// created_at is absent from the UPDATE list and last_activity_seq is
	// computed from the STORED value rather than the argument: the seq is the
	// bridge's own counter for how many activities this object has produced,
	// and letting a caller supply it would let a replayed handler reuse — or
	// skip — an activity id a peer has already seen.
	//
	// tombstoned_at is likewise untouched here. A tombstoned row that receives
	// a later write keeps its tombstone: clearing it is an explicit decision,
	// never a side effect of an upsert. (Task 17's echo-moderation restore is a
	// fresh acceptance, not an un-tombstone of this row.)
	query := `
		INSERT INTO outbound_objects (
			at_uri, ap_object_id, last_cid, last_rev,
			community_did, community_ap_id, translated_snapshot, depth
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (at_uri) DO UPDATE SET
			ap_object_id = EXCLUDED.ap_object_id,
			last_cid = EXCLUDED.last_cid,
			last_rev = EXCLUDED.last_rev,
			community_did = EXCLUDED.community_did,
			community_ap_id = EXCLUDED.community_ap_id,
			translated_snapshot = EXCLUDED.translated_snapshot,
			depth = EXCLUDED.depth,
			last_activity_seq = outbound_objects.last_activity_seq + 1,
			updated_at = now()
		RETURNING` + outboundObjectColumns

	row := q.QueryRowContext(ctx, query,
		object.ATURI, object.APObjectID, object.LastCID, object.LastRev,
		object.CommunityDID, object.CommunityAPID, object.TranslatedSnapshot, object.Depth,
	)
	stored, err := scanOutboundObject(row)
	if err != nil {
		return nil, fmt.Errorf("upsert outbound_object %q: %w", object.ATURI, err)
	}
	return stored, nil
}

func (r *postgresOutboundObjects) GetByATURI(ctx context.Context, atURI string) (*OutboundObject, error) {
	query := `SELECT` + outboundObjectColumns + ` FROM outbound_objects WHERE at_uri = $1`
	object, err := scanOutboundObject(r.db.QueryRowContext(ctx, query, atURI))
	if err != nil {
		if stderrors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewNotFoundError("outbound_object", atURI)
		}
		return nil, fmt.Errorf("get outbound_object %q: %w", atURI, err)
	}
	return object, nil
}

func (r *postgresOutboundObjects) Tombstone(ctx context.Context, atURI string) (*OutboundObject, error) {
	return r.tombstone(ctx, r.db, atURI)
}

func (r *postgresOutboundObjects) TombstoneTx(ctx context.Context, tx *sql.Tx, atURI string) (*OutboundObject, error) {
	if tx == nil {
		return nil, errors.NewValidationError("tx", "must not be nil")
	}
	return r.tombstone(ctx, tx, atURI)
}

func (r *postgresOutboundObjects) tombstone(ctx context.Context, q execer, atURI string) (*OutboundObject, error) {
	// One statement stamps the tombstone AND returns the state the Delete is
	// built from, so no read/write window exists for a concurrent handler to
	// interleave in.
	//
	// Every SET is guarded on tombstoned_at IS NULL, which is what makes a
	// redelivered delete a no-op: the seq must NOT bump a second time, because
	// the Delete that already went out was addressed with the FIRST seq's
	// activity id and the peer must see that same id again.
	query := `
		UPDATE outbound_objects SET
			tombstoned_at = COALESCE(tombstoned_at, now()),
			last_activity_seq = CASE WHEN tombstoned_at IS NULL
				THEN last_activity_seq + 1 ELSE last_activity_seq END,
			updated_at = CASE WHEN tombstoned_at IS NULL THEN now() ELSE updated_at END
		WHERE at_uri = $1
		RETURNING` + outboundObjectColumns

	object, err := scanOutboundObject(q.QueryRowContext(ctx, query, atURI))
	if err != nil {
		if stderrors.Is(err, sql.ErrNoRows) {
			// A delete for a record we never federated. The caller needs to
			// tell that apart from a real tombstone: there is no Delete to
			// send, and inventing one would address an AP id no peer knows.
			return nil, errors.NewNotFoundError("outbound_object", atURI)
		}
		return nil, fmt.Errorf("tombstone outbound_object %q: %w", atURI, err)
	}
	return object, nil
}

// SetAccepted stamps accepted_at, the causal-gating marker (task 15).
//
// COALESCE preserves the original time on a redelivery: accepted_at is the
// causal boundary a bridge-origin child gates on, and a re-accept must not move
// it. A missing object is NotFound, not a no-op — accepting an object we hold no
// state for is a bug, since there is nothing whose children we could unblock.
func (r *postgresOutboundObjects) SetAccepted(ctx context.Context, atURI string) error {
	query := `
		UPDATE outbound_objects
		SET accepted_at = COALESCE(accepted_at, now())
		WHERE at_uri = $1`
	result, err := r.db.ExecContext(ctx, query, atURI)
	if err != nil {
		return fmt.Errorf("set accepted_at for outbound_object %q: %w", atURI, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("set accepted_at for outbound_object %q: rows affected: %w", atURI, err)
	}
	if affected == 0 {
		return errors.NewNotFoundError("outbound_object", atURI)
	}
	return nil
}

func scanOutboundObject(row rowScanner) (*OutboundObject, error) {
	var object OutboundObject
	var tombstonedAt, acceptedAt sql.NullTime
	err := row.Scan(
		&object.ATURI, &object.APObjectID, &object.LastCID, &object.LastRev,
		&object.CommunityDID, &object.CommunityAPID, &object.TranslatedSnapshot,
		&object.LastActivitySeq, &object.Depth,
		&object.CreatedAt, &object.UpdatedAt, &tombstonedAt, &acceptedAt,
	)
	if err != nil {
		return nil, err
	}
	if tombstonedAt.Valid {
		object.TombstonedAt = &tombstonedAt.Time
	}
	if acceptedAt.Valid {
		object.AcceptedAt = &acceptedAt.Time
	}
	return &object, nil
}
