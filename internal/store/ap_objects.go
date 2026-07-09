package store

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/lib/pq"

	"tidepool/internal/errors"
)

type postgresAPObjects struct {
	db *sql.DB
}

// NewAPObjects creates the postgres-backed ap_objects repository.
func NewAPObjects(db *sql.DB) APObjects {
	return &postgresAPObjects{db: db}
}

const apObjectColumns = `
	id, ap_id, ap_type, origin_instance, origin, did, author_did, collection,
	rkey, at_uri, cid, ap_published_at, indexed_at, deleted_at`

func (r *postgresAPObjects) PutMapping(ctx context.Context, mapping APObjectMapping) (*APObjectMapping, error) {
	if err := validateMapping(&mapping); err != nil {
		return nil, err
	}

	query := `
		INSERT INTO ap_objects (
			ap_id, ap_type, origin_instance, origin, did, author_did,
			collection, rkey, at_uri, cid, ap_published_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (ap_id) DO UPDATE SET
			ap_type = EXCLUDED.ap_type,
			origin = EXCLUDED.origin,
			did = EXCLUDED.did,
			author_did = EXCLUDED.author_did,
			collection = EXCLUDED.collection,
			rkey = EXCLUDED.rkey,
			at_uri = EXCLUDED.at_uri,
			cid = EXCLUDED.cid,
			ap_published_at = EXCLUDED.ap_published_at,
			indexed_at = CURRENT_TIMESTAMP,
			deleted_at = NULL
		RETURNING` + apObjectColumns

	row := r.db.QueryRowContext(ctx, query,
		mapping.APID, mapping.APType, mapping.OriginInstance, string(mapping.Origin),
		mapping.DID, nullIfEmpty(mapping.AuthorDID), mapping.Collection, mapping.RKey,
		mapping.ATURI, mapping.CID, mapping.PublishedAt,
	)
	stored, err := scanAPObject(row)
	if err != nil {
		// ap_id conflicts are handled by the upsert, so the only expected
		// unique violation is the at_uri constraint: a different AP object
		// already claimed this at-uri. Deterministic rkeys make this a
		// caller bug.
		if constraint, ok := uniqueViolation(err); ok && constraint == "ap_objects_at_uri_key" {
			return nil, errors.NewConflictError("ap_object", "at_uri", mapping.ATURI)
		}
		return nil, fmt.Errorf("put ap_object mapping for %q: %w", mapping.APID, err)
	}
	return stored, nil
}

func (r *postgresAPObjects) GetByAPID(ctx context.Context, apID string) (*APObjectMapping, error) {
	query := `SELECT` + apObjectColumns + ` FROM ap_objects WHERE ap_id = $1`
	mapping, err := scanAPObject(r.db.QueryRowContext(ctx, query, apID))
	if err != nil {
		if stderrors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewNotFoundError("ap_object", apID)
		}
		return nil, fmt.Errorf("get ap_object by ap_id %q: %w", apID, err)
	}
	return mapping, nil
}

func (r *postgresAPObjects) GetByATURI(ctx context.Context, atURI string) (*APObjectMapping, error) {
	query := `SELECT` + apObjectColumns + ` FROM ap_objects WHERE at_uri = $1`
	mapping, err := scanAPObject(r.db.QueryRowContext(ctx, query, atURI))
	if err != nil {
		if stderrors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewNotFoundError("ap_object", atURI)
		}
		return nil, fmt.Errorf("get ap_object by at_uri %q: %w", atURI, err)
	}
	return mapping, nil
}

func (r *postgresAPObjects) ResolveStrongRef(ctx context.Context, apID string) (string, string, error) {
	query := `SELECT at_uri, cid, deleted_at FROM ap_objects WHERE ap_id = $1`

	var atURI, cid string
	var deletedAt sql.NullTime
	err := r.db.QueryRowContext(ctx, query, apID).Scan(&atURI, &cid, &deletedAt)
	if err != nil {
		if stderrors.Is(err, sql.ErrNoRows) {
			return "", "", errors.NewNotFoundError("ap_object", apID)
		}
		return "", "", fmt.Errorf("resolve strongRef for %q: %w", apID, err)
	}
	if deletedAt.Valid {
		return "", "", errors.NewTombstonedError("ap_object", apID)
	}
	return atURI, cid, nil
}

func (r *postgresAPObjects) ListByActorDID(ctx context.Context, did string) ([]*APObjectMapping, error) {
	if did == "" {
		return nil, errors.NewValidationError("did", "must not be empty")
	}
	query := `SELECT` + apObjectColumns + `
		FROM ap_objects
		WHERE (did = $1 OR author_did = $1) AND deleted_at IS NULL
		ORDER BY id`
	rows, err := r.db.QueryContext(ctx, query, did)
	if err != nil {
		return nil, fmt.Errorf("list ap_objects for actor %q: %w", did, err)
	}
	defer func() { _ = rows.Close() }()

	var mappings []*APObjectMapping
	for rows.Next() {
		mapping, err := scanAPObject(rows)
		if err != nil {
			return nil, fmt.Errorf("scan ap_object for actor %q: %w", did, err)
		}
		mappings = append(mappings, mapping)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list ap_objects for actor %q: %w", did, err)
	}
	return mappings, nil
}

func (r *postgresAPObjects) SoftDelete(ctx context.Context, apID string) error {
	// COALESCE keeps the original tombstone time on re-delete, making the
	// whole operation a single atomic statement: affected == 0 can only
	// mean the row does not exist.
	query := `
		UPDATE ap_objects
		SET deleted_at = COALESCE(deleted_at, CURRENT_TIMESTAMP)
		WHERE ap_id = $1`

	result, err := r.db.ExecContext(ctx, query, apID)
	if err != nil {
		return fmt.Errorf("soft delete ap_object %q: %w", apID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("soft delete ap_object %q: rows affected: %w", apID, err)
	}
	if affected == 0 {
		return errors.NewNotFoundError("ap_object", apID)
	}
	return nil
}

func (r *postgresAPObjects) Restore(ctx context.Context, apID string) error {
	// Clearing an already-clear deleted_at is harmless, so affected == 0
	// can only mean the row does not exist.
	query := `
		UPDATE ap_objects
		SET deleted_at = NULL
		WHERE ap_id = $1`

	result, err := r.db.ExecContext(ctx, query, apID)
	if err != nil {
		return fmt.Errorf("restore ap_object %q: %w", apID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("restore ap_object %q: rows affected: %w", apID, err)
	}
	if affected == 0 {
		return errors.NewNotFoundError("ap_object", apID)
	}
	return nil
}

// validateMapping checks the atproto identifiers with indigo's syntax
// package, defaults Origin to fediverse, and derives ATURI from
// (DID, Collection, RKey).
func validateMapping(mapping *APObjectMapping) error {
	if mapping.APID == "" {
		return errors.NewValidationError("ap_id", "must not be empty")
	}
	if mapping.APType == "" {
		return errors.NewValidationError("ap_type", "must not be empty")
	}
	if mapping.OriginInstance == "" {
		return errors.NewValidationError("origin_instance", "must not be empty")
	}
	if mapping.Origin == "" {
		mapping.Origin = OriginFediverse
	}
	if !mapping.Origin.Valid() {
		return errors.NewValidationError("origin",
			fmt.Sprintf("must be %q or %q, got %q", OriginFediverse, OriginBridge, mapping.Origin))
	}
	if _, err := syntax.ParseCID(mapping.CID); err != nil {
		return errors.NewValidationError("cid", err.Error())
	}
	did, err := syntax.ParseDID(mapping.DID)
	if err != nil {
		return errors.NewValidationError("did", err.Error())
	}
	if mapping.AuthorDID != "" {
		if _, err := syntax.ParseDID(mapping.AuthorDID); err != nil {
			return errors.NewValidationError("author_did", err.Error())
		}
	}
	collection, err := syntax.ParseNSID(mapping.Collection)
	if err != nil {
		return errors.NewValidationError("collection", err.Error())
	}
	rkey, err := syntax.ParseRecordKey(mapping.RKey)
	if err != nil {
		return errors.NewValidationError("rkey", err.Error())
	}
	mapping.ATURI = fmt.Sprintf("at://%s/%s/%s", did, collection, rkey)
	return nil
}

// rowScanner covers *sql.Row and *sql.Rows.
type rowScanner interface {
	Scan(destinations ...any) error
}

func scanAPObject(row rowScanner) (*APObjectMapping, error) {
	var mapping APObjectMapping
	var origin string
	var authorDID sql.NullString
	err := row.Scan(
		&mapping.ID, &mapping.APID, &mapping.APType, &mapping.OriginInstance,
		&origin, &mapping.DID, &authorDID, &mapping.Collection, &mapping.RKey,
		&mapping.ATURI, &mapping.CID,
		&mapping.PublishedAt, &mapping.IndexedAt, &mapping.DeletedAt,
	)
	if err != nil {
		return nil, err
	}
	mapping.Origin = Origin(origin)
	mapping.AuthorDID = authorDID.String
	return &mapping, nil
}

// nullIfEmpty maps "" to SQL NULL for optional text columns.
func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// uniqueViolation reports whether err is a postgres unique constraint
// violation (SQLSTATE 23505) and, if so, which named constraint fired.
// Callers map known constraint names to precise conflict errors and let
// unknown ones fall through as wrapped internal errors.
func uniqueViolation(err error) (constraint string, ok bool) {
	var pqError *pq.Error
	if stderrors.As(err, &pqError) && pqError.Code == "23505" {
		return pqError.Constraint, true
	}
	return "", false
}
