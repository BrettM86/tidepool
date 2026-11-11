package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// DeferredItem is one backfill item skipped because its origin host was dead,
// kept for a later retry.
type DeferredItem struct {
	ObjectIRI     string
	CommunityAPID string
	CommunityDID  string
	Host          string
	ObjectType    string
	Attempts      int
	NextRetryAt   time.Time
	CreatedAt     time.Time
}

// DeferredBackfillItems stores backfill items deferred for retry.
type DeferredBackfillItems interface {
	DeferItem(ctx context.Context, item DeferredItem) error
	ListDueItems(ctx context.Context, now time.Time, limit int) ([]DeferredItem, error)
	RescheduleItem(ctx context.Context, objectIRI, communityAPID string, attempts int, nextRetryAt time.Time) error
	DeleteItem(ctx context.Context, objectIRI, communityAPID string) error
}

type postgresDeferredBackfillItems struct {
	db *sql.DB
}

// NewDeferredBackfillItems returns the Postgres DeferredBackfillItems.
func NewDeferredBackfillItems(db *sql.DB) DeferredBackfillItems {
	return &postgresDeferredBackfillItems{db: db}
}

// DeferItem records item. A second deferral of the same object in the same
// community changes nothing: the first skip's created_at and schedule stand.
func (r *postgresDeferredBackfillItems) DeferItem(ctx context.Context, item DeferredItem) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO backfill_deferred_items
			(object_iri, community_ap_id, community_did, host, object_type, attempts, next_retry_at, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (object_iri, community_ap_id) DO NOTHING`,
		item.ObjectIRI, item.CommunityAPID, item.CommunityDID, item.Host, item.ObjectType,
		item.Attempts, item.NextRetryAt, item.CreatedAt)
	if err != nil {
		return fmt.Errorf("defer backfill item %q in %q: %w", item.ObjectIRI, item.CommunityAPID, err)
	}
	return nil
}

// ListDueItems returns at most limit items whose next_retry_at is at or
// before now, the earliest due first.
func (r *postgresDeferredBackfillItems) ListDueItems(ctx context.Context, now time.Time, limit int) ([]DeferredItem, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT object_iri, community_ap_id, community_did, host, object_type,
			attempts, next_retry_at, created_at
		FROM backfill_deferred_items
		WHERE next_retry_at <= $1
		ORDER BY next_retry_at, object_iri, community_ap_id
		LIMIT $2`, now, limit)
	if err != nil {
		return nil, fmt.Errorf("list due deferred backfill items: %w", err)
	}
	defer rows.Close()
	var items []DeferredItem
	for rows.Next() {
		var item DeferredItem
		if err := rows.Scan(&item.ObjectIRI, &item.CommunityAPID, &item.CommunityDID, &item.Host,
			&item.ObjectType, &item.Attempts, &item.NextRetryAt, &item.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan deferred backfill item: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list due deferred backfill items: %w", err)
	}
	return items, nil
}

// RescheduleItem sets one item's attempts and next_retry_at.
func (r *postgresDeferredBackfillItems) RescheduleItem(ctx context.Context, objectIRI, communityAPID string, attempts int, nextRetryAt time.Time) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE backfill_deferred_items SET attempts = $3, next_retry_at = $4
		WHERE object_iri = $1 AND community_ap_id = $2`,
		objectIRI, communityAPID, attempts, nextRetryAt)
	if err != nil {
		return fmt.Errorf("reschedule deferred backfill item %q in %q: %w", objectIRI, communityAPID, err)
	}
	return nil
}

// DeleteItem removes one item.
func (r *postgresDeferredBackfillItems) DeleteItem(ctx context.Context, objectIRI, communityAPID string) error {
	_, err := r.db.ExecContext(ctx, `
		DELETE FROM backfill_deferred_items WHERE object_iri = $1 AND community_ap_id = $2`,
		objectIRI, communityAPID)
	if err != nil {
		return fmt.Errorf("delete deferred backfill item %q in %q: %w", objectIRI, communityAPID, err)
	}
	return nil
}
