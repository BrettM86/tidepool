package store

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"time"

	"tidepool/internal/errors"
)

// DeferredItem is one backfill item skipped because its origin host was
// unreachable, or one failing inbox event's content, kept for a later retry.
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

// MaxDueItemsPerHost is the most due items ListDueItems returns for one
// host, so one sweep attempts at most this many items per host: a recovered
// host is not burst, and one large host does not fill the batch.
const MaxDueItemsPerHost = 20

// Sources of a deferred row (the source column): a backfill walk's skip or a
// failing inbox event's deferral.
const (
	deferredSourceBackfill = "backfill"
	deferredSourceInbox    = "inbox"
)

// DeferredBackfillItems stores backfill items deferred for retry.
type DeferredBackfillItems interface {
	// DeferItem records a backfill walk's skipped item. A second deferral of
	// the same object in the same community changes nothing.
	DeferItem(ctx context.Context, item DeferredItem) error
	// DeferInboxItem records a failing inbox event's content. It refreshes an
	// existing row for the same object and community: created_at and the
	// source become this deferral's, attempts restart at 0, and the next
	// retry is the earlier of the row's and item's.
	DeferInboxItem(ctx context.Context, item DeferredItem) error
	// ListDueItems returns at most limit items due at now, the earliest due
	// first, and at most MaxDueItemsPerHost of them per host.
	ListDueItems(ctx context.Context, now time.Time, limit int) ([]DeferredItem, error)
	// The sweep mutations below take the item as ListDueItems read it and
	// touch its row only while the row's created_at is still item's: an
	// inbox deferral that refreshed the row meanwhile (a new created_at)
	// makes the mutation a no-op, not an error, so the refresh's schedule
	// stands.

	// RescheduleItem sets item's attempts and next_retry_at.
	RescheduleItem(ctx context.Context, item DeferredItem, attempts int, nextRetryAt time.Time) error
	// DeleteItem removes item's row.
	DeleteItem(ctx context.Context, item DeferredItem) error
	// DeleteLandedItem removes the row of an item whose content landed as
	// contentIRI. When the row came from the inbox, contentIRI's mapping is
	// marked community-announced in the same transaction (a missing mapping
	// is an error satisfying errors.IsNotFound, and the row stays).
	DeleteLandedItem(ctx context.Context, item DeferredItem, contentIRI string) error
	// MarkInboxContentAnnounced marks contentIRI's mapping
	// community-announced when item's row came from the inbox, keeping the
	// row: content that landed while the row stays for its replies. A
	// missing mapping is an error satisfying errors.IsNotFound. A row not
	// from the inbox, or gone, changes nothing.
	MarkInboxContentAnnounced(ctx context.Context, item DeferredItem, contentIRI string) error
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
			(object_iri, community_ap_id, community_did, host, object_type, attempts, next_retry_at, created_at, source)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (object_iri, community_ap_id) DO NOTHING`,
		item.ObjectIRI, item.CommunityAPID, item.CommunityDID, item.Host, item.ObjectType,
		item.Attempts, item.NextRetryAt, item.CreatedAt, deferredSourceBackfill)
	if err != nil {
		return fmt.Errorf("defer backfill item %q in %q: %w", item.ObjectIRI, item.CommunityAPID, err)
	}
	return nil
}

// DeferInboxItem records item as an inbox deferral, refreshing an existing
// row for the same object and community so the new deferral gets its full
// retry schedule: an old row would otherwise keep its created_at and may
// have no retries left.
func (r *postgresDeferredBackfillItems) DeferInboxItem(ctx context.Context, item DeferredItem) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO backfill_deferred_items
			(object_iri, community_ap_id, community_did, host, object_type, attempts, next_retry_at, created_at, source)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (object_iri, community_ap_id) DO UPDATE SET
			created_at = EXCLUDED.created_at,
			attempts = 0,
			next_retry_at = LEAST(backfill_deferred_items.next_retry_at, EXCLUDED.next_retry_at),
			source = EXCLUDED.source`,
		item.ObjectIRI, item.CommunityAPID, item.CommunityDID, item.Host, item.ObjectType,
		item.Attempts, item.NextRetryAt, item.CreatedAt, deferredSourceInbox)
	if err != nil {
		return fmt.Errorf("defer inbox item %q in %q: %w", item.ObjectIRI, item.CommunityAPID, err)
	}
	return nil
}

// ListDueItems returns at most limit items whose next_retry_at is at or
// before now, the earliest due first, and at most MaxDueItemsPerHost of them
// per host (each host's earliest due).
func (r *postgresDeferredBackfillItems) ListDueItems(ctx context.Context, now time.Time, limit int) ([]DeferredItem, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT object_iri, community_ap_id, community_did, host, object_type,
			attempts, next_retry_at, created_at
		FROM (
			SELECT *, row_number() OVER (
				PARTITION BY host ORDER BY next_retry_at, object_iri, community_ap_id) AS host_rank
			FROM backfill_deferred_items
			WHERE next_retry_at <= $1
		) due
		WHERE host_rank <= $3
		ORDER BY next_retry_at, object_iri, community_ap_id
		LIMIT $2`, now, limit, MaxDueItemsPerHost)
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

// RescheduleItem sets item's attempts and next_retry_at while its row still
// has item's created_at.
func (r *postgresDeferredBackfillItems) RescheduleItem(ctx context.Context, item DeferredItem, attempts int, nextRetryAt time.Time) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE backfill_deferred_items SET attempts = $3, next_retry_at = $4
		WHERE object_iri = $1 AND community_ap_id = $2 AND created_at = $5`,
		item.ObjectIRI, item.CommunityAPID, attempts, nextRetryAt, item.CreatedAt)
	if err != nil {
		return fmt.Errorf("reschedule deferred backfill item %q in %q: %w", item.ObjectIRI, item.CommunityAPID, err)
	}
	return nil
}

// DeleteItem removes item's row while it still has item's created_at.
func (r *postgresDeferredBackfillItems) DeleteItem(ctx context.Context, item DeferredItem) error {
	_, err := r.db.ExecContext(ctx, `
		DELETE FROM backfill_deferred_items
		WHERE object_iri = $1 AND community_ap_id = $2 AND created_at = $3`,
		item.ObjectIRI, item.CommunityAPID, item.CreatedAt)
	if err != nil {
		return fmt.Errorf("delete deferred backfill item %q in %q: %w", item.ObjectIRI, item.CommunityAPID, err)
	}
	return nil
}

// DeleteLandedItem removes one landed item's row while it still has item's
// created_at, marking contentIRI community-announced when the row came from
// the inbox: the community's Announce carried it, as
// Handler.materializeContent marks inbox content.
func (r *postgresDeferredBackfillItems) DeleteLandedItem(ctx context.Context, item DeferredItem, contentIRI string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("delete landed deferred item %q in %q: begin: %w", item.ObjectIRI, item.CommunityAPID, err)
	}
	defer func() { _ = tx.Rollback() }()
	var source string
	err = tx.QueryRowContext(ctx, `
		DELETE FROM backfill_deferred_items
		WHERE object_iri = $1 AND community_ap_id = $2 AND created_at = $3
		RETURNING source`,
		item.ObjectIRI, item.CommunityAPID, item.CreatedAt).Scan(&source)
	switch {
	case stderrors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return fmt.Errorf("delete landed deferred item %q in %q: %w", item.ObjectIRI, item.CommunityAPID, err)
	}
	if source == deferredSourceInbox {
		result, err := tx.ExecContext(ctx,
			`UPDATE ap_objects SET arrival = 'community_announced' WHERE ap_id = $1`, contentIRI)
		if err != nil {
			return fmt.Errorf("mark landed inbox item %q community announced: %w", contentIRI, err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("mark landed inbox item %q community announced: rows affected: %w", contentIRI, err)
		}
		if affected == 0 {
			return errors.NewNotFoundError("ap_object", contentIRI)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("delete landed deferred item %q in %q: commit: %w", item.ObjectIRI, item.CommunityAPID, err)
	}
	return nil
}

// MarkInboxContentAnnounced marks contentIRI community-announced when item's
// row came from the inbox, leaving the row in place.
func (r *postgresDeferredBackfillItems) MarkInboxContentAnnounced(ctx context.Context, item DeferredItem, contentIRI string) error {
	var source string
	err := r.db.QueryRowContext(ctx, `
		SELECT source FROM backfill_deferred_items WHERE object_iri = $1 AND community_ap_id = $2`,
		item.ObjectIRI, item.CommunityAPID).Scan(&source)
	switch {
	case stderrors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return fmt.Errorf("read source of deferred item %q in %q: %w", item.ObjectIRI, item.CommunityAPID, err)
	}
	if source != deferredSourceInbox {
		return nil
	}
	result, err := r.db.ExecContext(ctx,
		`UPDATE ap_objects SET arrival = 'community_announced' WHERE ap_id = $1`, contentIRI)
	if err != nil {
		return fmt.Errorf("mark landed inbox item %q community announced: %w", contentIRI, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("mark landed inbox item %q community announced: rows affected: %w", contentIRI, err)
	}
	if affected == 0 {
		return errors.NewNotFoundError("ap_object", contentIRI)
	}
	return nil
}
