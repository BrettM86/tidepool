package labels

import (
	"context"
	"database/sql"
	"fmt"
)

// Activator is implemented by appliers that act on stored labels at start: an
// applier added after labels were stored (or one whose effects a store without
// it skipped) brings its state up to the standing labels. Activate must be
// idempotent, because every start replays every standing label.
type Activator interface {
	Activate(ctx context.Context, tx *sql.Tx, l Stored) error
}

// Activate replays every standing label, in stream order, through the store's
// appliers that implement Activator, one transaction per label. Appliers that
// do not implement it are not called. A store with no Activator reads nothing.
func (s *PostgresStore) Activate(ctx context.Context) error {
	var activators []Activator
	for _, applier := range s.appliers {
		if activator, ok := applier.(Activator); ok {
			activators = append(activators, activator)
		}
	}
	if len(activators) == 0 {
		return nil
	}
	standing, err := s.standingAll(ctx)
	if err != nil {
		return err
	}
	for _, l := range standing {
		if err := s.activate(ctx, activators, l); err != nil {
			return err
		}
	}
	return nil
}

func (s *PostgresStore) activate(ctx context.Context, activators []Activator, l Stored) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("labels: activate (%d,%d): begin: %w", l.Epoch, l.Seq, err)
	}
	defer func() { _ = tx.Rollback() }()
	applyCtx, hooks := withAfterCommit(ctx)
	for _, activator := range activators {
		if err := activator.Activate(applyCtx, tx, l); err != nil {
			return fmt.Errorf("labels: activate (%d,%d): %w", l.Epoch, l.Seq, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("labels: activate (%d,%d): commit: %w", l.Epoch, l.Seq, err)
	}
	hooks.run(ctx)
	return nil
}

// standingAll is Standing for every value: each (uri, val) whose current label
// stands, in stream order.
func (s *PostgresStore) standingAll(ctx context.Context) ([]Stored, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+storedColumns+` FROM (
			SELECT DISTINCT ON (uri, val) `+storedColumns+`
			FROM coves_labels
			ORDER BY uri, val, reset_epoch DESC, seq DESC
		) current
		WHERE `+standingCondition+`
		ORDER BY reset_epoch, seq`)
	if err != nil {
		return nil, fmt.Errorf("labels: standing labels: %w", err)
	}
	defer func() { _ = rows.Close() }()
	standing := []Stored{}
	for rows.Next() {
		var l Stored
		if err := rows.Scan(&l.Epoch, &l.Seq, &l.Src, &l.URI, &l.CID, &l.Val, &l.Neg,
			&l.Cts, &l.Exp, &l.Sig, &l.ReceivedAt); err != nil {
			return nil, fmt.Errorf("labels: standing labels: scan: %w", err)
		}
		l.ReceivedAt = l.ReceivedAt.UTC()
		standing = append(standing, l)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("labels: standing labels: %w", err)
	}
	return standing, nil
}
