package labels

import (
	"context"
	"fmt"
)

// ApplyLapsed applies every standing label whose exp is at or before the
// database clock as a negation through every applier, once, and returns how
// many it applied. Each lapse commits in its own transaction, which marks the
// label applied. Only a (uri, val)'s latest label lapses: a newer label (a
// re-ban) replaces the expiry by being latest, so an older label's exp never
// clears it.
func (s *PostgresStore) ApplyLapsed(ctx context.Context) (int, error) {
	lapsed, err := s.lapsedPositions(ctx)
	if err != nil {
		return 0, err
	}
	applied := 0
	for _, at := range lapsed {
		done, err := s.applyLapse(ctx, at)
		if err != nil {
			return applied, err
		}
		if done {
			applied++
		}
	}
	return applied, nil
}

// lapsedPositions lists the positions of current labels past their exp whose
// lapse is not yet applied, in stream order.
func (s *PostgresStore) lapsedPositions(ctx context.Context) ([]Cursor, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT reset_epoch, seq FROM (
			SELECT DISTINCT ON (uri, val) reset_epoch, seq, neg, exp, lapse_applied_at
			FROM coves_labels
			WHERE uri IN (SELECT uri FROM coves_labels
			              WHERE exp IS NOT NULL AND NOT neg AND lapse_applied_at IS NULL)
			ORDER BY uri, val, reset_epoch DESC, seq DESC
		) current
		WHERE NOT neg AND exp IS NOT NULL AND lapse_applied_at IS NULL
		  AND exp::timestamptz <= now()
		ORDER BY reset_epoch, seq`)
	if err != nil {
		return nil, fmt.Errorf("labels: lapsed labels: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var positions []Cursor
	for rows.Next() {
		var at Cursor
		if err := rows.Scan(&at.Epoch, &at.Seq); err != nil {
			return nil, fmt.Errorf("labels: lapsed labels: scan: %w", err)
		}
		positions = append(positions, at)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("labels: lapsed labels: %w", err)
	}
	return positions, nil
}

// applyLapse applies the lapse of the label at at, in one transaction under
// the cursor lock (so no commit of the same subject interleaves), after
// rechecking that it is still its (uri, val)'s latest label, past its exp and
// unapplied. It reports whether it applied the lapse.
func (s *PostgresStore) applyLapse(ctx context.Context, at Cursor) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("labels: lapse (%d,%d): begin: %w", at.Epoch, at.Seq, err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := lockCursor(ctx, tx); err != nil {
		return false, err
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT `+storedColumns+` FROM coves_labels l
		WHERE reset_epoch = $1 AND seq = $2
		  AND NOT neg AND exp IS NOT NULL AND lapse_applied_at IS NULL
		  AND exp::timestamptz <= now()
		  AND NOT EXISTS (
		      SELECT 1 FROM coves_labels newer
		      WHERE newer.uri = l.uri AND newer.val = l.val
		        AND (newer.reset_epoch, newer.seq) > (l.reset_epoch, l.seq))
		FOR UPDATE`, at.Epoch, at.Seq)
	if err != nil {
		return false, fmt.Errorf("labels: lapse (%d,%d): read: %w", at.Epoch, at.Seq, err)
	}
	var l Stored
	found := false
	for rows.Next() {
		if err := rows.Scan(&l.Epoch, &l.Seq, &l.Src, &l.URI, &l.CID, &l.Val, &l.Neg,
			&l.Cts, &l.Exp, &l.Sig, &l.ReceivedAt); err != nil {
			_ = rows.Close()
			return false, fmt.Errorf("labels: lapse (%d,%d): scan: %w", at.Epoch, at.Seq, err)
		}
		found = true
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return false, fmt.Errorf("labels: lapse (%d,%d): read: %w", at.Epoch, at.Seq, err)
	}
	if err := rows.Close(); err != nil {
		return false, fmt.Errorf("labels: lapse (%d,%d): read: %w", at.Epoch, at.Seq, err)
	}
	if !found {
		return false, nil
	}
	l.ReceivedAt = l.ReceivedAt.UTC()
	// Applied as a negation: StandingTx already reads the label as not
	// standing, so each applier lifts what the label put in force.
	l.Neg = true
	applyCtx, hooks := withAfterCommit(ctx)
	for _, applier := range s.appliers {
		if err := applier.ApplyLabel(applyCtx, tx, l); err != nil {
			return false, fmt.Errorf("labels: lapse (%d,%d): apply: %w", at.Epoch, at.Seq, err)
		}
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE coves_labels SET lapse_applied_at = now()
		WHERE reset_epoch = $1 AND seq = $2`, at.Epoch, at.Seq); err != nil {
		return false, fmt.Errorf("labels: lapse (%d,%d): mark applied: %w", at.Epoch, at.Seq, err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("labels: lapse (%d,%d): commit: %w", at.Epoch, at.Seq, err)
	}
	hooks.run(ctx)
	return true, nil
}
