package labels

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"time"

	"github.com/bluesky-social/indigo/atproto/labeling"

	"tidepool/internal/store"
)

// Stored is one committed label at its stream position.
type Stored struct {
	Epoch, Seq int64
	Src, URI   string
	CID        *string
	Val        string
	Neg        bool
	Cts        string
	Exp        *string
	Sig        []byte
	ReceivedAt time.Time
}

// Applier applies a committed label inside the label's transaction. The
// label's row is already written in tx, so StandingTx reads the subject's
// state with this label applied.
type Applier interface {
	ApplyLabel(ctx context.Context, tx *sql.Tx, l Stored) error
}

// PostgresStore is the Postgres Store: labels, cursor and appliers commit
// in one transaction.
type PostgresStore struct {
	db       *sql.DB
	appliers []Applier
}

// NewPostgresStore builds a PostgresStore that runs appliers on every commit.
func NewPostgresStore(db *sql.DB, appliers ...Applier) *PostgresStore {
	return &PostgresStore{db: db, appliers: appliers}
}

// Cursor returns the stored cursor; with none stored it is (0, 0).
func (s *PostgresStore) Cursor(ctx context.Context) (Cursor, error) {
	var at Cursor
	err := s.db.QueryRowContext(ctx,
		`SELECT reset_epoch, seq FROM coves_label_cursor WHERE singleton`).Scan(&at.Epoch, &at.Seq)
	if stderrors.Is(err, sql.ErrNoRows) {
		return Cursor{}, nil
	}
	if err != nil {
		return Cursor{}, fmt.Errorf("labels: read cursor: %w", err)
	}
	return at, nil
}

// lockCursor reads the cursor in tx and holds its row until tx ends, so a
// commit and a reset cannot interleave.
func lockCursor(ctx context.Context, tx *sql.Tx) (Cursor, error) {
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO coves_label_cursor (singleton, reset_epoch, seq) VALUES (true, 0, 0)
		ON CONFLICT (singleton) DO NOTHING`); err != nil {
		return Cursor{}, fmt.Errorf("labels: create cursor: %w", err)
	}
	var at Cursor
	if err := tx.QueryRowContext(ctx,
		`SELECT reset_epoch, seq FROM coves_label_cursor WHERE singleton FOR UPDATE`).Scan(&at.Epoch, &at.Seq); err != nil {
		return Cursor{}, fmt.Errorf("labels: lock cursor: %w", err)
	}
	return at, nil
}

func setCursor(ctx context.Context, tx *sql.Tx, at Cursor) error {
	if _, err := tx.ExecContext(ctx, `
		UPDATE coves_label_cursor SET reset_epoch = $1, seq = $2, updated_at = now()
		WHERE singleton`, at.Epoch, at.Seq); err != nil {
		return fmt.Errorf("labels: advance cursor to (%d,%d): %w", at.Epoch, at.Seq, err)
	}
	return nil
}

// ErrUnusableLabel marks a Commit that skipped its label: the label was not
// stored or applied, and the cursor was advanced past it, so the stream does
// not retry it.
var ErrUnusableLabel = stderrors.New("unusable label")

// maxExpOffset bounds an exp's UTC offset: Postgres refuses to cast a
// timestamptz whose displacement is 16 hours or more.
const maxExpOffset = 16 * time.Hour

// checkExp reports why exp, already valid atproto datetime syntax, cannot be
// read as a Postgres timestamptz (coves_labels.exp is TEXT and every standing
// and lapse query casts it): its written year is 0000, or its offset is 16
// hours or more.
func checkExp(exp string) error {
	parsed, err := time.Parse(time.RFC3339Nano, exp)
	if err != nil {
		return fmt.Errorf("exp %q: %w", exp, err)
	}
	if parsed.Year() < 1 {
		return fmt.Errorf("exp %q: year before 0001", exp)
	}
	if _, offset := parsed.Zone(); time.Duration(offset)*time.Second >= maxExpOffset ||
		time.Duration(-offset)*time.Second >= maxExpOffset {
		return fmt.Errorf("exp %q: offset of 16 hours or more", exp)
	}
	return nil
}

// Commit stores l, runs the appliers and advances the cursor to at, all in
// one transaction: a failing applier stores nothing and leaves the cursor.
// The appliers' AfterCommit hooks run once the transaction has committed.
// at must be past the stored cursor within its epoch. A label whose exp
// Postgres cannot cast is not stored: Commit advances the cursor past it and
// returns an error wrapping ErrUnusableLabel.
func (s *PostgresStore) Commit(ctx context.Context, at Cursor, l labeling.Label) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("labels: commit (%d,%d): begin: %w", at.Epoch, at.Seq, err)
	}
	defer func() { _ = tx.Rollback() }()
	current, err := lockCursor(ctx, tx)
	if err != nil {
		return err
	}
	if at.Epoch != current.Epoch || at.Seq <= current.Seq {
		return fmt.Errorf("labels: commit (%d,%d) is not after the stored cursor (%d,%d)",
			at.Epoch, at.Seq, current.Epoch, current.Seq)
	}
	if l.ExpiresAt != nil {
		if expErr := checkExp(*l.ExpiresAt); expErr != nil {
			if err := setCursor(ctx, tx, at); err != nil {
				return err
			}
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("labels: skip unusable label (%d,%d): commit: %w", at.Epoch, at.Seq, err)
			}
			return fmt.Errorf("labels: label (%d,%d) skipped: %w: %w", at.Epoch, at.Seq, ErrUnusableLabel, expErr)
		}
	}
	stored := Stored{
		Epoch: at.Epoch,
		Seq:   at.Seq,
		Src:   l.SourceDID,
		URI:   l.URI,
		CID:   l.CID,
		Val:   l.Val,
		Neg:   l.Negated != nil && *l.Negated,
		Cts:   l.CreatedAt,
		Exp:   l.ExpiresAt,
		Sig:   []byte(l.Sig),
	}
	if err := tx.QueryRowContext(ctx, `
		INSERT INTO coves_labels (reset_epoch, seq, src, uri, cid, val, neg, cts, exp, sig)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		RETURNING received_at`,
		stored.Epoch, stored.Seq, stored.Src, stored.URI, stored.CID, stored.Val,
		stored.Neg, stored.Cts, stored.Exp, stored.Sig).Scan(&stored.ReceivedAt); err != nil {
		return fmt.Errorf("labels: store label (%d,%d): %w", at.Epoch, at.Seq, err)
	}
	stored.ReceivedAt = stored.ReceivedAt.UTC()
	applyCtx, hooks := withAfterCommit(ctx)
	for _, applier := range s.appliers {
		if err := applier.ApplyLabel(applyCtx, tx, stored); err != nil {
			return fmt.Errorf("labels: apply label (%d,%d): %w", at.Epoch, at.Seq, err)
		}
	}
	if err := setCursor(ctx, tx, at); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("labels: commit (%d,%d): %w", at.Epoch, at.Seq, err)
	}
	hooks.run(ctx)
	return nil
}

// Reset durably sets the cursor to (epoch+1, 0) and returns it. Stored
// labels stay: a replayed label supersedes an earlier epoch's state, and a
// subject the replay never names keeps its state.
func (s *PostgresStore) Reset(ctx context.Context) (Cursor, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Cursor{}, fmt.Errorf("labels: reset: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	current, err := lockCursor(ctx, tx)
	if err != nil {
		return Cursor{}, err
	}
	next := Cursor{Epoch: current.Epoch + 1, Seq: 0}
	if err := setCursor(ctx, tx, next); err != nil {
		return Cursor{}, err
	}
	if err := tx.Commit(); err != nil {
		return Cursor{}, fmt.Errorf("labels: reset: commit: %w", err)
	}
	return next, nil
}

// storedColumns is the SELECT list of a Stored row, in its field order.
const storedColumns = `reset_epoch, seq, src, uri, cid, val, neg, cts, exp, sig, received_at`

// standingCondition is the SQL condition that the current label row aliased
// current stands: it is not a negation and its exp, if any, is still ahead of
// the database clock. A label past its exp stands no longer, whether or not
// ApplyLapsed has applied its lapse yet.
var standingCondition = store.StandingLabelCondition("current")

// Standing returns every subject whose current val label stands (not
// negated, not past its exp), in stream order. The current label of a
// subject is its val row with the greatest (reset_epoch, seq).
func (s *PostgresStore) Standing(ctx context.Context, val string) ([]Stored, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+storedColumns+` FROM (
			SELECT DISTINCT ON (uri) `+storedColumns+`
			FROM coves_labels WHERE val = $1
			ORDER BY uri, reset_epoch DESC, seq DESC
		) current
		WHERE `+standingCondition+`
		ORDER BY reset_epoch, seq`, val)
	if err != nil {
		return nil, fmt.Errorf("labels: standing %s labels: %w", val, err)
	}
	defer func() { _ = rows.Close() }()
	standing := []Stored{}
	for rows.Next() {
		var l Stored
		if err := rows.Scan(&l.Epoch, &l.Seq, &l.Src, &l.URI, &l.CID, &l.Val, &l.Neg,
			&l.Cts, &l.Exp, &l.Sig, &l.ReceivedAt); err != nil {
			return nil, fmt.Errorf("labels: standing %s labels: scan: %w", val, err)
		}
		l.ReceivedAt = l.ReceivedAt.UTC()
		standing = append(standing, l)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("labels: standing %s labels: %w", val, err)
	}
	return standing, nil
}

// StandingTx reports, read in tx, whether uri's current val label stands
// (exists, is not negated and is not past its exp).
func StandingTx(ctx context.Context, tx *sql.Tx, uri, val string) (bool, error) {
	var standing bool
	err := tx.QueryRowContext(ctx, `
		SELECT `+standingCondition+` FROM (
			SELECT neg, exp FROM coves_labels WHERE val = $1 AND uri = $2
			ORDER BY reset_epoch DESC, seq DESC LIMIT 1
		) current`, val, uri).Scan(&standing)
	if stderrors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("labels: %s label of %s: %w", val, uri, err)
	}
	return standing, nil
}
