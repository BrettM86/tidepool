package accept

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"

	"tidepool/internal/errors"
)

// Admission statuses (migration 021). accepted/rejected/removed are terminal
// for a given evaluated CID; pending/pending_reacceptance are in-flight.
const (
	StatusPending             = "pending"
	StatusAccepted            = "accepted"
	StatusPendingReacceptance = "pending_reacceptance"
	StatusRejected            = "rejected"
	StatusRemoved             = "removed"
)

// Admission is one row of the engine's decision ledger: the machine-readable
// WHY and the state a post was left in, per (community, post). It is the admin/
// debug surface, NOT the correctness path (Coves reads state from the
// firehose-visible acceptance/removal records).
type Admission struct {
	CommunityDID   string
	PostURI        string
	AuthorDID      string
	Status         string
	DecisionCode   string
	EvaluatedCID   string
	AcceptanceRKey string
	AcceptedCID    string
	Redrivable     bool
}

// Admissions persists the decision ledger.
type Admissions struct{ db *sql.DB }

// NewAdmissions builds the store.
func NewAdmissions(db *sql.DB) *Admissions { return &Admissions{db: db} }

// Record upserts one admission on its own connection — the path a REJECTION
// takes, which writes no repo record and rides no commit.
func (a *Admissions) Record(ctx context.Context, adm Admission) error {
	return a.record(ctx, a.db, adm)
}

// RecordTx upserts one admission on an existing transaction — the path an
// ACCEPT takes, so the "accepted" ledger row commits with the acceptance record
// (a rolled-back acceptance must not leave an accepted admission behind). A nil
// tx is an error satisfying errors.IsValidation.
func (a *Admissions) RecordTx(ctx context.Context, tx *sql.Tx, adm Admission) error {
	if tx == nil {
		return errors.NewValidationError("tx", "must not be nil")
	}
	return a.record(ctx, tx, adm)
}

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func (a *Admissions) record(ctx context.Context, ex execer, adm Admission) error {
	if adm.CommunityDID == "" || adm.PostURI == "" {
		return errors.NewValidationError("admission", "community_did and post_uri are required")
	}
	if adm.Status == "" {
		return errors.NewValidationError("admission.status", "must be set")
	}
	_, err := ex.ExecContext(ctx, `
		INSERT INTO admissions
		    (community_did, post_uri, author_did, status, decision_code, evaluated_cid,
		     acceptance_rkey, accepted_cid, redrivable)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (community_did, post_uri) DO UPDATE SET
		    author_did = EXCLUDED.author_did,
		    status = EXCLUDED.status,
		    decision_code = EXCLUDED.decision_code,
		    evaluated_cid = EXCLUDED.evaluated_cid,
		    acceptance_rkey = EXCLUDED.acceptance_rkey,
		    accepted_cid = EXCLUDED.accepted_cid,
		    redrivable = EXCLUDED.redrivable,
		    updated_at = now()`,
		adm.CommunityDID, adm.PostURI, adm.AuthorDID, adm.Status, adm.DecisionCode, adm.EvaluatedCID,
		adm.AcceptanceRKey, adm.AcceptedCID, adm.Redrivable)
	if err != nil {
		return fmt.Errorf("accept: record admission %s/%s: %w", adm.CommunityDID, adm.PostURI, err)
	}
	return nil
}

// Get returns the admission for a (community, post), or an error satisfying
// errors.IsNotFound when the engine has never decided on it.
func (a *Admissions) Get(ctx context.Context, communityDID, postURI string) (*Admission, error) {
	var adm Admission
	err := a.db.QueryRowContext(ctx, `
		SELECT community_did, post_uri, author_did, status, decision_code, evaluated_cid,
		       acceptance_rkey, accepted_cid, redrivable
		  FROM admissions WHERE community_did = $1 AND post_uri = $2`,
		communityDID, postURI).Scan(
		&adm.CommunityDID, &adm.PostURI, &adm.AuthorDID, &adm.Status, &adm.DecisionCode, &adm.EvaluatedCID,
		&adm.AcceptanceRKey, &adm.AcceptedCID, &adm.Redrivable)
	if stderrors.Is(err, sql.ErrNoRows) {
		return nil, errors.NewNotFoundError("admission", communityDID+"/"+postURI)
	}
	if err != nil {
		return nil, fmt.Errorf("accept: get admission %s/%s: %w", communityDID, postURI, err)
	}
	return &adm, nil
}

// CountAccepted reports how many posts one author currently has ACCEPTED in one
// community, excluding one post_uri (the post being decided — a repin must not
// count against its own author). It backs the per-author-per-community flood
// cap. The (author_did, community_did, created_at) index serves it index-only.
func (a *Admissions) CountAccepted(ctx context.Context, authorDID, communityDID, excludePostURI string) (int, error) {
	var n int
	err := a.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM admissions
		 WHERE author_did = $1 AND community_did = $2 AND status = $3 AND post_uri <> $4`,
		authorDID, communityDID, StatusAccepted, excludePostURI).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("accept: count accepted for %s in %s: %w", authorDID, communityDID, err)
	}
	return n, nil
}

// DeleteTx removes the ledger row for a (community, post) on an existing
// transaction — the author-delete path, which rides the acceptance-delete commit
// so the ledger row and the acceptance record go away together. The post no
// longer exists to re-decide, and no removal record stands to explain a
// 'removed' status, so the row is dropped rather than left behind. Deleting a
// missing row is a no-op success (a redelivered delete). A nil tx is a
// validation error.
func (a *Admissions) DeleteTx(ctx context.Context, tx *sql.Tx, communityDID, postURI string) error {
	if tx == nil {
		return errors.NewValidationError("tx", "must not be nil")
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM admissions WHERE community_did = $1 AND post_uri = $2`,
		communityDID, postURI); err != nil {
		return fmt.Errorf("accept: delete admission %s/%s: %w", communityDID, postURI, err)
	}
	return nil
}
