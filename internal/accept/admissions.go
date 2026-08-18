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
	CommunityDID string
	PostURI      string
	AuthorDID    string
	Status       string
	// DecisionCode is the machine-readable WHY of the row's current state.
	// Migration 021 phrases it as "'' for a clean accept, else the
	// rejection/removal reason", and that is still the rule for how a post came to
	// be accepted — but accepted + a code is a LEGAL pair, not a contradiction: a
	// standing acceptance can carry the cause of a LATER refusal that deliberately
	// left it alone (the ban carve-out — see RecordRefusal). Reading a non-empty
	// code as "this post is not live" is therefore wrong; status is the only thing
	// that answers that.
	DecisionCode   string
	EvaluatedCID   string
	AcceptanceRKey string
	AcceptedCID    string
	Redrivable     bool
	// EvaluatedSnapshot is the postv2 record (plus resolved context) this
	// decision was made against, stored on EVERY decision so /admin/admissions/
	// readmit can re-run admission from stored state (migration 022). Empty
	// ('{}') means the body did not survive — readmit is unrecoverable without a
	// getRecord fetch from the author's PDS (task 18).
	EvaluatedSnapshot []byte
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
	// A JSONB NOT NULL column rejects a NULL, so an unset snapshot coalesces to
	// the empty object the DEFAULT would have used.
	snapshot := adm.EvaluatedSnapshot
	if len(snapshot) == 0 {
		snapshot = []byte("{}")
	}
	_, err := ex.ExecContext(ctx, `
		INSERT INTO admissions
		    (community_did, post_uri, author_did, status, decision_code, evaluated_cid,
		     acceptance_rkey, accepted_cid, redrivable, evaluated_snapshot)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (community_did, post_uri) DO UPDATE SET
		    author_did = EXCLUDED.author_did,
		    status = EXCLUDED.status,
		    decision_code = EXCLUDED.decision_code,
		    evaluated_cid = EXCLUDED.evaluated_cid,
		    acceptance_rkey = EXCLUDED.acceptance_rkey,
		    accepted_cid = EXCLUDED.accepted_cid,
		    redrivable = EXCLUDED.redrivable,
		    evaluated_snapshot = EXCLUDED.evaluated_snapshot,
		    updated_at = now()`,
		adm.CommunityDID, adm.PostURI, adm.AuthorDID, adm.Status, adm.DecisionCode, adm.EvaluatedCID,
		adm.AcceptanceRKey, adm.AcceptedCID, adm.Redrivable, snapshot)
	if err != nil {
		return fmt.Errorf("accept: record admission %s/%s: %w", adm.CommunityDID, adm.PostURI, err)
	}
	return nil
}

// RecordRefusal records WHY an event was refused WITHOUT destroying what the
// post already stands as. It is the upsert sibling of Record for the decisions
// Record cannot express: a refusal that deliberately LEAVES THE ACCEPTANCE
// STANDING, and one that removes the post but must keep naming the version that
// was accepted.
//
// acceptance_rkey and accepted_cid are ALWAYS preserved (a refused edit writes no
// acceptance, so it has none of its own to pin), and status moves only when the
// caller says so:
//
//   - adm.Status EMPTY means keep the status the row already has. This is the ban
//     carve-out: a ban is author-state, not a judgement of a post, so a banned
//     author's edit is refused while the post the moderators chose to leave up
//     stays ACCEPTED. Recording that through Record would flip the row to
//     'rejected' — and ListAccepted, the removeData purge's ONLY input, selects
//     exactly status='accepted'. The sequence ban → the author fixes a typo →
//     re-ban with removeData=true would then purge every post of theirs EXCEPT
//     the edited one: Lemmy erases it, Coves keeps serving it under the
//     community's name. CountAccepted would likewise free a live post's quota.
//   - adm.Status SET moves it (an edit refused against a standing moderator
//     removal is 'removed'), on top of the same preserved pins.
//
// A post with no ledger row has nothing standing to preserve, so the INSERT path
// records the refusal on its own terms — defaulting to 'rejected' when the caller
// named no status.
//
// evaluated_cid and evaluated_snapshot move to THIS event, because a refusal is
// still a decision about a specific version — but an ABSENT cid or snapshot
// leaves the stored one alone rather than blanking it: losing the snapshot would
// make the post permanently unreadmittable, which is the same trap that keeps
// RecordRemoval off the full upsert.
func (a *Admissions) RecordRefusal(ctx context.Context, adm Admission) error {
	if adm.CommunityDID == "" || adm.PostURI == "" {
		return errors.NewValidationError("admission", "community_did and post_uri are required")
	}
	if adm.DecisionCode == "" {
		// This method exists to record a WHY; without one it would be a silent
		// touch of a row whose status it deliberately does not change.
		return errors.NewValidationError("admission.decision_code", "must be set on a refusal")
	}
	// The status the row is CREATED with when the engine has decided nothing about
	// this post before. Never used on the update path.
	insertStatus := adm.Status
	if insertStatus == "" {
		insertStatus = StatusRejected
	}
	snapshot := adm.EvaluatedSnapshot
	if len(snapshot) == 0 {
		snapshot = []byte("{}") // the JSONB NOT NULL default; see record()
	}
	_, err := a.db.ExecContext(ctx, `
		INSERT INTO admissions
		    (community_did, post_uri, author_did, status, decision_code, evaluated_cid,
		     evaluated_snapshot)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (community_did, post_uri) DO UPDATE SET
		    author_did = COALESCE(NULLIF(EXCLUDED.author_did, ''), admissions.author_did),
		    status = COALESCE(NULLIF($8, ''), admissions.status),
		    decision_code = EXCLUDED.decision_code,
		    evaluated_cid = COALESCE(NULLIF(EXCLUDED.evaluated_cid, ''), admissions.evaluated_cid),
		    evaluated_snapshot = CASE WHEN $9 THEN EXCLUDED.evaluated_snapshot
		                              ELSE admissions.evaluated_snapshot END,
		    updated_at = now()`,
		adm.CommunityDID, adm.PostURI, adm.AuthorDID, insertStatus, adm.DecisionCode,
		adm.EvaluatedCID, snapshot, adm.Status, len(adm.EvaluatedSnapshot) > 0)
	if err != nil {
		return fmt.Errorf("accept: record refusal %s/%s: %w", adm.CommunityDID, adm.PostURI, err)
	}
	return nil
}

// RecordRemoval marks a post removed BY ITS COMMUNITY, with the removal
// record's own code. It satisfies materialize.ModerationLedger, so the inbound
// moderation path can keep the ledger honest without importing this package.
//
// Why the ledger must learn about it at all: the community repo's removal
// record is the source of truth, but the ledger is what an operator reads when
// an author asks why their post is gone, and what decide()'s per-community rate
// cap counts (a removed post must stop consuming the author's quota).
//
// It is a NARROW UPDATE, never Record(): that upsert rewrites every column,
// including evaluated_snapshot, and blanking that would make the post
// permanently unreadmittable — a moderation action must not destroy the state a
// later readmit needs. A post with no ledger row is left alone: the engine
// writes one for every native post it decides on, so a miss means this post is
// not the acceptance engine's business.
func (a *Admissions) RecordRemoval(ctx context.Context, communityDID, postURI, authorDID, code string) error {
	return a.markModeration(ctx, "record removal", communityDID, postURI, authorDID,
		StatusRemoved, code, "")
}

// RecordRestore marks a post accepted again after a moderator restore, pinning
// the CID the fresh acceptance was written against. Same narrow-update contract
// as RecordRemoval.
func (a *Admissions) RecordRestore(ctx context.Context, communityDID, postURI, authorDID, cid string) error {
	return a.markModeration(ctx, "record restore", communityDID, postURI, authorDID,
		StatusAccepted, "", cid)
}

func (a *Admissions) markModeration(ctx context.Context, op, communityDID, postURI, authorDID, status, code, acceptedCID string) error {
	if communityDID == "" || postURI == "" {
		return errors.NewValidationError("admission", "community_did and post_uri are required")
	}
	// accepted_cid moves only on a restore (the empty string leaves it alone),
	// so a removal keeps naming the version that was accepted when it was
	// removed — the same pin the removal record carries.
	_, err := a.db.ExecContext(ctx, `
		UPDATE admissions
		   SET status = $3,
		       decision_code = $4,
		       author_did = COALESCE(NULLIF($5, ''), author_did),
		       accepted_cid = COALESCE(NULLIF($6, ''), accepted_cid),
		       updated_at = now()
		 WHERE community_did = $1 AND post_uri = $2`,
		communityDID, postURI, status, code, authorDID, acceptedCID)
	if err != nil {
		return fmt.Errorf("accept: %s %s/%s: %w", op, communityDID, postURI, err)
	}
	return nil
}

// LastEvaluatedCID is the CID of the most recent version of a post this engine
// decided on. It satisfies materialize.ModerationLedger.
//
// It is the ONLY record of the current version for a post whose latest decision
// wrote nothing outward — an edit refused against a standing moderator removal
// writes no acceptance and enqueues nothing, so outbound_objects keeps naming
// the version that was removed. A missing row answers "" (no error): a post the
// engine never decided on has no evaluated version, and the caller falls back.
func (a *Admissions) LastEvaluatedCID(ctx context.Context, communityDID, postURI string) (string, error) {
	var cid sql.NullString
	err := a.db.QueryRowContext(ctx,
		`SELECT evaluated_cid FROM admissions WHERE community_did = $1 AND post_uri = $2`,
		communityDID, postURI).Scan(&cid)
	if stderrors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("accept: read evaluated cid %s/%s: %w", communityDID, postURI, err)
	}
	return cid.String, nil
}

// Get returns the admission for a (community, post), or an error satisfying
// errors.IsNotFound when the engine has never decided on it.
func (a *Admissions) Get(ctx context.Context, communityDID, postURI string) (*Admission, error) {
	var adm Admission
	err := a.db.QueryRowContext(ctx, `
		SELECT community_did, post_uri, author_did, status, decision_code, evaluated_cid,
		       acceptance_rkey, accepted_cid, redrivable, evaluated_snapshot
		  FROM admissions WHERE community_did = $1 AND post_uri = $2`,
		communityDID, postURI).Scan(
		&adm.CommunityDID, &adm.PostURI, &adm.AuthorDID, &adm.Status, &adm.DecisionCode, &adm.EvaluatedCID,
		&adm.AcceptanceRKey, &adm.AcceptedCID, &adm.Redrivable, &adm.EvaluatedSnapshot)
	if stderrors.Is(err, sql.ErrNoRows) {
		return nil, errors.NewNotFoundError("admission", communityDID+"/"+postURI)
	}
	if err != nil {
		return nil, fmt.Errorf("accept: get admission %s/%s: %w", communityDID, postURI, err)
	}
	return &adm, nil
}

// GetByPostURI returns the admission for a post at-uri alone — the readmit and
// admin-list path, which knows the post but not necessarily its community.
//
// At most one row matches, and that is a SCHEMA guarantee, not an assumption
// about at-uris: migration 031's unique index on post_uri. It has to be, because
// this query has no ORDER BY — if two communities could hold one post_uri,
// postgres would pick the winner, and boundCommunityOf (which reads the post's
// community binding through this call) would be asking a question with two
// answers. A miss satisfies errors.IsNotFound.
func (a *Admissions) GetByPostURI(ctx context.Context, postURI string) (*Admission, error) {
	var adm Admission
	err := a.db.QueryRowContext(ctx, `
		SELECT community_did, post_uri, author_did, status, decision_code, evaluated_cid,
		       acceptance_rkey, accepted_cid, redrivable, evaluated_snapshot
		  FROM admissions WHERE post_uri = $1`, postURI).Scan(
		&adm.CommunityDID, &adm.PostURI, &adm.AuthorDID, &adm.Status, &adm.DecisionCode, &adm.EvaluatedCID,
		&adm.AcceptanceRKey, &adm.AcceptedCID, &adm.Redrivable, &adm.EvaluatedSnapshot)
	if stderrors.Is(err, sql.ErrNoRows) {
		return nil, errors.NewNotFoundError("admission", postURI)
	}
	if err != nil {
		return nil, fmt.Errorf("accept: get admission %s: %w", postURI, err)
	}
	return &adm, nil
}

// AdmissionFilter narrows a List. Empty fields are wildcards.
type AdmissionFilter struct {
	Status    string
	Community string
}

// List returns admissions matching the filter, newest first — the admin
// surface's read of pending/rejected/removed decisions with their reasons. The
// snapshot is deliberately NOT returned (it can be large and the listing is a
// triage view); readmit reads it through GetByPostURI.
func (a *Admissions) List(ctx context.Context, filter AdmissionFilter) ([]Admission, error) {
	rows, err := a.db.QueryContext(ctx, `
		SELECT community_did, post_uri, author_did, status, decision_code, evaluated_cid,
		       acceptance_rkey, accepted_cid, redrivable
		  FROM admissions
		 WHERE ($1 = '' OR status = $1)
		   AND ($2 = '' OR community_did = $2)
		 ORDER BY updated_at DESC`, filter.Status, filter.Community)
	if err != nil {
		return nil, fmt.Errorf("accept: list admissions: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Admission
	for rows.Next() {
		var adm Admission
		if err := rows.Scan(&adm.CommunityDID, &adm.PostURI, &adm.AuthorDID, &adm.Status,
			&adm.DecisionCode, &adm.EvaluatedCID, &adm.AcceptanceRKey, &adm.AcceptedCID, &adm.Redrivable); err != nil {
			return nil, fmt.Errorf("accept: scan admission: %w", err)
		}
		out = append(out, adm)
	}
	return out, rows.Err()
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

// ListAccepted returns the at-uris of the posts an author currently has ACCEPTED
// in one community, oldest first — the input to a ban's removeData purge, and
// the reason idx_admissions_author_community leads with (author_did,
// community_did).
//
// This ledger is the ONLY table that records which community admitted a post,
// which is exactly the scope a ban is entitled to act on. The obvious
// alternatives are both wrong: ap_objects and outbound_objects know what was
// materialized and federated but not by whose decision, so either would purge an
// author's writing in every community over one community's ban.
func (a *Admissions) ListAccepted(ctx context.Context, communityDID, authorDID string) ([]string, error) {
	rows, err := a.db.QueryContext(ctx, `
		SELECT post_uri FROM admissions
		 WHERE author_did = $1 AND community_did = $2 AND status = $3
		 ORDER BY created_at`,
		authorDID, communityDID, StatusAccepted)
	if err != nil {
		return nil, fmt.Errorf("accept: list accepted for %s in %s: %w", authorDID, communityDID, err)
	}
	defer func() { _ = rows.Close() }()

	var postURIs []string
	for rows.Next() {
		var postURI string
		if err := rows.Scan(&postURI); err != nil {
			return nil, fmt.Errorf("accept: scan accepted for %s in %s: %w", authorDID, communityDID, err)
		}
		postURIs = append(postURIs, postURI)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("accept: list accepted for %s in %s: %w", authorDID, communityDID, err)
	}
	return postURIs, nil
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
