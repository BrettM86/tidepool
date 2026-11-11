// Package blobwithhold is the per-source blob withhold store and the single
// "may (DID, CID) be stored or served" check. Every withhold source writes
// rows into the blob_withholds table; Covers reads every active row by scope,
// so a new source adds rows, never a second check. This package must not
// import internal/repo (repo imports it).
package blobwithhold

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/lib/pq"

	"tidepool/internal/errors"
)

// Sources, scopes and reason codes.
const (
	SourceOperator         = "operator"
	SourceCommunityRemoval = "community-removal"
	SourceLabel            = "label"

	ScopeEveryOwner = "every-owner"
	ScopeSubject    = "subject"
	ScopeOwner      = "owner"
	ScopeCommunity  = "community"

	ReasonIllegalContent     = "illegal-content"
	ReasonCSAM               = "csam"
	ReasonOperatorDiscretion = "operator-discretion"
	// ReasonLabelTakedown is a label row's code: a `!takedown` label.
	ReasonLabelTakedown = "label-takedown"
	// ReasonLabelTakedownAuthorDeleted is the code of a label row whose
	// subject record its author deleted while the label stood: the bytes were
	// kept, and a negation does not lift the row while the record is gone. The
	// author's undelete clears the mark in the record's commit.
	ReasonLabelTakedownAuthorDeleted = "label-takedown-author-deleted"
)

// Withhold is one withhold row. A nil LiftedAt means active.
type Withhold struct {
	ID                 int64      `json:"id"`
	CID                string     `json:"cid"`
	Source             string     `json:"source"`
	Scope              string     `json:"scope"`
	OwnerDID           string     `json:"owner_did,omitempty"`
	SubjectURI         string     `json:"subject_uri,omitempty"`
	CommunityDID       string     `json:"community_did,omitempty"`
	ReasonCode         string     `json:"reason_code"`
	ModerationActionID string     `json:"moderation_action_id,omitempty"`
	CSAMCaseID         string     `json:"csam_case_id,omitempty"`
	CreatedBy          string     `json:"created_by,omitempty"`
	LiftedBy           string     `json:"lifted_by,omitempty"`
	BytesDeletedBy     string     `json:"bytes_deleted_by,omitempty"`
	CreatedAt          time.Time  `json:"created_at"`
	LiftedAt           *time.Time `json:"lifted_at,omitempty"`
	DeletedAt          *time.Time `json:"deleted_at,omitempty"`
}

// State is a CID's withhold state as every admin call returns it.
type State struct {
	CID          string     `json:"cid"`
	Withholds    []Withhold `json:"withholds"`
	BytesDeleted bool       `json:"bytes_deleted"`
	StoredOwners int        `json:"stored_owners"`
}

// OperatorRequest is an operator withhold.
type OperatorRequest struct {
	CID                string
	ReasonCode         string
	Actor              string
	ModerationActionID string
	CSAMCaseID         string
}

// Canonicalize parses a CID in any multibase and returns it as CIDv1 base32.
// A CIDv0 is converted to the CIDv1 with the same codec and hash. An
// unparseable CID is a validation error.
func Canonicalize(cidStr string) (string, error) {
	if cidStr == "" {
		return "", errors.NewValidationError("cid", "must not be empty")
	}
	c, err := cid.Decode(cidStr)
	if err != nil {
		return "", errors.NewValidationError("cid", err.Error())
	}
	if c.Version() == 0 {
		c = cid.NewCidV1(c.Type(), c.Hash())
	}
	return c.String(), nil
}

// operatorReasons are the reason codes an operator withhold may carry.
var operatorReasons = map[string]bool{
	ReasonIllegalContent:     true,
	ReasonCSAM:               true,
	ReasonOperatorDiscretion: true,
}

// ValidOperatorReason reports whether code is an operator reason code.
func ValidOperatorReason(code string) bool { return operatorReasons[code] }

// errNoActiveOperatorWithhold builds the error Lift and DeleteBytes return
// when the CID has no active operator withhold.
func errNoActiveOperatorWithhold(cidStr string) error {
	return errors.NewNotFoundError("active operator blob withhold", cidStr)
}

// Store is the withhold store.
type Store struct{ db *sql.DB }

// NewStore builds a Store.
func NewStore(db *sql.DB) *Store { return &Store{db: db} }

// withholdColumns is the SELECT list scanWithhold reads.
const withholdColumns = `id, cid, source, scope,
	COALESCE(owner_did, ''), COALESCE(subject_uri, ''), COALESCE(community_did, ''),
	reason_code, COALESCE(moderation_action_id, ''), COALESCE(csam_case_id, ''),
	COALESCE(created_by, ''), COALESCE(lifted_by, ''), COALESCE(bytes_deleted_by, ''),
	created_at, lifted_at, deleted_at`

type scanner interface{ Scan(dest ...any) error }

func scanWithhold(row scanner) (Withhold, error) {
	var (
		w                   Withhold
		liftedAt, deletedAt sql.NullTime
	)
	if err := row.Scan(&w.ID, &w.CID, &w.Source, &w.Scope,
		&w.OwnerDID, &w.SubjectURI, &w.CommunityDID,
		&w.ReasonCode, &w.ModerationActionID, &w.CSAMCaseID,
		&w.CreatedBy, &w.LiftedBy, &w.BytesDeletedBy,
		&w.CreatedAt, &liftedAt, &deletedAt); err != nil {
		return Withhold{}, err
	}
	w.CreatedAt = w.CreatedAt.UTC()
	if liftedAt.Valid {
		t := liftedAt.Time.UTC()
		w.LiftedAt = &t
	}
	if deletedAt.Valid {
		t := deletedAt.Time.UTC()
		w.DeletedAt = &t
	}
	return w, nil
}

// queryer is satisfied by *sql.DB and *sql.Tx.
type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Withhold places an operator every-owner withhold; idempotent. While an
// active operator withhold exists for the CID, the call changes nothing and
// returns the existing state.
func (s *Store) Withhold(ctx context.Context, req OperatorRequest) (State, error) {
	canonical, err := Canonicalize(req.CID)
	if err != nil {
		return State{}, err
	}
	if !ValidOperatorReason(req.ReasonCode) {
		return State{}, errors.NewValidationError("reason_code",
			"must be one of illegal-content, csam, operator-discretion")
	}
	if req.Actor == "" {
		return State{}, errors.NewValidationError("actor", "must not be empty")
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO blob_withholds
			(cid, source, scope, reason_code, moderation_action_id, csam_case_id, created_by)
		VALUES ($1, $2, $3, $4, NULLIF($5, ''), NULLIF($6, ''), $7)
		ON CONFLICT (cid) WHERE source = 'operator' AND lifted_at IS NULL DO NOTHING`,
		canonical, SourceOperator, ScopeEveryOwner, req.ReasonCode,
		req.ModerationActionID, req.CSAMCaseID, req.Actor); err != nil {
		return State{}, fmt.Errorf("blobwithhold: withhold %s: %w", canonical, err)
	}
	return s.inspect(ctx, s.db, canonical)
}

// Lift lifts the active operator withhold, recording lifted_at and the
// actor. With no active operator withhold it changes nothing and returns an
// error satisfying errors.IsNotFound. Withholds of other sources stay.
func (s *Store) Lift(ctx context.Context, cidStr, actor string) (State, error) {
	canonical, err := Canonicalize(cidStr)
	if err != nil {
		return State{}, err
	}
	if actor == "" {
		return State{}, errors.NewValidationError("actor", "must not be empty")
	}
	res, err := s.db.ExecContext(ctx, `
		UPDATE blob_withholds SET lifted_at = now(), lifted_by = $2
		WHERE cid = $1 AND source = 'operator' AND lifted_at IS NULL`,
		canonical, actor)
	if err != nil {
		return State{}, fmt.Errorf("blobwithhold: lift %s: %w", canonical, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return State{}, fmt.Errorf("blobwithhold: lift %s: rows affected: %w", canonical, err)
	}
	if n == 0 {
		return State{}, errNoActiveOperatorWithhold(canonical)
	}
	return s.inspect(ctx, s.db, canonical)
}

// DeleteBytes deletes the CID's bytes for every owner under an active
// operator withhold. deleted_at and bytes_deleted_by are set by the first
// delete only. With no active operator withhold it deletes nothing and
// returns an error satisfying errors.IsNotFound.
func (s *Store) DeleteBytes(ctx context.Context, cidStr, actor string) (State, error) {
	canonical, err := Canonicalize(cidStr)
	if err != nil {
		return State{}, err
	}
	if actor == "" {
		return State{}, errors.NewValidationError("actor", "must not be empty")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return State{}, fmt.Errorf("blobwithhold: delete bytes %s: begin: %w", canonical, err)
	}
	defer func() { _ = tx.Rollback() }()
	var id int64
	err = tx.QueryRowContext(ctx, `
		SELECT id FROM blob_withholds
		WHERE cid = $1 AND source = 'operator' AND lifted_at IS NULL
		FOR UPDATE`, canonical).Scan(&id)
	if stderrors.Is(err, sql.ErrNoRows) {
		return State{}, errNoActiveOperatorWithhold(canonical)
	}
	if err != nil {
		return State{}, fmt.Errorf("blobwithhold: delete bytes %s: find withhold: %w", canonical, err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM blobs WHERE cid = $1`, canonical); err != nil {
		return State{}, fmt.Errorf("blobwithhold: delete bytes %s: %w", canonical, err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE blob_withholds SET deleted_at = now(), bytes_deleted_by = $2
		WHERE id = $1 AND deleted_at IS NULL`, id, actor); err != nil {
		return State{}, fmt.Errorf("blobwithhold: delete bytes %s: record: %w", canonical, err)
	}
	state, err := s.inspect(ctx, tx, canonical)
	if err != nil {
		return State{}, err
	}
	if err := tx.Commit(); err != nil {
		return State{}, fmt.Errorf("blobwithhold: delete bytes %s: commit: %w", canonical, err)
	}
	return state, nil
}

// Inspect returns the CID's state: every withhold row (active and lifted),
// whether its bytes were deleted under a withhold, and how many DIDs hold a
// stored copy.
func (s *Store) Inspect(ctx context.Context, cidStr string) (State, error) {
	canonical, err := Canonicalize(cidStr)
	if err != nil {
		return State{}, err
	}
	return s.inspect(ctx, s.db, canonical)
}

func (s *Store) inspect(ctx context.Context, q queryer, canonical string) (State, error) {
	rows, err := q.QueryContext(ctx,
		`SELECT `+withholdColumns+` FROM blob_withholds WHERE cid = $1 ORDER BY id`, canonical)
	if err != nil {
		return State{}, fmt.Errorf("blobwithhold: inspect %s: %w", canonical, err)
	}
	defer func() { _ = rows.Close() }()
	state := State{CID: canonical, Withholds: []Withhold{}}
	for rows.Next() {
		w, err := scanWithhold(rows)
		if err != nil {
			return State{}, fmt.Errorf("blobwithhold: inspect %s: scan: %w", canonical, err)
		}
		if w.DeletedAt != nil {
			state.BytesDeleted = true
		}
		state.Withholds = append(state.Withholds, w)
	}
	if err := rows.Err(); err != nil {
		return State{}, fmt.Errorf("blobwithhold: inspect %s: %w", canonical, err)
	}
	if err := q.QueryRowContext(ctx,
		`SELECT count(*) FROM blobs WHERE cid = $1`, canonical).Scan(&state.StoredOwners); err != nil {
		return State{}, fmt.Errorf("blobwithhold: inspect %s: stored owners: %w", canonical, err)
	}
	return state, nil
}

// ListActiveOperator lists active operator withholds, oldest first.
func (s *Store) ListActiveOperator(ctx context.Context) ([]Withhold, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+withholdColumns+` FROM blob_withholds
		WHERE source = 'operator' AND lifted_at IS NULL ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("blobwithhold: list active operator withholds: %w", err)
	}
	defer func() { _ = rows.Close() }()
	withholds := []Withhold{}
	for rows.Next() {
		w, err := scanWithhold(rows)
		if err != nil {
			return nil, fmt.Errorf("blobwithhold: list active operator withholds: scan: %w", err)
		}
		withholds = append(withholds, w)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("blobwithhold: list active operator withholds: %w", err)
	}
	return withholds, nil
}

// Insert writes a withhold row of any source directly (the writer for
// sources that have no dedicated method yet). The CID is canonicalized; a
// zero CreatedAt takes the database time. The schema's CHECKs reject a
// source, scope or reason code outside the fixed sets.
func (s *Store) Insert(ctx context.Context, w Withhold) (Withhold, error) {
	canonical, err := Canonicalize(w.CID)
	if err != nil {
		return Withhold{}, err
	}
	var createdAt any
	if !w.CreatedAt.IsZero() {
		createdAt = w.CreatedAt
	}
	row := s.db.QueryRowContext(ctx, `
		INSERT INTO blob_withholds
			(cid, source, scope, owner_did, subject_uri, community_did, reason_code,
			 moderation_action_id, csam_case_id, created_by, lifted_by, bytes_deleted_by,
			 created_at, lifted_at, deleted_at)
		VALUES ($1, $2, $3, NULLIF($4, ''), NULLIF($5, ''), NULLIF($6, ''), $7,
			NULLIF($8, ''), NULLIF($9, ''), NULLIF($10, ''), NULLIF($11, ''), NULLIF($12, ''),
			COALESCE($13::timestamptz, now()), $14, $15)
		RETURNING `+withholdColumns,
		canonical, w.Source, w.Scope, w.OwnerDID, w.SubjectURI, w.CommunityDID, w.ReasonCode,
		w.ModerationActionID, w.CSAMCaseID, w.CreatedBy, w.LiftedBy, w.BytesDeletedBy,
		createdAt, w.LiftedAt, w.DeletedAt)
	inserted, err := scanWithhold(row)
	if err != nil {
		return Withhold{}, fmt.Errorf("blobwithhold: insert %s withhold %s: %w", w.Source, canonical, err)
	}
	return inserted, nil
}

// Execer is satisfied by *sql.Tx (and *sql.DB). The community-removal
// writers are functions over the caller's transaction rather than Store
// methods: they run inside a repo commit's side effect, so a removal record
// and its withholds commit or roll back together.
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// AddSubjectRemovalWithholds inserts a community-removal row of the subject
// for each CID in cids that has none (idempotent under the
// one-active-row-per-(subject, CID) index), and lifts nothing: while a removal
// stands its rows only grow, and only a restore or the author's delete of the
// subject lifts them (a CID a later
// version of the record no longer references may still be stored under the
// owner, so lifting it would serve the removed image). ownerDID is the repo
// DID of the subject record, the DID whose serve the rows refuse. reasonCode
// is the standing removal record's code; an active row under an earlier code
// takes it. Only community-removal rows of subjectURI are touched; operator
// and other subjects' rows stay.
func AddSubjectRemovalWithholds(ctx context.Context, q Execer, ownerDID, subjectURI, reasonCode string, cids []string) error {
	canonical, err := canonicalRemovalWithholds(ownerDID, subjectURI, reasonCode, cids)
	if err != nil {
		return err
	}
	for _, c := range canonical {
		if _, err := q.ExecContext(ctx, `
			INSERT INTO blob_withholds (cid, source, scope, owner_did, subject_uri, reason_code)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (subject_uri, cid) WHERE source = 'community-removal' AND lifted_at IS NULL
			DO UPDATE SET reason_code = EXCLUDED.reason_code
			WHERE blob_withholds.reason_code <> EXCLUDED.reason_code`,
			c, SourceCommunityRemoval, ScopeSubject, ownerDID, subjectURI, reasonCode); err != nil {
			return fmt.Errorf("blobwithhold: community-removal withhold %s for %s: %w", c, subjectURI, err)
		}
	}
	return nil
}

// canonicalRemovalWithholds validates a subject withhold write's inputs (a
// community-removal or label row) and returns cids canonicalized.
func canonicalRemovalWithholds(ownerDID, subjectURI, reasonCode string, cids []string) ([]string, error) {
	if ownerDID == "" {
		return nil, errors.NewValidationError("owner_did", "must not be empty")
	}
	if subjectURI == "" {
		return nil, errors.NewValidationError("subject_uri", "must not be empty")
	}
	if reasonCode == "" {
		return nil, errors.NewValidationError("reason_code", "must be the removal record's code")
	}
	canonical := make([]string, 0, len(cids))
	for _, c := range cids {
		cc, err := Canonicalize(c)
		if err != nil {
			return nil, err
		}
		canonical = append(canonical, cc)
	}
	return canonical, nil
}

// LiftSubjectRemovalWithholds lifts every active community-removal withhold
// of the subject. Other sources' rows and other subjects' rows stay, so a CID
// another removed subject or an operator still withholds stays refused.
func LiftSubjectRemovalWithholds(ctx context.Context, q Execer, subjectURI string) error {
	if subjectURI == "" {
		return errors.NewValidationError("subject_uri", "must not be empty")
	}
	if _, err := q.ExecContext(ctx, `
		UPDATE blob_withholds SET lifted_at = now()
		WHERE source = 'community-removal' AND subject_uri = $1 AND lifted_at IS NULL`,
		subjectURI); err != nil {
		return fmt.Errorf("blobwithhold: lift community-removal withholds of %s: %w", subjectURI, err)
	}
	return nil
}

// AddSubjectLabelWithholds inserts a label row of the subject for each CID
// in cids that has none (idempotent under the one-active-row-per-(subject,
// CID) index), and lifts nothing: like community-removal rows, label rows
// only grow while the label stands. ownerDID is the subject record's repo
// DID, the DID whose store and serve the rows refuse.
func AddSubjectLabelWithholds(ctx context.Context, q Execer, ownerDID, subjectURI string, cids []string) error {
	canonical, err := canonicalRemovalWithholds(ownerDID, subjectURI, ReasonLabelTakedown, cids)
	if err != nil {
		return err
	}
	for _, c := range canonical {
		if _, err := q.ExecContext(ctx, `
			INSERT INTO blob_withholds (cid, source, scope, owner_did, subject_uri, reason_code)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (subject_uri, cid) WHERE source = 'label' AND lifted_at IS NULL DO NOTHING`,
			c, SourceLabel, ScopeSubject, ownerDID, subjectURI, ReasonLabelTakedown); err != nil {
			return fmt.Errorf("blobwithhold: label withhold %s for %s: %w", c, subjectURI, err)
		}
	}
	return nil
}

// LiftSubjectLabelWithholds lifts every active label withhold of the
// subject. Other sources' rows and other subjects' rows stay.
func LiftSubjectLabelWithholds(ctx context.Context, q Execer, subjectURI string) error {
	if subjectURI == "" {
		return errors.NewValidationError("subject_uri", "must not be empty")
	}
	if _, err := q.ExecContext(ctx, `
		UPDATE blob_withholds SET lifted_at = now()
		WHERE source = 'label' AND subject_uri = $1 AND lifted_at IS NULL`,
		subjectURI); err != nil {
		return fmt.Errorf("blobwithhold: lift label withholds of %s: %w", subjectURI, err)
	}
	return nil
}

// AddOwnerLabelWithhold places the account-scope label withhold of
// ownerDID: one active owner-scope label row naming no CID, so it covers
// every blob the DID owns, including blobs stored after it (an account
// `!takedown`). Idempotent under the one-active-row-per-owner index.
func AddOwnerLabelWithhold(ctx context.Context, q Execer, ownerDID string) error {
	if ownerDID == "" {
		return errors.NewValidationError("owner_did", "must not be empty")
	}
	if _, err := q.ExecContext(ctx, `
		INSERT INTO blob_withholds (cid, source, scope, owner_did, reason_code)
		VALUES (NULL, $1, $2, $3, $4)
		ON CONFLICT (owner_did) WHERE source = 'label' AND scope = 'owner' AND cid IS NULL AND lifted_at IS NULL
		DO NOTHING`,
		SourceLabel, ScopeOwner, ownerDID, ReasonLabelTakedown); err != nil {
		return fmt.Errorf("blobwithhold: account label withhold for %s: %w", ownerDID, err)
	}
	return nil
}

// LiftOwnerLabelWithhold lifts ownerDID's account-scope label withhold.
// Every other row, of any source or scope, stays.
func LiftOwnerLabelWithhold(ctx context.Context, q Execer, ownerDID string) error {
	if ownerDID == "" {
		return errors.NewValidationError("owner_did", "must not be empty")
	}
	if _, err := q.ExecContext(ctx, `
		UPDATE blob_withholds SET lifted_at = now()
		WHERE source = 'label' AND scope = 'owner' AND cid IS NULL
		  AND owner_did = $1 AND lifted_at IS NULL`,
		ownerDID); err != nil {
		return fmt.Errorf("blobwithhold: lift account label withhold of %s: %w", ownerDID, err)
	}
	return nil
}

// AddCommunityLabelWithholds inserts a community-scope label row of
// communityDID for ownerDID and each CID in cids that has none (idempotent
// under the one-active-row-per-(community, owner, CID) index), and lifts
// nothing: the rows only grow while the community's `!takedown` stands.
// ownerDID is the repo DID of the record in the community that references the
// CIDs, the DID whose store and serve the rows refuse.
func AddCommunityLabelWithholds(ctx context.Context, q Execer, communityDID, ownerDID string, cids []string) error {
	if communityDID == "" {
		return errors.NewValidationError("community_did", "must not be empty")
	}
	if ownerDID == "" {
		return errors.NewValidationError("owner_did", "must not be empty")
	}
	for _, c := range cids {
		canonical, err := Canonicalize(c)
		if err != nil {
			return err
		}
		if _, err := q.ExecContext(ctx, `
			INSERT INTO blob_withholds (cid, source, scope, owner_did, community_did, reason_code)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (community_did, owner_did, cid)
			WHERE source = 'label' AND scope = 'community' AND lifted_at IS NULL
			DO NOTHING`,
			canonical, SourceLabel, ScopeCommunity, ownerDID, communityDID, ReasonLabelTakedown); err != nil {
			return fmt.Errorf("blobwithhold: community label withhold %s of %s in %s: %w", canonical, ownerDID, communityDID, err)
		}
	}
	return nil
}

// LiftCommunityLabelWithholds lifts every active community-scope label
// withhold of communityDID except those marked author-deleted, which stay
// active while their record is gone (see
// MarkUnreferencedCommunityLabelWithholdsAuthorDeleted). Every other row, of any source
// or scope (a record label's or an operator's on the same CID), stays.
func LiftCommunityLabelWithholds(ctx context.Context, q Execer, communityDID string) error {
	if communityDID == "" {
		return errors.NewValidationError("community_did", "must not be empty")
	}
	if _, err := q.ExecContext(ctx, `
		UPDATE blob_withholds SET lifted_at = now()
		WHERE source = 'label' AND scope = 'community'
		  AND community_did = $1 AND lifted_at IS NULL
		  AND reason_code <> $2`,
		communityDID, ReasonLabelTakedownAuthorDeleted); err != nil {
		return fmt.Errorf("blobwithhold: lift community label withholds of %s: %w", communityDID, err)
	}
	return nil
}

// MarkUnreferencedCommunityLabelWithholdsAuthorDeleted gives the
// author-deleted code to every active community-scope label withhold of
// communityDID for ownerDID whose CID is not in referenced, in the transaction
// of an author's delete while the community's `!takedown` stands. referenced
// is every CID the owner's remaining records reference, read after the delete
// under the commit lock, so a CID only an earlier version of the deleted
// record referenced (an edit committed under the label dropped or replaced
// it) is marked too. The delete kept the bytes, and the community's negation
// keeps these rows, so the kept bytes stay refused until an operator's
// delete-bytes or the record's undelete (see
// ClearCommunityLabelWithholdsAuthorDeleted).
func MarkUnreferencedCommunityLabelWithholdsAuthorDeleted(ctx context.Context, q Execer, communityDID, ownerDID string, referenced []string) error {
	if communityDID == "" {
		return errors.NewValidationError("community_did", "must not be empty")
	}
	if ownerDID == "" {
		return errors.NewValidationError("owner_did", "must not be empty")
	}
	canonical := make([]string, 0, len(referenced))
	for _, c := range referenced {
		normalized, err := Canonicalize(c)
		if err != nil {
			return err
		}
		canonical = append(canonical, normalized)
	}
	if _, err := q.ExecContext(ctx, `
		UPDATE blob_withholds SET reason_code = $3
		WHERE source = 'label' AND scope = 'community' AND lifted_at IS NULL
		  AND community_did = $1 AND owner_did = $2
		  AND NOT (cid = ANY($4::text[]))`,
		communityDID, ownerDID, ReasonLabelTakedownAuthorDeleted, pq.Array(canonical)); err != nil {
		return fmt.Errorf("blobwithhold: author-deleted mark of unreferenced community label withholds of %s in %s: %w",
			ownerDID, communityDID, err)
	}
	return nil
}

// ClearCommunityLabelWithholdsAuthorDeleted handles the author-deleted
// community-scope label withholds of communityDID for ownerDID and each CID in
// cids in the commit of a record of the owner in the community that
// references them again (an undelete): while the community's `!takedown`
// stands (standing) they get the label-takedown code again, so the negation
// lifts them; otherwise they are lifted. Rows not marked author-deleted stay.
func ClearCommunityLabelWithholdsAuthorDeleted(ctx context.Context, q Execer, communityDID, ownerDID string, cids []string, standing bool) error {
	query := `UPDATE blob_withholds SET lifted_at = now()
		WHERE source = 'label' AND scope = 'community' AND lifted_at IS NULL
		  AND community_did = $1 AND owner_did = $2 AND cid = $3 AND reason_code = $4`
	if standing {
		query = `UPDATE blob_withholds SET reason_code = '` + ReasonLabelTakedown + `'
		WHERE source = 'label' AND scope = 'community' AND lifted_at IS NULL
		  AND community_did = $1 AND owner_did = $2 AND cid = $3 AND reason_code = $4`
	}
	return setCommunityLabelWithholdsAuthorDeleted(ctx, q, communityDID, ownerDID, cids, query)
}

// setCommunityLabelWithholdsAuthorDeleted runs query, with parameters
// (communityDID, ownerDID, canonical CID, author-deleted code), once per CID.
func setCommunityLabelWithholdsAuthorDeleted(ctx context.Context, q Execer, communityDID, ownerDID string, cids []string, query string) error {
	if communityDID == "" {
		return errors.NewValidationError("community_did", "must not be empty")
	}
	if ownerDID == "" {
		return errors.NewValidationError("owner_did", "must not be empty")
	}
	for _, c := range cids {
		canonical, err := Canonicalize(c)
		if err != nil {
			return err
		}
		if _, err := q.ExecContext(ctx, query, communityDID, ownerDID, canonical, ReasonLabelTakedownAuthorDeleted); err != nil {
			return fmt.Errorf("blobwithhold: author-deleted mark of community label withhold %s of %s in %s: %w",
				canonical, ownerDID, communityDID, err)
		}
	}
	return nil
}

// MarkSubjectLabelWithholdsAuthorDeleted gives every active label withhold
// of the subject the author-deleted code, in the transaction of the author's
// delete of the subject record, so a later negation keeps those rows.
func MarkSubjectLabelWithholdsAuthorDeleted(ctx context.Context, q Execer, subjectURI string) error {
	if subjectURI == "" {
		return errors.NewValidationError("subject_uri", "must not be empty")
	}
	if _, err := q.ExecContext(ctx, `
		UPDATE blob_withholds SET reason_code = $2
		WHERE source = 'label' AND subject_uri = $1 AND lifted_at IS NULL`,
		subjectURI, ReasonLabelTakedownAuthorDeleted); err != nil {
		return fmt.Errorf("blobwithhold: mark label withholds of %s author-deleted: %w", subjectURI, err)
	}
	return nil
}

// UnmarkSubjectLabelWithholdsAuthorDeleted gives every active label withhold
// of the subject marked author-deleted the label-takedown code again, in the
// commit of the subject record's undelete while its label stands, so a later
// negation lifts those rows. Other sources' rows and other subjects' rows
// stay.
func UnmarkSubjectLabelWithholdsAuthorDeleted(ctx context.Context, q Execer, subjectURI string) error {
	if subjectURI == "" {
		return errors.NewValidationError("subject_uri", "must not be empty")
	}
	if _, err := q.ExecContext(ctx, `
		UPDATE blob_withholds SET reason_code = $2
		WHERE source = 'label' AND subject_uri = $1 AND lifted_at IS NULL
		  AND reason_code = $3`,
		subjectURI, ReasonLabelTakedown, ReasonLabelTakedownAuthorDeleted); err != nil {
		return fmt.Errorf("blobwithhold: unmark label withholds of %s author-deleted: %w", subjectURI, err)
	}
	return nil
}

// LiftSubjectLabelWithholdsExceptAuthorDeleted lifts every active label
// withhold of the subject except those marked author-deleted, which stay
// active while the subject record is gone: until an operator's delete-bytes,
// or until the author's undelete re-materializes the record, whose commit
// lifts them (no label standing) or unmarks them (label standing). Other
// sources' rows and other subjects' rows stay.
func LiftSubjectLabelWithholdsExceptAuthorDeleted(ctx context.Context, q Execer, subjectURI string) error {
	if subjectURI == "" {
		return errors.NewValidationError("subject_uri", "must not be empty")
	}
	if _, err := q.ExecContext(ctx, `
		UPDATE blob_withholds SET lifted_at = now()
		WHERE source = 'label' AND subject_uri = $1 AND lifted_at IS NULL
		  AND reason_code <> $2`,
		subjectURI, ReasonLabelTakedownAuthorDeleted); err != nil {
		return fmt.Errorf("blobwithhold: lift label withholds of %s: %w", subjectURI, err)
	}
	return nil
}

// ActiveSubjectRemovalCIDs lists the CIDs the subject's active
// community-removal withholds cover, read in tx (the back-fill reads them
// inside the commit that writes the missing ones).
func ActiveSubjectRemovalCIDs(ctx context.Context, tx *sql.Tx, subjectURI string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT cid FROM blob_withholds
		WHERE source = 'community-removal' AND subject_uri = $1 AND lifted_at IS NULL
		ORDER BY cid`, subjectURI)
	if err != nil {
		return nil, fmt.Errorf("blobwithhold: list community-removal withholds of %s: %w", subjectURI, err)
	}
	defer func() { _ = rows.Close() }()
	var cids []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, fmt.Errorf("blobwithhold: list community-removal withholds of %s: scan: %w", subjectURI, err)
		}
		cids = append(cids, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("blobwithhold: list community-removal withholds of %s: %w", subjectURI, err)
	}
	return cids, nil
}

// Covers reports whether any active withhold, of any source, refuses
// (DID, CID): an every-owner row covers every DID; subject and owner rows
// cover only their owner DID (a subject row therefore refuses the CID for
// its owner even where another live record of that owner references it), and
// an account-scope label row (owner scope, no CID) covers every CID of its
// owner DID, stored or not. A community-scope row (a community label's)
// covers its CID for its owner DID only, so the same bytes under an owner
// outside the community are served.
func (s *Store) Covers(ctx context.Context, did, cidStr string) (bool, error) {
	return covers(ctx, s.db, did, cidStr)
}

// CoversTx is Covers read in tx, for a store that holds a lock while it
// checks and inserts.
func (s *Store) CoversTx(ctx context.Context, tx *sql.Tx, did, cidStr string) (bool, error) {
	return covers(ctx, tx, did, cidStr)
}

// rowQuerier is the single-row read *sql.DB and *sql.Tx share.
type rowQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func covers(ctx context.Context, q rowQuerier, did, cidStr string) (bool, error) {
	canonical, err := Canonicalize(cidStr)
	if err != nil {
		return false, err
	}
	var covered bool
	if err := q.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM blob_withholds
			WHERE cid = $1 AND lifted_at IS NULL
			  AND (scope = 'every-owner'
			       OR (scope IN ('subject', 'owner', 'community') AND owner_did = $2))
		) OR EXISTS (
			SELECT 1 FROM blob_withholds
			WHERE cid IS NULL AND source = 'label' AND scope = 'owner'
			  AND owner_did = $2 AND lifted_at IS NULL
		)`, canonical, did).Scan(&covered); err != nil {
		return false, fmt.Errorf("blobwithhold: covers %s for %s: %w", canonical, did, err)
	}
	return covered, nil
}

// KeepsOwnerBytesTx reports whether an active subject- or community-scope
// row for ownerDID and cidStr, read in tx, still claims the bytes: a kept
// label row of a record its author deleted under a `!takedown` (another
// subject than the one being deleted), a community-removal row, or a
// community label row. An author delete that finds no remaining reference to
// the CID keeps the bytes while such a row stands, so deleting a second
// record that shared the CID does not destroy the first record's evidence.
func KeepsOwnerBytesTx(ctx context.Context, tx *sql.Tx, ownerDID, cidStr string) (bool, error) {
	canonical, err := Canonicalize(cidStr)
	if err != nil {
		return false, err
	}
	var kept bool
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM blob_withholds
			WHERE cid = $1 AND owner_did = $2 AND lifted_at IS NULL
			  AND scope IN ('subject', 'community')
		)`, canonical, ownerDID).Scan(&kept); err != nil {
		return false, fmt.Errorf("blobwithhold: kept bytes of %s for %s: %w", canonical, ownerDID, err)
	}
	return kept, nil
}

// HasActiveOperator reports whether the CID has an active operator
// withhold, the condition under which automatic deletes keep its bytes.
func (s *Store) HasActiveOperator(ctx context.Context, cidStr string) (bool, error) {
	canonical, err := Canonicalize(cidStr)
	if err != nil {
		return false, err
	}
	var active bool
	if err := s.db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM blob_withholds
			WHERE cid = $1 AND source = 'operator' AND lifted_at IS NULL
		)`, canonical).Scan(&active); err != nil {
		return false, fmt.Errorf("blobwithhold: active operator withhold %s: %w", canonical, err)
	}
	return active, nil
}
