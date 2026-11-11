package materialize

import (
	"context"
	"database/sql"
	stderrors "errors"
	"expvar"
	"fmt"
	"strings"

	"github.com/bluesky-social/indigo/atproto/syntax"

	"tidepool/internal/blobwithhold"
	"tidepool/internal/errors"
	"tidepool/internal/labels"
	"tidepool/internal/repo"
	"tidepool/internal/store"
)

// LabelTakedown is the Coves label value that takes a subject down.
const LabelTakedown = "!takedown"

// LabelsSkippedUnusableSubject counts `!takedown` labels stored but not
// applied because their at-uri names no record a repo can hold (a handle
// authority, an invalid collection NSID or record key). Exported via the
// expvar registry as "tidepool_labels_skipped_unusable_subject".
var LabelsSkippedUnusableSubject = expvar.NewInt("tidepool_labels_skipped_unusable_subject")

// Label withholds. One rule per subject, like the community-removal rule in
// removal_withholds.go: while a `!takedown` label stands for a record
// at-uri (its current state per labels.StandingTx is not negated) AND the
// record lives in a repo this bridge hosts, every blob CID its current record
// references has an active label withhold row (subject scope, owner = the
// record's repo DID). The rows are add-only while the label stands; the
// negation lifts every label row of the subject and no other row, whether or
// not the subject record is still hosted, with one exception below.
//
// The author's own delete of the subject (HandleAuthorDeleteRecord) under a
// standing label, when no community removal of the subject stands, keeps the
// bytes (decision 2026-11-08, see deleteAuthorRecord): the delete's commit
// adds any missing label row, gives every active label row of the subject the
// label-takedown-author-deleted code, and deletes no bytes. A negation that
// finds the record gone lifts every label row except those, so the kept bytes
// stay refused until an operator's delete-bytes action removes them or the
// author undeletes the record. The undelete re-materializes the record, and
// its commit clears the marks (labelWithholdOnCommit): with no label standing
// it lifts every label row of the subject, and while one stands it gives the
// marked rows the label-takedown code again, so a later negation lifts them.
// Any other delete (a moderator's, the sweep's, an actor scrub's) marks
// nothing, so a negation lifts its rows and a restore of the record serves its
// blobs again.
//
// The rule is applied in the transaction of whichever side changes: the
// label's commit (labelApplier) and every commit of the subject record
// (commitRecord). Both read under the global repo commit lock, so a label and
// an edit of the same record racing apply the rule in commit order.
//
// Native records (their repo is not hosted here), labels on unknown subjects
// and every other value (nsfw) are stored by the label store and have no
// effect here (a DID label is an account label, below, or a community
// label, community_label_withholds.go); so is a `!takedown` whose at-uri names no record a
// repo can hold, which is logged and counted. The label path itself never
// deletes bytes and never calls the purger.

// labelApplier applies Coves labels to the bridge's hosted records.
type labelApplier struct{ m *Materializer }

// ApplyLabel applies l inside the label's transaction, after its row is
// written there. A `!takedown` at-uri that names no record a repo can hold
// is skipped (logged and counted): the label is already stored, and failing
// here would roll the commit back and replay the label forever.
func (a labelApplier) ApplyLabel(ctx context.Context, tx *sql.Tx, l labels.Stored) error {
	if l.Val == LabelTakedown && isAccountSubject(l.URI) {
		return a.didLabelWithholdTx(ctx, tx, l.URI)
	}
	if l.Val != LabelTakedown || !strings.HasPrefix(l.URI, "at://") {
		return nil
	}
	if !isRecordURI(l.URI) {
		a.skipUnusableSubject(l, "not a DID-authority record at-uri")
		return nil
	}
	// Serialize with repo commits before reading the subject record, so an
	// edit committing concurrently is either seen here or sees this label.
	if err := repo.LockCommitsTx(ctx, tx); err != nil {
		return err
	}
	standing, err := labels.StandingTx(ctx, tx, l.URI, LabelTakedown)
	if err != nil {
		return err
	}
	did, cids, hosted, err := a.m.hostedSubjectBlobCIDs(ctx, tx, l.URI)
	if errors.IsValidation(err) {
		a.skipUnusableSubject(l, err.Error())
		return nil
	}
	if err != nil {
		return err
	}
	if !standing {
		lift := blobwithhold.LiftSubjectLabelWithholds
		if !hosted {
			// The record is gone. Rows the author's own delete marked
			// under the standing label keep the kept bytes refused until an
			// operator's delete-bytes or the author's undelete, whose commit
			// lifts them (labelWithholdOnCommit); every other row lifts, so a
			// record restored at this at-uri, or another record of the owner
			// sharing a CID, is served again.
			lift = blobwithhold.LiftSubjectLabelWithholdsExceptAuthorDeleted
		}
		if err := lift(ctx, tx, l.URI); err != nil {
			return fmt.Errorf("materialize: lift label withholds of %s: %w", l.URI, err)
		}
		return nil
	}
	if !hosted {
		return nil
	}
	if err := blobwithhold.AddSubjectLabelWithholds(ctx, tx, did, l.URI, cids); err != nil {
		return fmt.Errorf("materialize: withhold labeled %s: %w", l.URI, err)
	}
	return nil
}

// skipUnusableSubject logs and counts a stored `!takedown` the applier
// cannot apply because its at-uri names no record a repo can hold.
func (a labelApplier) skipUnusableSubject(l labels.Stored, reason string) {
	LabelsSkippedUnusableSubject.Add(1)
	a.m.logger.Warn("labels: skipping !takedown on an unusable subject",
		"epoch", l.Epoch, "seq", l.Seq, "uri", l.URI, "reason", reason)
}

// isRecordURI reports whether uri is an at-uri of a record: a DID authority,
// a valid collection NSID and a valid record key.
func isRecordURI(uri string) bool {
	parsed, err := syntax.ParseATURI(uri)
	if err != nil || !parsed.Authority().IsDID() {
		return false
	}
	return parsed.Collection() != "" && parsed.RecordKey() != ""
}

// Activate brings a bridged person's account-scope withhold, or a bridged
// community's community-scope withholds, up to its standing bare-DID
// `!takedown` at start, for a label stored by a consumer without this applier
// or before the person was bridged. Record labels are left to ApplyLabel.
// Idempotent.
func (a labelApplier) Activate(ctx context.Context, tx *sql.Tx, l labels.Stored) error {
	if l.Val != LabelTakedown || !isAccountSubject(l.URI) {
		return nil
	}
	return a.didLabelWithholdTx(ctx, tx, l.URI)
}

// didLabelWithholdTx applies a bare-DID `!takedown`: a bridged community's
// DID (a communities row has it) gets the community rule, any other DID the
// account rule.
func (a labelApplier) didLabelWithholdTx(ctx context.Context, tx *sql.Tx, did string) error {
	community, err := bridgedCommunityTx(ctx, tx, did)
	if err != nil {
		return err
	}
	if community {
		return a.m.communityLabelWithholdTx(ctx, tx, did)
	}
	return accountLabelWithholdTx(ctx, tx, did)
}

// LabelApplier is the label applier for the records this bridge hosts and
// the bridged accounts it mints.
func (m *Materializer) LabelApplier() labels.Applier {
	return labelApplier{m: m}
}

// Account label withholds. While an account `!takedown` stands for a bridged
// person's bare DID (not negated, exp not passed), the DID has one active
// account-scope label row (owner scope, no CID), which covers every blob the
// DID owns, including blobs stored later. The negation or the label's lapse
// lifts that row alone, so a CID another withhold covers stays covered. A
// bridged community's DID gets the community rule instead
// (community_label_withholds.go). A DID that is not bridged yet keeps its label stored, and the
// person's mint applies it (accountLabelWithholdTx from mintAndUpsert's
// insert callback).
//
// The label's transaction and the mint's both take the DID's account lock
// before reading the other side, so a label committed concurrently with the
// mint is either seen by the mint or sees the minted person.

// isAccountSubject reports whether a label subject is a bare DID (an account
// or community label) rather than a record at-uri.
func isAccountSubject(uri string) bool {
	_, err := syntax.ParseDID(uri)
	return err == nil
}

// accountLabelWithholdTx brings did's account-scope withhold in line with
// its current account `!takedown`, inside tx: it adds the row while the label
// stands and lifts it otherwise, when did is a bridged person. Any other DID
// is left alone.
func accountLabelWithholdTx(ctx context.Context, tx *sql.Tx, did string) error {
	if err := store.LockAccountTx(ctx, tx, did); err != nil {
		return err
	}
	var actorType string
	err := tx.QueryRowContext(ctx, `SELECT actor_type FROM bridged_actors WHERE did = $1`, did).Scan(&actorType)
	if stderrors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("materialize: read bridged actor type of %s: %w", did, err)
	}
	if store.ActorType(actorType) != store.ActorTypePerson {
		return nil
	}
	standing, err := labels.StandingTx(ctx, tx, did, LabelTakedown)
	if err != nil {
		return err
	}
	if !standing {
		if err := blobwithhold.LiftOwnerLabelWithhold(ctx, tx, did); err != nil {
			return fmt.Errorf("materialize: lift account label withhold of %s: %w", did, err)
		}
		return nil
	}
	if err := blobwithhold.AddOwnerLabelWithhold(ctx, tx, did); err != nil {
		return fmt.Errorf("materialize: withhold labeled account %s: %w", did, err)
	}
	return nil
}

// labelWithholdOnCommit applies the label rule in a subject record's commit
// transaction. While a `!takedown` stands for its at-uri it gives any row the
// author's delete marked author-deleted the label-takedown code again (the
// commit is the author's undelete, or a later edit), so a later negation
// lifts it, and adds a label withhold for every blob the committed record
// references. With no label standing it lifts every label row of the subject:
// label rows exist only because of a label, and the only ones a negation
// leaves active are those an author's delete marked, which the record's
// return releases. Other sources' rows are not touched.
func labelWithholdOnCommit(ctx context.Context, tx *sql.Tx, did, subjectURI string, record map[string]any) error {
	standing, err := labels.StandingTx(ctx, tx, subjectURI, LabelTakedown)
	if err != nil {
		return err
	}
	if !standing {
		if err := blobwithhold.LiftSubjectLabelWithholds(ctx, tx, subjectURI); err != nil {
			return fmt.Errorf("materialize: lift label withholds of %s on commit: %w", subjectURI, err)
		}
		return nil
	}
	if err := blobwithhold.UnmarkSubjectLabelWithholdsAuthorDeleted(ctx, tx, subjectURI); err != nil {
		return fmt.Errorf("materialize: unmark label withholds of %s on commit: %w", subjectURI, err)
	}
	cids, err := committedBlobCIDs(record)
	if err != nil {
		return err
	}
	if err := blobwithhold.AddSubjectLabelWithholds(ctx, tx, did, subjectURI, cids); err != nil {
		return fmt.Errorf("materialize: withhold labeled %s: %w", subjectURI, err)
	}
	return nil
}
