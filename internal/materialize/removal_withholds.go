package materialize

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"strings"

	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/ipfs/go-cid"

	"tidepool/internal/blobwithhold"
	"tidepool/internal/errors"
	"tidepool/internal/repo"
)

// Community-removal withholds. One rule per subject: while the community's
// removal record for the subject stands AND the subject record lives in a repo
// this bridge hosts, every blob CID its current record references has an
// active community-removal withhold under the removal record's code. The rows
// are add-only while the removal stands: a CID a later version of the record
// drops keeps its row, and so does every row after the subject record is
// deleted, because neither an edit, a failed image fetch, nor a plain delete
// deletes the bytes, and lifting would serve the removed image. A restore (the
// removal record's delete) lifts them all. The author's own delete of a
// removed subject deletes no bytes and lifts no row (HandleAuthorDeleteRecord,
// decision 2026-11-08). The rule is applied inside the
// commit that changes either side of it — the removal write, the restore, and
// every re-materialization of a subject — so a failed withhold write rolls the
// change back and the activity's retry converges.
//
// Every input the rule reads (the removal record, the subject record) is read
// inside that commit's transaction. Every repo commit holds the global commit
// lock (repo commitAdvisoryLockKey) until it commits, so those reads see every
// commit before this one and no other commit can land between the read and
// the write: a removal, a restore and an edit of the same subject racing on
// different repos apply the rule in commit order.
//
// A NATIVE subject (its record is in the author's own PDS, not here) gets no
// rows: the bridge serves none of its blobs.
//
// Rows name no community: a subject is removed by the one community it was
// posted to, so a restore lifts every community-removal row of the subject.

// removalWithholdSync returns the commit side effect that adds a
// community-removal withhold, under code, for every blob the subject's current
// record references, and lifts none; it writes nothing when the subject record
// is not in a repo this bridge hosts. The record is read in the commit's
// transaction.
func (m *Materializer) removalWithholdSync(subjectURI, code string) repo.TxSideEffect {
	return func(ctx context.Context, tx *sql.Tx, _ *repo.CommitResult) error {
		did, cids, hosted, err := m.hostedSubjectBlobCIDs(ctx, tx, subjectURI)
		if err != nil || !hosted {
			return err
		}
		return blobwithhold.AddSubjectRemovalWithholds(ctx, tx, did, subjectURI, code, cids)
	}
}

// hostedSubjectBlobCIDs reads the subject record, in tx, from the repos this
// bridge hosts and returns its repo DID and the blob CIDs it references.
// hosted is false when no such record is here (a native subject, or one
// deleted since).
func (m *Materializer) hostedSubjectBlobCIDs(ctx context.Context, tx *sql.Tx, subjectURI string) (did string, cids []string, hosted bool, err error) {
	did, collection, rkey, ok := splitATURI(subjectURI)
	if !ok {
		return "", nil, false, errors.NewValidationError("subject", "not an at-uri: "+subjectURI)
	}
	record, _, err := m.repos.GetRecordTx(ctx, tx, did, collection, rkey)
	if errors.IsNotFound(err) {
		return "", nil, false, nil
	}
	if err != nil {
		return "", nil, false, fmt.Errorf("materialize: read removed subject %s: %w", subjectURI, err)
	}
	return did, recordBlobCIDs(record), true, nil
}

// standingRemovalCode returns, read in tx, the code of communityDID's
// standing removal of the subject, for the collections a community removal
// applies to (postv2 and comments). stands is false when no removal stands;
// anything else, or content bound to no community, is never removed.
func (m *Materializer) standingRemovalCode(ctx context.Context, tx *sql.Tx, collection, subjectURI, communityDID string) (code string, stands bool, err error) {
	if communityDID == "" || (collection != CollectionPostV2 && collection != CollectionComment) {
		return "", false, nil
	}
	rkey := SubjectRKey(subjectURI)
	removal, _, err := m.repos.GetRecordTx(ctx, tx, communityDID, CollectionRemoval, rkey)
	if errors.IsNotFound(err) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("materialize: read removal %s/%s/%s: %w",
			communityDID, CollectionRemoval, rkey, err)
	}
	code, _ = removal["code"].(string)
	if code == "" {
		return "", false, errors.NewValidationError("code",
			fmt.Sprintf("removal %s/%s/%s carries no code", communityDID, CollectionRemoval, rkey))
	}
	return code, true, nil
}

// removalWithholdLift is the commit side effect that lifts every active
// community-removal withhold of the subject.
func removalWithholdLift(subjectURI string) repo.TxSideEffect {
	return func(ctx context.Context, tx *sql.Tx, _ *repo.CommitResult) error {
		return blobwithhold.LiftSubjectRemovalWithholds(ctx, tx, subjectURI)
	}
}

// errRestoreSubjectGone rolls back a restore whose hosted subject record was
// deleted after the restore read it and before the restore's commit.
var errRestoreSubjectGone = stderrors.New("materialize: restore subject record is gone")

// hostedRestoreLift is a hosted subject's restore side effect: it reads the
// subject record in the restore's commit transaction, under the commit lock,
// and lifts the subject's community-removal withholds only when the record is
// still there (and, with a non-empty pinnedCID, still at that version). A
// record deleted since the restore read it fails the commit with
// errRestoreSubjectGone, so the removal record stands and no acceptance or
// lift lands: an author delete of a removed subject keeps its bytes refused
// until an operator acts (decision 2026-11-08), and a restore racing it must
// not serve them. A record edited since fails with a plain error, so a retry
// pins the new version.
func (m *Materializer) hostedRestoreLift(subjectURI, pinnedCID string) repo.TxSideEffect {
	return func(ctx context.Context, tx *sql.Tx, res *repo.CommitResult) error {
		did, collection, rkey, ok := splitATURI(subjectURI)
		if !ok {
			return errors.NewValidationError("subject", "not an at-uri: "+subjectURI)
		}
		_, currentCID, err := m.repos.GetRecordTx(ctx, tx, did, collection, rkey)
		if errors.IsNotFound(err) {
			return errRestoreSubjectGone
		}
		if err != nil {
			return fmt.Errorf("materialize: read restored subject %s: %w", subjectURI, err)
		}
		if pinnedCID != "" && currentCID != pinnedCID {
			return fmt.Errorf("materialize: restored subject %s moved from %s to %s before the restore committed",
				subjectURI, pinnedCID, currentCID)
		}
		return removalWithholdLift(subjectURI)(ctx, tx, res)
	}
}

// committedBlobCIDs is the blob CID set of a record about to be committed,
// read from its DAG-CBOR round trip so it names exactly what the committed
// record references, whatever Go types the builder used for its blob refs.
func committedBlobCIDs(record map[string]any) ([]string, error) {
	encoded, err := atdata.MarshalCBOR(record)
	if err != nil {
		return nil, fmt.Errorf("materialize: encode record for blob references: %w", err)
	}
	decoded, err := atdata.UnmarshalCBOR(encoded)
	if err != nil {
		return nil, fmt.Errorf("materialize: decode record for blob references: %w", err)
	}
	return recordBlobCIDs(decoded), nil
}

// recordBlobCIDs lists the distinct blob CIDs a stored record references.
func recordBlobCIDs(record map[string]any) []string {
	blobs := atdata.ExtractBlobs(record)
	seen := make(map[string]struct{}, len(blobs))
	cids := make([]string, 0, len(blobs))
	for _, blob := range blobs {
		c := cid.Cid(blob.Ref).String()
		if _, dup := seen[c]; dup {
			continue
		}
		seen[c] = struct{}{}
		cids = append(cids, c)
	}
	return cids
}

// splitATURI splits at://did/collection/rkey.
func splitATURI(uri string) (did, collection, rkey string, ok bool) {
	rest, ok := strings.CutPrefix(uri, "at://")
	if !ok {
		return "", "", "", false
	}
	did, rest, ok = strings.Cut(rest, "/")
	if !ok {
		return "", "", "", false
	}
	collection, rkey, ok = strings.Cut(rest, "/")
	if !ok || did == "" || collection == "" || rkey == "" {
		return "", "", "", false
	}
	return did, collection, rkey, true
}
