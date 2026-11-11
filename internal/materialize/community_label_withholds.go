package materialize

import (
	"context"
	"database/sql"
	"expvar"
	"fmt"

	"github.com/bluesky-social/indigo/atproto/syntax"

	"tidepool/internal/blobwithhold"
	"tidepool/internal/labels"
	"tidepool/internal/repo"
	"tidepool/internal/store"
)

// Community label withholds. While a `!takedown` stands for a bridged
// community's bare DID (a communities row has that DID), every blob CID of
// every hosted bridged record bound to the community (ap_objects.community_did)
// has an active community-scope label row naming the community, the record's
// repo DID as owner, and the CID. The rows cover a CID for its owner only, so
// the same bytes under an owner outside the community, and the owner's blobs
// outside it, are served. They are add-only while the label stands: a record
// committed into the community (new, or an edit) adds rows for every blob it
// references in its own commit. The negation (or the label's lapse) lifts the
// community's rows and no other row, so a record label's or an operator's
// withhold of the same CID stays.
//
// The rule is decided by the label alone (labels.StandingTx on the bare DID),
// on both sides: in the label's transaction the community removal ingest
// records may not be written or cleared yet when this applier runs, and a
// commit that added rows on a removal the negation no longer sees would leave
// rows nothing lifts.
//
// Both sides read under the global repo commit lock (repo.LockCommitsTx in the
// label's transaction, the repo commit's own lock in commitRecord), so a
// record committed concurrently with the label is either seen by the label's
// scan of the community or sees the label in its commit.

// CommunityLabelUnresolvedRecords counts the bridged records a community
// label's scan found with no stored community (ap_objects.community_did NULL)
// whose community could not be derived from the stored record either, so no
// community label covers their blobs. Each scan counts every such row again.
var CommunityLabelUnresolvedRecords = expvar.NewInt("tidepool_community_label_unresolved_records")

// bridgedCommunityTx reports whether did is a bridged community's DID.
func bridgedCommunityTx(ctx context.Context, tx *sql.Tx, did string) (bool, error) {
	var exists bool
	if err := tx.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM communities WHERE did = $1)`, did).Scan(&exists); err != nil {
		return false, fmt.Errorf("materialize: classify %s as a community: %w", did, err)
	}
	return exists, nil
}

// communityLabelWithholdTx brings the community-scope withholds of the
// community with did in line with its current `!takedown`, inside the
// label's transaction: while the label stands it adds a row for every blob of
// every hosted bridged record bound to the community, and otherwise lifts the
// community's rows. Idempotent, so Activate replays it.
func (m *Materializer) communityLabelWithholdTx(ctx context.Context, tx *sql.Tx, did string) error {
	// Serialize with repo commits before reading the community's records, so
	// a record committing concurrently is either listed here or sees the label.
	if err := repo.LockCommitsTx(ctx, tx); err != nil {
		return err
	}
	standing, err := labels.StandingTx(ctx, tx, did, LabelTakedown)
	if err != nil {
		return err
	}
	if !standing {
		if err := blobwithhold.LiftCommunityLabelWithholds(ctx, tx, did); err != nil {
			return fmt.Errorf("materialize: lift community label withholds of %s: %w", did, err)
		}
		return nil
	}
	subjects, err := communityRecordURIsTx(ctx, tx, did)
	if err != nil {
		return err
	}
	unbound, err := m.bindUnboundRecordsTx(ctx, tx, did)
	if err != nil {
		return err
	}
	subjects = append(subjects, unbound...)
	for _, subjectURI := range subjects {
		owner, cids, hosted, err := m.hostedSubjectBlobCIDs(ctx, tx, subjectURI)
		if err != nil {
			return err
		}
		if !hosted || len(cids) == 0 {
			continue
		}
		if err := blobwithhold.AddCommunityLabelWithholds(ctx, tx, did, owner, cids); err != nil {
			return fmt.Errorf("materialize: withhold %s in labeled community %s: %w", subjectURI, did, err)
		}
	}
	return nil
}

// communityRecordURIsTx lists, read in tx, the at-uris of the live bridged
// records bound to the community with did. They are read in full before the
// caller reads any record, so the transaction runs one statement at a time.
func communityRecordURIsTx(ctx context.Context, tx *sql.Tx, did string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT at_uri FROM ap_objects
		WHERE community_did = $1 AND origin = $2 AND deleted_at IS NULL
		ORDER BY at_uri`, did, string(store.OriginFediverse))
	if err != nil {
		return nil, fmt.Errorf("materialize: list records of community %s: %w", did, err)
	}
	defer func() { _ = rows.Close() }()
	var uris []string
	for rows.Next() {
		var uri string
		if err := rows.Scan(&uri); err != nil {
			return nil, fmt.Errorf("materialize: list records of community %s: scan: %w", did, err)
		}
		uris = append(uris, uri)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("materialize: list records of community %s: %w", did, err)
	}
	return uris, nil
}

// bindUnboundRecordsTx finds, read in tx, the live bridged content records
// with no stored community (ap_objects.community_did NULL: postv2 and comment
// rows written before the column existed, and comments whose thread's
// community did not resolve when they were committed), derives each one's
// community from its stored record (CommunityDIDOf), and writes the derived
// binding into the row, in the label's transaction. It returns the at-uris of
// those bound to the community with did, so the label covers their blobs; and
// because the binding commits with the label, the SQL readers of the column
// later in this transaction and after it (the outbound parent check and the
// community's held-reply release) see these records as the community's too.
// A row whose community cannot be derived stays NULL and is counted
// (CommunityLabelUnresolvedRecords) and logged: no community label covers it.
//
// The candidates cannot be narrowed to the community: a row with no stored
// community names none, and any bridged person's legacy record can be in it.
// So every unbound live row is read. Each derivable one is bound here, for
// every community and not only this one, so a later scan reads only the rows
// that could not be derived.
func (m *Materializer) bindUnboundRecordsTx(ctx context.Context, tx *sql.Tx, did string) ([]string, error) {
	candidates, err := unboundRecordsTx(ctx, tx)
	if err != nil {
		return nil, err
	}
	records := txRecordGetter{repos: m.repos, tx: tx}
	var bound []string
	var unresolved int64
	for i := range candidates {
		mapping := &candidates[i]
		communityDID, err := CommunityDIDOf(ctx, records, mapping)
		if err != nil {
			return nil, fmt.Errorf("materialize: community of unbound %s: %w", mapping.ATURI, err)
		}
		if _, parseErr := syntax.ParseDID(communityDID); parseErr != nil {
			unresolved++
			continue
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE ap_objects SET community_did = $2 WHERE ap_id = $1 AND community_did IS NULL`,
			mapping.APID, communityDID); err != nil {
			return nil, fmt.Errorf("materialize: bind %s to community %s: %w", mapping.ATURI, communityDID, err)
		}
		if communityDID == did {
			bound = append(bound, mapping.ATURI)
		}
	}
	if unresolved > 0 {
		CommunityLabelUnresolvedRecords.Add(unresolved)
		m.logger.Warn("community label: bridged records with no derivable community are not covered by it",
			"community", did, "records", unresolved)
	}
	return bound, nil
}

// unboundRecordsTx lists, read in tx and in full before any record is read,
// the live bridged content rows with no stored community.
func unboundRecordsTx(ctx context.Context, tx *sql.Tx) ([]store.APObjectMapping, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT ap_id, did, collection, rkey, at_uri FROM ap_objects
		WHERE community_did IS NULL AND origin = $1 AND deleted_at IS NULL
		  AND collection IN ($2, $3, $4)
		ORDER BY at_uri`,
		string(store.OriginFediverse), CollectionPost, CollectionPostV2, CollectionComment)
	if err != nil {
		return nil, fmt.Errorf("materialize: list records with no stored community: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var mappings []store.APObjectMapping
	for rows.Next() {
		var mapping store.APObjectMapping
		if err := rows.Scan(&mapping.APID, &mapping.DID, &mapping.Collection, &mapping.RKey, &mapping.ATURI); err != nil {
			return nil, fmt.Errorf("materialize: list records with no stored community: scan: %w", err)
		}
		mappings = append(mappings, mapping)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("materialize: list records with no stored community: %w", err)
	}
	return mappings, nil
}

// txRecordGetter reads records in tx, for CommunityDIDOf inside the label's
// transaction.
type txRecordGetter struct {
	repos *repo.Manager
	tx    *sql.Tx
}

func (g txRecordGetter) GetRecord(ctx context.Context, did, collection, rkey string) (map[string]any, string, error) {
	return g.repos.GetRecordTx(ctx, g.tx, did, collection, rkey)
}

// communityLabelWithholdOnCommit applies the community label rule in the
// commit transaction of a record bound to communityDID (empty for a record
// bound to none): while the community's `!takedown` stands, every blob the
// committed record references gets a community-scope row for did, the
// record's repo DID. A row an author's delete marked author-deleted for one of
// those blobs is the record's undelete: it gets the label-takedown code back
// while the label stands, and is lifted otherwise. Nothing else is lifted
// here; only the negation lifts.
func communityLabelWithholdOnCommit(ctx context.Context, tx *sql.Tx, did, communityDID string, record map[string]any) error {
	if communityDID == "" {
		return nil
	}
	standing, err := labels.StandingTx(ctx, tx, communityDID, LabelTakedown)
	if err != nil {
		return err
	}
	cids, err := committedBlobCIDs(record)
	if err != nil {
		return err
	}
	if len(cids) == 0 {
		return nil
	}
	if err := blobwithhold.ClearCommunityLabelWithholdsAuthorDeleted(ctx, tx, communityDID, did, cids, standing); err != nil {
		return fmt.Errorf("materialize: clear author-deleted withholds of %s in community %s on commit: %w", did, communityDID, err)
	}
	if !standing {
		return nil
	}
	if err := blobwithhold.AddCommunityLabelWithholds(ctx, tx, communityDID, did, cids); err != nil {
		return fmt.Errorf("materialize: withhold committed record of %s in labeled community %s: %w", did, communityDID, err)
	}
	return nil
}
