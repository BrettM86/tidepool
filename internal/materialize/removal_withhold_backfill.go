package materialize

import (
	"context"
	"database/sql"
	"fmt"
	"slices"

	"tidepool/internal/blobwithhold"
	"tidepool/internal/errors"
	"tidepool/internal/repo"
)

// RemovalWithholdBackfillReport is what one back-fill run did (or, in a dry
// run, would do).
type RemovalWithholdBackfillReport struct {
	DryRun bool `json:"dry_run"`
	// Added lists each subject that got (would get) at least one new row.
	Added []RemovalWithholdAddition `json:"added"`
	// AlreadyWithheld counts hosted subjects that needed no new row.
	AlreadyWithheld int `json:"already_withheld"`
	// SkippedUnhosted counts removal records whose subject record is not in a
	// repo this bridge hosts.
	SkippedUnhosted int `json:"skipped_unhosted"`
	// Failed counts removal records the back-fill could not process, and
	// community repos it could not list; each is logged and the walk goes on.
	Failed int `json:"failed"`
}

// RemovalWithholdAddition is one subject's new community-removal rows.
type RemovalWithholdAddition struct {
	Subject   string   `json:"subject"`
	Community string   `json:"community"`
	CIDs      []string `json:"cids"`
}

// BackfillRemovalWithholds gives every subject removed by one of
// communityDIDs, and hosted here, the community-removal withholds a removal
// writes today: the one-time repair for removal records written before
// removals withheld blobs. It walks each community repo's
// social.coves.community.removal records (posts and comments) and, for each
// hosted subject, inserts a row under the removal record's code for every CID
// the subject's current record references that has no active row. It only
// adds: rows already there, or for CIDs the record no longer references, are
// the removal_withholds.go rule's to keep or lift. It is idempotent; a dry run
// computes the same report and writes nothing.
//
// Each removal record, its subject's record and rows are read, and the rows
// written, in one transaction that holds the global commit lock and makes no
// repo commit (so it needs no signing key, and a tombstoned community's
// removals are still back-filled). The reads see every earlier commit: an
// edit, restore or removal that committed first has already applied the rule,
// and one that commits later applies it after. A removal deleted or rewritten
// since the walk is skipped, so the back-fill never withholds a restored
// subject. A removal record or community repo that fails is logged, counted
// in Failed, and the walk goes on; only a cancelled ctx stops it.
func (m *Materializer) BackfillRemovalWithholds(ctx context.Context, communityDIDs []string, dryRun bool) (*RemovalWithholdBackfillReport, error) {
	report := &RemovalWithholdBackfillReport{DryRun: dryRun, Added: []RemovalWithholdAddition{}}
	for _, communityDID := range communityDIDs {
		// A cancelled request stops the walk rather than failing every record
		// left in it.
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		entries, err := m.repos.ListRecords(ctx, communityDID)
		if errors.IsNotFound(err) {
			continue
		}
		if err != nil {
			m.logger.ErrorContext(ctx, "removal withhold back-fill: list community records",
				"community", communityDID, "error", err)
			report.Failed++
			continue
		}
		for _, entry := range entries {
			if entry.Collection != CollectionRemoval {
				continue
			}
			if err := m.backfillRemovalWithhold(ctx, communityDID, entry, dryRun, report); err != nil {
				m.logger.ErrorContext(ctx, "removal withhold back-fill: removal record failed",
					"community", communityDID, "rkey", entry.Rkey, "error", err)
				report.Failed++
			}
		}
	}
	return report, nil
}

// backfillRemovalWithhold applies the back-fill to one removal record and
// adds its outcome to report.
func (m *Materializer) backfillRemovalWithhold(ctx context.Context, communityDID string, entry repo.RecordEntry, dryRun bool, report *RemovalWithholdBackfillReport) error {
	subject, _ := entry.Value["subject"].(map[string]any)
	subjectURI, _ := subject["uri"].(string)
	if subjectURI == "" {
		return errors.NewValidationError("subject",
			fmt.Sprintf("removal %s/%s/%s names no subject uri", communityDID, entry.Collection, entry.Rkey))
	}
	code, _ := entry.Value["code"].(string)
	if code == "" {
		return errors.NewValidationError("code",
			fmt.Sprintf("removal %s/%s/%s carries no code", communityDID, entry.Collection, entry.Rkey))
	}

	var hosted, gone bool
	var missing []string
	err := m.repos.WithCommitLock(ctx, communityDID, func(ctx context.Context, tx *sql.Tx) error {
		_, currentCID, err := m.repos.GetRecordTx(ctx, tx, communityDID, CollectionRemoval, entry.Rkey)
		if errors.IsNotFound(err) {
			gone = true
			return nil
		}
		if err != nil {
			return fmt.Errorf("materialize: read removal %s/%s/%s: %w", communityDID, CollectionRemoval, entry.Rkey, err)
		}
		if currentCID != entry.CID {
			gone = true
			return nil
		}
		ownerDID, cids, isHosted, err := m.hostedSubjectBlobCIDs(ctx, tx, subjectURI)
		if err != nil {
			return err
		}
		hosted, missing = isHosted, nil
		if !hosted {
			return nil
		}
		active, err := blobwithhold.ActiveSubjectRemovalCIDs(ctx, tx, subjectURI)
		if err != nil {
			return err
		}
		for _, c := range cids {
			canonical, err := blobwithhold.Canonicalize(c)
			if err != nil {
				return err
			}
			if !slices.Contains(active, canonical) {
				missing = append(missing, canonical)
			}
		}
		if dryRun || len(missing) == 0 {
			return nil
		}
		return blobwithhold.AddSubjectRemovalWithholds(ctx, tx, ownerDID, subjectURI, code, missing)
	})
	if err != nil {
		return fmt.Errorf("materialize: back-fill withholds of %s in %s: %w", subjectURI, communityDID, err)
	}
	if gone {
		// Restored (or rewritten) since the walk: the commit that changed it
		// set the subject's withholds itself.
		return nil
	}
	if !hosted {
		report.SkippedUnhosted++
		return nil
	}
	if len(missing) == 0 {
		report.AlreadyWithheld++
		return nil
	}
	report.Added = append(report.Added, RemovalWithholdAddition{
		Subject: subjectURI, Community: communityDID, CIDs: missing,
	})
	return nil
}
