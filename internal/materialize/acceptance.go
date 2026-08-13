package materialize

import (
	"context"
	stderrors "errors"
	"fmt"
	"time"

	"tidepool/internal/errors"
	"tidepool/internal/repo"
)

// acceptPost writes the community's acceptance of one post: the attestation
// that makes the post VISIBLE in that community. Coves renders community
// surfaces from acceptance records alone, so a postv2 with no acceptance is a
// post nobody can see — the record exists, the author owns it, and it appears
// nowhere.
//
// This is MECHANISM, not policy. It attests what the bridge already decided by
// materializing the post; it does not decide whether the post should be in the
// community. The removal record for a subject shares this exact key (same
// digest, different collection), which is where a policy layer that had to
// refuse a re-acceptance would read.
//
// WHY THIS CANNOT SHARE THE POST'S TRANSACTION. The postv2 lands in the
// AUTHOR's repo and the acceptance in the COMMUNITY's — two repos, two
// commits, and a crash between them leaves the post invisible. Nothing
// reconciles that gap on its own, so the heal is redelivery: acceptPost runs
// on EVERY materialization of a post, including one whose postv2 commit was an
// idempotent no-op. Conditioning the acceptance on a fresh post CID would mean
// a post lost to the crash window stays lost until somebody edits it upstream.
//
// WHY createdAt IS DERIVED, NEVER STAMPED. Redelivery must produce
// byte-identical bytes so the repo layer's no-op path absorbs it. A wall-clock
// createdAt would mint a community-repo firehose event on every redelivery and
// make Coves re-run admission on a post nothing changed about. On a REPIN —
// the author edited, so the subject CID moved — the stored createdAt is
// carried forward rather than restamped: the community accepted this post
// once, and re-pinning the version it accepts is not a new acceptance.
func (m *Materializer) acceptPost(ctx context.Context, communityDID, postURI, postCID string, publishedAt time.Time) error {
	rkey := SubjectRKey(postURI)

	// Read-modify-write under a CAS precondition, bounded like the stats
	// stamp: the read happens outside the commit serialization, so a racing
	// repin (or a stats-driven CID change on the subject) can move the record
	// underneath it. Losing that race means re-reading, never overwriting.
	for attempt := 0; ; attempt++ {
		createdAt := recordDatetime(publishedAt)
		expectPrevCID := ""

		stored, storedCID, err := m.repos.GetRecord(ctx, communityDID, CollectionAcceptance, rkey)
		switch {
		case err == nil:
			expectPrevCID = storedCID
			if when, ok := stored["createdAt"].(string); ok && when != "" {
				createdAt = when
			}
		case errors.IsNotFound(err):
			// Either the first acceptance or the crash-window heal. The empty
			// precondition asserts the record is still absent, so a concurrent
			// writer that got there first sends us round the loop instead of
			// clobbering its acceptance.
		default:
			return fmt.Errorf("materialize: read acceptance %s/%s/%s: %w",
				communityDID, CollectionAcceptance, rkey, err)
		}

		record := map[string]any{
			"$type":     CollectionAcceptance,
			"subject":   strongRef(postURI, postCID),
			"createdAt": createdAt,
		}
		if err := m.validateRecord(record); err != nil {
			return err
		}

		// Deliberately NOT commitRecord: that path upserts an ap_objects row,
		// and an acceptance has no AP object behind it — the bridge mints it as
		// the community's own attestation. A mapping would invent an ap_id for
		// it, expose it to every spine consumer, and let an announced delete
		// aimed at the POST address the acceptance through the same key space.
		_, err = m.repos.PutRecordCAS(ctx, communityDID, CollectionAcceptance, rkey, record, expectPrevCID, nil)
		if stderrors.Is(err, repo.ErrPreconditionFailed) {
			if attempt+1 < maxStatsCommitAttempts {
				continue
			}
			return fmt.Errorf("materialize: accept %s: acceptance kept changing across %d attempts: %w",
				postURI, maxStatsCommitAttempts, err)
		}
		if err != nil {
			return fmt.Errorf("materialize: put acceptance %s/%s/%s for %s: %w",
				communityDID, CollectionAcceptance, rkey, postURI, err)
		}
		return nil
	}
}
