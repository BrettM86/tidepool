// Package acceptrec owns the shared mechanics of a community's acceptance and
// removal records: the digest record key both share (SubjectRKey), and the
// multi-op, side-effect-parameterized commits that write, repin and delete
// them. It is a LEAF: it depends only on internal/repo (the commit primitive)
// and internal/errors, so both the materializer (which drives it with a nil
// side effect, characterizing the existing behavior) and the acceptance engine
// (which drives it with an outbound enqueue as the side effect) build on ONE
// implementation of the acceptance-commit discipline instead of two that drift.
//
// The subject of every record here is the post's atproto at-uri. Two engines
// write into the same community repos — Coves' own for native posts, this
// bridge for bridged ones — so SubjectRKey MUST byte-match Coves' derivation
// or a post acquires two acceptance keys and neither side can see the other's.
package acceptrec

import (
	"context"
	"crypto/sha256"
	"encoding/base32"
	stderrors "errors"
	"fmt"
	"strings"
	"time"

	"tidepool/internal/errors"
	"tidepool/internal/repo"
)

// maxAcceptAttempts bounds the CAS retry loop: a subject whose acceptance keeps
// moving underneath the read-modify-write (a racing repin or a stats stamp)
// eventually gives up rather than spinning forever. Matches the materializer's
// stats-commit cap it was lifted from.
const maxAcceptAttempts = 4

// Record collections written here. They must match Coves' and the
// materializer's exactly (a post's acceptance and removal share ONE digest
// rkey, one per subject).
const (
	// CollectionAcceptance is the community's attestation that it accepts a
	// post — what makes a postv2 visible in the community at all.
	CollectionAcceptance = "social.coves.community.acceptance"
	// CollectionRemoval is the community's record that a post was removed. It
	// shares the acceptance's digest rkey and replaces it in one atomic commit.
	CollectionRemoval = "social.coves.community.removal"
)

// ErrRemovalStands is returned by AcceptSubject when a removal record already
// stands at the subject's rkey. A removal is terminal — exited only by an
// explicit restore — so a fresh acceptance is refused, the acceptance record is
// NOT written, and the side effect is NOT run. The caller (the engine) treats
// it as a decided-out post, not a failure to retry.
var ErrRemovalStands = stderrors.New("acceptrec: a removal stands at the subject rkey")

// RepoManager is the slice of *repo.Manager the acceptance commits need: the
// multi-op, side-effect-parameterized commit and a point read of the current
// acceptance (to carry createdAt forward and derive the CAS precondition).
type RepoManager interface {
	ApplyOpsTx(ctx context.Context, did string, ops []repo.RecordOp, sideEffect repo.TxSideEffect) (*repo.CommitResult, error)
	GetRecord(ctx context.Context, did, collection, rkey string) (record map[string]any, recordCID string, err error)
}

// subjectRKeyEncoding is RFC 4648 base32 (STANDARD alphabet), padding dropped
// and lowercased so the key stays inside the atProto record-key charset. This
// must match materialize.SubjectRKey and Coves' posts.SubjectRkey byte for byte.
var subjectRKeyEncoding = base32.StdEncoding.WithPadding(base32.NoPadding)

// SubjectRKey derives the record key a community's acceptance and removal for
// one post share: the unpadded lowercase base32 of SHA-256 of the post's
// at-uri, a fixed 52 rkey-safe characters. The argument is BYTES, not a parsed
// URI — no normalization — because those bytes are the identity Coves indexes
// under. See the golden vectors in the test beside this file.
func SubjectRKey(subjectATURI string) string {
	digest := sha256.Sum256([]byte(subjectATURI))
	return strings.ToLower(subjectRKeyEncoding.EncodeToString(digest[:]))
}

// AcceptSubject writes the community's acceptance of one post at SubjectRKey,
// with a removal guard (an inert delete of the removal at that rkey with a
// "must not exist" precondition, so the batch fails if a removal stands), and
// runs sideEffect INSIDE the acceptance commit transaction — the outbound
// enqueue rides here, so acceptance and enqueue land together or not at all.
//
// createdAt is derived deterministically (carried forward from any existing
// acceptance, else from publishedAt) so a redelivery re-puts byte-identical
// bytes and the repo layer's NoOp path absorbs it — but the side effect STILL
// runs on that NoOp path (at-least-once enqueue). A standing removal returns
// ErrRemovalStands with no acceptance written and the side effect not run.
func AcceptSubject(ctx context.Context, repos RepoManager, communityDID, subjectURI, subjectCID string, publishedAt time.Time, sideEffect repo.TxSideEffect) (*repo.CommitResult, error) {
	rkey := SubjectRKey(subjectURI)

	// Read-modify-write under a CAS precondition: the read happens outside the
	// commit serialization, so a racing repin (or a stats-driven CID change on
	// the subject) can move the record underneath it. Losing that race means
	// re-reading, never overwriting.
	for attempt := 0; ; attempt++ {
		createdAt := recordDatetime(publishedAt)
		expectPrevCID := ""

		stored, storedCID, err := repos.GetRecord(ctx, communityDID, CollectionAcceptance, rkey)
		switch {
		case err == nil:
			expectPrevCID = storedCID
			// createdAt is carried forward, never restamped: the community
			// accepted this post once, and re-pinning the version it accepts is
			// not a new acceptance. Deriving it (rather than stamping a clock)
			// is what makes a redelivery re-put byte-identical bytes.
			if when, ok := stored["createdAt"].(string); ok && when != "" {
				createdAt = when
			}
		case errors.IsNotFound(err):
			// Either the first acceptance or a crash-window heal. The empty
			// precondition asserts the record is still absent, so a concurrent
			// writer that got there first sends us round the loop instead of
			// clobbering its acceptance.
		default:
			return nil, fmt.Errorf("acceptrec: read acceptance %s/%s/%s: %w",
				communityDID, CollectionAcceptance, rkey, err)
		}

		record := map[string]any{
			"$type":     CollectionAcceptance,
			"subject":   strongRef(subjectURI, subjectCID),
			"createdAt": createdAt,
		}

		// The terminality guard is an inert op: deleting the removal at this
		// rkey with a precondition of "must not exist" claims nothing and
		// changes nothing, but it makes the batch FAIL if a removal has appeared
		// since the read above — so the refusal holds at COMMIT time, under the
		// commit's own locks, not merely at read time. Without it a RemovePost
		// landing in the window would leave an acceptance beside a standing
		// removal, a post simultaneously visible and removed.
		noRemoval := ""
		res, err := repos.ApplyOpsTx(ctx, communityDID, []repo.RecordOp{
			{Action: repo.OpActionDelete, Collection: CollectionRemoval, RKey: rkey, ExpectPrevCID: &noRemoval},
			{Action: repo.OpActionUpdate, Collection: CollectionAcceptance, RKey: rkey, Record: record, ExpectPrevCID: &expectPrevCID},
		}, sideEffect)
		if stderrors.Is(err, repo.ErrPreconditionFailed) {
			// Either a removal appeared or the acceptance moved. Ask which: a
			// removal means the community has since decided this post is out, and
			// that decision is terminal — the acceptance is refused and the side
			// effect (which the failed batch never reached) does not fire.
			removed, rerr := removalStands(ctx, repos, communityDID, rkey)
			if rerr != nil {
				return nil, rerr
			}
			if removed {
				return nil, ErrRemovalStands
			}
			if attempt+1 < maxAcceptAttempts {
				continue
			}
			return nil, fmt.Errorf("acceptrec: accept %s: acceptance kept changing across %d attempts: %w",
				subjectURI, maxAcceptAttempts, err)
		}
		if err != nil {
			return nil, fmt.Errorf("acceptrec: put acceptance %s/%s/%s: %w",
				communityDID, CollectionAcceptance, rkey, err)
		}
		return res, nil
	}
}

// removalStands reports whether the community currently holds a removal for the
// subject at rkey. Acceptance and removal share the digest key, so this is a
// point lookup rather than a search.
func removalStands(ctx context.Context, repos RepoManager, communityDID, rkey string) (bool, error) {
	_, _, err := repos.GetRecord(ctx, communityDID, CollectionRemoval, rkey)
	switch {
	case err == nil:
		return true, nil
	case errors.IsNotFound(err):
		return false, nil
	default:
		return false, fmt.Errorf("acceptrec: read removal %s/%s/%s: %w",
			communityDID, CollectionRemoval, rkey, err)
	}
}

// recordDatetime renders a timestamp in the atproto datetime format (RFC3339,
// UTC, millisecond precision). It must match materialize.recordDatetime byte for
// byte so a post accepted by either engine yields identical acceptance bytes.
func recordDatetime(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

// strongRef builds a com.atproto.repo.strongRef value.
func strongRef(uri, cid string) map[string]any {
	return map[string]any{"uri": uri, "cid": cid}
}

// DeleteAcceptance removes a post's acceptance from its community and runs
// sideEffect inside that commit — the author-delete path, whose side effect is
// the outbound Delete{Page} enqueue. A missing acceptance is inert (the batch
// commits nothing), but the side effect still runs so the retraction still goes
// out.
func DeleteAcceptance(ctx context.Context, repos RepoManager, communityDID, subjectURI string, sideEffect repo.TxSideEffect) (*repo.CommitResult, error) {
	rkey := SubjectRKey(subjectURI)
	res, err := repos.ApplyOpsTx(ctx, communityDID, []repo.RecordOp{
		{Action: repo.OpActionDelete, Collection: CollectionAcceptance, RKey: rkey},
	}, sideEffect)
	if err != nil {
		return nil, fmt.Errorf("acceptrec: delete acceptance %s/%s/%s: %w",
			communityDID, CollectionAcceptance, rkey, err)
	}
	return res, nil
}

// Remove atomically deletes the acceptance and writes a removal at the shared
// rkey (moderation / edit-fail), running sideEffect (the Delete{Page} enqueue)
// inside the commit.
func Remove(ctx context.Context, repos RepoManager, communityDID, subjectURI, subjectCID, code, reason string, at time.Time, sideEffect repo.TxSideEffect) (*repo.CommitResult, error) {
	rkey := SubjectRKey(subjectURI)
	// createdAt is the DECISION time (`at`, the engine's clock), but a standing
	// removal carries its createdAt FORWARD so a redelivery re-puts byte-identical
	// bytes and the repo layer's NoOp path absorbs it — only the first write
	// stamps the clock.
	createdAt := recordDatetime(at)
	if existing, _, err := repos.GetRecord(ctx, communityDID, CollectionRemoval, rkey); err == nil {
		if when, ok := existing["createdAt"].(string); ok && when != "" {
			createdAt = when
		}
	}
	removal := map[string]any{
		"$type":     CollectionRemoval,
		"subject":   strongRef(subjectURI, subjectCID),
		"code":      code,
		"createdAt": createdAt,
	}
	// Omitted rather than written blank: an empty reason renders in a moderation
	// log as a blank explanation instead of as none given.
	if reason != "" {
		removal["reason"] = reason
	}
	// One commit: the acceptance is withdrawn and the removal written together,
	// so the firehose never shows a window where the post is neither accepted
	// nor removed.
	res, err := repos.ApplyOpsTx(ctx, communityDID, []repo.RecordOp{
		{Action: repo.OpActionDelete, Collection: CollectionAcceptance, RKey: rkey},
		{Action: repo.OpActionUpdate, Collection: CollectionRemoval, RKey: rkey, Record: removal},
	}, sideEffect)
	if err != nil {
		return nil, fmt.Errorf("acceptrec: remove %s from %s: %w", subjectURI, communityDID, err)
	}
	return res, nil
}

// Restore is the inverse of Remove: it deletes the standing removal and writes a
// fresh acceptance at the shared rkey in ONE commit, running sideEffect (the
// re-delivery enqueue) inside it. It is what a corrective edit routes through
// after an admission-revoked removal — the removal was OUR decision, so a post
// that now passes admission is reinstated rather than left withdrawn. Unlike
// AcceptSubject, it does NOT trip the removal guard: deleting the removal is the
// point. createdAt is derived from publishedAt (a fresh acceptance stands for the
// current version), so a redelivery re-puts byte-identical bytes.
// expectRemovalCID is a REQUIRED compare-and-set token: the CID of the removal
// the caller inspected before deciding to reverse it. The delete only applies
// if that exact record is still standing, so an edit can never delete a removal
// it never read — a moderator replacing our admission-revoked removal with
// their own in the decision window would otherwise have theirs deleted, the
// acceptance written over it, and the side effect (the outbound enqueue) fired,
// pushing the post back at the community that just removed it. On a mismatch
// the commit returns repo.ErrPreconditionFailed and NOTHING runs, side effect
// included.
func Restore(ctx context.Context, repos RepoManager, communityDID, subjectURI, subjectCID, expectRemovalCID string, publishedAt time.Time, sideEffect repo.TxSideEffect) (*repo.CommitResult, error) {
	if expectRemovalCID == "" {
		return nil, errors.NewValidationError("expect_removal_cid",
			"a restore must name the removal it inspected")
	}
	rkey := SubjectRKey(subjectURI)
	acceptance := map[string]any{
		"$type":     CollectionAcceptance,
		"subject":   strongRef(subjectURI, subjectCID),
		"createdAt": recordDatetime(publishedAt),
	}
	// One commit: the removal is withdrawn and the acceptance written together,
	// so the firehose never shows a window where the post is neither removed nor
	// accepted.
	res, err := repos.ApplyOpsTx(ctx, communityDID, []repo.RecordOp{
		{Action: repo.OpActionDelete, Collection: CollectionRemoval, RKey: rkey, ExpectPrevCID: &expectRemovalCID},
		{Action: repo.OpActionUpdate, Collection: CollectionAcceptance, RKey: rkey, Record: acceptance},
	}, sideEffect)
	if err != nil {
		return nil, fmt.Errorf("acceptrec: restore %s into %s: %w", subjectURI, communityDID, err)
	}
	return res, nil
}
