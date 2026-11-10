package materialize

import (
	"context"
	stderrors "errors"
	"fmt"

	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/ipfs/go-cid"

	"tidepool/internal/errors"
	"tidepool/internal/repo"
	"tidepool/internal/store"
)

// aspectRatioBackfillPageSize is how many mappings one page of the back-fill
// walk reads (a variable so tests can walk several pages).
var aspectRatioBackfillPageSize = 200

// maxAspectRatioCommitAttempts bounds the compare-and-swap retries for one
// post, as maxStatsCommitAttempts does for a stats stamp.
const maxAspectRatioCommitAttempts = 4

// AspectRatioBackfillReport is what one back-fill run did (or, in a dry run,
// would do).
type AspectRatioBackfillReport struct {
	DryRun bool `json:"dry_run"`
	// Updated counts posts that got (would get) a new version, including a
	// post whose acceptance repin then failed (also counted in Failed).
	Updated int `json:"updated"`
	// ImagesSet counts images that got (would get) an aspectRatio.
	ImagesSet int `json:"images_set"`
	// AlreadySet counts images that already carried one.
	AlreadySet int `json:"already_set"`
	// UnreadableImages counts images left without one: their bytes are
	// withheld, missing, or have no readable header, or the entry carries no
	// blob ref.
	UnreadableImages int `json:"unreadable_images"`
	// WithoutImageEmbed counts posts with no images embed.
	WithoutImageEmbed int `json:"without_image_embed"`
	// Gone counts posts deleted while the run walked them, whose record is
	// missing behind a live mapping, or whose repo is frozen by a consent
	// revocation. Posts deleted before the run are not walked at all.
	Gone int `json:"gone"`
	// Failed counts posts the back-fill could not process; each is logged and
	// the walk goes on.
	Failed int `json:"failed"`
}

// aspectRatioOutcome is one post's share of the report.
type aspectRatioOutcome struct {
	updated, withoutImageEmbed, gone  bool
	imagesSet, alreadySet, unreadable int
}

func (r *AspectRatioBackfillReport) add(outcome aspectRatioOutcome) {
	if outcome.updated {
		r.Updated++
	}
	if outcome.withoutImageEmbed {
		r.WithoutImageEmbed++
	}
	if outcome.gone {
		r.Gone++
	}
	r.ImagesSet += outcome.imagesSet
	r.AlreadySet += outcome.alreadySet
	r.UnreadableImages += outcome.unreadable
}

// BackfillImageAspectRatios gives every live bridged post image without an
// aspectRatio the ratio of the bytes its blob ref names: the one-time repair
// for posts materialized before the bridge recorded ratios. The bytes are
// read from this bridge's blob store, so nothing is fetched upstream; an image
// whose bytes are withheld, missing or unreadable is left without one. Each
// post that changes is rewritten in one compare-and-swap commit that moves its
// mapping, then a postv2's acceptance is repinned. As on a stats stamp's no-op
// path, a post with nothing to set still has its acceptance reconciled, so a
// rerun heals a pin a cut-short run left on the old version. It only adds
// ratios and is idempotent; a dry run computes the same report and writes
// nothing. A post that fails is logged, counted in Failed, and the walk
// goes on; only a cancelled ctx or a failed page read stops it.
func (m *Materializer) BackfillImageAspectRatios(ctx context.Context, dryRun bool) (*AspectRatioBackfillReport, error) {
	report := &AspectRatioBackfillReport{DryRun: dryRun}
	collections := []string{CollectionPost, CollectionPostV2}
	var afterID int64
	for {
		mappings, err := m.objects.ListLiveFediverseAfter(ctx, collections, afterID, aspectRatioBackfillPageSize)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if err != nil {
			return nil, fmt.Errorf("materialize: aspect ratio back-fill: list posts after %d: %w", afterID, err)
		}
		if len(mappings) == 0 {
			return report, nil
		}
		for _, mapping := range mappings {
			// A cancelled request stops the walk rather than failing every
			// post left in it.
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			afterID = mapping.ID
			outcome, err := m.backfillAspectRatio(ctx, mapping, dryRun)
			// A post whose commit landed before its repin failed still
			// counts as updated, and as failed.
			report.add(outcome)
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			if err != nil {
				m.logger.ErrorContext(ctx, "aspect ratio back-fill: post failed",
					"at_uri", mapping.ATURI, "error", err)
				report.Failed++
			}
		}
	}
}

// backfillAspectRatio sets the missing aspect ratios on one post's images.
func (m *Materializer) backfillAspectRatio(ctx context.Context, mapping *store.APObjectMapping, dryRun bool) (aspectRatioOutcome, error) {
	for attempt := 0; ; attempt++ {
		record, prevCID, err := m.repos.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
		if errors.IsNotFound(err) {
			return aspectRatioOutcome{gone: true}, nil
		}
		if err != nil {
			return aspectRatioOutcome{}, fmt.Errorf("read record: %w", err)
		}
		var outcome aspectRatioOutcome
		embed, _ := record["embed"].(map[string]any)
		images, _ := embed["images"].([]any)
		if embed["$type"] != "social.coves.embed.images" || len(images) == 0 {
			outcome.withoutImageEmbed = true
			images = nil
		}
		for index, entry := range images {
			image, ok := entry.(map[string]any)
			if !ok {
				m.logger.DebugContext(ctx, "aspect ratio back-fill: image entry is not an object",
					"at_uri", mapping.ATURI, "index", index)
				outcome.unreadable++
				continue
			}
			if _, ok := image["aspectRatio"]; ok {
				outcome.alreadySet++
				continue
			}
			aspectRatio, err := m.storedAspectRatio(ctx, mapping.DID, image["image"])
			if err != nil {
				return aspectRatioOutcome{}, err
			}
			if aspectRatio == nil {
				m.logger.DebugContext(ctx, "aspect ratio back-fill: image bytes withheld, missing or unreadable",
					"at_uri", mapping.ATURI, "index", index)
				outcome.unreadable++
				continue
			}
			image["aspectRatio"] = aspectRatio
			outcome.imagesSet++
		}
		if outcome.imagesSet == 0 {
			// Any post left unchanged, with or without images, is reconciled
			// against prevCID, the version this read proved: an
			// earlier run whose repin never landed left the pin on the old
			// version. A correct pin re-puts as a no-op.
			if !dryRun {
				if err := m.repinAcceptance(ctx, mapping, prevCID); err != nil {
					return aspectRatioOutcome{}, fmt.Errorf("repin acceptance: %w", err)
				}
			}
			return outcome, nil
		}
		outcome.updated = true
		if err := m.validateRecord(record); err != nil {
			return aspectRatioOutcome{}, fmt.Errorf("validate rewritten record: %w", err)
		}
		if dryRun {
			return outcome, nil
		}

		sideEffect, updated := m.casMappingSideEffect(mapping, "aspect ratio target")
		res, err := m.repos.PutRecordCAS(ctx, mapping.DID, mapping.Collection, mapping.RKey, record, prevCID, sideEffect)
		if stderrors.Is(err, repo.ErrPreconditionFailed) {
			if attempt+1 < maxAspectRatioCommitAttempts {
				continue // the record changed under us; re-read and retry
			}
			return aspectRatioOutcome{}, fmt.Errorf("record kept changing across %d attempts: %w", maxAspectRatioCommitAttempts, err)
		}
		if errors.IsRecordGone(err) || errors.IsTombstoned(err) {
			return aspectRatioOutcome{gone: true}, nil
		}
		if err != nil {
			return aspectRatioOutcome{}, fmt.Errorf("put record: %w", err)
		}
		// Not cancelled with the request: a client disconnecting between the
		// two commits must not leave the pin on the version just replaced.
		if err := m.repinAcceptance(context.WithoutCancel(ctx), updated, res.RecordCID); err != nil {
			return outcome, fmt.Errorf("repin acceptance after commit: %w", err)
		}
		return outcome, nil
	}
}

// storedAspectRatio reads the bytes a record's blob ref names from did's blob
// store and returns their aspect ratio, or nil when the ref is not a blob or
// its bytes are withheld, missing or unreadable.
func (m *Materializer) storedAspectRatio(ctx context.Context, did string, ref any) (map[string]any, error) {
	var blob atdata.Blob
	switch typed := ref.(type) {
	case atdata.Blob:
		blob = typed
	case *atdata.Blob:
		if typed == nil {
			return nil, nil
		}
		blob = *typed
	default:
		return nil, nil
	}
	data, _, err := m.repos.GetBlob(ctx, did, cid.Cid(blob.Ref).String())
	if errors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read blob: %w", err)
	}
	return imageAspectRatio(blob.MimeType, data), nil
}
