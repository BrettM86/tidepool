// Package materialize is the translation heart of the bridge: given a
// fetched or delivered ActivityPub object, it produces the correct
// social.coves.* record(s) in the correct repo(s), idempotently, with valid
// strongRefs.
//
// Placement rules (PLAN.md locked decision 3): posts are written into the
// COMMUNITY's repo with author = the bridged user's DID (the Coves post
// consumer validates repo DID == record.community); comments are written
// into the AUTHOR's repo. Profiles (rkey "self") are committed before any
// content that references them, so the AppView never sees content whose
// community/author is not indexed yet.
//
// Record keys are deterministic TIDs (repo.DeterministicTID over the AP
// `published` time and canonical AP id), so re-ingesting the same object
// re-puts an identical record — an idempotent no-op at the repo layer — and
// the ap_objects mapping row is simply refreshed.
package materialize

import (
	"context"
	"database/sql"
	"encoding/json"
	stderrors "errors"
	"expvar"
	"fmt"
	"log/slog"
	"time"

	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/bluesky-social/indigo/atproto/lexicon"
	"github.com/bluesky-social/indigo/atproto/syntax"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/identity"
	"tidepool/internal/repo"
	"tidepool/internal/store"
	"tidepool/lexicons"
)

// ValidationFailures counts records that failed lexicon validation
// (strict and log-and-write modes alike). Exported via the expvar registry
// as "tidepool_lexicon_validation_failures" — cmd/tidepool serves the
// registry on the admin surface, so a non-zero counter in production (where
// failures log-and-write) is observable without log scraping. Task 05
// deferred a strict-first production rollout; this is the metric that
// decision waits on.
var ValidationFailures = expvar.NewInt("tidepool_lexicon_validation_failures")

// Record collections the materializer produces.
const (
	CollectionActorProfile     = "social.coves.actor.profile"
	CollectionCommunityProfile = "social.coves.community.profile"
	CollectionPost             = "social.coves.community.post"
	CollectionComment          = "social.coves.community.comment"
)

// ProfileRKey is the fixed record key of actor and community profiles.
const ProfileRKey = "self"

// defaultProfileRefreshTTL is how stale a materialized profile may get
// before a content-triggered EnsureActor/EnsureCommunity re-fetches it.
const defaultProfileRefreshTTL = 24 * time.Hour

// defaultMaxBlobBytes is the outer transport budget for one media download.
const defaultMaxBlobBytes int64 = 5 << 20

// ErrSkipped marks content the bridge deliberately did not materialize:
// nobridge/deleted authors, tombstoned ancestors, cycles, unusable inputs.
// It is not a failure — callers (task 06) log the reason and move on, and
// must never retry the object.
var ErrSkipped = stderrors.New("materialize: skipped")

// SkipError carries why an object was skipped. Unwraps to ErrSkipped.
type SkipError struct {
	APID   string
	Reason string
}

func (e *SkipError) Error() string {
	return fmt.Sprintf("materialize: skipped %s: %s", e.APID, e.Reason)
}

func (e *SkipError) Unwrap() error { return ErrSkipped }

// IsSkip reports whether err is (or wraps) a deliberate skip.
func IsSkip(err error) bool { return stderrors.Is(err, ErrSkipped) }

func skip(apID, reason string) error { return &SkipError{APID: apID, Reason: reason} }

// Fetcher is the slice of the AP client the materializer uses. *ap.Client
// implements it; tests may substitute failures.
type Fetcher interface {
	FetchObject(ctx context.Context, iri string) (*ap.Object, error)
	FetchActor(ctx context.Context, iri string) (*ap.Object, error)
	FetchMedia(ctx context.Context, iri string, maxBytes int64) (data []byte, contentType string, err error)
}

// ActorMinter mints atproto identities for unseen fediverse actors.
// *identity.Minter implements it; tests use a local fake so golden and
// postgres tests never need a PLC directory.
type ActorMinter interface {
	MintActor(ctx context.Context, req identity.MintRequest) (*identity.Identity, error)
}

// VoteScrubber erases an actor's vote_events rows — the vote counterpart of
// the record scrub (votes.Aggregator implements it). Optional: a nil
// scrubber skips vote scrubbing (tests that don't exercise votes).
type VoteScrubber interface {
	ScrubVoter(ctx context.Context, voterAPID string) error
}

// Options configures New. Fetcher, Objects, Actors, Communities, Repos,
// Minter, and ServiceDID are required.
type Options struct {
	Fetcher     Fetcher
	Objects     store.APObjects
	Actors      store.BridgedActors
	Communities store.Communities
	Repos       *repo.Manager
	Minter      ActorMinter
	// Votes scrubs a deleted actor's vote_events rows alongside the record
	// scrub (optional; nil skips it).
	Votes VoteScrubber
	// ServiceDID is the bridge's own DID: community.profile createdBy and
	// hostedBy (PLAN.md locked decision 6).
	ServiceDID string
	// ProfileRefreshTTL bounds profile staleness (config.ProfileRefreshTTL;
	// defaults to 24h).
	ProfileRefreshTTL time.Duration
	// MaxBlobBytes is the outer per-blob download budget
	// (config.MaxBlobBytes; defaults to 5 MiB). Lexicon slot caps (avatar
	// 1 MB, banner 2 MB, ...) tighten it further.
	MaxBlobBytes int64
	// StrictValidation makes a record that fails lexicon validation an
	// error (development and tests). When false the record is still
	// validated, but failures are logged loudly and the record is written
	// anyway — production should not silently drop content over a
	// validator disagreement, but it must be visible.
	StrictValidation bool
	Logger           *slog.Logger
}

// Materializer translates AP objects into social.coves.* records.
type Materializer struct {
	fetcher     Fetcher
	objects     store.APObjects
	actors      store.BridgedActors
	communities store.Communities
	repos       *repo.Manager
	minter      ActorMinter
	votes       VoteScrubber
	serviceDID  string
	profileTTL  time.Duration
	maxBlob     int64
	strict      bool
	catalog     *lexicon.BaseCatalog
	logger      *slog.Logger
	now         func() time.Time // test seam for profile TTL
	// deleteBlob deletes one blob by (did, cid); defaults to repos.DeleteBlob.
	// A test seam so the scrub's retryable-error path can be exercised
	// without a real storage failure.
	deleteBlob func(ctx context.Context, did, cid string) error
}

// New validates options and builds a Materializer. The vendored lexicon
// catalog is loaded once; a broken vendored file fails construction.
func New(opts Options) (*Materializer, error) {
	if opts.Fetcher == nil {
		return nil, errors.NewValidationError("fetcher", "must not be nil")
	}
	if opts.Objects == nil {
		return nil, errors.NewValidationError("objects", "must not be nil")
	}
	if opts.Actors == nil {
		return nil, errors.NewValidationError("actors", "must not be nil")
	}
	if opts.Communities == nil {
		return nil, errors.NewValidationError("communities", "must not be nil")
	}
	if opts.Repos == nil {
		return nil, errors.NewValidationError("repos", "must not be nil")
	}
	if opts.Minter == nil {
		return nil, errors.NewValidationError("minter", "must not be nil")
	}
	if _, err := syntax.ParseDID(opts.ServiceDID); err != nil {
		return nil, errors.NewValidationError("service_did", err.Error())
	}
	catalog, err := lexicons.Catalog()
	if err != nil {
		return nil, err
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	m := &Materializer{
		fetcher:     opts.Fetcher,
		objects:     opts.Objects,
		actors:      opts.Actors,
		communities: opts.Communities,
		repos:       opts.Repos,
		minter:      opts.Minter,
		votes:       opts.Votes,
		serviceDID:  opts.ServiceDID,
		profileTTL:  opts.ProfileRefreshTTL,
		maxBlob:     opts.MaxBlobBytes,
		strict:      opts.StrictValidation,
		catalog:     catalog,
		logger:      logger,
		now:         time.Now,
	}
	m.deleteBlob = m.repos.DeleteBlob
	if m.profileTTL <= 0 {
		m.profileTTL = defaultProfileRefreshTTL
	}
	if m.maxBlob <= 0 {
		m.maxBlob = defaultMaxBlobBytes
	}
	return m, nil
}

// Result reports what a materialization produced.
type Result struct {
	// DID is the repo the record lives in.
	DID string
	// ATURI and CID identify the record version (strongRef material).
	ATURI string
	CID   string
	// NoOp marks an idempotent re-materialization: the identical record
	// already existed, no new commit or firehose event was produced.
	NoOp bool
}

// commitRecord is the single write path for every materialized record:
// lexicon-validate, then commit the record AND upsert its ap_objects
// mapping in ONE transaction (repo.PutRecordTx + PutMappingTx — task 11
// closed the crash window where a record could land on the firehose with
// no mapping). authorDID records who authored the record (differs from did
// for posts).
func (m *Materializer) commitRecord(ctx context.Context, did, collection, rkey string, record map[string]any, obj *ap.Object, authorDID string) (*Result, error) {
	// Don't resurrect deleted content. AP delivery is unordered, so a Create
	// or Update can arrive (or be re-delivered) after a Delete already
	// tombstoned this object's mapping. Re-materializing would un-tombstone it
	// (PutMapping resets deleted_at) and re-commit the record. The ingest
	// layer clears the tombstone explicitly on an Undo(Delete)/restore, and
	// its ap_tombstones marker covers the delete-before-create ordering
	// (a Delete for a never-materialized object).
	if existing, err := m.objects.GetByAPID(ctx, obj.ID); err == nil {
		if existing.IsDeleted() {
			return nil, skip(obj.ID, "object was deleted upstream; not resurrecting")
		}
	} else if !errors.IsNotFound(err) {
		return nil, fmt.Errorf("materialize: check mapping for %s: %w", obj.ID, err)
	}

	if err := m.validateRecord(record); err != nil {
		return nil, err
	}

	mapping := store.APObjectMapping{
		APID:           obj.ID,
		APType:         obj.Type,
		OriginInstance: obj.Host(),
		Origin:         store.OriginFediverse,
		DID:            did,
		AuthorDID:      authorDID,
		Collection:     collection,
		RKey:           rkey,
	}
	if obj.Published.OK() {
		published := obj.Published.Time
		mapping.PublishedAt = &published
	}

	var stored *store.APObjectMapping
	res, err := m.repos.PutRecordTx(ctx, did, collection, rkey, record,
		func(ctx context.Context, tx *sql.Tx, res *repo.CommitResult) error {
			mapping.CID = res.RecordCID
			var mapErr error
			stored, mapErr = m.objects.PutMappingTx(ctx, tx, mapping)
			if mapErr != nil {
				if errors.IsAlreadyExists(mapErr) {
					// A different AP id already claimed this at-uri: a
					// deterministic TID collision (near-impossible after the
					// hash-filled-micros change; see repo.DeterministicTID).
					// Loud by design — this is a bug signal, and failing here
					// now rolls the record write back with it.
					m.logger.Error("deterministic rkey collision: different ap_id claimed the same at-uri",
						"ap_id", obj.ID, "did", did, "collection", collection, "rkey", rkey)
				}
				return fmt.Errorf("materialize: map %s: %w", obj.ID, mapErr)
			}
			return nil
		})
	if err != nil {
		return nil, fmt.Errorf("materialize: put %s/%s/%s for %s: %w", did, collection, rkey, obj.ID, err)
	}
	return &Result{DID: did, ATURI: stored.ATURI, CID: res.RecordCID, NoOp: res.NoOp}, nil
}

// validateRecord checks the record against the vendored Coves lexicons —
// the same indigo validator Coves' own lexicon tooling runs. In strict mode
// (dev/tests) a failure is an error; otherwise it is logged loudly and the
// record proceeds (visibility without dropping content in production).
func (m *Materializer) validateRecord(record map[string]any) error {
	recordType, _ := record["$type"].(string)
	if recordType == "" {
		return errors.NewValidationError("record", "must carry a non-empty $type")
	}
	data, err := jsonRoundTrip(record)
	if err != nil {
		return fmt.Errorf("materialize: encode record for validation: %w", err)
	}
	if err := lexicon.ValidateRecord(m.catalog, data, recordType, lexicon.ValidateFlags(0)); err != nil {
		// Counted in BOTH modes: the metric is how a production operator
		// (log-and-write mode) notices validator disagreements without log
		// scraping, and strict-mode counts keep dev/prod dashboards
		// comparable.
		ValidationFailures.Add(1)
		if m.strict {
			return errors.NewValidationError("record",
				fmt.Sprintf("%s fails lexicon validation: %v", recordType, err))
		}
		m.logger.Error("materialized record fails lexicon validation (writing anyway; investigate)",
			"type", recordType, "error", err)
	}
	return nil
}

// jsonRoundTrip re-parses a record through the atproto data model
// (atdata.UnmarshalJSON), which is the shape indigo's lexicon validator
// expects: blob refs become typed atdata.Blob values, $link maps become
// CIDLinks, and numbers become int64.
func jsonRoundTrip(record map[string]any) (map[string]any, error) {
	raw, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	return atdata.UnmarshalJSON(raw)
}

// recordDatetime renders a timestamp in the atproto datetime format
// (RFC3339, UTC, millisecond precision — Coves parses with time.RFC3339).
func recordDatetime(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

// recordRKey derives the deterministic record key for an AP object,
// failing closed (as a skip) when the object has no usable published time.
func recordRKey(obj *ap.Object) (string, error) {
	if !obj.Published.OK() {
		return "", skip(obj.ID, "missing or unparseable `published` timestamp (needed for the deterministic rkey)")
	}
	tid, err := repo.DeterministicTID(obj.Published.Time, obj.ID)
	if err != nil {
		return "", fmt.Errorf("materialize: derive rkey for %s: %w", obj.ID, err)
	}
	return tid.String(), nil
}

// strongRef builds a com.atproto.repo.strongRef object.
func strongRef(uri, cid string) map[string]any {
	return map[string]any{"uri": uri, "cid": cid}
}

// selfLabels builds a com.atproto.label.defs#selfLabels value.
func selfLabels(values ...string) map[string]any {
	list := make([]any, 0, len(values))
	for _, v := range values {
		list = append(list, map[string]any{"val": v})
	}
	return map[string]any{
		"$type":  "com.atproto.label.defs#selfLabels",
		"values": list,
	}
}

// recordLangs extracts up to three valid language tags.
func recordLangs(langs ap.Languages) []any {
	var out []any
	for _, lang := range langs {
		if lang.Identifier == "" {
			continue
		}
		if _, err := syntax.ParseLanguage(lang.Identifier); err != nil {
			continue
		}
		out = append(out, lang.Identifier)
		if len(out) == 3 {
			break
		}
	}
	return out
}
