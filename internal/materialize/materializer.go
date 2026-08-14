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
	// CollectionActorProfile and CollectionCommunityProfile are the identity
	// records, each at the fixed rkey "self" in its own subject's repo. They
	// are committed before any content that references them, so an AppView
	// never indexes a post or comment whose author or community is unknown.
	CollectionActorProfile     = "social.coves.actor.profile"
	CollectionCommunityProfile = "social.coves.community.profile"
	// CollectionPost is the DEPRECATED post collection: a post in the
	// COMMUNITY's repo carrying an in-record `author`. Nothing is created
	// under it any more (PLAN.md decision 20 flipped new posts to postv2),
	// but it stays because the records already written under it do not
	// migrate — Coves indexes both collections indefinitely — so update,
	// delete, stats and comment-parent dispatch still meet it.
	CollectionPost = "social.coves.community.post"
	// CollectionPostV2 is the post collection after the author-owned flip:
	// the record lives in the AUTHOR's repo, authorship IS that repo (no
	// in-record author), and `community` names the community it was
	// submitted to.
	CollectionPostV2 = "social.coves.community.postv2"
	// CollectionAcceptance is the community's attestation that it accepts a
	// post. It lives in the COMMUNITY's repo at a digest of the subject's
	// at-uri (SubjectRKey), and it is what makes a postv2 visible in the
	// community at all.
	CollectionAcceptance = "social.coves.community.acceptance"
	// CollectionRemoval is the community's record that a post was removed
	// from it. It shares the acceptance's digest rkey (one derivation per
	// subject) and replaces the acceptance in one atomic commit.
	CollectionRemoval = "social.coves.community.removal"
	// CollectionComment is a reply, in its AUTHOR's repo in both eras — the
	// flip changed where posts live, never comments. Its community is not a
	// field on the record but a property of the thread it hangs from
	// (reply.root), which is why comment mappings carry community_did.
	CollectionComment = "social.coves.community.comment"
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

// ModerationLedger records a community moderation decision against a NATIVE
// post in the admissions ledger — the operator surface that answers "why is
// this post not in the community?".
//
// It is an INTERFACE rather than an *accept.Admissions so this package keeps no
// dependency on the acceptance engine (which already depends on the stores this
// one writes through); main adapts the concrete type.
type ModerationLedger interface {
	// RecordRemoval marks the post removed by its community, with the removal
	// record's own code. authorDID is the repo the post lives in.
	RecordRemoval(ctx context.Context, communityDID, postURI, authorDID, code string) error
	// RecordRestore marks the post accepted again, pinning the CID the fresh
	// acceptance was written against.
	RecordRestore(ctx context.Context, communityDID, postURI, authorDID, cid string) error
	// LastEvaluatedCID is the CID of the most recent version of the post the
	// acceptance engine DECIDED on — including a decision that wrote nothing
	// outward, which is exactly the case a restore has to pin. "" means the
	// ledger knows of no decision for this (community, post).
	LastEvaluatedCID(ctx context.Context, communityDID, postURI string) (string, error)
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
	// Ledger records inbound moderation decisions against NATIVE posts in the
	// admissions ledger, so a moderator's removal is visible there when the
	// MODERATOR acts rather than only if the author later edits (which is the
	// only thing that writes a row otherwise). It also frees the author's
	// per-community rate quota, which counts accepted rows.
	//
	// OPTIONAL: nil skips the ledger write and changes nothing else — the
	// community repo records remain the source of truth for removal state.
	Ledger ModerationLedger
	// OutboundObjects is the bridge's own outbound state for NATIVE records.
	// RestorePost needs it: a native post lives in the AUTHOR's repo, which
	// this bridge does not host, so its CID cannot be read back through Repos.
	// OPTIONAL: nil leaves a bridge-origin restore refusing (it logs and leaves
	// the removal standing) exactly as it did before this seam existed.
	OutboundObjects store.OutboundObjects
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
	outbound    store.OutboundObjects
	ledger      ModerationLedger
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
	// removalCheck reports whether a removal stands for a subject; defaults to
	// removalStands. A test seam so acceptPost's check-then-act window can be
	// opened deterministically — the commit-time refusal has to hold even when
	// this read answers stale, and a timing test could only prove that by
	// accident.
	removalCheck func(ctx context.Context, communityDID, rkey string) (bool, error)
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
		outbound:    opts.OutboundObjects,
		ledger:      opts.Ledger,
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
	m.removalCheck = m.removalStands
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
// only for LEGACY posts, which sit in the community's repo; for a postv2 and
// for comments the author's repo IS did); communityDID records which
// community's content it is, for the
// membership binding announced deletes and announced votes authorize against.
func (m *Materializer) commitRecord(ctx context.Context, did, collection, rkey string, record map[string]any, obj *ap.Object, authorDID, communityDID string) (*Result, error) {
	// Don't resurrect deleted content. AP delivery is unordered, so a Create
	// or Update can arrive (or be re-delivered) after a Delete already
	// tombstoned this object's mapping. Re-materializing would un-tombstone it
	// (PutMapping resets deleted_at) and re-commit the record. The ingest
	// layer clears the tombstone explicitly on an Undo(Delete)/restore, and
	// its ap_tombstones marker covers the delete-before-create ordering
	// (a Delete for a never-materialized object).
	//
	// carryForward marks the update path for a live post/comment mapping: only
	// there does the rebuild carry fields it cannot reconstruct forward
	// (bridgedStats, postv2's immutable community, and comments' reply refs),
	// and only there does the commit need the optimistic-concurrency guard
	// against a racing stats stamp. postv2 is gated exactly as the collection
	// it replaced: an edit that skipped the carry would drop the vote counts
	// the refresher stamped and mint a needless firehose event.
	carryForward := false
	// storedCommunityDID is the binding a previous materialization already
	// made. It is preferred over anything derived from THIS delivery: the
	// community a comment belongs to authorizes announced deletes and binds
	// announced votes, so re-deriving it from an edited (attacker-influenced)
	// inReplyTo would hand another community moderation authority over content
	// posted somewhere else.
	var storedCommunityDID string
	// storedThreadRoot is the thread a previous materialization recorded. A
	// comment cannot change threads, so a binding already made wins — exactly
	// like the community above, and for the same reason: it is read back on the
	// moderation path, and re-deriving it from an edited delivery would let an
	// edit move a comment out from under its thread's lock.
	var storedThreadRoot string
	if existing, err := m.objects.GetByAPID(ctx, obj.ID); err == nil {
		if existing.IsDeleted() {
			return nil, skip(obj.ID, "object was deleted upstream; not resurrecting")
		}
		carryForward = collection == CollectionPost ||
			collection == CollectionPostV2 ||
			collection == CollectionComment
		storedCommunityDID = existing.CommunityDID
		storedThreadRoot = existing.ThreadRootATURI
	} else if !errors.IsNotFound(err) {
		return nil, fmt.Errorf("materialize: check mapping for %s: %w", obj.ID, err)
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
	putMapping := func(ctx context.Context, tx *sql.Tx, res *repo.CommitResult) error {
		mapping.CID = res.RecordCID
		// Derived HERE, not above, because carryForwardFields may have
		// rewritten the record between the two points: an update whose
		// audience was retargeted has had the stored (immutable) community
		// restored by then, and the mapping must agree with the record it
		// maps or the two would authorize different communities.
		mapping.CommunityDID = mappingCommunityDID(collection, did, record, communityDID, storedCommunityDID)
		// Same rule, same moment, for the same reason: the thread a comment
		// hangs in is decided once and read off the RECORD being committed, so
		// an update whose reply refs were carried forward maps the thread the
		// record actually names rather than one this delivery asserted.
		mapping.ThreadRootATURI = mappingThreadRootATURI(collection, record, storedThreadRoot)
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
	}

	if !carryForward {
		// Create, or a profile update: no read-modify-write, so no precondition
		// — an idempotent re-put must still reach the repo's NoOp path.
		if err := m.validateRecord(record); err != nil {
			return nil, err
		}
		res, err := m.repos.PutRecordTx(ctx, did, collection, rkey, record, putMapping)
		if err != nil {
			return nil, fmt.Errorf("materialize: put %s/%s/%s for %s: %w", did, collection, rkey, obj.ID, err)
		}
		return &Result{DID: did, ATURI: stored.ATURI, CID: res.RecordCID, NoOp: res.NoOp}, nil
	}

	// Update path: carry forward the fields the AP rebuild cannot reconstruct,
	// under a CAS precondition on the stored record's CID. If a concurrent
	// stats stamp (or another edit) changed the record between the read and the
	// commit, the commit fails ErrPreconditionFailed and we re-read and retry —
	// so an edit can neither drop a just-committed stamp nor churn reply refs a
	// stats stamp already moved.
	for attempt := 0; ; attempt++ {
		expectPrevCID, cerr := m.carryForwardFields(ctx, did, collection, rkey, record, obj, attempt)
		if cerr != nil {
			return nil, cerr
		}
		if err := m.validateRecord(record); err != nil {
			return nil, err
		}
		res, err := m.repos.PutRecordCAS(ctx, did, collection, rkey, record, expectPrevCID, putMapping)
		if stderrors.Is(err, repo.ErrPreconditionFailed) {
			if attempt+1 < maxStatsCommitAttempts {
				continue
			}
			return nil, fmt.Errorf("materialize: put %s: record kept changing across %d attempts: %w", obj.ID, maxStatsCommitAttempts, err)
		}
		if err != nil {
			return nil, fmt.Errorf("materialize: put %s/%s/%s for %s: %w", did, collection, rkey, obj.ID, err)
		}
		return &Result{DID: did, ATURI: stored.ATURI, CID: res.RecordCID, NoOp: res.NoOp}, nil
	}
}

// carryForwardFields folds the fields a Lemmy rebuild cannot reconstruct out of
// the currently-stored record onto the freshly-built one, and returns the
// stored record's CID for use as the commit's CAS precondition:
//
//   - bridgedStats: the vote-stats refresher writes it; the rebuild never
//     does. Dropping it would lose the counts until the next sweep and mint a
//     needless firehose event (breaking idempotent re-ingest).
//   - reply (comments only): Coves' comment consumer treats reply root/parent
//     strongRef CIDs as IMMUTABLE across updates, but a stats stamp on the
//     parent churns its CID — so re-resolving would hand Coves changed refs it
//     rejects as thread hijacking. Carrying the stored refs verbatim keeps the
//     thread anchoring stable across edits. The CREATE path still resolves
//     fresh refs (this runs only for a live existing mapping).
//   - community (postv2 only): the lexicon makes it IMMUTABLE, and Coves'
//     consumers discard the WHOLE update event that changes it — so a rebuild
//     that re-derived it from an edited (or hostile) `audience` would not
//     retarget the post, it would freeze the post at its pre-edit version
//     while every later edit was thrown away too. The stored value wins.
//
// A record absent on the FIRST attempt is the crash window between a delete
// commit and its soft-delete: let the rebuild stand as a guarded create
// (expectCID ""). Absent on a RETRY means it was deleted concurrently — skip
// rather than resurrect it.
func (m *Materializer) carryForwardFields(ctx context.Context, did, collection, rkey string, record map[string]any, obj *ap.Object, attempt int) (expectPrevCID string, err error) {
	stored, storedCID, rerr := m.repos.GetRecord(ctx, did, collection, rkey)
	switch {
	case rerr == nil:
		if stats, ok := stored[bridgedStatsField]; ok {
			record[bridgedStatsField] = stats
		} else {
			delete(record, bridgedStatsField) // clear any carried by a prior attempt
		}
		if collection == CollectionComment {
			if reply, ok := stored["reply"]; ok {
				record["reply"] = reply
			}
		}
		if collection == CollectionPostV2 {
			if community, ok := stored["community"]; ok {
				record["community"] = community
			}
			// originalAuthor is provenance about who wrote the post UPSTREAM,
			// and an edit is not a claim about that. attributedTo on an updated
			// Page is proposed by whoever delivered the update, so rebuilding
			// provenance from it would let one delivery reattribute a post to
			// somebody who never wrote it.
			if author, ok := stored["originalAuthor"]; ok {
				record["originalAuthor"] = author
			}
		}
		return storedCID, nil
	case errors.IsNotFound(rerr):
		if attempt > 0 {
			return "", skip(obj.ID, "record deleted concurrently during rebuild; not resurrecting")
		}
		delete(record, bridgedStatsField)
		return "", nil
	default:
		return "", fmt.Errorf("materialize: read record for carry-forward %s: %w", obj.ID, rerr)
	}
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

// recordDatetimeMicros renders a timestamp at MICROSECOND precision — the
// resolution vote_aggregates.updated_at (a postgres timestamptz) actually
// carries. bridgedStats.asOf uses this, not recordDatetime's millisecond
// dialect: two distinct aggregate versions landing in the same millisecond
// (concurrent voters under clock_timestamp()) would otherwise serialize to
// EQUAL asOf strings, letting a newer-or-equal consumer guard and the
// watermark bookkeeping conflate them. atproto's datetime format permits
// fractional-second digits, so six is as valid as three; createdAt keeps its
// existing millisecond dialect (its source `published` time is only that
// precise, and Coves already parses it).
func recordDatetimeMicros(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000000Z")
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
