package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"time"

	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/repo"
	"tidepool/internal/store"
)

// RepoReemitter is the slice of the repo manager the admin re-emit endpoint
// drives. An interface so the handler is testable without postgres.
type RepoReemitter interface {
	ListDIDs(ctx context.Context) ([]string, error)
	ListRecords(ctx context.Context, did string) ([]repo.RecordEntry, error)
	DeleteRecord(ctx context.Context, did, collection, rkey string) (*repo.CommitResult, error)
	PutRecord(ctx context.Context, did, collection, rkey string, record map[string]any) (*repo.CommitResult, error)
	// GetRecord reads a record of any repo: re-emit reads a comment's
	// postv2 thread root to learn the comment's community.
	GetRecord(ctx context.Context, did, collection, rkey string) (record map[string]any, recordCID string, err error)
}

// StandingBanReader reads the ban record standing in a community repo for one
// subject, or nil when none stands. Re-emit consults it so it never re-creates
// a comment its author is banned from the comment's community for.
type StandingBanReader interface {
	StandingBanRecord(ctx context.Context, communityDID, subjectDID string) (*materialize.BanRecord, error)
}

// APObjectMappingReader reads a record's stored ap_objects mapping by at-uri.
// Re-emit reads a comment's mapping for the community recorded at
// materialization time, the only answer for a comment whose thread root is
// not hosted here.
type APObjectMappingReader interface {
	GetByATURI(ctx context.Context, atURI string) (*store.APObjectMapping, error)
}

// reemitCollectionRank orders a repo's records so downstream indexers see
// identity-bearing records before the content that references them: an
// AppView consuming the re-emitted stream needs a community's profile
// indexed before its posts arrive (and an author's profile before their
// comments), or the content lands with dangling references. Cross-repo
// order is relay-dependent regardless (see docs/E2E.md), so this is best-effort
// within one repo, not a delivery guarantee.
//
// The tiers are what references what: profiles are referenced by everything,
// posts are referenced by the comments that reply to them, and acceptance and
// removal records strongRef a post, so they come last. BOTH post eras share
// tier 1 — a postv2 is a post, and the deprecated collection is the same tier
// because the records already written under it still index the same way. That
// pairing is the whole point: matching only the old name put an author's
// comments ahead of the roots they reply to the moment the flip landed, which
// is precisely the dangling reference this ordering exists to prevent.
func reemitCollectionRank(collection string) int {
	switch collection {
	case "social.coves.community.profile", "social.coves.actor.profile":
		return 0
	case "social.coves.community.post", "social.coves.community.postv2":
		return 1
	case "social.coves.community.acceptance", "social.coves.community.removal":
		// After ALL content: each pins a post by strongRef, and a community's
		// attestation about a post is meaningless to an indexer that has not
		// seen the post yet.
		return 3
	default:
		// Comments and anything not listed. Content, so after posts.
		return 2
	}
}

// ReemitResult reports one repo's re-emission.
type ReemitResult struct {
	DID      string `json:"did"`
	Records  int    `json:"records"`
	Reemited int    `json:"reemitted"`
	// SkippedBanned counts comments left alone because their author is
	// banned, in force, in the comment's community (see reemitRepo).
	SkippedBanned int `json:"skipped_banned"`
	// SkippedUnknownCommunity counts comments left alone because their
	// community cannot be determined, so no ban can be ruled out (see
	// reemitRepo).
	SkippedUnknownCommunity int    `json:"skipped_unknown_community"`
	Error                   string `json:"error,omitempty"`
}

// reemitRepo re-emits every record of one DID onto the firehose as a
// delete commit followed by a create commit with the identical value.
//
// Why delete+create and not a "touch" update: re-putting an identical
// record is an idempotent no-op by design (no commit, no event), and a
// forced update op whose value is unchanged would produce a commit whose
// op list does not match its (empty) MST diff — which sync-v1.1-validating
// relays are entitled to reject. The delete/create pair makes two honest
// commits with real diffs. Deterministic rkeys and identical bytes mean
// the record returns under the same at-uri with the same CID, so existing
// strongRefs stay valid; consumers see a transient delete, which they
// already tolerate (out-of-order tombstones are part of the AP diet).
//
// This exists for the relay cold-start gap (FOLLOWUPS): records committed
// before a relay's first subscription never re-emit on their own, so a
// Jetstream-fed AppView can never index them.
//
// A comment whose author (this repo's DID) is banned, in force, in the
// comment's community is skipped and counted in SkippedBanned: Coves records a
// permanent community_ban hide on every comment it indexes while a ban stands,
// a re-creation included, and that hide survives the unban. Re-emitting it
// would hide a comment that predates the ban for good. A comment whose
// community cannot be determined is skipped too, counted in
// SkippedUnknownCommunity: no ban can be ruled out for it. A failed mapping or
// ban read aborts the repo rather than guess either way.
func reemitRepo(ctx context.Context, repos RepoReemitter, bans StandingBanReader, mappings APObjectMappingReader, did string, logger *slog.Logger) ReemitResult {
	res := ReemitResult{DID: did}
	entries, err := repos.ListRecords(ctx, did)
	if err != nil {
		res.Error = err.Error()
		return res
	}
	res.Records = len(entries)
	sort.SliceStable(entries, func(i, j int) bool {
		return reemitCollectionRank(entries[i].Collection) < reemitCollectionRank(entries[j].Collection)
	})
	// bannedIn caches only a banned answer per community: a repo's comments
	// cluster in a few communities. A not-banned answer is re-read for every
	// comment right before its delete, so a ban published mid-walk stops the
	// community's later comments.
	bannedIn := map[string]bool{}
	for _, e := range entries {
		if ctx.Err() != nil {
			res.Error = ctx.Err().Error()
			return res
		}
		if e.Collection == materialize.CollectionComment {
			communityDID, banned, err := commentAuthorBanned(ctx, repos, bans, mappings, bannedIn, did, e, time.Now())
			if err != nil {
				res.Error = fmt.Sprintf("ban check %s/%s: %v", e.Collection, e.Rkey, err)
				return res
			}
			if communityDID == "" {
				res.SkippedUnknownCommunity++
				continue
			}
			if banned {
				res.SkippedBanned++
				continue
			}
		}
		if _, err := repos.DeleteRecord(ctx, did, e.Collection, e.Rkey); err != nil {
			// A vanished record (raced by a concurrent scrub) is fine to
			// skip; anything else aborts this repo so the operator sees it.
			if errors.IsNotFound(err) {
				continue
			}
			res.Error = fmt.Sprintf("delete %s/%s: %v", e.Collection, e.Rkey, err)
			return res
		}
		if _, err := repos.PutRecord(ctx, did, e.Collection, e.Rkey, e.Value); err != nil {
			// The record is now deleted but not recreated — surface loudly;
			// re-running the endpoint cannot restore it (the value is gone
			// from the tree), so log the full value for manual recovery.
			logger.Error("reemit: recreate failed after delete — record dropped from repo",
				"did", did, "collection", e.Collection, "rkey", e.Rkey, "error", err)
			res.Error = fmt.Sprintf("recreate %s/%s (RECORD DROPPED, see logs): %v", e.Collection, e.Rkey, err)
			return res
		}
		res.Reemited++
	}
	return res
}

// commentAuthorBanned resolves the comment's community and reports whether
// the comment's author (the repo DID) is banned, in force at now, in it. The
// community comes from the comment's stored mapping when one exists, so a
// comment whose thread root is not hosted here (a native postv2 in another
// repo) or is gone still resolves; with no mapping it is derived from the
// record alone. An empty communityDID means it cannot be determined, and
// banned is then false and meaningless. bannedIn holds the communities already
// found banned this walk; a community not in it is read again.
func commentAuthorBanned(ctx context.Context, repos RepoReemitter, bans StandingBanReader, mappings APObjectMappingReader, bannedIn map[string]bool, did string, comment repo.RecordEntry, now time.Time) (communityDID string, banned bool, err error) {
	atURI := fmt.Sprintf("at://%s/%s/%s", did, comment.Collection, comment.Rkey)
	mapping, err := mappings.GetByATURI(ctx, atURI)
	switch {
	case err == nil:
	case errors.IsNotFound(err):
		mapping = &store.APObjectMapping{DID: did, Collection: comment.Collection, RKey: comment.Rkey}
	default:
		return "", false, fmt.Errorf("read mapping %s: %w", atURI, err)
	}
	communityDID, err = materialize.CommunityDIDOf(ctx, heldRecord{RepoReemitter: repos, entry: comment, did: did}, mapping)
	if err != nil {
		return "", false, fmt.Errorf("resolve community: %w", err)
	}
	if communityDID == "" {
		return "", false, nil
	}
	if bannedIn[communityDID] {
		return communityDID, true, nil
	}
	standing, err := bans.StandingBanRecord(ctx, communityDID, did)
	if err != nil {
		return "", false, fmt.Errorf("read ban record in %s: %w", communityDID, err)
	}
	banned = standing != nil && standing.InForce(now)
	if banned {
		bannedIn[communityDID] = true
	}
	return communityDID, banned, nil
}

// heldRecord answers GetRecord for the record re-emit already holds from
// ListRecords, so resolving its community does not read it again, and passes
// every other read (a postv2 thread root) to the repo.
type heldRecord struct {
	RepoReemitter
	entry repo.RecordEntry
	did   string
}

func (h heldRecord) GetRecord(ctx context.Context, did, collection, rkey string) (map[string]any, string, error) {
	if did == h.did && collection == h.entry.Collection && rkey == h.entry.Rkey {
		return h.entry.Value, "", nil
	}
	record, recordCID, err := h.RepoReemitter.GetRecord(ctx, did, collection, rkey)
	if err != nil {
		return nil, "", fmt.Errorf("read %s/%s/%s: %w", did, collection, rkey, err)
	}
	return record, recordCID, nil
}

// handleReemit serves POST /admin/reemit: {"did":"did:plc:..."} re-emits one
// repo, an empty body (or {}) re-emits every active repo. Answers 200 with
// per-repo results; a body-level "failed" count > 0 means at least one repo
// aborted early and the response details why.
func (a *Admin) handleReemit(w http.ResponseWriter, r *http.Request) {
	if a.repos == nil || a.bans == nil || a.objects == nil {
		http.Error(w, "re-emit not configured", http.StatusNotImplemented)
		return
	}
	var req struct {
		DID string `json:"did"`
	}
	if r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `body must be {} or {"did":"did:..."}`, http.StatusBadRequest)
			return
		}
	}

	ctx := r.Context()
	var dids []string
	if req.DID != "" {
		dids = []string{req.DID}
	} else {
		all, err := a.repos.ListDIDs(ctx)
		if err != nil {
			a.logger.Error("reemit: list dids", "error", err)
			http.Error(w, "listing repos failed", http.StatusInternalServerError)
			return
		}
		dids = all
	}

	results := make([]ReemitResult, 0, len(dids))
	failed := 0
	for _, did := range dids {
		res := reemitRepo(ctx, a.repos, a.bans, a.objects, did, a.logger)
		if res.Error != "" {
			failed++
		}
		results = append(results, res)
	}
	a.logger.Info("reemit complete", "repos", len(results), "failed", failed)
	writeJSON(w, http.StatusOK, map[string]any{
		"repos":  len(results),
		"failed": failed,
		"result": results,
	})
}
