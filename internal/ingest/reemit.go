package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"

	"tidepool/internal/errors"
	"tidepool/internal/repo"
)

// RepoReemitter is the slice of the repo manager the admin re-emit endpoint
// drives. An interface so the handler is testable without postgres.
type RepoReemitter interface {
	ListDIDs(ctx context.Context) ([]string, error)
	ListRecords(ctx context.Context, did string) ([]repo.RecordEntry, error)
	DeleteRecord(ctx context.Context, did, collection, rkey string) (*repo.CommitResult, error)
	PutRecord(ctx context.Context, did, collection, rkey string, record map[string]any) (*repo.CommitResult, error)
}

// reemitCollectionRank orders a repo's records so downstream indexers see
// identity-bearing records before the content that references them: an
// AppView consuming the re-emitted stream needs a community's profile
// indexed before its posts arrive (and an author's profile before their
// comments), or the content lands with dangling references. Cross-repo
// order is relay-dependent regardless (see README), so this is best-effort
// within one repo, not a delivery guarantee.
func reemitCollectionRank(collection string) int {
	switch collection {
	case "social.coves.community.profile", "social.coves.actor.profile":
		return 0
	case "social.coves.community.post":
		return 1
	default:
		return 2
	}
}

// ReemitResult reports one repo's re-emission.
type ReemitResult struct {
	DID      string `json:"did"`
	Records  int    `json:"records"`
	Reemited int    `json:"reemitted"`
	Error    string `json:"error,omitempty"`
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
func reemitRepo(ctx context.Context, repos RepoReemitter, did string, logger *slog.Logger) ReemitResult {
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
	for _, e := range entries {
		if ctx.Err() != nil {
			res.Error = ctx.Err().Error()
			return res
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

// handleReemit serves POST /admin/reemit: {"did":"did:plc:..."} re-emits one
// repo, an empty body (or {}) re-emits every active repo. Answers 200 with
// per-repo results; a body-level "failed" count > 0 means at least one repo
// aborted early and the response details why.
func (a *Admin) handleReemit(w http.ResponseWriter, r *http.Request) {
	if a.repos == nil {
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
		res := reemitRepo(ctx, a.repos, did, a.logger)
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
