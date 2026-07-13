package ingest

import (
	"bytes"
	"context"
	stderrors "errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// defaultFollowListInterval is the reconciler's sweep cadence when the
// options leave it zero (config's FOLLOW_LIST_INTERVAL default mirrors it).
const defaultFollowListInterval = 15 * time.Minute

// followListDoc is the FOLLOW_LIST_PATH YAML document:
//
//	communities:
//	  - "!technology@lemmy.world"   # entries MUST be quoted (bare ! is a YAML tag)
//	  - "https://lemmy.ml/c/linux"
//
// Communities is a pointer so a present-but-null key (`communities:`) and a
// missing key (`{}`) are distinguishable from an explicit empty sequence
// (`communities: []`): only the last may unfollow everything.
type followListDoc struct {
	Communities *[]string `yaml:"communities"`
}

// ParseFollowList reads and validates the declarative follow list: every
// entry must be a "!name@host" handle or an http(s) Group URL, deduplicated
// case-insensitively (first spelling wins). Validation is strict because a
// typo'd entry would otherwise mint a garbage did:plc via EnsureCommunity —
// PLC registrations are forever. Unfollowing everything requires an
// explicit `communities: []`: an empty file, a missing communities key, or
// a present-but-null one (`communities:` with nothing under it) are all
// errors, so a truncated or accidentally emptied file cannot read as
// "unfollow all".
func ParseFollowList(path string) ([]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("follow list %s: %w", path, err)
	}
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true) // catch "communites:" and friends
	var doc followListDoc
	if err := dec.Decode(&doc); err != nil {
		if stderrors.Is(err, io.EOF) {
			return nil, fmt.Errorf("follow list %s: file is empty; write \"communities: []\" to explicitly unfollow everything", path)
		}
		return nil, fmt.Errorf("follow list %s: %w", path, err)
	}
	if doc.Communities == nil {
		return nil, fmt.Errorf("follow list %s: communities key is missing or null; write \"communities: []\" to explicitly unfollow everything", path)
	}

	seen := make(map[string]struct{}, len(*doc.Communities))
	entries := make([]string, 0, len(*doc.Communities))
	for i, raw := range *doc.Communities {
		entry, err := validateFollowEntry(raw)
		if err != nil {
			return nil, fmt.Errorf("follow list %s: entry %d: %w", path, i+1, err)
		}
		key := strings.ToLower(entry)
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		entries = append(entries, entry)
	}
	return entries, nil
}

// validateFollowEntry normalizes one follow-list entry and rejects anything
// that is neither a "!name@host" handle nor an http(s) URL.
func validateFollowEntry(raw string) (string, error) {
	entry := strings.TrimSpace(raw)
	if entry == "" {
		return "", fmt.Errorf("entry must not be empty")
	}
	if strings.ContainsAny(entry, " \t") {
		return "", fmt.Errorf("entry %q must not contain whitespace", entry)
	}
	if strings.HasPrefix(entry, "https://") || strings.HasPrefix(entry, "http://") {
		u, err := url.Parse(entry)
		if err != nil || u.Host == "" {
			return "", fmt.Errorf("entry %q is not a valid group URL", entry)
		}
		return entry, nil
	}
	if !strings.HasPrefix(entry, "!") {
		return "", fmt.Errorf("entry %q must be \"!name@host\" or an http(s) group URL (handles need the leading '!', and YAML requires quoting it)", entry)
	}
	if name, host, ok := splitHandle(entry); !ok || name == "" || host == "" {
		return "", fmt.Errorf("entry %q must be \"!name@host\" with exactly one '@'", entry)
	}
	return entry, nil
}

// splitHandle splits "!name@host" into (name, host).
func splitHandle(entry string) (name, host string, ok bool) {
	rest := strings.TrimPrefix(entry, "!")
	name, host, ok = strings.Cut(rest, "@")
	if strings.Contains(host, "@") {
		return "", "", false
	}
	return name, host, ok
}

// entryMatchesCommunity reports whether a follow-list entry names an
// already-tracked community WITHOUT any network resolution: URL entries
// compare against the canonical AP group id, handle entries against the
// stored (preferred_username, instance) pair. Keeping this offline is the
// reconciler's mass-unfollow fail-safe — a resolver or remote-instance
// outage can never make a desired community look "removed".
func entryMatchesCommunity(entry string, c *store.Community) bool {
	if strings.HasPrefix(entry, "https://") || strings.HasPrefix(entry, "http://") {
		return strings.EqualFold(entry, c.APGroupID)
	}
	name, host, ok := splitHandle(entry)
	if !ok {
		return false
	}
	return strings.EqualFold(name, c.PreferredUsername) && strings.EqualFold(host, c.Instance)
}

// FollowReconcilerOptions configures NewFollowReconciler. Admin and Path are
// required; a zero Interval takes defaultFollowListInterval.
type FollowReconcilerOptions struct {
	// Admin supplies the subscribe/unsubscribe cores (and their deps) the
	// sweeps drive.
	Admin *Admin
	// Path is the follow-list YAML (config.FollowListPath).
	Path string
	// Interval is the sweep cadence (config.FollowListInterval).
	Interval time.Duration
	Logger   *slog.Logger
}

// FollowReconciler converges the communities table to the declarative
// follow list: file entries missing from the table are subscribed, tracked
// subscriptions missing from the file are unfollowed. The file is
// authoritative — manual /admin subscriptions not in it are unfollowed at
// the next sweep. Materialized records are never deleted.
type FollowReconciler struct {
	admin    *Admin
	path     string
	interval time.Duration
	logger   *slog.Logger
	// mu serializes sweeps: the ticker and POST /admin/communities/reconcile
	// could otherwise both snapshot a community as absent and subscribe it
	// twice — duplicate Follows are merely noisy, but the racing
	// EnsureCommunity calls can mint twice, and the materializer documents
	// that the losing mint leaves an orphaned permanent DID.
	mu sync.Mutex
}

// NewFollowReconciler validates options and builds the reconciler.
func NewFollowReconciler(opts FollowReconcilerOptions) (*FollowReconciler, error) {
	if opts.Admin == nil {
		return nil, errors.NewValidationError("admin", "must not be nil")
	}
	if opts.Path == "" {
		return nil, errors.NewValidationError("path", "must not be empty")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	interval := opts.Interval
	if interval <= 0 {
		interval = defaultFollowListInterval
	}
	return &FollowReconciler{
		admin:    opts.Admin,
		path:     opts.Path,
		interval: interval,
		logger:   logger,
	}, nil
}

// SweepResult reports what one reconcile pass changed. Subscribed and
// Failed carry follow-list entries; Unsubscribed carries canonical AP group
// ids (the file no longer names those communities, so their entries are
// gone by definition).
type SweepResult struct {
	Subscribed   []string `json:"subscribed"`
	Unsubscribed []string `json:"unsubscribed"`
	Failed       []string `json:"failed"`
}

// Run converges once immediately, then on every interval tick until ctx is
// cancelled. Sweep failures are logged, never fatal — the periodic re-sweep
// is the retry.
func (r *FollowReconciler) Run(ctx context.Context) {
	r.logger.Info("follow reconciler started", "path", r.path, "interval", r.interval)
	if _, err := r.Sweep(ctx); err != nil && ctx.Err() == nil {
		r.logger.Error("follow reconciler: startup sweep failed", "error", err)
	}
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := r.Sweep(ctx); err != nil && ctx.Err() == nil {
				r.logger.Error("follow reconciler: sweep failed", "error", err)
			}
		}
	}
}

// Sweep runs one reconcile pass. Exported so tests and the admin trigger
// (POST /admin/communities/reconcile) can drive one synchronously, like
// FollowRetrier.Sweep. A parse or list failure aborts the pass with NO
// state changes (a broken file must never read as "unfollow everything");
// per-community subscribe/unsubscribe failures are recorded in the result
// and the pass keeps converging the rest. Sweeps are serialized (see mu): a
// concurrent caller blocks, then runs against the converged state (a no-op
// when nothing changed in between).
func (r *FollowReconciler) Sweep(ctx context.Context) (SweepResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	result := SweepResult{Subscribed: []string{}, Unsubscribed: []string{}, Failed: []string{}}

	entries, err := ParseFollowList(r.path)
	if err != nil {
		return result, err
	}

	// Union of accepted+pending — everything the bridge currently follows
	// (the same union the admin list endpoint reports).
	var current []*store.Community
	for _, state := range []store.FollowState{store.FollowStateAccepted, store.FollowStatePending} {
		list, err := r.admin.communities.ListByFollowState(ctx, state)
		if err != nil {
			return result, fmt.Errorf("follow reconciler: list %s communities: %w", state, err)
		}
		current = append(current, list...)
	}

	if len(entries) == 0 && len(current) > 0 {
		r.logger.Warn("follow list is empty: unfollowing every current subscription",
			"path", r.path, "current", len(current))
	}

	// Offline diff (see entryMatchesCommunity): which tracked communities
	// the file still names, and which entries are not tracked yet.
	matched := make([]bool, len(entries))
	var extras []*store.Community
	for _, c := range current {
		found := false
		for i, entry := range entries {
			if entryMatchesCommunity(entry, c) {
				matched[i] = true
				found = true
				break
			}
		}
		if !found {
			extras = append(extras, c)
		}
	}

	for _, c := range extras {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		if _, err := r.admin.unsubscribeByGroupIRI(ctx, c.APGroupID); err != nil {
			// unsubscribeByGroupIRI already logged the details.
			result.Failed = append(result.Failed, c.APGroupID)
			continue
		}
		result.Unsubscribed = append(result.Unsubscribed, c.APGroupID)
	}

	for i, entry := range entries {
		if matched[i] {
			continue
		}
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		if _, err := r.admin.subscribe(ctx, entry); err != nil {
			// subscribe already logged the details (including consent
			// refusals, which stay refused until the community drops its
			// opt-out marker — the sweep keeps going either way).
			result.Failed = append(result.Failed, entry)
			continue
		}
		result.Subscribed = append(result.Subscribed, entry)
	}

	if len(result.Subscribed) > 0 || len(result.Unsubscribed) > 0 || len(result.Failed) > 0 {
		r.logger.Info("follow reconciler: sweep complete",
			"desired", len(entries),
			"subscribed", len(result.Subscribed),
			"unsubscribed", len(result.Unsubscribed),
			"failed", len(result.Failed))
	}
	return result, nil
}
