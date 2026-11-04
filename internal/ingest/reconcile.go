package ingest

import (
	"bytes"
	"context"
	stderrors "errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	"tidepool/internal/errors"
	"tidepool/internal/identity"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// defaultFollowListInterval is the reconciler's sweep cadence when the
// options leave it zero (config's FOLLOW_LIST_INTERVAL default mirrors it).
const defaultFollowListInterval = 15 * time.Minute

const (
	// maxListedAddsPerSweep caps listed additions across all instances.
	maxListedAddsPerSweep = 20
	// maxBackfillBacklog is the unfinished backlog past which listed adds stop.
	maxBackfillBacklog = 20
	// refusalSkip is how long a refused listed community is left alone.
	refusalSkip = 24 * time.Hour
	// failedAddSkip is how long a listed community whose add failed is left
	// alone.
	failedAddSkip = time.Hour
)

// followListDoc is the FOLLOW_LIST_PATH YAML document:
//
//	communities:
//	  - "!technology@lemmy.world"   # entries MUST be quoted (bare ! is a YAML tag)
//	  - "https://lemmy.ml/c/linux"
//	instances:                      # optional
//	  - host: "lemmy.world"         # every local community of the host
//	    exclude:                    # optional, entries on that host
//	      - "!news@lemmy.world"
//	outbound_disabled_hosts:        # optional
//	  - "example.com"
//	outbound_disabled_communities:  # optional
//	  - "https://lemmy.world/c/comicstrips"
//
// Communities is a pointer so a present-but-null key (`communities:`) and a
// missing key (`{}`) are distinguishable from an explicit empty sequence
// (`communities: []`): only the last may unfollow everything. List items are
// pointers because yaml.v3 drops a null item decoded into a string; a pointer
// keeps it at its index, so the parser can reject it.
type followListDoc struct {
	Communities                 *[]*string `yaml:"communities"`
	OutboundDisabledHosts       []*string  `yaml:"outbound_disabled_hosts"`
	OutboundDisabledCommunities []*string  `yaml:"outbound_disabled_communities"`
	// Instances is optional. A present-but-null key is refused by
	// instancesKeyIsNull, because this field cannot tell it from a missing one.
	Instances []*instanceEntryDoc `yaml:"instances"`
}

// instanceEntryDoc is one `instances` item. Exclude is a node because only the
// node tells a present-but-null key from a missing one.
type instanceEntryDoc struct {
	Host    string    `yaml:"host"`
	Exclude yaml.Node `yaml:"exclude"`
}

// bareHostname matches a lowercased hostname with no scheme, port or path. One
// label is valid: the e2e peer is plain "lemmy".
var bareHostname = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)*$`)

// FollowList is the parsed follow-list file. Both outbound sets are non-nil,
// and hold lowercased hosts and exact community ActivityPub ids.
type FollowList struct {
	Communities                 []string
	Instances                   []InstanceEntry
	OutboundDisabledHosts       map[string]struct{}
	OutboundDisabledCommunities map[string]struct{}
}

// InstanceEntry names a whole instance: every community local to Host except
// those Exclude names.
type InstanceEntry struct {
	Host    string
	Exclude []string
}

// Desires reports whether the list wants the community: an entry in Communities
// names it, or its instance is listed and no Exclude entry names it. It is
// offline, like entryMatchesCommunity, and that is why a failed or short listing
// can never unfollow anything. A listing result has no row yet, so it passes an
// empty preferredUsername.
func (l *FollowList) Desires(groupID, instance, preferredUsername string) bool {
	matches := func(entry string) bool {
		return entryMatchesCommunity(entry, groupID, instance, preferredUsername)
	}
	if slices.ContainsFunc(l.Communities, matches) {
		return true
	}
	for _, listed := range l.Instances {
		if strings.EqualFold(listed.Host, instance) {
			return !slices.ContainsFunc(listed.Exclude, matches)
		}
	}
	return false
}

// OutboundSwitchSetter receives the outbound host and community sets from each
// successful parse.
type OutboundSwitchSetter interface {
	SetDisabledSets(hosts, communities map[string]struct{})
}

// ParseFollowList reads and validates the declarative follow list: every
// entry must be a "!name@host" handle or an http(s) Group URL, deduplicated
// case-insensitively (first spelling wins). Validation is strict because a
// typo'd entry would otherwise mint a garbage did:plc via EnsureCommunity —
// PLC registrations are forever. Unfollowing everything requires an
// explicit `communities: []`: an empty file, a missing communities key, a
// present-but-null one (`communities:` with nothing under it), or a null list
// item are all errors, so a truncated or accidentally emptied file cannot read
// as "unfollow all". The `instances` key is optional, but present with a null
// value it is an error for the same reason: that would unfollow every community
// only an instance entry names. A present `exclude` with a null value is an error
// too, because it would make every excluded community a candidate. The outbound
// switch keys are optional; a null item under one is an error as well. An entry
// the parser rejects fails the whole file, so the last good sets stay on after
// startup. A well-formed misspelling is accepted and replaces the old entry.
func ParseFollowList(path string) (*FollowList, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("follow list %s: %w", path, err)
	}
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true) //nolint:misspell // catch typo'd keys such as "communites:"
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
	nullInstances, err := instancesKeyIsNull(raw)
	if err != nil {
		return nil, fmt.Errorf("follow list %s: %w", path, err)
	}
	if nullInstances {
		return nil, fmt.Errorf("follow list %s: instances key is null; remove it or write \"instances: []\"", path)
	}

	seen := make(map[string]struct{}, len(*doc.Communities))
	entries := make([]string, 0, len(*doc.Communities))
	for i, raw := range *doc.Communities {
		if raw == nil {
			return nil, fmt.Errorf("follow list %s: entry %d: must not be null", path, i+1)
		}
		entry, err := validateFollowEntry(*raw)
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

	instances, err := parseInstanceEntries(doc.Instances)
	if err != nil {
		return nil, fmt.Errorf("follow list %s: %w", path, err)
	}

	hosts := make(map[string]struct{}, len(doc.OutboundDisabledHosts))
	for i, raw := range doc.OutboundDisabledHosts {
		if raw == nil {
			return nil, fmt.Errorf("follow list %s: outbound_disabled_hosts entry %d: must not be null", path, i+1)
		}
		host := strings.ToLower(*raw)
		if !bareHostname.MatchString(host) {
			return nil, fmt.Errorf("follow list %s: outbound_disabled_hosts entry %d: %q must be a bare hostname, with no scheme, port or path", path, i+1, *raw)
		}
		hosts[host] = struct{}{}
	}
	communities := make(map[string]struct{}, len(doc.OutboundDisabledCommunities))
	for i, raw := range doc.OutboundDisabledCommunities {
		if raw == nil {
			return nil, fmt.Errorf("follow list %s: outbound_disabled_communities entry %d: must not be null", path, i+1)
		}
		if !isCommunitySwitchURL(*raw) {
			return nil, fmt.Errorf("follow list %s: outbound_disabled_communities entry %d: %q must be an http(s) URL with a host", path, i+1, *raw)
		}
		communities[*raw] = struct{}{}
	}
	return &FollowList{
		Communities:                 entries,
		Instances:                   instances,
		OutboundDisabledHosts:       hosts,
		OutboundDisabledCommunities: communities,
	}, nil
}

// instancesKeyIsNull reports an `instances` key present with a null value. It
// decodes the document a second time because only the node shows the difference
// from a missing key.
func instancesKeyIsNull(raw []byte) (bool, error) {
	var keys map[string]yaml.Node
	if err := yaml.Unmarshal(raw, &keys); err != nil {
		return false, err
	}
	node, present := keys["instances"]
	return present && node.ShortTag() == "!!null", nil
}

// parseInstanceEntries validates the `instances` items. A host is a bare
// lowercase hostname, unique in the file, and each exclude entry belongs to it.
func parseInstanceEntries(docs []*instanceEntryDoc) ([]InstanceEntry, error) {
	entries := make([]InstanceEntry, 0, len(docs))
	seen := make(map[string]struct{}, len(docs))
	for i, doc := range docs {
		if doc == nil {
			return nil, fmt.Errorf("instances entry %d: must not be null", i+1)
		}
		if !bareHostname.MatchString(doc.Host) {
			return nil, fmt.Errorf("instances entry %d: host %q must be a bare lowercase hostname, with no scheme, port or path", i+1, doc.Host)
		}
		if _, dup := seen[doc.Host]; dup {
			return nil, fmt.Errorf("instances entry %d: host %q is listed twice", i+1, doc.Host)
		}
		seen[doc.Host] = struct{}{}
		exclude, err := parseInstanceExclude(doc)
		if err != nil {
			return nil, fmt.Errorf("instances entry %d: %w", i+1, err)
		}
		entries = append(entries, InstanceEntry{Host: doc.Host, Exclude: exclude})
	}
	return entries, nil
}

// parseInstanceExclude validates the exclude entries of one instance entry.
func parseInstanceExclude(doc *instanceEntryDoc) ([]string, error) {
	if doc.Exclude.Kind == 0 {
		return nil, nil
	}
	if doc.Exclude.ShortTag() == "!!null" {
		return nil, fmt.Errorf("exclude is null; remove the key or write \"exclude: []\"")
	}
	var items []*string
	if err := doc.Exclude.Decode(&items); err != nil {
		return nil, fmt.Errorf("exclude: %w", err)
	}
	var exclude []string
	for i, raw := range items {
		if raw == nil {
			return nil, fmt.Errorf("exclude entry %d: must not be null", i+1)
		}
		entry, err := validateFollowEntry(*raw)
		if err != nil {
			return nil, fmt.Errorf("exclude entry %d: %w", i+1, err)
		}
		if !strings.EqualFold(followEntryHost(entry), doc.Host) {
			return nil, fmt.Errorf("exclude entry %d: %q is not on host %q", i+1, entry, doc.Host)
		}
		exclude = append(exclude, entry)
	}
	return exclude, nil
}

// followEntryHost gives the hostname of an entry validateFollowEntry accepted.
func followEntryHost(entry string) string {
	if isFollowEntryURL(entry) {
		parsed, err := url.Parse(entry)
		if err != nil {
			return ""
		}
		return parsed.Hostname()
	}
	_, host, _ := splitHandle(entry)
	return host
}

// isFollowEntryURL tells a URL entry from a handle entry.
func isFollowEntryURL(entry string) bool {
	return strings.HasPrefix(entry, "https://") || strings.HasPrefix(entry, "http://")
}

// isCommunitySwitchURL reports whether entry can match a community ActivityPub
// id. The id is compared exactly, so an entry with a space or an uppercase
// scheme is refused rather than kept as a switch that never blocks.
func isCommunitySwitchURL(entry string) bool {
	if !strings.HasPrefix(entry, "https://") && !strings.HasPrefix(entry, "http://") {
		return false
	}
	u, err := url.Parse(entry)
	return err == nil && u.Hostname() != "" && !strings.Contains(entry, " ")
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
	if isFollowEntryURL(entry) {
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

// entryMatchesCommunity reports whether a follow-list entry names a community
// WITHOUT any network resolution: URL entries compare against the canonical AP
// group id, handle entries against the instance and the id's last path segment.
// The stored preferred_username cannot be compared when the id has a segment: an
// ID-only reference stores the handle label (c-technology, c-some-community,
// possibly truncated or suffixed) and an embedded Group document the raw name.
// So it counts only for an id with no segment. Keeping this offline is the
// reconciler's mass-unfollow fail-safe — a resolver or remote-instance outage
// can never make a desired community look "removed".
func entryMatchesCommunity(entry, groupID, instance, preferredUsername string) bool {
	if isFollowEntryURL(entry) {
		return strings.EqualFold(entry, groupID)
	}
	name, host, ok := splitHandle(entry)
	if !ok || !strings.EqualFold(host, instance) {
		return false
	}
	communityName := lastPathSegment(groupID)
	if communityName == "" {
		communityName = preferredUsername
	}
	return strings.EqualFold(name, communityName)
}

// lastPathSegment gives the last segment of the id's URL path, or "" when it has
// none, as with https://host and https://host/.
func lastPathSegment(groupID string) string {
	parsed, err := url.Parse(groupID)
	if err != nil {
		return ""
	}
	path := strings.TrimRight(parsed.Path, "/")
	return path[strings.LastIndexByte(path, '/')+1:]
}

// BackfillRetrier re-triggers a community whose backfill is unfinished. A sweep
// calls it for every such community on every pass while holding its lock, so
// RetryUnfinished must not block, and it decides for itself whether a walk is
// due.
type BackfillRetrier interface {
	RetryUnfinished(community *store.Community)
	// UnfinishedBacklog counts the given communities whose last_backfill_at is
	// unset and that are not inside a backoff.
	UnfinishedBacklog(communities []*store.Community) int
}

// FollowReconcilerOptions configures NewFollowReconciler. Admin, Path, Backfill,
// Lister, Posts and Budget are required; a zero Interval takes
// defaultFollowListInterval.
type FollowReconcilerOptions struct {
	// Admin supplies the subscribe/unsubscribe cores (and their deps) the
	// sweeps drive.
	Admin *Admin
	// Path is the follow-list YAML (config.FollowListPath).
	Path string
	// Interval is the sweep cadence (config.FollowListInterval).
	Interval time.Duration
	Logger   *slog.Logger
	// Switches receives the outbound sets from each parse. Nil means no swap.
	Switches OutboundSwitchSetter
	// Backfill retries unfinished backfills. Required, because a reconciler
	// without it would leave them unfinished for good with no sign of it.
	Backfill BackfillRetrier
	// Lister reads the communities of each `instances` host.
	Lister CommunityLister
	// Posts reads the communities of each `instances` host's recent local posts.
	Posts RecentPostReader
	// Budget is the PLC write budget, read to stop adds during a pause.
	Budget *identity.PLCWriteBudget
	// Now defaults to time.Now.
	Now func() time.Time
}

// FollowReconciler converges the communities table to the declarative
// follow list: file entries missing from the table are subscribed, communities
// listed on an `instances` host are subscribed a bounded batch at a time, and
// tracked subscriptions the file does not want are unfollowed. The file is
// authoritative — manual /admin subscriptions not in it are unfollowed at
// the next sweep. Materialized records are never deleted. Each sweep also
// retries the unfinished backfills of the accepted communities the file wants.
type FollowReconciler struct {
	admin    *Admin
	path     string
	interval time.Duration
	logger   *slog.Logger
	// parse reads the follow list.
	parse func(path string) (*FollowList, error)
	// switches receives the outbound sets. Nil means no swap.
	switches OutboundSwitchSetter
	backfill BackfillRetrier
	lister   CommunityLister
	posts    RecentPostReader
	budget   *identity.PLCWriteBudget
	now      func() time.Time
	// swapMu makes each parse-and-swap one step, so a sweep holding an older
	// parse cannot overwrite the sets from a newer one. It is held only for the
	// file read and the swap, and is separate from mu so that a swap does not
	// wait behind a slow sweep.
	swapMu sync.Mutex
	// mu serializes sweeps: the ticker and POST /admin/communities/reconcile
	// could otherwise both snapshot a community as absent and subscribe it
	// twice — duplicate Follows are merely noisy, but the racing
	// EnsureCommunity calls can mint twice, and the materializer documents
	// that the losing mint leaves an orphaned permanent DID.
	mu sync.Mutex
	// positions, skips, postMarks and discovered are guarded by mu. None survives
	// a restart: the next sweep starts each host at firstListingPosition and walks
	// its posts from no mark, and a restart costs each skipped community one more
	// fetch.
	//
	// positions is where the next read of each host starts. A host with no entry
	// starts at firstListingPosition, not the zero value.
	positions map[string]ListingPosition
	// skips holds, per listed actor id, when the community may be tried again.
	skips map[string]time.Time
	// postMarks is, per host, the newest post of its last successful post walk.
	// The next walk reads back to it.
	postMarks map[string]PostMark
	// discovered holds the actor ids post walks returned, each with its host,
	// until they are added, tracked, named in Communities or no longer desired.
	// A leftover stays a candidate without being seen in a post again.
	discovered map[string]string
}

// NewFollowReconciler validates options and builds the reconciler.
func NewFollowReconciler(opts FollowReconcilerOptions) (*FollowReconciler, error) {
	if opts.Admin == nil {
		return nil, errors.NewValidationError("admin", "must not be nil")
	}
	if opts.Path == "" {
		return nil, errors.NewValidationError("path", "must not be empty")
	}
	if opts.Backfill == nil {
		return nil, errors.NewValidationError("backfill", "must not be nil")
	}
	if opts.Lister == nil {
		return nil, errors.NewValidationError("lister", "must not be nil")
	}
	if opts.Posts == nil {
		return nil, errors.NewValidationError("posts", "must not be nil")
	}
	if opts.Budget == nil {
		return nil, errors.NewValidationError("budget", "must not be nil")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	interval := opts.Interval
	if interval <= 0 {
		interval = defaultFollowListInterval
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &FollowReconciler{
		admin:      opts.Admin,
		path:       opts.Path,
		interval:   interval,
		logger:     logger,
		parse:      ParseFollowList,
		switches:   opts.Switches,
		backfill:   opts.Backfill,
		lister:     opts.Lister,
		posts:      opts.Posts,
		budget:     opts.Budget,
		now:        now,
		positions:  make(map[string]ListingPosition),
		skips:      make(map[string]time.Time),
		postMarks:  make(map[string]PostMark),
		discovered: make(map[string]string),
	}, nil
}

// SweepResult reports what one reconcile pass changed. Subscribed and
// Failed carry follow-list entries, and actor ids for listed communities;
// Unsubscribed carries canonical AP group ids (the file no longer names those
// communities, so their entries are gone by definition). A listed community the
// budget or a limit stopped is left for a later sweep and appears nowhere.
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

// parseAndSwap reads the follow list and installs its outbound sets. A parse
// failure swaps nothing, so the last good sets stay on.
func (r *FollowReconciler) parseAndSwap() error {
	r.swapMu.Lock()
	defer r.swapMu.Unlock()
	list, err := r.parse(r.path)
	if err != nil {
		return err
	}
	if r.switches != nil {
		r.switches.SetDisabledSets(list.OutboundDisabledHosts, list.OutboundDisabledCommunities)
	}
	return nil
}

// Sweep runs one reconcile pass. Exported so tests and the admin trigger
// (POST /admin/communities/reconcile) can drive one synchronously, like
// FollowRetrier.Sweep. The outbound sets are swapped first, before mu, so a
// kill switch applies at once even behind a slow sweep. A parse or list
// failure aborts the pass with no subscription changes (a broken file must
// never read as "unfollow everything"); the outbound sets may already be
// applied. Per-community subscribe/unsubscribe failures are recorded in the
// result and the pass keeps converging the rest. Sweeps are serialized (see
// mu): a concurrent caller blocks, then runs against the converged state (a
// no-op when nothing changed in between). A pass that converged, whatever its
// per-community failures, ends by retrying unfinished backfills; one that
// stopped early retries nothing.
//
// The order: unfollow extras, add explicit entries, list each host, walk each
// host's recent posts, add listed and post-discovered communities, move each
// host's position, retry backfills. Listing and walking come after the unfollows
// and can only add to the desired set, so a read that fails or comes back short
// unfollows nothing. Every host is listed and walked even when no add is
// possible.
func (r *FollowReconciler) Sweep(ctx context.Context) (SweepResult, error) {
	result := SweepResult{Subscribed: []string{}, Unsubscribed: []string{}, Failed: []string{}}

	if err := r.parseAndSwap(); err != nil {
		return result, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Mints under a sweep wait for a token but not for a pause, which can last an
	// hour with mu held.
	ctx = withSweepMint(ctx)

	// Parsed again: this sweep may have waited behind a slow one, and it must
	// converge on the file as it stands now.
	list, err := r.parse(r.path)
	if err != nil {
		return result, err
	}
	current, err := r.trackedCommunities(ctx)
	if err != nil {
		return result, err
	}
	if len(list.Communities) == 0 && len(list.Instances) == 0 && len(current) > 0 {
		r.logger.Warn("follow list is empty: unfollowing every current subscription",
			"path", r.path, "current", len(current))
	}
	diff := diffFollowList(list, current)

	if err := r.unfollowExtras(ctx, diff.extras, &result); err != nil {
		return result, err
	}

	tracked := make(map[string]struct{}, len(current))
	for _, c := range current {
		tracked[c.APGroupID] = struct{}{}
	}
	budgetStopped, err := r.addExplicitEntries(ctx, diff.missing, tracked, &result)
	if err != nil {
		return result, err
	}

	listings, err := r.listInstances(ctx, list.Instances)
	if err != nil {
		return result, err
	}
	if err := r.walkPosts(ctx, list.Instances); err != nil {
		return result, err
	}
	candidates := r.listedCandidates(list, listings, tracked)
	leftovers := candidates
	if !budgetStopped {
		leftovers = r.addListed(ctx, candidates, &result)
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	r.movePositions(listings, leftovers)

	if len(result.Subscribed) > 0 || len(result.Unsubscribed) > 0 || len(result.Failed) > 0 || len(leftovers) > 0 {
		r.logger.Info("follow reconciler: sweep complete",
			"desired", len(list.Communities),
			"instances", len(list.Instances),
			"subscribed", len(result.Subscribed),
			"unsubscribed", len(result.Unsubscribed),
			"failed", len(result.Failed),
			"leftover", len(leftovers))
	}

	// From the snapshot, so this adds no query and no way to fail the pass. A
	// community this pass subscribed is not in it: its Accept starts its walk.
	for _, c := range diff.unfinished {
		r.backfill.RetryUnfinished(c)
	}
	return result, nil
}

// trackedCommunities is the union of accepted and pending: everything the bridge
// currently follows (the same union the admin list endpoint reports).
func (r *FollowReconciler) trackedCommunities(ctx context.Context) ([]*store.Community, error) {
	var current []*store.Community
	for _, state := range []store.FollowState{store.FollowStateAccepted, store.FollowStatePending} {
		communities, err := r.admin.communities.ListByFollowState(ctx, state)
		if err != nil {
			return nil, fmt.Errorf("follow reconciler: list %s communities: %w", state, err)
		}
		current = append(current, communities...)
	}
	return current, nil
}

// followListDiff is the offline comparison of the file with the tracked
// communities (see entryMatchesCommunity and FollowList.Desires).
type followListDiff struct {
	// missing holds the Communities entries that match no tracked community.
	missing []string
	// extras holds the tracked communities the file does not want.
	extras []*store.Community
	// unfinished holds the wanted accepted communities with no finished backfill.
	unfinished []*store.Community
}

func diffFollowList(list *FollowList, current []*store.Community) followListDiff {
	var diff followListDiff
	matched := make([]bool, len(list.Communities))
	for _, c := range current {
		for i, entry := range list.Communities {
			if entryMatchesCommunity(entry, c.APGroupID, c.Instance, c.PreferredUsername) {
				matched[i] = true
			}
		}
		if !list.Desires(c.APGroupID, c.Instance, c.PreferredUsername) {
			diff.extras = append(diff.extras, c)
		} else if c.FollowState == store.FollowStateAccepted && c.LastBackfillAt == nil {
			diff.unfinished = append(diff.unfinished, c)
		}
	}
	for i, entry := range list.Communities {
		if !matched[i] {
			diff.missing = append(diff.missing, entry)
		}
	}
	return diff
}

// unfollowExtras undoes each extra. It returns an error only when ctx ends.
func (r *FollowReconciler) unfollowExtras(ctx context.Context, extras []*store.Community, result *SweepResult) error {
	for _, c := range extras {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := r.admin.unsubscribeByGroupIRI(ctx, c.APGroupID); err != nil {
			// unsubscribeByGroupIRI already logged the details.
			result.Failed = append(result.Failed, c.APGroupID)
			continue
		}
		result.Unsubscribed = append(result.Unsubscribed, c.APGroupID)
	}
	return nil
}

// addExplicitEntries subscribes the entries, with no add limit, and records each
// added community in tracked. Once the budget is paused or an add is throttled,
// every remaining entry goes to Failed untried and budgetStopped is true, so a
// pause costs no fetch. The next sweep retries them. It returns an error only
// when ctx ends.
func (r *FollowReconciler) addExplicitEntries(ctx context.Context, entries []string, tracked map[string]struct{}, result *SweepResult) (budgetStopped bool, err error) {
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return budgetStopped, err
		}
		budgetStopped = budgetStopped || r.budget.Paused()
		if budgetStopped {
			result.Failed = append(result.Failed, entry)
			continue
		}
		community, err := r.admin.subscribe(ctx, entry)
		if err != nil {
			// subscribe already logged the details (including consent
			// refusals, which stay refused until the community drops its
			// opt-out marker — the sweep keeps going either way).
			result.Failed = append(result.Failed, entry)
			budgetStopped = isThrottled(err)
			continue
		}
		tracked[community.APGroupID] = struct{}{}
		result.Subscribed = append(result.Subscribed, entry)
	}
	return budgetStopped, nil
}

// listInstances reads each host from its position. It returns an error only when
// ctx ends. A failed read is not one: its results are used, and the lister has
// logged it.
func (r *FollowReconciler) listInstances(ctx context.Context, instances []InstanceEntry) (map[string]CommunityListing, error) {
	listings := make(map[string]CommunityListing, len(instances))
	for _, instance := range instances {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		position, known := r.positions[instance.Host]
		if !known {
			position = firstListingPosition
		}
		listings[instance.Host] = r.lister.ListCommunities(ctx, instance.Host, position)
	}
	return listings, nil
}

// walkPosts walks each host's recent posts from its mark and records every
// community returned in discovered. A successful walk moves the host's mark to
// its newest post; a failed one keeps the old mark, so the next walk reads the
// same posts again. A host no longer listed loses its mark, so re-adding it
// starts with a first walk. It returns an error only when ctx ends.
func (r *FollowReconciler) walkPosts(ctx context.Context, instances []InstanceEntry) error {
	listed := make(map[string]struct{}, len(instances))
	for _, instance := range instances {
		listed[instance.Host] = struct{}{}
	}
	maps.DeleteFunc(r.postMarks, func(host string, _ PostMark) bool {
		_, found := listed[host]
		return !found
	})
	for _, instance := range instances {
		if err := ctx.Err(); err != nil {
			return err
		}
		walk := r.posts.RecentPostCommunities(ctx, instance.Host, r.postMarks[instance.Host])
		for _, actorID := range walk.ActorIDs {
			r.discovered[actorID] = instance.Host
		}
		if walk.Err == nil && walk.Newest != (PostMark{}) {
			r.postMarks[instance.Host] = walk.Newest
		}
	}
	return nil
}

// listedCandidate is one listing or post-discovered result that may be added.
// discovered is set for any community a post walk found, even when a listing
// also returned it.
type listedCandidate struct {
	actorID    string
	host       string
	discovered bool
}

// listedCandidates merges the listings and the discovered communities into the
// communities to try, deduplicated by actor id across hosts: post-discovered
// communities first, then listing-only ones, each by actor id. It leaves
// out what is tracked, excluded or skipped, and anything the file names in
// Communities: those belong to the explicit adds, which never use the skip map or
// a batch slot. A discovered community left out for any reason but a skip is
// deleted from discovered; a skipped one is tried again when its skip ends.
func (r *FollowReconciler) listedCandidates(list *FollowList, listings map[string]CommunityListing, tracked map[string]struct{}) []listedCandidate {
	now := r.now()
	maps.DeleteFunc(r.skips, func(_ string, expiry time.Time) bool { return !expiry.After(now) })

	// wanted reports whether the community belongs to the listed adds at all,
	// leaving the skip map aside.
	wanted := func(actorID, host string) bool {
		_, isTracked := tracked[actorID]
		explicit := slices.ContainsFunc(list.Communities, func(entry string) bool {
			return entryMatchesCommunity(entry, actorID, host, "")
		})
		return !isTracked && !explicit && list.Desires(actorID, host, "")
	}
	var candidates []listedCandidate
	for host, listing := range listings {
		for _, actorID := range listing.ActorIDs {
			if _, skipped := r.skips[actorID]; skipped || !wanted(actorID, host) {
				continue
			}
			_, discovered := r.discovered[actorID]
			candidates = append(candidates, listedCandidate{actorID: actorID, host: host, discovered: discovered})
		}
	}
	for actorID, host := range r.discovered {
		if !wanted(actorID, host) {
			delete(r.discovered, actorID)
			continue
		}
		if _, skipped := r.skips[actorID]; !skipped {
			candidates = append(candidates, listedCandidate{actorID: actorID, host: host, discovered: true})
		}
	}
	slices.SortFunc(candidates, func(a, b listedCandidate) int {
		if a.discovered != b.discovered {
			if a.discovered {
				return -1
			}
			return 1
		}
		return strings.Compare(a.actorID, b.actorID)
	})
	return slices.CompactFunc(candidates, func(a, b listedCandidate) bool { return a.actorID == b.actorID })
}

// addListed subscribes candidates in order, up to maxListedAddsPerSweep tries,
// and returns the ones it did not get to. A leftover the host's listing returned
// keeps that host's position (see movePositions). A tried candidate uses a slot
// whether it was added, refused or failed. One the budget stopped, or that ctx
// ended, was not really tried: it is a leftover, and reported nowhere.
func (r *FollowReconciler) addListed(ctx context.Context, candidates []listedCandidate, result *SweepResult) []listedCandidate {
	tried := 0
	for index, candidate := range candidates {
		if ctx.Err() != nil || tried == maxListedAddsPerSweep || r.listedAddsBlocked(ctx) {
			return candidates[index:]
		}
		_, err := r.admin.subscribe(ctx, candidate.actorID)
		switch {
		case err == nil:
			result.Subscribed = append(result.Subscribed, candidate.actorID)
		case isThrottled(err), ctx.Err() != nil:
			return candidates[index:]
		case materialize.IsSkip(err):
			r.skips[candidate.actorID] = r.now().Add(refusalSkip)
			result.Failed = append(result.Failed, candidate.actorID)
		default:
			// A permanent failure at the head of a stable listing would otherwise
			// fill every batch and stall onboarding.
			r.skips[candidate.actorID] = r.now().Add(failedAddSkip)
			result.Failed = append(result.Failed, candidate.actorID)
		}
		tried++
	}
	return nil
}

// listedAddsBlocked reports a pause or a backfill backlog over its limit. The
// accepted communities are read again for each add, so an Accept landing during
// the batch counts.
func (r *FollowReconciler) listedAddsBlocked(ctx context.Context) bool {
	if r.budget.Paused() {
		return true
	}
	accepted, err := r.admin.communities.ListByFollowState(ctx, store.FollowStateAccepted)
	if err != nil {
		r.logger.Error("follow reconciler: read backfill backlog", "error", err)
		return true
	}
	return r.backfill.UnfinishedBacklog(accepted) > maxBackfillBacklog
}

// movePositions moves each host to its next position, unless a leftover is one
// of the communities this sweep's listing of the host returned: then the same
// pages are read again. A leftover only a post walk found does not hold the
// position, since it stays in discovered; holding it would keep the listing from
// wrapping to new communities. The listing limit is logged here, so a held read
// does not repeat it.
func (r *FollowReconciler) movePositions(listings map[string]CommunityListing, leftovers []listedCandidate) {
	leftoverIDs := make(map[string]struct{}, len(leftovers))
	for _, leftover := range leftovers {
		leftoverIDs[leftover.actorID] = struct{}{}
	}
	for host, listing := range listings {
		held := slices.ContainsFunc(listing.ActorIDs, func(actorID string) bool {
			_, leftover := leftoverIDs[actorID]
			return leftover
		})
		if held {
			continue
		}
		r.positions[host] = listing.Next
		if listing.ReachedLimit {
			r.logger.Warn("community listing reached its limit: communities between the two ends may be unreachable",
				"host", host, "limit", listingReachLimit)
		}
	}
}
