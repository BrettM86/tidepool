package ingest

import (
	"cmp"
	"context"
	stderrors "errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/identity"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// A second listed host. It sorts before lemmyFixtureHost, so every actor id on
// it is lower than every actor id on lemmy.world.
const lemmyMLHost = "lemmy.ml"

// peerExplicitID is a community on a host nothing lists.
const peerExplicitID = "https://peer.example/c/explicit-one"

// instanceRig is a real FollowReconciler over a real LemmyLister and the strict
// fixture, reading a follow list the test can rewrite. Listing goes to the
// fixture and everything else to the harness fixture server, which keys on path
// alone: two hosts' communities need different names to both be served.
type instanceRig struct {
	h          *harness
	fixture    *lemmyFixture
	listerLogs *syncBuffer
	path       string
	clock      *manualClock
	budget     *identity.PLCWriteBudget
	reconciler *FollowReconciler
	// posts is the reconciler's RecentPostReader: the real lister, recorded.
	posts *recordingPostReader
	// postCount is how many posts post has made.
	postCount int
}

// instanceRigOptions swaps what the rig builds for itself. The zero value is the
// harness Admin and recording backfill, a budget that never makes anything wait
// on a new manual clock, and the default page caps and post page size.
type instanceRigOptions struct {
	admin    *Admin
	backfill BackfillRetrier
	budget   *identity.PLCWriteBudget
	clock    *manualClock
	maxPages int
	// postLimit and maxPostPages are the post walk's page size and page cap.
	postLimit    int
	maxPostPages int
}

func newInstanceRig(t *testing.T, h *harness, followList string, options instanceRigOptions) *instanceRig {
	t.Helper()
	rig := &instanceRig{
		h:       h,
		fixture: newLemmyFixture(t),
		path:    writeFollowList(t, followList),
		clock:   options.clock,
		budget:  options.budget,
	}
	if rig.clock == nil {
		rig.clock = newManualClock(t)
	}
	if rig.budget == nil {
		budget, err := identity.NewPLCWriteBudget(identity.PLCWriteBudgetOptions{
			WritesPerMinute: 600000, Logger: slog.New(slog.DiscardHandler),
			Now: rig.clock.Now, Wait: rig.clock.Wait,
		})
		require.NoError(t, err)
		rig.budget = budget
	}
	admin := options.admin
	if admin == nil {
		admin = h.admin
	}
	backfill := options.backfill
	if backfill == nil {
		backfill = h.backfills
	}
	lister, logs := newFixtureListerWith(t, rig.fixture, LemmyListerOptions{
		MaxPages: options.maxPages, MaxPostPages: options.maxPostPages, PostLimit: options.postLimit,
	})
	rig.listerLogs = logs
	rig.posts = &recordingPostReader{reader: lister}
	reconciler, err := NewFollowReconciler(FollowReconcilerOptions{
		Admin: admin, Path: rig.path, Backfill: backfill, Lister: lister, Posts: rig.posts, Budget: rig.budget,
		Now: rig.clock.Now, Logger: slog.New(slog.DiscardHandler),
	})
	require.NoError(t, err)
	rig.reconciler = reconciler
	return rig
}

// recordedPostWalk is one walk a sweep asked for, and what it returned.
type recordedPostWalk struct {
	host string
	walk PostWalk
}

// recordingPostReader passes each walk to reader and keeps its result.
type recordingPostReader struct {
	reader RecentPostReader

	mutex sync.Mutex
	walks []recordedPostWalk
}

func (r *recordingPostReader) RecentPostCommunities(ctx context.Context, host string, since PostMark) PostWalk {
	walk := r.reader.RecentPostCommunities(ctx, host, since)
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.walks = append(r.walks, recordedPostWalk{host: host, walk: walk})
	return walk
}

// walkCount marks a point in the post walks for walksSince.
func (r *instanceRig) walkCount() int {
	r.posts.mutex.Lock()
	defer r.posts.mutex.Unlock()
	return len(r.posts.walks)
}

// walksSince lists the post walks after mark, in order.
func (r *instanceRig) walksSince(mark int) []recordedPostWalk {
	r.posts.mutex.Lock()
	defer r.posts.mutex.Unlock()
	return slices.Clone(r.posts.walks[mark:])
}

// postRequestCount marks a point in the post list requests for
// postCursorsSince.
func (r *instanceRig) postRequestCount() int {
	return len(r.fixture.recordedPosts())
}

// postCursorsSince lists the page_cursor of each post list request of host after
// mark, in arrival order, "" for a request with none.
func (r *instanceRig) postCursorsSince(mark int, host string) []string {
	cursors := []string{}
	for _, request := range r.fixture.recordedPosts()[mark:] {
		if request.host == host {
			cursors = append(cursors, request.query.Get("page_cursor"))
		}
	}
	return cursors
}

// post gives host one local post in each community, in order. Post n, counted
// across the rig, has id n and is published at postMinute(n), so each is newer
// than every post before it.
func (r *instanceRig) post(host string, communityIDs ...string) {
	for _, communityID := range communityIDs {
		r.postCount++
		r.fixture.addPosts(host, lemmyPost{
			id: int64(r.postCount), published: postMinute(r.postCount), community: communityID,
		})
	}
}

// addPostOnlyCommunity serves a Group for host's community name that the host's
// community list does not return, and returns its id. Only a post can reach it.
func (r *instanceRig) addPostOnlyCommunity(host, name string) string {
	r.h.t.Helper()
	id := "https://" + host + "/c/" + name
	r.serveGroup(id, nil)
	return id
}

// newMintingAdmin is an Admin over a real Minter behind a real MintGate on
// budget, with a scripted PLC directory behind the minter. A nil budget takes
// the fixture's own 5 a minute budget on its manual clock.
func newMintingAdmin(t *testing.T, h *harness, budget *identity.PLCWriteBudget) (*Admin, *mintFixture) {
	t.Helper()
	fixture := newMintFixtureOver(t, h.actors, h.custodian)
	if budget == nil {
		budget = fixture.budget
	}
	gate, err := NewMintGate(fixture.minter, budget, fixture.logger)
	require.NoError(t, err)
	materializerOptions := h.materializerOptions
	materializerOptions.Minter = gate
	materializer, err := materialize.New(materializerOptions)
	require.NoError(t, err)
	adminOptions := h.adminOptions()
	adminOptions.Materializer = materializer
	admin, err := NewAdmin(adminOptions)
	require.NoError(t, err)
	return admin, fixture
}

// instanceFollowList is a follow list naming hosts and nothing else.
func instanceFollowList(hosts ...string) string {
	var list strings.Builder
	list.WriteString("communities: []\ninstances:\n")
	for _, host := range hosts {
		list.WriteString("  - host: " + host + "\n")
	}
	return list.String()
}

func (r *instanceRig) writeFollowList(content string) {
	r.h.t.Helper()
	require.NoError(r.h.t, os.WriteFile(r.path, []byte(content), 0o644))
}

// sweep runs one reconcile pass.
func (r *instanceRig) sweep() SweepResult {
	r.h.t.Helper()
	result, err := r.reconciler.Sweep(context.Background())
	require.NoError(r.h.t, err)
	return result
}

// serveGroup publishes a Group whose name is the id's last path segment, on the
// one RSA key every actor here shares.
func (r *instanceRig) serveGroup(id string, extra map[string]any) {
	r.h.t.Helper()
	doc := map[string]any{
		"type":              "Group",
		"id":                id,
		"preferredUsername": id[strings.LastIndexByte(id, '/')+1:],
		"inbox":             id + "/inbox",
		"endpoints":         map[string]any{"sharedInbox": originOf(id) + "/inbox"},
		"published":         "2024-01-01T00:00:00.000000Z",
	}
	maps.Copy(doc, extra)
	r.h.serveActorDoc(id, doc, &r.h.service.Key.PublicKey)
}

// addCommunity gives host a community with a served Group and returns its id.
func (r *instanceRig) addCommunity(host, name string) string {
	r.h.t.Helper()
	r.fixture.add(host, lemmyCommunity{name: name})
	id := "https://" + host + "/c/" + name
	r.serveGroup(id, nil)
	return id
}

// track inserts one row per id in one statement: a community the bridge already
// follows, stored under the handle label subscribe stores, last_backfill_at set
// when backfilled is. Ids are https://host/c/name.
func (r *instanceRig) track(state store.FollowState, backfilled bool, ids ...string) {
	r.h.t.Helper()
	_, err := r.h.db.ExecContext(context.Background(), `
		INSERT INTO communities (ap_group_id, did, preferred_username, instance, follow_state, last_backfill_at)
		SELECT id, 'did:plc:' || left(md5(id), 24), 'c-' || split_part(id, '/', 5), split_part(id, '/', 3), $2::text,
		       CASE WHEN $3::boolean THEN CURRENT_TIMESTAMP END
		FROM unnest(string_to_array($1, ',')) AS id`,
		strings.Join(ids, ","), string(state), backfilled)
	require.NoError(r.h.t, err)
}

// trackStoredAs is one finished accepted row stored under the given name.
func (r *instanceRig) trackStoredAs(id, preferredUsername string) {
	r.h.t.Helper()
	r.track(store.FollowStateAccepted, true, id)
	_, err := r.h.db.ExecContext(context.Background(),
		`UPDATE communities SET preferred_username = $2 WHERE ap_group_id = $1`, id, preferredUsername)
	require.NoError(r.h.t, err)
}

// storedRow is the whole communities row, for comparing across a sweep.
func (r *instanceRig) storedRow(id string) string {
	r.h.t.Helper()
	var row string
	require.NoError(r.h.t, r.h.db.QueryRowContext(context.Background(),
		`SELECT to_jsonb(communities)::text FROM communities WHERE ap_group_id = $1`, id).Scan(&row))
	return row
}

func (r *instanceRig) followState(id string) store.FollowState {
	r.h.t.Helper()
	community, err := r.h.communities.GetByAPGroupID(context.Background(), id)
	require.NoError(r.h.t, err)
	return community.FollowState
}

func (r *instanceRig) acceptedCount() int {
	r.h.t.Helper()
	accepted, err := r.h.communities.ListByFollowState(context.Background(), store.FollowStateAccepted)
	require.NoError(r.h.t, err)
	return len(accepted)
}

// deliveryCount marks a point in the deliveries for deliveredSince.
func (r *instanceRig) deliveryCount() int { return r.h.deliveryCount() }

// deliveredSince gives the groups named by the Follows and the Undo{Follow}s
// delivered after mark, each in delivery order.
func (r *instanceRig) deliveredSince(mark int) (follows, undos []string) {
	return r.h.deliveredSince(mark)
}

// listedPage is one page a host was asked for.
type listedPage struct {
	host string
	sort string
	page int
}

// oldPages lists the sort=Old pages first to last of host.
func oldPages(host string, first, last int) []listedPage {
	pages := make([]listedPage, 0, last-first+1)
	for page := first; page <= last; page++ {
		pages = append(pages, listedPage{host, "Old", page})
	}
	return pages
}

// requestCount marks a point in the listing requests for pagesSince.
func (r *instanceRig) requestCount() int {
	return len(r.fixture.recorded())
}

// pagesSince lists the pages asked for after mark, in arrival order.
func (r *instanceRig) pagesSince(mark int) []listedPage {
	r.h.t.Helper()
	requests := r.fixture.recorded()[mark:]
	pages := make([]listedPage, 0, len(requests))
	for _, request := range requests {
		page, err := strconv.Atoi(request.query.Get("page"))
		require.NoError(r.h.t, err)
		pages = append(pages, listedPage{request.host, request.query.Get("sort"), page})
	}
	return pages
}

// requireConverged asserts a sweep neither unfollowed nor failed anything.
func requireConverged(t *testing.T, result SweepResult) {
	t.Helper()
	require.Empty(t, result.Unsubscribed)
	require.Empty(t, result.Failed)
}

// TestSweep_HandleEntriesMatchWhatARealMintStores: only the real Minter stores
// the c- label, so only it shows a handle entry matching by id. The URL entry
// reaches some_community without a second WebFinger document.
func TestSweep_HandleEntriesMatchWhatARealMintStores(t *testing.T) {
	const (
		someCommunityID = "https://lemmy.world/c/some_community"
		listedID        = "https://lemmy.world/c/listed"
		technology      = `communities:
  - "!technology@lemmy.world"
`
	)
	h := newHarness(t)
	budget := unlimitedBudget(t)
	admin, mint := newMintingAdmin(t, h, budget)
	rig := newInstanceRig(t, h, technology+`  - "https://lemmy.world/c/some_community"
`, instanceRigOptions{admin: admin, budget: budget})
	h.technologyGroup()
	h.serveTechnologyWebfinger()
	rig.serveGroup(someCommunityID, nil)
	ctx := context.Background()

	result := rig.sweep()

	require.Equal(t, []string{"!technology@lemmy.world", someCommunityID}, result.Subscribed)
	requireConverged(t, result)
	require.Len(t, mint.plc.recordedPosts(), 2, "one mint per community")
	for id, want := range map[string]string{groupID: "c-technology", someCommunityID: "c-some-community"} {
		community, err := h.communities.GetByAPGroupID(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, want, community.PreferredUsername, id)
	}

	mark := rig.deliveryCount()
	for range 2 {
		result := rig.sweep()
		assert.Empty(t, result.Subscribed)
		assert.Empty(t, result.Unsubscribed)
		assert.Empty(t, result.Failed)
	}
	follows, undos := rig.deliveredSince(mark)
	assert.Empty(t, undos, "a handle entry must match the row a mint stored")
	assert.Empty(t, follows)
	require.Len(t, mint.plc.recordedPosts(), 2, "a repeat sweep mints nothing")

	rig.fixture.add(lemmyFixtureHost, lemmyCommunity{name: "technology"})
	rig.fixture.add(lemmyFixtureHost, lemmyCommunity{name: "some_community"})
	rig.addCommunity(lemmyFixtureHost, "listed")
	rig.writeFollowList(technology + `instances:
  - host: lemmy.world
    exclude:
      - "!some_community@lemmy.world"
`)
	mark = rig.deliveryCount()

	result = rig.sweep()

	assert.Equal(t, []string{listedID}, result.Subscribed)
	assert.Equal(t, []string{someCommunityID}, result.Unsubscribed)
	assert.Empty(t, result.Failed)
	follows, undos = rig.deliveredSince(mark)
	assert.Equal(t, []string{listedID}, follows, "the excluded community must not be followed again")
	assert.Equal(t, []string{someCommunityID}, undos)

	rig.writeFollowList(technology)
	mark = rig.deliveryCount()

	result = rig.sweep()

	assert.Equal(t, []string{listedID}, result.Unsubscribed, "only the community the entry alone named")
	follows, undos = rig.deliveredSince(mark)
	assert.Equal(t, []string{listedID}, undos)
	assert.Empty(t, follows)
	assert.Equal(t, store.FollowStatePending, rig.followState(groupID), "!technology@lemmy.world stays followed")
}

// TestSweep_RowsStoredUnderEitherNameMatchWithNoWrite: both names were written
// before this change, by an ID-only reference and by an embedded Group document.
func TestSweep_RowsStoredUnderEitherNameMatchWithNoWrite(t *testing.T) {
	const (
		rawNameID     = "https://lemmy.world/c/technology"
		handleLabelID = "https://lemmy.ml/c/technology"
	)
	h := newHarness(t)
	rig := newInstanceRig(t, h, `communities:
  - "!technology@lemmy.world"
  - "!technology@lemmy.ml"
`, instanceRigOptions{})
	rig.trackStoredAs(rawNameID, "technology")
	rig.trackStoredAs(handleLabelID, "c-technology")
	rawNameRow, handleLabelRow := rig.storedRow(rawNameID), rig.storedRow(handleLabelID)

	result := rig.sweep()

	assert.Empty(t, result.Subscribed)
	assert.Empty(t, result.Unsubscribed)
	assert.Empty(t, result.Failed)
	follows, undos := rig.deliveredSince(0)
	assert.Empty(t, follows)
	assert.Empty(t, undos)
	assert.Equal(t, rawNameRow, rig.storedRow(rawNameID))
	assert.Equal(t, handleLabelRow, rig.storedRow(handleLabelID))
}

// TestSweep_TwoEntriesForOneCommunityBothMatchItsRow: a row a real subscribe left
// accepted, so an entry left missing would be subscribed again on every sweep.
func TestSweep_TwoEntriesForOneCommunityBothMatchItsRow(t *testing.T) {
	h := newHarness(t)
	h.subscribeTechnology()
	rig := newInstanceRig(t, h, `communities:
  - "!technology@lemmy.world"
  - "https://lemmy.world/c/technology"
`, instanceRigOptions{})
	mark := rig.deliveryCount()

	result := rig.sweep()

	assert.Empty(t, result.Subscribed)
	assert.Empty(t, result.Unsubscribed)
	assert.Empty(t, result.Failed)
	follows, undos := rig.deliveredSince(mark)
	assert.Empty(t, follows)
	assert.Empty(t, undos)
}

// TestSweep_ReachesEveryWindowAcrossSweeps is the acceptance test for the
// walk: a host past one sort's reach is walked at the default cap, a sweep at a
// time, and comes back to sort=Old page 1.
func TestSweep_ReachesEveryWindowAcrossSweeps(t *testing.T) {
	const (
		host           = lemmyFixtureHost
		createdSinceID = "https://lemmy.world/c/created-since"
	)
	h := newHarness(t)
	rig := newInstanceRig(t, h, instanceFollowList(host), instanceRigOptions{})
	rig.fixture.addCommunities(host, 5001)
	rig.track(store.FollowStateAccepted, true, lemmyActorIDs(host, 1, 5000)...)
	newestID := lemmyActorID(host, 5001)
	rig.serveGroup(newestID, nil)

	newPageOne := []listedPage{{host, "New", 1}}
	for _, step := range []struct {
		name           string
		before         func()
		wantPages      []listedPage
		wantSubscribed []string
	}{
		{
			name:           "the sort=Old pass holds only tracked communities",
			wantPages:      oldPages(host, 1, 100),
			wantSubscribed: []string{},
		},
		{
			name:           "sort=New follows the 5,001st",
			wantPages:      newPageOne,
			wantSubscribed: []string{newestID},
		},
		{
			name:           "the walk restarts at sort=Old page 1",
			before:         func() { rig.addCommunity(host, "created-since") },
			wantPages:      oldPages(host, 1, 100),
			wantSubscribed: []string{},
		},
		{
			name:           "a later sort=New follows a community created since",
			wantPages:      newPageOne,
			wantSubscribed: []string{createdSinceID},
		},
	} {
		if step.before != nil {
			step.before()
		}
		mark := rig.requestCount()

		result := rig.sweep()

		require.Equal(t, step.wantPages, rig.pagesSince(mark), step.name)
		require.Equal(t, step.wantSubscribed, result.Subscribed, step.name)
		requireConverged(t, result)
	}
	assert.Equal(t, 5000, rig.acceptedCount(), "nothing was unfollowed")
}

// failingAcceptedRead fails one read of the accepted communities, counted from 1
// across sweeps. A sweep's first read is its snapshot of the tracked communities,
// so read 2 is the first sweep's first backlog check.
type failingAcceptedRead struct {
	store.Communities

	failedRead int
	reads      int
}

func (c *failingAcceptedRead) ListByFollowState(ctx context.Context, state store.FollowState) ([]*store.Community, error) {
	if state == store.FollowStateAccepted {
		c.reads++
		if c.reads == c.failedRead {
			return nil, stderrors.New("accepted communities read failed")
		}
	}
	return c.Communities.ListByFollowState(ctx, state)
}

// TestSweep_PositionHoldsUntilLeftoversAreAdded: a cap of 2 pages over 101
// communities, so a host that moves reads page 3 next and a host that holds
// reads pages 1 and 2 again. Everything outside the row's untracked range is
// already tracked. Pages hold 50 communities, so ordinals 51 to 100 are page 2.
func TestSweep_PositionHoldsUntilLeftoversAreAdded(t *testing.T) {
	const (
		host      = lemmyFixtureHost
		refusedID = "https://lemmy.world/c/community-00075"
	)
	holding, moved := oldPages(host, 1, 2), oldPages(host, 3, 3)
	none := []string{}

	type step struct {
		before     func(rig *instanceRig)
		wantPages  []listedPage
		wantAdded  []string
		wantFailed []string
	}
	for _, tc := range []struct {
		name                          string
		firstUntracked, lastUntracked int
		// followList replaces a list naming host alone when set.
		followList string
		// communities replaces the Admin's store when set.
		communities func(h *harness) store.Communities
		// rateLimitedPost, when set, puts adds behind a real mint on the 5 a minute
		// budget, with PLC answering 429 to the POST of that ordinal.
		rateLimitedPost int
		arrange         func(rig *instanceRig)
		steps           []step
	}{
		{
			name:           "more than 20 candidates",
			firstUntracked: 76, lastUntracked: 100,
			steps: []step{
				{wantPages: holding, wantAdded: lemmyActorIDs(host, 76, 95)},
				{wantPages: holding, wantAdded: lemmyActorIDs(host, 96, 100)},
				{wantPages: moved, wantAdded: none},
			},
		},
		{
			name:           "a paused budget",
			firstUntracked: 96, lastUntracked: 100,
			arrange: func(rig *instanceRig) { rig.budget.RateLimited("3600") },
			steps: []step{
				{wantPages: holding, wantAdded: none},
				{wantPages: holding, wantAdded: none},
				{
					before:    func(rig *instanceRig) { rig.clock.Advance(time.Hour) },
					wantPages: holding, wantAdded: lemmyActorIDs(host, 96, 100),
				},
				{wantPages: moved, wantAdded: none},
			},
		},
		{
			name:           "a backfill backlog over its limit",
			firstUntracked: 96, lastUntracked: 100,
			arrange: func(rig *instanceRig) { rig.h.backfills.backlog = 21 },
			steps: []step{
				{wantPages: holding, wantAdded: none},
				{
					before:    func(rig *instanceRig) { rig.h.backfills.backlog = 0 },
					wantPages: holding, wantAdded: lemmyActorIDs(host, 96, 100),
				},
				{wantPages: moved, wantAdded: none},
			},
		},
		{
			name:           "a backfill backlog that cannot be read",
			firstUntracked: 96, lastUntracked: 100,
			communities: func(h *harness) store.Communities {
				return &failingAcceptedRead{Communities: h.communities, failedRead: 2}
			},
			steps: []step{
				{wantPages: holding, wantAdded: none},
				{wantPages: holding, wantAdded: lemmyActorIDs(host, 96, 100)},
				{wantPages: moved, wantAdded: none},
			},
		},
		{
			// The ids of lemmy.ml sort first, so its 3 are added and the 20 slots run
			// out on lemmy.world. Different ordinals, since Groups are served by path.
			name:           "a host with no leftovers moves while another holds",
			firstUntracked: 76, lastUntracked: 100,
			followList: instanceFollowList(host, lemmyMLHost),
			arrange: func(rig *instanceRig) {
				rig.fixture.addCommunities(lemmyMLHost, 101)
				rig.track(store.FollowStateAccepted, true, lemmyActorIDs(lemmyMLHost, 1, 50)...)
				rig.track(store.FollowStateAccepted, true, lemmyActorIDs(lemmyMLHost, 54, 101)...)
				for _, id := range lemmyActorIDs(lemmyMLHost, 51, 53) {
					rig.serveGroup(id, nil)
				}
			},
			steps: []step{
				{
					wantPages: slices.Concat(holding, oldPages(lemmyMLHost, 1, 2)),
					wantAdded: slices.Concat(lemmyActorIDs(lemmyMLHost, 51, 53), lemmyActorIDs(host, 76, 92)),
				},
				{
					wantPages: slices.Concat(holding, oldPages(lemmyMLHost, 3, 3)),
					wantAdded: lemmyActorIDs(host, 93, 100),
				},
				{wantPages: slices.Concat(moved, oldPages(lemmyMLHost, 1, 2)), wantAdded: none},
			},
		},
		{
			name:           "a missing explicit entry with a paused budget",
			firstUntracked: 96, lastUntracked: 100,
			followList: `communities:
  - "https://peer.example/c/explicit-one"
instances:
  - host: lemmy.world
`,
			arrange: func(rig *instanceRig) {
				rig.serveGroup(peerExplicitID, nil)
				rig.budget.RateLimited("3600")
			},
			steps: []step{
				{wantPages: holding, wantAdded: none, wantFailed: []string{peerExplicitID}},
				{wantPages: holding, wantAdded: none, wantFailed: []string{peerExplicitID}},
				{
					before:    func(rig *instanceRig) { rig.clock.Advance(time.Hour) },
					wantPages: holding,
					wantAdded: slices.Concat([]string{peerExplicitID}, lemmyActorIDs(host, 96, 100)),
				},
				{wantPages: moved, wantAdded: none},
			},
		},
		{
			name:           "a 429 on the only untracked candidate",
			firstUntracked: 96, lastUntracked: 96,
			rateLimitedPost: 1,
			steps: []step{
				{wantPages: holding, wantAdded: none},
				{wantPages: holding, wantAdded: none},
				{
					before:    func(rig *instanceRig) { rig.clock.Advance(time.Minute) },
					wantPages: holding, wantAdded: lemmyActorIDs(host, 96, 96),
				},
				{wantPages: moved, wantAdded: none},
			},
		},
		{
			// The refusal is skipped for 24 hours and was tried, so it holds nothing.
			name:           "a refused community",
			firstUntracked: 75, lastUntracked: 75,
			arrange: func(rig *instanceRig) {
				rig.serveGroup(refusedID, map[string]any{"summary": "<p>#nobridge</p>"})
			},
			steps: []step{
				{wantPages: holding, wantAdded: none, wantFailed: []string{refusedID}},
				{wantPages: moved, wantAdded: none},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			options := instanceRigOptions{maxPages: 2}
			if tc.communities != nil {
				adminOptions := h.adminOptions()
				adminOptions.Communities = tc.communities(h)
				admin, err := NewAdmin(adminOptions)
				require.NoError(t, err)
				options.admin = admin
			}
			if tc.rateLimitedPost != 0 {
				admin, mint := newMintingAdmin(t, h, nil)
				mint.plc.rateLimitedPost = tc.rateLimitedPost
				options.admin, options.budget, options.clock = admin, mint.budget, mint.clock
			}
			rig := newInstanceRig(t, h, cmp.Or(tc.followList, instanceFollowList(host)), options)
			rig.fixture.addCommunities(host, 101)
			rig.track(store.FollowStateAccepted, true, lemmyActorIDs(host, 1, tc.firstUntracked-1)...)
			rig.track(store.FollowStateAccepted, true, lemmyActorIDs(host, tc.lastUntracked+1, 101)...)
			for _, id := range lemmyActorIDs(host, tc.firstUntracked, tc.lastUntracked) {
				rig.serveGroup(id, nil)
			}
			if tc.arrange != nil {
				tc.arrange(rig)
			}

			for index, step := range tc.steps {
				if step.before != nil {
					step.before(rig)
				}
				mark := rig.requestCount()

				result := rig.sweep()

				require.Equal(t, step.wantPages, rig.pagesSince(mark), "sweep %d", index+1)
				require.Equal(t, step.wantAdded, result.Subscribed, "sweep %d", index+1)
				require.ElementsMatch(t, step.wantFailed, result.Failed, "sweep %d", index+1)
				require.Empty(t, result.Unsubscribed, "sweep %d", index+1)
			}
		})
	}
}

// TestSweep_ContextEndingDuringAListedAddLeavesItsCandidate: the context ends
// inside the first candidate's actor fetch, with a cap of 2 pages over 101
// communities so a host that moves reads page 3 next. The clock never moves, so
// a skip of any length would keep the candidate out of the next sweep.
func TestSweep_ContextEndingDuringAListedAddLeavesItsCandidate(t *testing.T) {
	const host = lemmyFixtureHost
	h := newHarness(t)
	rig := newInstanceRig(t, h, instanceFollowList(host), instanceRigOptions{maxPages: 2})
	rig.fixture.addCommunities(host, 101)
	rig.track(store.FollowStateAccepted, true, lemmyActorIDs(host, 1, 95)...)
	rig.track(store.FollowStateAccepted, true, lemmyActorIDs(host, 101, 101)...)
	untracked := lemmyActorIDs(host, 96, 100)
	for _, id := range untracked {
		rig.serveGroup(id, nil)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	h.mu.Lock()
	h.onFetch[urlPath(t, untracked[0])] = cancel
	h.mu.Unlock()

	result, err := rig.reconciler.Sweep(ctx)

	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, result.Subscribed)
	require.Empty(t, result.Failed, "an add the context ended was not really tried")
	require.Equal(t, oldPages(host, 1, 2), rig.pagesSince(0))

	mark := rig.requestCount()
	result = rig.sweep()

	require.Equal(t, oldPages(host, 1, 2), rig.pagesSince(mark), "the position did not move")
	require.Equal(t, untracked, result.Subscribed, "the candidate was not skipped")
	requireConverged(t, result)

	mark = rig.requestCount()
	result = rig.sweep()

	assert.Equal(t, oldPages(host, 3, 3), rig.pagesSince(mark))
	assert.Empty(t, result.Subscribed)
	follows, undos := rig.deliveredSince(0)
	assert.Equal(t, untracked, follows)
	assert.Empty(t, undos)
}

// TestSweep_FailedOrShortListingUnfollowsNothing: a host's tracked communities
// stay desired by the entry whatever the listing shows, and each row's untracked
// community proves the sweep read past the failure instead of stopping.
func TestSweep_FailedOrShortListingUnfollowsNothing(t *testing.T) {
	const healthyID = "https://lemmy.ml/c/healthy-candidate"

	for _, tc := range []struct {
		name string
		// arrange gives lemmy.world its listing and tracked rows, and returns its
		// untracked community.
		arrange      func(rig *instanceRig) string
		wantPages    []int
		wantAccepted int
	}{
		{
			name: "a read that fails on page 3 after two good pages",
			arrange: func(rig *instanceRig) string {
				rig.fixture.addCommunities(lemmyFixtureHost, 120)
				rig.fixture.failPage(lemmyFixtureHost, "Old", 3, http.StatusInternalServerError)
				rig.track(store.FollowStateAccepted, true, lemmyActorIDs(lemmyFixtureHost, 1, 59)...)
				rig.track(store.FollowStateAccepted, true, lemmyActorIDs(lemmyFixtureHost, 61, 120)...)
				candidate := lemmyActorID(lemmyFixtureHost, 60)
				rig.serveGroup(candidate, nil)
				return candidate
			},
			wantPages:    []int{1, 2, 3},
			wantAccepted: 119,
		},
		{
			name: "a list shorter than what is tracked",
			arrange: func(rig *instanceRig) string {
				rig.fixture.addCommunities(lemmyFixtureHost, 3)
				rig.track(store.FollowStateAccepted, true, lemmyActorIDs(lemmyFixtureHost, 1, 2)...)
				rig.track(store.FollowStateAccepted, true, "https://lemmy.world/c/not-listed")
				candidate := lemmyActorID(lemmyFixtureHost, 3)
				rig.serveGroup(candidate, nil)
				return candidate
			},
			wantPages:    []int{1},
			wantAccepted: 3,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			rig := newInstanceRig(t, h, instanceFollowList(lemmyFixtureHost, lemmyMLHost), instanceRigOptions{})
			candidate := tc.arrange(rig)
			rig.addCommunity(lemmyMLHost, "healthy-candidate")

			result := rig.sweep()

			assert.Equal(t, tc.wantPages, rig.fixture.pagesRequested(lemmyFixtureHost, "Old"))
			assert.Equal(t, []int{1}, rig.fixture.pagesRequested(lemmyMLHost, "Old"))
			assert.Equal(t, []string{healthyID, candidate}, result.Subscribed)
			assert.Empty(t, result.Unsubscribed)
			assert.Empty(t, result.Failed)
			follows, undos := rig.deliveredSince(0)
			assert.Equal(t, []string{healthyID, candidate}, follows)
			assert.Empty(t, undos)
			assert.Equal(t, tc.wantAccepted, rig.acceptedCount())
		})
	}
}

// TestSweep_RemovedInstanceEntryUnfollowsItsCommunities is the removal path.
// The kept row holds the handle label, so only the id-based match keeps it.
func TestSweep_RemovedInstanceEntryUnfollowsItsCommunities(t *testing.T) {
	const (
		keptID          = "https://lemmy.world/c/kept"
		firstLeavingID  = "https://lemmy.world/c/first-leaving"
		secondLeavingID = "https://lemmy.world/c/second-leaving"
		otherHostID     = "https://lemmy.ml/c/other-host"
		keptEntry       = `communities:
  - "!kept@lemmy.world"
`
	)
	h := newHarness(t)
	rig := newInstanceRig(t, h, keptEntry+`instances:
  - host: lemmy.world
  - host: lemmy.ml
`, instanceRigOptions{})
	rig.addCommunity(lemmyFixtureHost, "kept")
	rig.addCommunity(lemmyFixtureHost, "first-leaving")
	rig.addCommunity(lemmyFixtureHost, "second-leaving")
	rig.addCommunity(lemmyMLHost, "other-host")
	rig.trackStoredAs(keptID, "c-kept")
	rig.track(store.FollowStateAccepted, true, firstLeavingID, secondLeavingID, otherHostID)

	result := rig.sweep()

	require.Equal(t, []int{1}, rig.fixture.pagesRequested(lemmyFixtureHost, "Old"))
	require.Equal(t, []int{1}, rig.fixture.pagesRequested(lemmyMLHost, "Old"))
	require.Empty(t, result.Subscribed)
	requireConverged(t, result)

	rig.writeFollowList(keptEntry + `instances:
  - host: lemmy.ml
`)

	result = rig.sweep()

	assert.ElementsMatch(t, []string{firstLeavingID, secondLeavingID}, result.Unsubscribed)
	assert.Empty(t, result.Subscribed)
	assert.Empty(t, result.Failed)
	follows, undos := rig.deliveredSince(0)
	assert.ElementsMatch(t, []string{firstLeavingID, secondLeavingID}, undos)
	assert.Empty(t, follows)
	assert.Equal(t, store.FollowStateAccepted, rig.followState(keptID), "named in communities")
	assert.Equal(t, store.FollowStateAccepted, rig.followState(otherHostID), "its own host is still listed")
	assert.Equal(t, store.FollowStateNone, rig.followState(firstLeavingID))
	assert.Equal(t, store.FollowStateNone, rig.followState(secondLeavingID))
}

// numberedIDs lists prefix01 up to prefix<last>.
func numberedIDs(prefix string, last int) []string {
	ids := make([]string, 0, last)
	for number := 1; number <= last; number++ {
		ids = append(ids, fmt.Sprintf("%s%02d", prefix, number))
	}
	return ids
}

// addReversedCommunities gives host one community per name, newest first, so
// its sort=Old order runs against its actor id order.
func addReversedCommunities(rig *instanceRig, host string, names []string) {
	for index, name := range names {
		rig.fixture.add(host, lemmyCommunity{
			name: name, published: lemmyFirstPublished.Add(time.Duration(len(names)-index) * time.Minute),
		})
	}
}

// TestSweep_AddsExplicitEntriesThenTheLowestTwentyListed: 12 communities on
// lemmy.ml and 38 on lemmy.world. The file lists lemmy.world first and each host
// serves its highest actor id first, so neither read order gives the selection.
// The explicit entries sort on either side of the listed ids, and one is also a
// listing result, which must not be added twice or take a slot. Only the 23
// expected Groups are served, so any other add lands in Failed.
func TestSweep_AddsExplicitEntriesThenTheLowestTwentyListed(t *testing.T) {
	const (
		explicitOnListedHostID = "https://lemmy.ml/c/aa-explicit"
		highestExplicitID      = "https://peer.example/c/zz-explicit"
	)
	h := newHarness(t)
	rig := newInstanceRig(t, h, `communities:
  - "https://peer.example/c/zz-explicit"
  - "https://peer.example/c/explicit-one"
  - "https://lemmy.ml/c/aa-explicit"
instances:
  - host: lemmy.world
  - host: lemmy.ml
`, instanceRigOptions{})
	addReversedCommunities(rig, lemmyMLHost, numberedIDs("ml-", 12))
	addReversedCommunities(rig, lemmyFixtureHost, numberedIDs("world-", 38))
	rig.fixture.add(lemmyMLHost, lemmyCommunity{name: "aa-explicit"})

	want := []string{highestExplicitID, peerExplicitID, explicitOnListedHostID}
	want = append(want, numberedIDs("https://lemmy.ml/c/ml-", 12)...)
	want = append(want, numberedIDs("https://lemmy.world/c/world-", 8)...)
	for _, id := range want {
		rig.serveGroup(id, nil)
	}

	result := rig.sweep()

	assert.Equal(t, want, result.Subscribed)
	assert.Empty(t, result.Unsubscribed)
	assert.Empty(t, result.Failed)
	follows, undos := rig.deliveredSince(0)
	assert.Equal(t, want, follows, "explicit entries first, then the lowest listed ids in order")
	assert.Empty(t, undos)
}

// newBackfilledInstanceRig is an instance rig over the retry rig's real Backfill
// and clock. The returned sweepRig's sweep drives this reconciler and drains
// the walks. A nil admin takes the harness Admin.
func newBackfilledInstanceRig(t *testing.T, h *harness, followList string, admin *Admin) (*instanceRig, *sweepRig) {
	t.Helper()
	retry := newSweepRig(t, h, followList)
	rig := newInstanceRig(t, h, followList, instanceRigOptions{admin: admin, clock: retry.clock, backfill: retry.backfill})
	retry.reconciler = rig.reconciler
	return rig, retry
}

// serveEmptyOutbox makes a walk of the group end cleanly, stamped.
func (r *instanceRig) serveEmptyOutbox(id string) {
	r.h.t.Helper()
	r.h.serveObject(urlPath(r.h.t, id)+"/outbox", map[string]any{
		"type": "OrderedCollection", "id": id + "/outbox", "orderedItems": []any{},
	})
}

// serveTruncatedGroup is a Group whose outbox is a next-pointer loop: a clean
// walk that never reaches the end, so it stamps nothing.
func (r *instanceRig) serveTruncatedGroup(id string) {
	r.h.t.Helper()
	first, second := id+"/outbox/page1", id+"/outbox/page2"
	r.serveGroup(id, map[string]any{"outbox": id + "/outbox"})
	r.h.serveObject(urlPath(r.h.t, id)+"/outbox", map[string]any{
		"type": "OrderedCollection", "id": id + "/outbox", "first": first,
	})
	r.h.serveObject(urlPath(r.h.t, first), map[string]any{
		"type": "OrderedCollectionPage", "id": first, "next": second, "orderedItems": []any{},
	})
	r.h.serveObject(urlPath(r.h.t, second), map[string]any{
		"type": "OrderedCollectionPage", "id": second, "next": first, "orderedItems": []any{},
	})
}

// serveFailingGroup is a Group whose outbox answers 404, which fails the walk.
func (r *instanceRig) serveFailingGroup(id string) {
	r.h.t.Helper()
	r.serveGroup(id, map[string]any{"outbox": id + "/outbox"})
}

// TestSweep_BacklogLimit: a community with no retry state counts, and the limit
// allows exactly 20. The tracked communities are desired only through the
// `instances` entry, and their Groups are not served, so their walks fail.
func TestSweep_BacklogLimit(t *testing.T) {
	const host = lemmyFixtureHost

	for _, tc := range []struct {
		name       string
		unfinished int
		wantListed bool
	}{
		{name: "21 unfinished stop listed adds", unfinished: 21},
		{name: "20 unfinished do not", unfinished: 20, wantListed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			rig, retry := newBackfilledInstanceRig(t, h, `communities:
  - "https://peer.example/c/explicit-one"
instances:
  - host: lemmy.world
`, nil)
			rig.fixture.addCommunities(host, tc.unfinished+1)
			unfinished := lemmyActorIDs(host, 1, tc.unfinished)
			rig.track(store.FollowStateAccepted, false, unfinished...)
			listedID := lemmyActorID(host, tc.unfinished+1)
			rig.serveGroup(listedID, nil)
			rig.serveGroup(peerExplicitID, nil)

			result := retry.sweep(t)

			want := []string{peerExplicitID}
			if tc.wantListed {
				want = append(want, listedID)
			}
			assert.Equal(t, want, result.Subscribed, "explicit entries are added either way")
			assert.Empty(t, result.Unsubscribed)
			for _, id := range unfinished {
				assert.Equal(t, 1, retry.fetcher.walksOf(id), "%s is desired through the entry", id)
			}
		})
	}
}

// TestSweep_BackedOffCommunitiesLeaveTheBacklog is the acceptance test for "a
// batch stops at the backlog limit". Sweep 1 finds 25 communities over the limit
// and walks them, and every walk leaves its community in a backoff. Before sweep
// 2 the row's newly accepted communities arrive with no retry state, so they are
// all that counts. An hour on, the backoffs have lapsed and the 25 count again,
// and the retry that sweep starts finishes one of them for real.
func TestSweep_BackedOffCommunitiesLeaveTheBacklog(t *testing.T) {
	const host = lemmyFixtureHost
	freshID := lemmyActorID(host, 26)

	for _, tc := range []struct {
		name string
		// outsideBackoff is how many accepted communities count at sweep 2, on top
		// of the 25 backing off.
		outsideBackoff int
		wantResumed    bool
	}{
		{name: "20 outside a backoff", outsideBackoff: 20, wantResumed: true},
		{name: "21 outside a backoff", outsideBackoff: 21},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			rig, retry := newBackfilledInstanceRig(t, h, `communities:
  - "https://peer.example/c/explicit-one"
instances:
  - host: lemmy.world
`, nil)
			rig.fixture.addCommunities(host, 26)
			unfinished := lemmyActorIDs(host, 1, 25)
			rig.track(store.FollowStateAccepted, false, unfinished...)
			for _, id := range unfinished[:9] {
				rig.serveTruncatedGroup(id)
			}
			for _, id := range unfinished[9:18] {
				rig.serveFailingGroup(id)
			}
			for _, id := range unfinished[18:] {
				rig.serveGroup(id, nil) // Advertises no outbox.
			}
			recovering := unfinished[0]
			rig.serveGroup(freshID, nil)
			rig.serveGroup(peerExplicitID, nil)

			result := retry.sweep(t)

			require.Equal(t, []string{peerExplicitID}, result.Subscribed, "25 unfinished stop listed adds, not explicit ones")
			requireConverged(t, result)
			for _, id := range unfinished {
				require.Equal(t, 1, retry.fetcher.walksOf(id), "%s is desired through the entry", id)
				require.Nil(t, h.lastBackfillAt(id), "the walk of %s must leave it unfinished", id)
			}

			// Accepted since sweep 1, so no walk has run. Their Groups are not served.
			rig.track(store.FollowStateAccepted, false, numberedIDs("https://lemmy.world/c/accepted-since-", tc.outsideBackoff)...)
			rig.clock.Advance(time.Minute)

			result = retry.sweep(t)

			wantResumed := []string{}
			if tc.wantResumed {
				wantResumed = append(wantResumed, freshID)
			}
			require.Equal(t, wantResumed, result.Subscribed)
			requireConverged(t, result)
			require.Equal(t, 1, retry.fetcher.walksOf(recovering), "inside its backoff")

			rig.addCommunity(host, "after-lapse")
			rig.serveEmptyOutbox(recovering)
			rig.clock.Advance(59 * time.Minute)

			result = retry.sweep(t)

			assert.Empty(t, result.Subscribed, "a lapsed backoff counts again")
			assert.Empty(t, result.Unsubscribed)
			assert.Equal(t, 2, retry.fetcher.walksOf(recovering), "the backoff has lapsed")
			assert.NotNil(t, h.lastBackfillAt(recovering), "the second walk finished")
			assert.Equal(t, 2, retry.fetcher.walksOf(unfinished[1]))
			assert.Nil(t, h.lastBackfillAt(unfinished[1]), "still truncated")
		})
	}
}

// acceptOnFollowRecorded makes the community accepted right after subscribe
// records its Follow. The hook is here and not at the fake inbox because
// subscribe writes pending after the delivery returns, which would overwrite an
// Accept taken during the delivery.
type acceptOnFollowRecorded struct {
	store.Communities

	groupID string
}

func (c acceptOnFollowRecorded) SetFollowState(ctx context.Context, id string, state store.FollowState) error {
	if err := c.Communities.SetFollowState(ctx, id, state); err != nil {
		return err
	}
	if id == c.groupID && state == store.FollowStatePending {
		return c.Communities.SetFollowState(ctx, id, store.FollowStateAccepted)
	}
	return nil
}

// TestSweep_AcceptDuringTheBatchStopsTheNextAdd: 20 unfinished at the start of
// the sweep are not over the limit, and the first add's Accept makes it 21. The
// backlog must be read before each add, not once.
func TestSweep_AcceptDuringTheBatchStopsTheNextAdd(t *testing.T) {
	const host = lemmyFixtureHost
	h := newHarness(t)
	firstAddedID, nextID := lemmyActorID(host, 21), lemmyActorID(host, 22)

	adminOptions := h.adminOptions()
	adminOptions.Communities = acceptOnFollowRecorded{Communities: h.communities, groupID: firstAddedID}
	admin, err := NewAdmin(adminOptions)
	require.NoError(t, err)

	rig, retry := newBackfilledInstanceRig(t, h, instanceFollowList(host), admin)
	rig.fixture.addCommunities(host, 22)
	rig.track(store.FollowStateAccepted, false, lemmyActorIDs(host, 1, 20)...)
	rig.serveGroup(firstAddedID, nil)
	rig.serveGroup(nextID, nil)

	result := retry.sweep(t)

	assert.Equal(t, []string{firstAddedID}, result.Subscribed)
	assert.Empty(t, result.Unsubscribed)
	follows, _ := rig.deliveredSince(0)
	assert.Equal(t, []string{firstAddedID}, follows)
	assert.Equal(t, store.FollowStateAccepted, rig.followState(firstAddedID))
	assert.Zero(t, h.hitCount(urlPath(t, nextID)), "the next add must not be tried")
}
