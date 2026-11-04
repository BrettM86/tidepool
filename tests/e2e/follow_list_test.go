//go:build e2e

package e2e

// The follow-list harness and the kill-switch scenario that drives it.
//
// installFollowList recreates the running bridge with FOLLOW_LIST_PATH pointing
// at a test-written file (docker-compose.follow-list.yml says why that is not in
// the base stack) and puts the base service back in cleanup. A scenario then
// rewrites the file and calls POST /admin/communities/reconcile, the same path an
// operator uses, with no restart in between.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/stretchr/testify/require"
)

// followListFileName is the file inside the mounted directory; the override
// file's FOLLOW_LIST_PATH names it.
const followListFileName = "communities.yaml"

// reconcileTimeout bounds one POST /admin/communities/reconcile. A sweep's adds
// can outlast the 30 seconds of the harness's client.
const reconcileTimeout = 3 * time.Minute

// followListComposeOverride locates docker-compose.follow-list.yml, which sits
// next to this source file.
func followListComposeOverride(t *testing.T) string {
	t.Helper()
	_, self, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate test source for compose override discovery")
	}
	return filepath.Join(filepath.Dir(self), "docker-compose.follow-list.yml")
}

// recreateTidepool replaces only the bridge container and waits for /healthz.
// --no-build keeps the image the stack came up with.
func recreateTidepool(ctx context.Context, env []string, composeFiles ...string) error {
	args := []string{"compose"}
	for _, file := range composeFiles {
		args = append(args, "-f", file)
	}
	args = append(args, "up", "-d", "--no-deps", "--no-build", "--force-recreate", "tidepool")
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Env = append(os.Environ(), env...)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("recreate tidepool: %w\n%s", err, out)
	}
	return awaitTidepoolHealthy()
}

// followList is a scenario's handle on the mounted file.
type followList struct {
	t           *testing.T
	path        string
	communities []string
	instances   []followListInstance
}

// followListInstance is one `instances` entry.
type followListInstance struct {
	host    string
	exclude []string
}

// installFollowList recreates the bridge reading a follow list that names every
// community tracked right now, because the startup sweep would otherwise
// unfollow them, and restores the base service when the test ends. interval is
// the running reconciler's cadence: a scenario that must prove only the reconcile
// call applied a change passes one its run time cannot reach.
func (h *harness) installFollowList(t *testing.T, interval time.Duration) *followList {
	t.Helper()

	// t.TempDir's root is 0700 and the container runs as UID 1000, so the mounted
	// directory carries its own mode. The chmod is for a restrictive umask.
	dir := filepath.Join(t.TempDir(), "follow-list")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatalf("create follow list directory: %v", err)
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatalf("chmod follow list directory: %v", err)
	}

	var tracked struct {
		Communities []adminCommunity `json:"communities"`
	}
	if err := h.adminDo(http.MethodGet, "/admin/communities", nil, &tracked); err != nil {
		t.Fatalf("snapshot tracked communities: %v", err)
	}
	list := &followList{t: t, path: filepath.Join(dir, followListFileName)}
	for _, community := range tracked.Communities {
		list.communities = append(list.communities, community.Community)
	}
	list.writeSwitches(nil, nil)

	base := composeFile(t)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		if err := recreateTidepool(ctx, nil, base); err != nil {
			t.Errorf("restore the base tidepool service: %v", err)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	env := []string{
		"TIDEPOOL_E2E_FOLLOW_LIST_DIR=" + dir,
		"TIDEPOOL_E2E_FOLLOW_LIST_INTERVAL=" + interval.String(),
	}
	if err := recreateTidepool(ctx, env, base, followListComposeOverride(t)); err != nil {
		t.Fatalf("install the follow list: %v", err)
	}
	return list
}

// writeSwitches replaces the file by atomic rename, keeping its communities and
// instances, so a sweep can never read a half-written list. A nil set omits its
// key.
func (l *followList) writeSwitches(disabledHosts, disabledCommunities []string) {
	l.t.Helper()
	var doc strings.Builder
	writeKey := func(key string, entries []string) {
		doc.WriteString(key + ":\n")
		for _, entry := range entries {
			fmt.Fprintf(&doc, "  - %q\n", entry)
		}
	}
	if len(l.communities) == 0 {
		doc.WriteString("communities: []\n")
	} else {
		writeKey("communities", l.communities)
	}
	if len(l.instances) > 0 {
		doc.WriteString("instances:\n")
		for _, instance := range l.instances {
			fmt.Fprintf(&doc, "  - host: %q\n", instance.host)
			if len(instance.exclude) > 0 {
				doc.WriteString("    exclude:\n")
				for _, entry := range instance.exclude {
					fmt.Fprintf(&doc, "      - %q\n", entry)
				}
			}
		}
	}
	if len(disabledHosts) > 0 {
		writeKey("outbound_disabled_hosts", disabledHosts)
	}
	if len(disabledCommunities) > 0 {
		writeKey("outbound_disabled_communities", disabledCommunities)
	}

	temp, err := os.CreateTemp(filepath.Dir(l.path), ".follow-list-*")
	if err != nil {
		l.t.Fatalf("create follow list temp file: %v", err)
	}
	if _, err := temp.WriteString(doc.String()); err != nil {
		l.t.Fatalf("write follow list temp file: %v", err)
	}
	if err := temp.Close(); err != nil {
		l.t.Fatalf("close follow list temp file: %v", err)
	}
	// CreateTemp makes it 0600; the container reads it as UID 1000.
	if err := os.Chmod(temp.Name(), 0o644); err != nil {
		l.t.Fatalf("chmod follow list temp file: %v", err)
	}
	if err := os.Rename(temp.Name(), l.path); err != nil {
		l.t.Fatalf("rename follow list into place: %v", err)
	}
}

// reconcileFollowList runs one sweep through the admin API. The list names
// everything tracked, so an unsubscribed or failed entry means the harness
// itself dropped a community.
func (h *harness) reconcileFollowList(t *testing.T) {
	t.Helper()
	result := h.reconcileSweep(t)
	if len(result.Unsubscribed) > 0 || len(result.Failed) > 0 {
		t.Fatalf("reconcile changed tracked communities: unsubscribed %v, failed %v", result.Unsubscribed, result.Failed)
	}
}

// followListSweep is one sweep's report.
type followListSweep struct {
	Subscribed   []string `json:"subscribed"`
	Unsubscribed []string `json:"unsubscribed"`
	Failed       []string `json:"failed"`
}

// reconcileSweep runs one sweep through the admin API and returns its report.
// Anything but a 2xx fails the test; judging the report is up to the caller.
func (h *harness) reconcileSweep(t *testing.T) followListSweep {
	t.Helper()
	patient := *h
	patient.http = &http.Client{Timeout: reconcileTimeout}
	var result followListSweep
	if err := patient.adminDo(http.MethodPost, "/admin/communities/reconcile", nil, &result); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	return result
}

// trackedCommunities is the set of group ids GET /admin/communities reports,
// pending and accepted alike.
func (h *harness) trackedCommunities(t *testing.T) map[string]bool {
	t.Helper()
	var list struct {
		Communities []adminCommunity `json:"communities"`
	}
	if err := h.adminDo(http.MethodGet, "/admin/communities", nil, &list); err != nil {
		t.Fatalf("list communities: %v", err)
	}
	tracked := make(map[string]bool, len(list.Communities))
	for _, community := range list.Communities {
		tracked[community.Community] = true
	}
	return tracked
}

// instanceSweeps runs sweeps over a list with an `instances` entry. No such sweep
// may unfollow anything, and it remembers every Failed entry, because a refused
// community is reported once and then skipped.
type instanceSweeps struct {
	t      *testing.T
	h      *harness
	failed map[string]bool
}

func (s *instanceSweeps) sweep() {
	s.t.Helper()
	result := s.h.reconcileSweep(s.t)
	if len(result.Unsubscribed) > 0 {
		s.t.Fatalf("an instance sweep unfollowed %v", result.Unsubscribed)
	}
	for _, community := range result.Failed {
		s.failed[community] = true
	}
}

// until sweeps until done holds. It is bounded on time, not on sweeps: a sweep
// adds at most 20 communities, and accepted communities with unfinished backfills
// can hold listed adds for as long as their walks take.
func (s *instanceSweeps) until(want string, done func(tracked map[string]bool) bool) {
	s.t.Helper()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	deadline := time.Now().Add(eventTimeout)
	for {
		s.sweep()
		tracked := s.h.trackedCommunities(s.t)
		if done(tracked) {
			return
		}
		if time.Now().After(deadline) {
			s.t.Fatalf("%s: not within %s; tracked %v, failed %v", want, eventTimeout, tracked, s.failed)
		}
		select {
		case <-s.t.Context().Done():
			s.t.Fatalf("%s: %v", want, s.t.Context().Err())
		case <-ticker.C:
		}
	}
}

// tidepoolContainerIdentity is the running bridge container's id and start time.
// A restart changes the start time and a recreate changes both.
func tidepoolContainerIdentity(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", "compose", "-f", composeFile(t), "ps", "-q", "tidepool").Output()
	if err != nil {
		t.Fatalf("find the tidepool container: %v", err)
	}
	id := strings.TrimSpace(string(out))
	if id == "" {
		t.Fatal("no running tidepool container")
	}
	out, err = exec.CommandContext(ctx, "docker", "inspect", "--format", "{{.Id}} {{.State.StartedAt}}", id).Output()
	if err != nil {
		t.Fatalf("inspect the tidepool container %s: %v", id, err)
	}
	return strings.TrimSpace(string(out))
}

// tidepoolLogsSince returns the bridge container's log from since on. Only the
// base compose file is passed, as in tidepoolContainerIdentity: the project
// name and the service are the base file's, and the follow-list override would
// need its directory variable set just to be parsed.
func tidepoolLogsSince(t *testing.T, since time.Time) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", "compose", "-f", composeFile(t),
		"logs", "--no-color", "--no-log-prefix", "--since", since.UTC().Format(time.RFC3339Nano), "tidepool").CombinedOutput()
	if err != nil {
		t.Fatalf("read the tidepool log: %v\n%s", err, out)
	}
	return string(out)
}

// postWalkFailureLogged reports whether a line of a text-handler log is the
// post walk's failure for host.
func postWalkFailureLogged(log, host string) bool {
	for line := range strings.Lines(log) {
		if strings.Contains(line, `msg="post walk failed"`) && slices.Contains(strings.Fields(line), "host="+host) {
			return true
		}
	}
	return false
}

// awaitPostWalkFailureLogged polls the bridge's log from since on until it holds
// the post walk's failure for host. The sweep has returned by then; the poll
// covers the log driver's lag.
func awaitPostWalkFailureLogged(t *testing.T, since time.Time, host string) {
	t.Helper()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.Now().Add(30 * time.Second)
	for {
		log := tidepoolLogsSince(t, since)
		if postWalkFailureLogged(log, host) {
			return
		}
		if time.Now().After(deadline) {
			t.Errorf("the tidepool log since %s holds no `post walk failed` line for host=%s:\n%s",
				since.UTC().Format(time.RFC3339Nano), host, log)
			return
		}
		select {
		case <-t.Context().Done():
			t.Fatalf("waiting for the post walk failure log: %v", t.Context().Err())
		case <-ticker.C:
		}
	}
}

// awaitPostDeliveryParked polls the delivery of postURI's Create until it is
// pending, parked by the switch, and scheduled later than after. The row is found
// through the activity's object id, so another delivery to the same community
// cannot satisfy it. It returns the row's next_attempt_at. A terminal state fails
// at once: the worker got past the switch.
func (h *harness) awaitPostDeliveryParked(t *testing.T, db *sql.DB, communityDID, postURI string, after time.Time) time.Time {
	t.Helper()
	ctx := t.Context()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.Now().Add(eventTimeout)
	last := "no delivery row"
	for time.Now().Before(deadline) {
		var state, errorClass, targetInbox string
		var nextAttemptAt time.Time
		err := db.QueryRowContext(ctx, `
			SELECT d.state, d.last_error_class, d.target_inbox, d.next_attempt_at
			FROM outbound_deliveries d
			JOIN outbound_activities a ON a.activity_id = d.activity_id
			WHERE a.kind = 'Create'
			  AND a.payload->'object'->>'id' LIKE '%/ap/object/' || $1
			ORDER BY d.seq DESC LIMIT 1`, strings.TrimPrefix(postURI, "at://")).
			Scan(&state, &errorClass, &targetInbox, &nextAttemptAt)
		switch {
		case err == nil:
			last = fmt.Sprintf("state=%q last_error_class=%q target_inbox=%q next_attempt_at=%s",
				state, errorClass, targetInbox, nextAttemptAt.Format(time.RFC3339Nano))
			if state != "pending" {
				t.Fatalf("delivery of %s left pending while the host switch was on: %s", postURI, last)
			}
			if errorClass == "switch_parked" && nextAttemptAt.After(after) {
				return nextAttemptAt
			}
		case !errors.Is(err, sql.ErrNoRows):
			t.Fatalf("query the delivery of %s: %v", postURI, err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for the delivery of %s to park: %v", postURI, ctx.Err())
		case <-ticker.C:
		}
	}
	t.Fatalf("delivery of %s never parked past %s within %s; last seen: %s; bridge admission: %s",
		postURI, after.Format(time.RFC3339Nano), eventTimeout, last, h.admissionFor(communityDID, postURI))
	return time.Time{}
}

// TestFollowList_HostSwitchParksDeliveriesWithoutRestart: a host added to the
// running bridge's follow list parks a native post's delivery, and removing the
// host releases it, all on the same container. The interval is an hour, so only
// the reconcile calls can have applied either change.
func TestFollowList_HostSwitchParksDeliveriesWithoutRestart(t *testing.T) {
	h := newHarness(t)
	lemmyHost := lemmyHostname(t)

	owner := h.registerUser(t, h.uniqueName(t, "fl_own"))
	name := h.uniqueName(t, "fl_comm")
	community := owner.createCommunity(t, name, "follow list "+name)
	sub := h.subscribeCommunity(t, "!"+name+"@"+lemmyHost)
	if sub.DID == "" {
		t.Fatalf("subscribed %s but the bridge reported no community DID: %+v", name, sub)
	}

	list := h.installFollowList(t, time.Hour)
	container := tidepoolContainerIdentity(t)

	list.writeSwitches([]string{lemmyHost}, nil)
	h.reconcileFollowList(t)

	pds := &pdsClient{http: h.http}
	if err := pds.createSession(nativeHandle, nativePassword); err != nil {
		t.Fatalf("createSession as the native bootstrap account %q at %s: %v", nativeHandle, pdsURL(), err)
	}
	rkey := syntax.NewTIDNow(0).String()
	title := fmt.Sprintf("follow list %s %s", h.suffix, rkey)
	const content = "held by the host switch until it clears"
	postURI, _, err := pds.createRecord(colPostV2, rkey, map[string]any{
		"$type":     colPostV2,
		"community": sub.DID,
		"title":     title,
		"content":   content,
		"createdAt": time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("createRecord: %v", err)
	}
	t.Logf("wrote %s", postURI)

	db := openTidepoolDB(t)
	parkedUntil := h.awaitPostDeliveryParked(t, db, sub.DID, postURI, time.Time{})
	// A later next_attempt_at on the same row means the worker claimed it again
	// and the switch refused it again.
	h.awaitPostDeliveryParked(t, db, sub.DID, postURI, parkedUntil)

	if _, found, err := h.lemmyPostBody(community.ID, title); err != nil {
		t.Fatalf("read Lemmy community %d while the host is blocked: %v", community.ID, err)
	} else if found {
		t.Fatalf("post %q reached Lemmy while host %q was blocked", title, lemmyHost)
	}

	list.writeSwitches(nil, nil)
	h.reconcileFollowList(t)

	if body := h.awaitLemmyPostBody(t, community.ID, title, sub.DID, postURI); body != content {
		t.Errorf("lemmy stored body %q, want %q", body, content)
	}
	if after := tidepoolContainerIdentity(t); after != container {
		t.Errorf("the tidepool container changed between the reconciles: %q before, %q after", container, after)
	}
}

// tracksAll reports whether every community is in tracked.
func tracksAll(tracked map[string]bool, communities ...lemmyCommunity) bool {
	return !slices.ContainsFunc(communities, func(community lemmyCommunity) bool {
		return !tracked[community.APID]
	})
}

// TestFollowList_InstanceEntryFollowsEveryLocalCommunity: an `instances` entry
// for the real Lemmy follows its communities, NSFW ones and later ones included,
// leaves out the excluded one and the one that opted out, and unfollows nothing
// while Lemmy is down. The interval is an hour, so only this test's sweeps run
// after the startup sweep, which has no `instances` entry to act on.
func TestFollowList_InstanceEntryFollowsEveryLocalCommunity(t *testing.T) {
	h := newHarness(t)
	lemmyHost := lemmyHostname(t)

	// The limits the strict listing fixture enforces have to be the pinned
	// Lemmy's. The two 200s show the 400s come from the limits.
	for _, request := range []struct {
		query string
		want  int
	}{
		{query: "page=100", want: http.StatusOK},
		{query: "page=101", want: http.StatusBadRequest},
		{query: "limit=50", want: http.StatusOK},
		{query: "limit=51", want: http.StatusBadRequest},
	} {
		response, err := h.http.Get(lemmyURL() + "/api/v3/community/list?" + request.query)
		require.NoError(t, err, request.query)
		require.NoError(t, response.Body.Close(), request.query)
		require.Equal(t, request.want, response.StatusCode, request.query)
	}

	owner := h.registerUser(t, h.uniqueName(t, "ie_own"))
	list := h.installFollowList(t, time.Hour)

	// Registered after installFollowList, so it runs before that cleanup and
	// while this list is still the running one: later scenarios must not inherit
	// a follow of every Lemmy community.
	t.Cleanup(func() {
		list.instances = nil
		list.writeSwitches(nil, nil)
		if result := h.reconcileSweep(t); len(result.Failed) > 0 {
			t.Errorf("removing the instances entry failed on %v", result.Failed)
		}
		for community := range h.trackedCommunities(t) {
			if !slices.Contains(list.communities, community) {
				t.Errorf("%s is still tracked after the instances entry was removed", community)
			}
		}
	})

	// Created first and so read first: once the later ones are tracked, the
	// listing has passed these two, and leaving them out is a decision. An
	// explicit entry overrides exclude, so none of these go in list.communities.
	excludedName := h.uniqueName(t, "ie_excl")
	excluded := owner.createCommunity(t, excludedName, "instance entry "+excludedName)
	refusedName := h.uniqueName(t, "ie_refuse")
	refused := owner.createCommunityWith(t, refusedName, "instance entry "+refusedName,
		lemmyCommunityOptions{Description: "opted out of bridging #nobridge"})
	plainName := h.uniqueName(t, "ie_plain")
	plain := owner.createCommunity(t, plainName, "instance entry "+plainName)
	nsfwName := h.uniqueName(t, "ie_nsfw")
	nsfw := owner.createCommunityWith(t, nsfwName, "instance entry "+nsfwName, lemmyCommunityOptions{NSFW: true})

	list.instances = []followListInstance{{
		host:    lemmyHost,
		exclude: []string{"!" + excludedName + "@" + lemmyHost},
	}}
	list.writeSwitches(nil, nil)

	// Tracked means the sweep's add took; the suite's own subscribe then closes
	// Lemmy's first-contact Accept race and waits for accepted.
	awaitAccepted := func(names ...string) {
		t.Helper()
		for _, name := range names {
			h.subscribeCommunity(t, "!"+name+"@"+lemmyHost)
		}
	}
	assertLeftOut := func() {
		t.Helper()
		tracked := h.trackedCommunities(t)
		if tracked[excluded.APID] {
			t.Errorf("excluded community %s is tracked", excluded.APID)
		}
		if tracked[refused.APID] {
			t.Errorf("#nobridge community %s is tracked", refused.APID)
		}
	}

	sweeps := &instanceSweeps{t: t, h: h, failed: map[string]bool{}}
	sweeps.until("the plain and NSFW communities followed and the #nobridge one refused", func(tracked map[string]bool) bool {
		return tracksAll(tracked, plain, nsfw) && sweeps.failed[refused.APID]
	})
	awaitAccepted(plainName, nsfwName)
	assertLeftOut()
	if sweeps.failed[excluded.APID] {
		t.Errorf("excluded community %s was tried", excluded.APID)
	}

	lateName := h.uniqueName(t, "ie_late")
	late := owner.createCommunity(t, lateName, "instance entry "+lateName)
	sweeps.until("a community created after the first sweeps followed", func(tracked map[string]bool) bool {
		return tracksAll(tracked, late)
	})
	awaitAccepted(lateName)

	// Registered after the cleanup above, so Lemmy is back before that one's
	// sweep sends its Undos.
	t.Cleanup(func() { startLemmy(t) })
	returnedName := h.uniqueName(t, "ie_outage")
	returned := owner.createCommunity(t, returnedName, "instance entry "+returnedName)
	before := h.trackedCommunities(t)
	stopLemmy(t)
	if err := probeHTTP(h.http, lemmyURL()+"/api/v3/site"); err == nil {
		t.Fatal("lemmy still answers after it was stopped")
	}
	outageSweepStart := time.Now()
	sweeps.sweep()
	require.Equal(t, before, h.trackedCommunities(t), "a sweep during a Lemmy outage changed the tracked set")
	// The failure log shows real sweeps walk the host's posts too.
	awaitPostWalkFailureLogged(t, outageSweepStart, lemmyHost)

	startLemmy(t)
	sweeps.until("following resumed after the outage", func(tracked map[string]bool) bool {
		return tracksAll(tracked, returned)
	})
	awaitAccepted(returnedName)
	assertLeftOut()
}
