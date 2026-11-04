//go:build e2e

package e2e

// The recent-post walk against the pinned Lemmy: the strict post fixture in
// internal/ingest models Lemmy 0.19.20's post list, and this test checks the
// real lister's requests are ones the real Lemmy accepts and that its filters
// (hidden communities, NSFW, featured posts) are the ones the fixture assumes.

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ingest"
)

// lemmyExchange is one request the walk made and the status Lemmy answered.
type lemmyExchange struct {
	url    string
	status int
}

// recordingLemmyTransport sends requests for the host in Lemmy's actor ids to
// the published Lemmy port and records every response. Any other host is
// refused, so the walk cannot leave the local stack.
type recordingLemmyTransport struct {
	lemmyHost string
	base      http.RoundTripper

	mu        sync.Mutex
	exchanges []lemmyExchange
}

func newRecordingLemmyTransport(t *testing.T) *recordingLemmyTransport {
	t.Helper()
	published, err := url.Parse(lemmyURL())
	require.NoError(t, err, "parse the published Lemmy URL")
	publishedAddress := published.Host
	if published.Port() == "" {
		publishedAddress = net.JoinHostPort(published.Hostname(), "80")
	}
	lemmyHost := lemmyHostname(t)
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	return &recordingLemmyTransport{
		lemmyHost: lemmyHost,
		base: &http.Transport{
			DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
				host, _, err := net.SplitHostPort(address)
				if err != nil {
					return nil, err
				}
				if host != lemmyHost {
					return nil, fmt.Errorf("refusing to dial %s: only %s is the local Lemmy", address, lemmyHost)
				}
				return dialer.DialContext(ctx, network, publishedAddress)
			},
		},
	}
}

func (transport *recordingLemmyTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.URL.Hostname() != transport.lemmyHost {
		return nil, fmt.Errorf("refusing a request to %s: only %s is the local Lemmy", request.URL, transport.lemmyHost)
	}
	response, err := transport.base.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	transport.mu.Lock()
	transport.exchanges = append(transport.exchanges, lemmyExchange{url: request.URL.String(), status: response.StatusCode})
	transport.mu.Unlock()
	return response, nil
}

// recorded returns the exchanges so far and forgets them.
func (transport *recordingLemmyTransport) recorded() []lemmyExchange {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	exchanges := transport.exchanges
	transport.exchanges = nil
	return exchanges
}

// hideCommunity sets a community's hidden flag (admin only).
func (c *lemmyClient) hideCommunity(communityID int, hidden bool) error {
	return c.do(http.MethodPut, "/api/v3/community/hide",
		map[string]any{"community_id": communityID, "hidden": hidden}, nil)
}

// featurePostLocal pins or unpins a post to the top of the instance (admin
// only), which puts it first in the local post list whatever its age.
func (c *lemmyClient) featurePostLocal(postID int, featured bool) error {
	return c.do(http.MethodPost, "/api/v3/post/feature",
		map[string]any{"post_id": postID, "featured": featured, "feature_type": "Local"}, nil)
}

// assertAllAccepted reports every request Lemmy did not answer 200 as an error
// and returns how many requests the walk made.
func assertAllAccepted(t *testing.T, walk string, exchanges []lemmyExchange) int {
	t.Helper()
	for _, exchange := range exchanges {
		if exchange.status != http.StatusOK {
			t.Errorf("%s: Lemmy answered %d to %s", walk, exchange.status, exchange.url)
		}
	}
	return len(exchanges)
}

// TestPostWalk_FindsCommunitiesOfLocalPosts: a LemmyLister walking the real
// Lemmy's local posts with a page size of 2 makes only requests Lemmy accepts,
// follows next_page, returns plain and NSFW communities and not a hidden one,
// and, from a mark, does not stop at an older post the admin featured.
func TestPostWalk_FindsCommunitiesOfLocalPosts(t *testing.T) {
	h := newHarness(t)
	lemmyHost := lemmyHostname(t)
	owner := h.registerUser(t, h.uniqueName(t, "pw_own"))

	plainName := h.uniqueName(t, "pw_plain")
	plain := owner.createCommunity(t, plainName, "post walk "+plainName)
	nsfwName := h.uniqueName(t, "pw_nsfw")
	nsfw := owner.createCommunityWith(t, nsfwName, "post walk "+nsfwName, lemmyCommunityOptions{NSFW: true})
	hiddenName := h.uniqueName(t, "pw_hidden")
	hidden := owner.createCommunity(t, hiddenName, "post walk "+hiddenName)

	// Created oldest first. The hidden community's post sits between the plain
	// and NSFW ones, so the page cap cannot explain its absence, and the NSFW
	// community's second post pushes the plain post to page 2 at a page size of
	// 2, so only following next_page finds it.
	plainPost := owner.createPost(t, plain.ID, "post walk plain "+h.suffix, "plain")
	owner.createPost(t, hidden.ID, "post walk hidden "+h.suffix, "hidden")
	owner.createPost(t, nsfw.ID, "post walk nsfw "+h.suffix, "nsfw")
	owner.createPost(t, nsfw.ID, "post walk nsfw again "+h.suffix, "nsfw again")

	require.NoError(t, h.admin.hideCommunity(hidden.ID, true), "hide community %s", hidden.APID)
	t.Cleanup(func() {
		if err := h.admin.hideCommunity(hidden.ID, false); err != nil {
			t.Errorf("unhide community %s: %v", hidden.APID, err)
		}
	})

	transport := newRecordingLemmyTransport(t)
	lister, err := ingest.NewLemmyLister(ingest.LemmyListerOptions{
		Client:    &http.Client{Transport: transport, Timeout: 30 * time.Second},
		Scheme:    "http",
		UserAgent: "tidepool-e2e post walk",
		Logger:    slog.New(slog.NewTextHandler(t.Output(), nil)),
		PostLimit: 2,
	})
	require.NoError(t, err, "build the lister")

	// Other scenarios leave posts in the shared Lemmy, but these are the newest,
	// well inside the 20 pages of 2 a walk from no mark reads.
	first := lister.RecentPostCommunities(t.Context(), lemmyHost, ingest.PostMark{})
	require.NoError(t, first.Err, "walk from no mark")
	if requests := assertAllAccepted(t, "walk from no mark", transport.recorded()); requests < 2 {
		t.Errorf("walk from no mark made %d requests, want it to follow next_page past page 1", requests)
	}
	assert.Contains(t, first.ActorIDs, plain.APID, "walk from no mark: plain community")
	assert.Contains(t, first.ActorIDs, nsfw.APID, "walk from no mark: NSFW community")
	assert.NotContains(t, first.ActorIDs, hidden.APID, "walk from no mark: hidden community")

	mark := first.Newest
	require.False(t, mark.Published.IsZero(), "walk from no mark set no mark: %+v", mark)

	// The featured post is older than the mark and comes first in the list, so
	// a walk that stopped at it would read only the newest of the posts below.
	require.NoError(t, h.admin.featurePostLocal(plainPost.ID, true), "feature post %s", plainPost.APID)
	t.Cleanup(func() {
		if err := h.admin.featurePostLocal(plainPost.ID, false); err != nil {
			t.Errorf("unfeature post %s: %v", plainPost.APID, err)
		}
	})
	middleName := h.uniqueName(t, "pw_middle")
	middle := owner.createCommunity(t, middleName, "post walk "+middleName)
	newestName := h.uniqueName(t, "pw_newest")
	newest := owner.createCommunity(t, newestName, "post walk "+newestName)
	owner.createPost(t, nsfw.ID, "post walk after the mark nsfw "+h.suffix, "after the mark")
	owner.createPost(t, middle.ID, "post walk after the mark middle "+h.suffix, "after the mark")
	owner.createPost(t, newest.ID, "post walk after the mark newest "+h.suffix, "after the mark")

	second := lister.RecentPostCommunities(t.Context(), lemmyHost, mark)
	require.NoError(t, second.Err, "walk from the mark %+v", mark)
	assertAllAccepted(t, "walk from the mark", transport.recorded())
	assert.Contains(t, second.ActorIDs, nsfw.APID, "walk from the mark: oldest post after the mark")
	assert.Contains(t, second.ActorIDs, middle.APID, "walk from the mark: middle post after the mark")
	assert.Contains(t, second.ActorIDs, newest.APID, "walk from the mark: newest post after the mark")
	assert.Contains(t, second.ActorIDs, plain.APID, "walk from the mark: the featured post's community")
}
