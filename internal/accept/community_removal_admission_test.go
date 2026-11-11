package accept

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/ingest"
	"tidepool/internal/labels"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// Chunk 05 B7: the acceptance test for a bridged community removal. A bare-DID
// `!takedown` on the community's DID runs through one label store wired with
// the outbound takedowns applier and the ingest Admin's community applier.
// The removal is not the author's doing, so an edit of an accepted native post
// in the removed community is kept as the object's latest state instead of
// becoming an admission removal, and reaches Lemmy after the negation.

const (
	crFirstEditTitle    = "hello again, first edit during the removal"
	crFirstEditContent  = "the body of the post, first edited while the community was removed"
	crSecondEditTitle   = "hello again, second edit during the removal"
	crSecondEditContent = "the body of the post, edited twice while the community was removed"

	crPostCID3       = "bafyreid4wq42vsigqkklplahll4htzlxlsngqoj5s75bvuqyyl5ghweusy"
	crRevSecondEdit  = "3lzpostrev004"
	crAdminToken     = "community-removal-accept-token"
	crBridgeHostname = "tidepool.test"
)

// crFollowClient is the ingest Admin's view of Lemmy: it serves the
// community's Group document and records every Follow-lifecycle activity the
// Admin sends, in order.
type crFollowClient struct {
	mu   sync.Mutex
	sent []map[string]any
}

func (c *crFollowClient) ResolveHandle(context.Context, string) (string, error) {
	return acCommunityAPID, nil
}

func (c *crFollowClient) FetchActor(_ context.Context, iri string) (*ap.Object, error) {
	return &ap.Object{ID: iri, Type: "Group", Inbox: acCommunityInbox}, nil
}

func (c *crFollowClient) SendActivity(_ context.Context, _ string, activity any) error {
	raw, err := json.Marshal(activity)
	if err != nil {
		return err
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sent = append(c.sent, body)
	return nil
}

// lines is every activity the Admin sent, as atLine renders it.
func (c *crFollowClient) lines() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := []string{}
	for _, body := range c.sent {
		out = append(out, atLine(body))
	}
	return out
}

// crNoMaterializer stands in for the bridge's materializer: removing and
// restoring a community already bridged never bridges one, so any call
// through the embedded nil interface panics.
type crNoMaterializer struct{ ingest.Materializer }

type crNoPersonActors struct{}

func (crNoPersonActors) ListLivePersonAPActorIDs(context.Context) ([]string, error) { return nil, nil }

// crWorld is the chunk-04 takedown world whose label store also runs the
// ingest Admin's community applier, with the Admin talking to crFollowClient.
type crWorld struct {
	*atWorld
	follows *crFollowClient
}

func newCRWorld(t *testing.T) *crWorld {
	t.Helper()
	w := newATWorld(t)
	testutil.TruncateIfExists(t, w.conn, "community_removal_suppressions")
	t.Cleanup(func() { testutil.TruncateIfExists(t, w.conn, "community_removal_suppressions") })

	follows := &crFollowClient{}
	key, err := ap.GenerateRSAKey()
	require.NoError(t, err)
	admin, err := ingest.NewAdmin(ingest.AdminOptions{
		Token:        crAdminToken,
		Client:       follows,
		Materializer: crNoMaterializer{},
		Communities:  store.NewCommunities(w.conn),
		Actors:       crNoPersonActors{},
		Service: &ap.ServiceActor{
			ID:       "https://" + crBridgeHostname + "/actor",
			Hostname: crBridgeHostname,
			Key:      key,
		},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	w.labelStore = labels.NewPostgresStore(w.conn, w.takedowns.LabelApplier(), admin.CommunityLabelApplier())
	return &crWorld{atWorld: w, follows: follows}
}

// commitCommunityLabel commits the community `!takedown` (or its negation)
// on the community's bare DID.
func (w *crWorld) commitCommunityLabel(t *testing.T, negate bool) {
	t.Helper()
	w.commitTakedownLabel(t, acCommunityDID, negate)
}

// edit hands the dispatcher the author's edit of the accepted post.
func (w *crWorld) edit(t *testing.T, rev, cid string, timeUS int64, title, content string) {
	t.Helper()
	record := pv2Record(func(r map[string]any) {
		r["title"] = title
		r["content"] = content
	})
	require.NoError(t, w.dispatcher.HandleEvent(context.Background(),
		postEvent("update", acPostRKey, rev, cid, timeUS, record)),
		"the edit %q during the removal is handled", title)
}

// TestCommunityRemoval_AnEditDuringTheRemovalIsDeliveredAfterTheNegation (B7):
// the native post is accepted and delivered into the bridged community, then
// the community `!takedown` commits: Lemmy gets the community's Undo{Follow}
// and the post's Delete. The author edits the post twice during the removal:
// neither edit becomes an admission removal or a tombstone, and nothing is
// sent. The negation sends the post's Undo{Delete}, then the second edit as an
// Update.
func TestCommunityRemoval_AnEditDuringTheRemovalIsDeliveredAfterTheNegation(t *testing.T) {
	w := newCRWorld(t)

	admittedCreate(t, w.dispatcher)
	w.drain(t)
	sent, _ := w.newlySent()
	require.Equal(t, []string{"Create " + atPostAPID}, sent, "setup: the post is delivered")

	w.commitCommunityLabel(t, false)
	w.drain(t)
	sent, _ = w.newlySent()
	assert.Equal(t, []string{"Undo{Follow} " + acCommunityAPID}, w.follows.lines(),
		"precondition: the removal unfollows the community")
	assert.Equal(t, []string{"Delete " + atPostAPID}, sent,
		"precondition: the removal deletes the delivered native post")
	assert.False(t, w.postTombstoned(t), "the removal does not tombstone the post")

	w.edit(t, acRevUpdate, acPostCID2, acPostTimeUS+1, crFirstEditTitle, crFirstEditContent)
	w.requireNoRemoval(t, "after the first edit")
	w.edit(t, crRevSecondEdit, crPostCID3, acPostTimeUS+2, crSecondEditTitle, crSecondEditContent)
	w.requireNoRemoval(t, "after the second edit")
	w.drain(t)
	sent, _ = w.newlySent()
	assert.Equal(t, []string{}, sent,
		"nothing is sent for the edits during the removal, and no second Delete")

	w.commitCommunityLabel(t, true)
	w.drain(t)
	sent, bodies := w.newlySent()
	require.NotEmpty(t, sent, "the negation sends the post's Undo{Delete} and its latest edit")
	assert.Equal(t, "Undo{Delete} "+atPostAPID, sent[0], "the post's Undo{Delete} comes first")
	require.Greater(t, len(sent), 1, "an Update follows the Undo{Delete}")
	for i, line := range sent[1:] {
		assert.Equal(t, "Update "+atPostAPID, line, "delivery %d after the Undo{Delete} is the post's Update", i+1)
	}
	last, _ := bodies[len(bodies)-1]["object"].(map[string]any)
	assert.Equal(t, crSecondEditTitle, last["name"], "the last Update carries the second edit's title")
	source, _ := last["source"].(map[string]any)
	assert.Equal(t, crSecondEditContent, source["content"], "the last Update carries the second edit's content")
	w.requireNoRemoval(t, "after the negation")
}

// TestCommunityRemoval_ANewPostIntoTheRemovedCommunityIsRefused (B7): only
// an edit of a post the community had accepted is admitted during the
// removal. A post never accepted that targets the removed community is
// rejected as not followed: no acceptance, nothing enqueued, nothing sent.
func TestCommunityRemoval_ANewPostIntoTheRemovedCommunityIsRefused(t *testing.T) {
	const (
		newPostRKey = "3lzpostcrnew1"
		newPostURI  = "at://" + acAuthorDID + "/social.coves.community.postv2/" + newPostRKey
	)
	w := newCRWorld(t)
	w.commitCommunityLabel(t, false)
	require.Equal(t, []string{"Undo{Follow} " + acCommunityAPID}, w.follows.lines(),
		"precondition: the removal unfollows the community")
	activities := countRows(t, w.conn, "outbound_activities")

	require.NoError(t, w.dispatcher.HandleEvent(context.Background(),
		postEvent("create", newPostRKey, acRevCreate, acPostCID, acPostTimeUS, pv2Record())),
		"the new post during the removal is handled")

	status, code := admissionOf(t, w.conn, acCommunityDID, newPostURI)
	assert.Equal(t, "rejected", status, "the new post is not admitted")
	assert.Equal(t, "community-not-followed", code, "the removed community is not followed")
	assert.Equal(t, activities, countRows(t, w.conn, "outbound_activities"), "nothing is enqueued for the new post")
	w.drain(t)
	sent, _ := w.newlySent()
	assert.Equal(t, []string{}, sent, "nothing is sent for the new post")
}

// TestCommunityRemoval_AnEditInAnUnfollowedCommunityWithNoLabelIsARemoval
// (control): the community the native post was accepted into is unfollowed
// with no community label standing. An edit of the post is then an admission
// removal and the post's outbound object is tombstoned; only a removal keeps
// the edit.
func TestCommunityRemoval_AnEditInAnUnfollowedCommunityWithNoLabelIsARemoval(t *testing.T) {
	w := newCRWorld(t)
	admittedCreate(t, w.dispatcher)
	w.drain(t)
	sent, _ := w.newlySent()
	require.Equal(t, []string{"Create " + atPostAPID}, sent, "setup: the post is delivered")

	_, err := w.conn.Exec(`UPDATE communities SET follow_state = $2 WHERE did = $1`,
		acCommunityDID, string(store.FollowStateNone))
	require.NoError(t, err, "setup: unfollow the community")

	w.edit(t, acRevUpdate, acPostCID2, acPostTimeUS+1, crFirstEditTitle, crFirstEditContent)

	status, _ := admissionOf(t, w.conn, acCommunityDID, acPostURI)
	assert.Equal(t, StatusRemoved, status, "the edit in the unfollowed community is an admission removal")
	_, removed := removalStandsAt(t, w.repos, acCommunityDID, acPostURI)
	assert.True(t, removed, "a removal record stands in the community repo")
	assert.True(t, w.postTombstoned(t), "outbound_objects.tombstoned_at is set")
}
