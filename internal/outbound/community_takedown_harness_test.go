package outbound

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/consume"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// The chunk-05 community-removal additions to the takedown harness. A
// community `!takedown` names the bridged community's BARE DID
// (outCommunityDID): the community is a communities row with that did, and
// bridged posts bound to it are ap_objects rows whose community_did is that
// DID. A second community, crOtherCommunityAPID, shares the fake Lemmy's
// shared inbox and is never labeled.

const (
	// crBridgedAuthorDID is the hosted repo of every bridged post below.
	crBridgedAuthorDID = "did:plc:n3wq5hbrbz7ydnkxlpqegrty"

	// crOtherCommunity is an unlabeled bridged community on the same Lemmy.
	crOtherCommunityName = "gardening"
	crOtherCommunityAPID = "https://lemmy.world/c/" + crOtherCommunityName
	crOtherCommunityDID  = "did:plc:7gq2kcrmx5wvdbhnys4ztl6e"
)

// crBridgedPost is a bridged post in outCommunityAPID, mapped in ap_objects
// with community_did = outCommunityDID and no outbound row.
type crBridgedPost struct{ rkey, apID string }

func (p crBridgedPost) atURI() string {
	return "at://" + crBridgedAuthorDID + "/social.coves.community.postv2/" + p.rkey
}

var (
	// crDeliveredVoteSubject is voted on before the label and delivered (Vd).
	crDeliveredVoteSubject = crBridgedPost{rkey: "3lzcrbrdg0001", apID: "https://lemmy.world/post/5201"}
	// crPendingVoteSubject is voted on before the label, not delivered (Vq).
	crPendingVoteSubject = crBridgedPost{rkey: "3lzcrbrdg0002", apID: "https://lemmy.world/post/5202"}
	// crNewVoteSubject is voted on after the label.
	crNewVoteSubject = crBridgedPost{rkey: "3lzcrbrdg0003", apID: "https://lemmy.world/post/5203"}
	// crDeliveryOnlyParent is the parent of the delivery-only reply Rd: bound
	// to the community only through its ap_objects.community_did.
	crDeliveryOnlyParent = crBridgedPost{rkey: "3lzcrbrdg0004", apID: "https://lemmy.world/post/5204"}
	// crLaterReplyParent is the parent of a native reply enqueued after the
	// label, which is held on this parent.
	crLaterReplyParent = crBridgedPost{rkey: "3lzcrbrdg0005", apID: "https://lemmy.world/post/5205"}
)

var (
	// crDeliveryOnlyReply (Rd) has a delivery row and no outbound_objects row.
	crDeliveryOnlyReply = tdReply{rkey: "3lztkdncmnt05", rev: "3lztkdncrev005",
		parentATURI: crDeliveryOnlyParent.atURI(), parentAPID: crDeliveryOnlyParent.apID}
	// crLaterComment is a native comment enqueued after the label.
	crLaterComment = tdReply{rkey: "3lztkdncmnt06", rev: "3lztkdncrev006",
		parentATURI: outRootATURI, parentAPID: outRootAPID}
	// crLaterReplyToBridged is a native reply to the bridged crLaterReplyParent
	// enqueued after the label.
	crLaterReplyToBridged = tdReply{rkey: "3lztkdncmnt07", rev: "3lztkdncrev007",
		parentATURI: crLaterReplyParent.atURI(), parentAPID: crLaterReplyParent.apID}
	// crOtherCommunityPost is a native post by tdAuthorDID in crOtherCommunityAPID.
	crOtherCommunityPost = tdNativePost{authorDID: tdAuthorDID, rkey: "3lztkdnpost09", rev: "3lztkdnrev0009",
		title: "A native post in a community nobody removed"}
)

// communityRemovalWorld is the takedown world plus a real consume.Dispatcher
// (votes go through its vote path), the two communities and the bridged posts.
type communityRemovalWorld struct {
	*accountStatusWorld
	voteSeq int64
}

func newCommunityRemovalWorld(t *testing.T) *communityRemovalWorld {
	t.Helper()
	s := newAccountStatusWorld(t)
	testutil.Truncate(t, s.conn, "jetstream_record_revs", "jetstream_dead_letters",
		"community_bans", "object_moderation")
	testutil.TruncateIfExists(t, s.conn, "community_removal_suppressions")
	// A community label left standing would hold the community's work in the
	// package's other harnesses, which seed the same community DID.
	t.Cleanup(func() {
		testutil.Truncate(t, s.conn, "coves_labels", "coves_label_cursor")
	})
	// Before any request: the other community's Group document, on the same
	// shared inbox.
	s.faults.next = otherCommunityGroup{next: s.lemmy}

	ctx := context.Background()
	communities := store.NewCommunities(s.conn)
	for _, c := range []store.Community{
		{APGroupID: outCommunityAPID, DID: outCommunityDID, PreferredUsername: outCommunityName,
			Instance: outLemmyHost, FollowState: store.FollowStateAccepted},
		{APGroupID: crOtherCommunityAPID, DID: crOtherCommunityDID, PreferredUsername: crOtherCommunityName,
			Instance: outLemmyHost, FollowState: store.FollowStateAccepted},
	} {
		_, err := communities.UpsertCommunity(ctx, c)
		require.NoError(t, err, "seed community %s", c.APGroupID)
	}
	objects := store.NewAPObjects(s.conn)
	for _, post := range []crBridgedPost{crDeliveredVoteSubject, crPendingVoteSubject, crNewVoteSubject, crDeliveryOnlyParent, crLaterReplyParent} {
		_, err := objects.PutMapping(ctx, store.APObjectMapping{
			APID:           post.apID,
			APType:         "Page",
			OriginInstance: outLemmyHost,
			DID:            crBridgedAuthorDID,
			AuthorDID:      crBridgedAuthorDID,
			CommunityDID:   outCommunityDID,
			Collection:     "social.coves.community.postv2",
			RKey:           post.rkey,
			CID:            tdPostCID,
		})
		require.NoError(t, err, "map bridged post %s", post.apID)
	}
	return &communityRemovalWorld{accountStatusWorld: s}
}

// otherCommunityGroup serves crOtherCommunityAPID's Group document (its
// shared inbox is the fake Lemmy's) and hands everything else to next.
type otherCommunityGroup struct{ next http.Handler }

func (g otherCommunityGroup) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet || r.URL.Path != "/c/"+crOtherCommunityName {
		g.next.ServeHTTP(w, r)
		return
	}
	w.Header().Set("Content-Type", ap.ContentTypeActivityJSON)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"@context":          "https://www.w3.org/ns/activitystreams",
		"id":                crOtherCommunityAPID,
		"type":              "Group",
		"preferredUsername": crOtherCommunityName,
		"inbox":             crOtherCommunityAPID + "/inbox",
		"endpoints":         map[string]any{"sharedInbox": outSharedInbox},
	})
}

// commitCommunityLabel commits the `!takedown` (or its negation) on the
// community's bare DID at the next seq.
func (w *communityRemovalWorld) commitCommunityLabel(t *testing.T, negate bool) {
	t.Helper()
	w.commitLabel(t, outCommunityDID, negate)
}

// vote casts tdAuthorDID's up vote with rkey on subject through the
// dispatcher's vote path and returns its Like's activity id.
func (w *communityRemovalWorld) vote(t *testing.T, rkey string, subject crBridgedPost) string {
	t.Helper()
	w.voteSeq++
	event := &consume.JetstreamEvent{
		DID: tdAuthorDID, TimeUS: 1_793_527_200_000_000 + w.voteSeq, Kind: "commit",
		Commit: &consume.CommitEvent{
			Rev: fmt.Sprintf("3mzakfuhe%04d", w.voteSeq), Operation: "create",
			Collection: "social.coves.feed.vote", RKey: rkey, CID: outCommentCID,
			Record: map[string]any{
				"$type":     "social.coves.feed.vote",
				"subject":   map[string]any{"uri": subject.atURI(), "cid": tdPostCID},
				"direction": "up",
				"createdAt": "2026-11-01T10:30:00.000Z",
			},
		},
	}
	require.NoError(t, w.handle(event), "the vote %s is handled", rkey)
	voteATURI := "at://" + tdAuthorDID + "/social.coves.feed.vote/" + rkey
	return consume.ActivityID(outUserOrigin, voteATURI, "create", 0)
}

// enqueueDeliveryOnlyReply enqueues reply's Create through the real enqueuer
// with its parent and no outbound_objects row: the delivery row is all there
// is of it.
func (w *communityRemovalWorld) enqueueDeliveryOnlyReply(t *testing.T, reply tdReply) {
	t.Helper()
	ctx := context.Background()
	_, intent := replyCreate(t, reply)
	tx, err := w.conn.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	require.NoError(t, w.enqueuer.EnqueueActivity(ctx, tx, tdAuthorDID, outCommunityAPID, reply.parentATURI, intent),
		"enqueue %s", intent.ID)
	require.NoError(t, tx.Commit())
	_, err = store.NewOutboundObjects(w.conn).GetByATURI(ctx, reply.atURI())
	require.Error(t, err, "setup: the delivery-only reply has no outbound_objects row")
}

// enqueueOtherCommunityPost enqueues crOtherCommunityPost's Create into
// crOtherCommunityAPID through the real enqueuer.
func (w *communityRemovalWorld) enqueueOtherCommunityPost(t *testing.T) {
	t.Helper()
	post := crOtherCommunityPost
	var snapshot map[string]any
	require.NoError(t, json.Unmarshal(tdNativePostSnapshot(t, post, post.rev, post.title), &snapshot))
	snapshot["communityApId"] = crOtherCommunityAPID
	snapshot["record"].(map[string]any)["community"] = crOtherCommunityDID
	encoded, err := json.Marshal(snapshot)
	require.NoError(t, err)
	w.enqueueNativeAs(t, post.authorDID, store.OutboundObject{
		ATURI:              post.atURI(),
		APObjectID:         post.apID(),
		LastCID:            tdPostCID,
		LastRev:            post.rev,
		CommunityDID:       crOtherCommunityDID,
		CommunityAPID:      crOtherCommunityAPID,
		TranslatedSnapshot: encoded,
	}, "", consume.PostIntent{
		Op:            "create",
		ATURI:         post.atURI(),
		ID:            consume.ActivityID(outUserOrigin, post.atURI(), "create", 0),
		CommunityAPID: crOtherCommunityAPID,
		CommunityDID:  crOtherCommunityDID,
		Snapshot:      encoded,
	})
}

// cancelledDeliveries counts the cancelled deliveries.
func cancelledDeliveries(t *testing.T, w *communityRemovalWorld) int {
	t.Helper()
	var n int
	require.NoError(t, w.conn.QueryRow(`SELECT count(*) FROM outbound_deliveries WHERE state = 'cancelled'`).Scan(&n))
	return n
}
