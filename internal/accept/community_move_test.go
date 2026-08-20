package accept

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// Community immutability must be decided BEFORE the record is judged on its
// merits. The lexicon check and the immutability check answer different
// questions — "is this a well-formed post?" and "may this event touch this
// community at all?" — and only the second one guards WHERE a decision gets
// written. Deciding merit first means a malformed community-moving edit gets a
// verdict (lexicon-invalid) that AdmitPost then records under the EVENT's
// community, which is precisely the community the move was refused into.
//
// These pin both halves of that: the never-accepted post (a second ledger row
// under the target) and the accepted one (a removal signed into a community
// that never accepted the post).

// A rejected post edited to name a DIFFERENT community while ALSO carrying a
// lexicon violation must still be discarded as a community move. The lexicon
// verdict must not pre-empt the immutability discard: a verdict is recorded
// under the event's community, so pre-empting writes a SECOND admissions row for
// one post_uri — the invariant GetByPostURI's single-row read depends on.
func TestH3_MalformedCommunityMoveOfRejectedPostWritesNothingToTheTarget(t *testing.T) {
	conn := acceptanceDB(t)
	ctx := context.Background()
	seedBridgedCommunity(t, conn)  // community A
	seedBridgedCommunityB(t, conn) // community B
	repos := newRepos(t, conn)
	enq := realEnqueuer(t, conn)
	dispatcher := wireDispatcher(t, conn, engineWith(t, conn, repos, enq), enq)

	// Opted out → REJECTED in community A (a ledger row under A, no outbound row).
	_, err := store.NewFederationPrefs(conn).Upsert(ctx, store.FederationPref{
		DID: acAuthorDID, Source: store.FederationPrefSourceRecord,
	})
	require.NoError(t, err)
	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("create", acPostRKey, acRevCreate, acPostCID, acPostTimeUS, pv2Record())))
	require.Equal(t, 1, admissionRowCount(t, conn, acCommunityDID, acPostURI),
		"precondition: rejected in A")

	// The move, carrying a lexicon violation too (createdAt must be a datetime
	// string). Both checks would fire; only the immutability one may decide.
	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("update", acPostRKey, acRevUpdate, acPostCID2, acPostTimeUS+1,
			pv2Record(func(r map[string]any) {
				r["community"] = acCommunityB_DID
				r["createdAt"] = 12345
			}))))

	assert.Zero(t, admissionRowCount(t, conn, acCommunityB_DID, acPostURI),
		"a lexicon-invalid community-moving edit must write NOTHING to the target community: "+
			"the verdict is recorded under the EVENT's community, so deciding merit before "+
			"immutability files the rejection under the community the move was refused into")
	assert.Equal(t, 1, admissionRowsForPost(t, conn, acPostURI),
		"one post_uri must never hold admissions rows under two communities")

	status, code := admissionOf(t, conn, acCommunityDID, acPostURI)
	assert.Equal(t, StatusRejected, status,
		"the original community's row keeps its status; a discarded event decides nothing new")
	assert.Equal(t, DecisionCommunityImmutable, code,
		"the attempted move is annotated on the ORIGINAL community's row (H3 semantics), not "+
			"recorded as a lexicon rejection somewhere else")
}

// The same malformed move against an ACCEPTED post. Deciding merit first turns
// the event into a re-admission FAILURE, and removeAccepted takes the EVENT's
// community — so a removal record gets signed into a community that never
// accepted the post, while community A's outbound row is tombstoned and a
// Delete{Page} is enqueued at A with A's acceptance left standing.
func TestH3_MalformedCommunityMoveOfAcceptedPostSignsNothingInTheTarget(t *testing.T) {
	conn := acceptanceDB(t)
	ctx := context.Background()
	seedBridgedCommunity(t, conn)
	seedBridgedCommunityB(t, conn)
	repos := newRepos(t, conn)
	enq := realEnqueuer(t, conn)
	dispatcher := wireDispatcher(t, conn, engineWith(t, conn, repos, enq), enq)

	admittedCreate(t, dispatcher) // accepted into community A
	require.Equal(t, 1, countRows(t, conn, "outbound_activities"), "precondition: one Create{Page}")

	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("update", acPostRKey, acRevUpdate, acPostCID2, acPostTimeUS+1,
			pv2Record(func(r map[string]any) {
				r["community"] = acCommunityB_DID
				r["createdAt"] = 12345
			}))),
		"a community-moving edit is discarded whole, however malformed it also is")

	// Nothing may be signed under community B — not an acceptance, not a removal.
	_, movedIn := acceptanceSubjectCID(t, repos, acCommunityB_DID, acPostURI)
	assert.False(t, movedIn, "no acceptance may appear in the target community")
	_, removedThere := removalStandsAt(t, repos, acCommunityB_DID, acPostURI)
	assert.False(t, removedThere,
		"and NO removal may appear either: a removal in B is B's key signing a decision about "+
			"a post B never accepted")
	assert.Zero(t, countWhere(t, conn, "repo_state", "did", acCommunityB_DID),
		"the target community's repo must not even be genesis-committed: the move never "+
			"reached a signing operation")
	assert.Zero(t, admissionRowCount(t, conn, acCommunityB_DID, acPostURI),
		"and no ledger row under the target")
	assert.Equal(t, 1, admissionRowsForPost(t, conn, acPostURI),
		"one post_uri, one community, one row")

	// Community A is left exactly as it was: acceptance standing on the original
	// version, nothing withdrawn, nothing enqueued.
	cid, ok := acceptanceSubjectCID(t, repos, acCommunityDID, acPostURI)
	require.True(t, ok, "the original acceptance stands")
	assert.Equal(t, acPostCID, cid, "still pinning the version A accepted")
	_, removedHere := removalStandsAt(t, repos, acCommunityDID, acPostURI)
	assert.False(t, removedHere, "and no removal stands in A either")
	assert.Zero(t, activityKindCount(t, conn, "Delete"),
		"no Delete{Page} may be enqueued: the event was discarded, not re-decided")
	assert.Equal(t, 1, countRows(t, conn, "outbound_activities"),
		"a discarded community-move enqueues nothing")

	stored, err := store.NewOutboundObjects(conn).GetByATURI(ctx, acPostURI)
	require.NoError(t, err)
	assert.False(t, stored.IsTombstoned(),
		"A's outbound state must not be tombstoned by an event decided about B")
	assert.Equal(t, acCommunityDID, stored.CommunityDID, "and it still names community A")

	status, _ := admissionOf(t, conn, acCommunityDID, acPostURI)
	assert.Equal(t, StatusAccepted, status,
		"the post is still accepted in A; the move decided nothing about its admission")
}

// The invariant the engine's single-row reads depend on — one post_uri, one
// community — is asserted in docs and tests but was enforced nowhere: migration
// 021's PK is (community_did, post_uri), so two communities holding one post_uri
// was a legal insert. Any writer with the ordering bug above silently produced
// it, and GetByPostURI then returned whichever row postgres felt like.
func TestAdmissionsPostURIIsGloballyUnique(t *testing.T) {
	conn := acceptanceDB(t)
	ctx := context.Background()
	admissions := NewAdmissions(conn)

	require.NoError(t, admissions.Record(ctx, Admission{
		AuthorDID:    acAuthorDID,
		CommunityDID: acCommunityDID,
		PostURI:      acPostURI,
		Status:       StatusRejected,
		DecisionCode: DecisionOptedOut,
	}), "the first community's decision is recorded normally")

	err := admissions.Record(ctx, Admission{
		AuthorDID:    acAuthorDID,
		CommunityDID: acCommunityB_DID,
		PostURI:      acPostURI,
		Status:       StatusRejected,
		DecisionCode: DecisionLexiconInvalid,
	})
	require.Error(t, err,
		"a second admissions row for the same post_uri under a DIFFERENT community must be "+
			"refused by the database: a post is bound to one community forever, and every "+
			"post_uri-keyed read (GetByPostURI, boundCommunityOf, Readmit) is a single-row "+
			"query that silently picks a winner once two rows exist")

	assert.Equal(t, 1, admissionRowsForPost(t, conn, acPostURI),
		"exactly one row survives")
	adm, gerr := admissions.GetByPostURI(ctx, acPostURI)
	require.NoError(t, gerr)
	assert.Equal(t, acCommunityDID, adm.CommunityDID,
		"and it is the community the post was actually bound to")

	// The (community_did, post_uri) upsert path must still work: re-recording the
	// SAME row is an update, not a unique violation.
	require.NoError(t, admissions.Record(ctx, Admission{
		AuthorDID:    acAuthorDID,
		CommunityDID: acCommunityDID,
		PostURI:      acPostURI,
		Status:       StatusRejected,
		DecisionCode: DecisionCommunityImmutable,
	}), "the unique index must not break the ledger's own idempotent re-record")
	_, code := admissionOf(t, conn, acCommunityDID, acPostURI)
	assert.Equal(t, DecisionCommunityImmutable, code)

	// A different post_uri under the second community is unaffected.
	otherURI := "at://" + acAuthorDID + "/social.coves.community.postv2/3lzpostuniq02"
	require.NoError(t, admissions.Record(ctx, Admission{
		AuthorDID:    acAuthorDID,
		CommunityDID: acCommunityB_DID,
		PostURI:      otherURI,
		Status:       StatusAccepted,
	}), "the index constrains post_uri, not the community")
	_, err = admissions.GetByPostURI(ctx, otherURI)
	require.NoError(t, err)
	require.False(t, errors.IsNotFound(err))
}
