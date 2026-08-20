package accept

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// THE BAN CARVE-OUT MUST NOT TAKE THE POST OUT OF THE PURGE SET.
//
// A banned author's edit of a still-accepted post is deliberately refused
// WITHOUT touching the acceptance: a ban is author-state, not a judgement of the
// post, and the moderators who banned without removeData chose to leave that post
// standing. That carve-out is right. Recording it through the full Record upsert
// is not: the row flips to status='rejected' with accepted_cid/acceptance_rkey
// blanked, and ListAccepted — the removeData purge's ONLY input — selects exactly
// status='accepted'.
//
// So the ordinary sequence
//
//	ban (removeData=false) → author fixes a typo → re-ban with removeData=true
//
// purges every post the author had in that community EXCEPT the one they edited.
// Lemmy erased it; Coves keeps serving it under the community's name, and the
// author's quota is freed for a post that is still live.
//
// These tests pin the ledger side of the carve-out: the post stays in the set the
// purge reads, and stays counted, while the ledger still records WHY the edit did
// nothing.

// admissionRow reads the whole ledger row (the pins the purge and the operator
// surface depend on, which admissionOf's status/code pair cannot show).
func admissionRow(t *testing.T, conn *sql.DB, communityDID, postURI string) *Admission {
	t.Helper()
	adm, err := NewAdmissions(conn).Get(context.Background(), communityDID, postURI)
	require.NoError(t, err, "the engine records every decision it makes")
	return adm
}

// banAuthor lands a community ban on the post's author.
func banAuthor(t *testing.T, conn *sql.DB, reason string) {
	t.Helper()
	_, err := store.NewCommunityBans(conn).Ban(context.Background(), store.CommunityBan{
		CommunityDID:  acCommunityDID,
		SubjectDID:    acAuthorDID,
		CommunityAPID: acCommunityAPID,
		Reason:        reason,
	})
	require.NoError(t, err, "the community bans the author")
}

// assertStillPurgeable is the shared property: whatever the ledger says about the
// refused edit, the post is still one of the author's ACCEPTED posts in this
// community — the set a later removeData ban erases, and the set the flood cap
// counts.
func assertStillPurgeable(t *testing.T, conn *sql.DB, before *Admission) {
	t.Helper()
	ctx := context.Background()
	admissions := NewAdmissions(conn)

	purgeSet, err := admissions.ListAccepted(ctx, acCommunityDID, acAuthorDID)
	require.NoError(t, err)
	assert.Contains(t, purgeSet, acPostURI,
		"the edited post MUST stay in the removeData purge set: ListAccepted is the purge's "+
			"only input, so a ban→edit→re-ban(removeData=true) that drops it purges everything "+
			"EXCEPT the post the author touched — Lemmy erases it and Coves keeps showing it "+
			"under the community's name")

	n, err := admissions.CountAccepted(ctx, acAuthorDID, acCommunityDID, "at://never-this-post")
	require.NoError(t, err)
	assert.Equal(t, 1, n,
		"and it must keep consuming the author's per-community quota: the post is live, so "+
			"freeing its slot lets a banned author's edit buy room for another post")

	adm := admissionRow(t, conn, acCommunityDID, acPostURI)
	assert.Equal(t, StatusAccepted, adm.Status,
		"the ledger must still say accepted — the acceptance was deliberately left standing, "+
			"and 'rejected' for content that is live is the operator surface lying about it")
	assert.Equal(t, DecisionAuthorBanned, adm.DecisionCode,
		"while the WHY is recorded: it is what an operator reads when the author asks why "+
			"their edit did nothing")
	assert.Equal(t, before.AcceptedCID, adm.AcceptedCID,
		"the accepted_cid pin must survive: it names the version the standing acceptance "+
			"actually pins")
	assert.Equal(t, before.AcceptanceRKey, adm.AcceptanceRKey,
		"as must the acceptance_rkey — the community-repo record that is still there")
	assert.NotEmpty(t, adm.AcceptanceRKey, "(and it was non-empty to begin with)")
}

// The plain carve-out: the ban is already standing when the edit arrives, so
// decide() returns author-banned and AdmitPost's carve-out branch records it.
func TestABannedAuthorsEditKeepsThePostInTheRemoveDataPurgeSet(t *testing.T) {
	conn := acceptanceDB(t)
	ctx := context.Background()
	repos := newRepos(t, conn)
	seedBridgedCommunity(t, conn)
	enq := realEnqueuer(t, conn)
	dispatcher := wireDispatcher(t, conn, wireEngine(t, conn, repos, enq), enq)

	admittedCreate(t, dispatcher)
	before := admissionRow(t, conn, acCommunityDID, acPostURI)
	require.Equal(t, StatusAccepted, before.Status, "precondition: the post is accepted")
	require.NotEmpty(t, before.AcceptedCID, "precondition: the acceptance pins a CID")

	banAuthor(t, conn, "banned without removeData: this post stays up")

	// The ordinary thing an author does minutes after being banned: fix a typo.
	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("update", acPostRKey, acRevUpdate, acPostCID2, acPostTimeUS+1, pv2Record())))

	// The carve-out itself still holds (this is the behaviour being protected,
	// not changed): the acceptance stands, pinning the version it always did.
	cid, ok := acceptanceSubjectCID(t, repos, acCommunityDID, acPostURI)
	require.True(t, ok, "the acceptance the moderators chose to leave up must still stand")
	assert.Equal(t, acPostCID, cid, "still pinning the version that was accepted")

	assertStillPurgeable(t, conn, before)

	adm := admissionRow(t, conn, acCommunityDID, acPostURI)
	assert.Equal(t, acPostCID2, adm.EvaluatedCID,
		"the refusal is recorded against the version it was decided on: evaluated_cid is what "+
			"tells a replay 'already decided this version' from 'new content'")
	assert.Contains(t, string(adm.EvaluatedSnapshot), acPostCID2,
		"and the stored snapshot moves with it, so a readmit re-runs against the edit")
}

// The same shape one door over: the ban lands INSIDE the acceptance transaction
// (accept()'s StandingTx re-read), so the edit fails with ErrAuthorBanned after
// the gate let it through. The transaction rolled back, which means the PRIOR
// acceptance is still standing — exactly the state the carve-out protects — so
// the ledger must not flip it to rejected either.
func TestABanLandingMidEditKeepsTheAcceptedPostInThePurgeSet(t *testing.T) {
	conn := acceptanceDB(t)
	ctx := context.Background()
	repos := newRepos(t, conn)
	seedBridgedCommunity(t, conn)
	racing := &racingRepos{RepoManager: repos}
	enq := realEnqueuer(t, conn)
	dispatcher := wireDispatcher(t, conn, wireEngine(t, conn, racing, enq), enq)

	admittedCreate(t, dispatcher)
	before := admissionRow(t, conn, acCommunityDID, acPostURI)
	require.Equal(t, StatusAccepted, before.Status, "precondition: the post is accepted")

	// The ban commits between the gate and the acceptance commit of the EDIT.
	racing.beforeAcceptanceWrite = func() {
		banAuthor(t, conn, "banned while their typo fix was in flight")
	}
	// Logged, not asserted: the live path treats a mid-admission ban as handled.
	t.Logf("the racing edit reported: %v", dispatcher.HandleEvent(ctx,
		postEvent("update", acPostRKey, acRevUpdate, acPostCID2, acPostTimeUS+1, pv2Record())))
	require.True(t, racing.acceptanceFired,
		"the fixture must have raced the acceptance commit, or this test asserts nothing")

	cid, ok := acceptanceSubjectCID(t, repos, acCommunityDID, acPostURI)
	require.True(t, ok, "the rolled-back edit leaves the ORIGINAL acceptance standing")
	assert.Equal(t, acPostCID, cid, "still pinning the version that was accepted")

	assertStillPurgeable(t, conn, before)
}
