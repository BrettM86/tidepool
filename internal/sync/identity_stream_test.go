package sync

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"tidepool/internal/repo"
	"tidepool/internal/testutil"
)

func appendIdentityForStream(t *testing.T, did, handle string) int64 {
	t.Helper()
	tx, err := testutil.DB(t).BeginTx(context.Background(), nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	seq, err := repo.AppendIdentityEventTx(context.Background(), tx, did, handle)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	return seq
}

func TestSubscribeRepos_IdentityLiveNotify(t *testing.T) {
	h := newHarnessWithPoll(t, time.Hour, func(o *Options) { o.PingInterval = time.Hour })
	currentHead := putRecord(t, h, "identity-seed", "seed")
	conn := h.dial(t, fmt.Sprint(currentHead.Seq))
	waitForSubscribers(t, h, 1)

	marker := putRecord(t, h, "identity-marker", "past initial scan")
	frame := readFrame(t, conn, 5*time.Second)
	require.NotNil(t, frame.RepoCommit, "marker must arrive before the identity write")
	require.Equal(t, marker.Seq, frame.RepoCommit.Seq)

	seq := appendIdentityForStream(t, testDID, "alice.lemmy.world.ap.tidepool.test")
	frame = readFrame(t, conn, 5*time.Second)
	require.NotNil(t, frame.RepoIdentity, "expected live #identity frame, got %+v", frame)
	require.Equal(t, seq, frame.RepoIdentity.Seq)
	require.Equal(t, testDID, frame.RepoIdentity.Did)
	require.NotNil(t, frame.RepoIdentity.Handle)
	require.Equal(t, "alice.lemmy.world.ap.tidepool.test", *frame.RepoIdentity.Handle)
}

func TestSubscribeRepos_IdentityCursorReplay(t *testing.T) {
	h := newHarness(t)
	commit := putRecord(t, h, "identity-replay", "first commit")
	accountSeq, err := h.manager.AppendAccountEvent(context.Background(), testDID, false, repo.AccountStatusDeleted)
	require.NoError(t, err)
	identitySeq := appendIdentityForStream(t, testDID, "alice.lemmy.world.ap.tidepool.test")

	conn := h.dial(t, "0")
	frame := readFrame(t, conn, 5*time.Second)
	require.NotNil(t, frame.RepoCommit, "expected first frame to be #commit, got %+v", frame)
	require.Equal(t, commit.Seq, frame.RepoCommit.Seq)

	frame = readFrame(t, conn, 5*time.Second)
	require.NotNil(t, frame.RepoAccount, "expected second frame to be #account, got %+v", frame)
	require.Equal(t, accountSeq, frame.RepoAccount.Seq)

	frame = readFrame(t, conn, 5*time.Second)
	require.NotNil(t, frame.RepoIdentity, "expected third frame to be #identity, got %+v", frame)
	require.Equal(t, identitySeq, frame.RepoIdentity.Seq)
	require.Equal(t, testDID, frame.RepoIdentity.Did)
	require.NotNil(t, frame.RepoIdentity.Handle)
	require.Equal(t, "alice.lemmy.world.ap.tidepool.test", *frame.RepoIdentity.Handle)
	require.Less(t, commit.Seq, accountSeq)
	require.Less(t, accountSeq, identitySeq)
}
