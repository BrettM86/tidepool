package sync

import (
	"bytes"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/events"
	"github.com/stretchr/testify/require"

	"tidepool/internal/repo"
)

func TestIdentityFrame_RoundTrip(t *testing.T) {
	createdAt, err := time.Parse(time.RFC3339Nano, "2026-10-09T14:30:05.123456+02:00")
	require.NoError(t, err)
	frame := identityFrame(&repo.Event{
		Kind: repo.EventKindIdentity, DID: "did:plc:ewvi7nxzyoun6zhxrhs64oiz",
		Seq: 42, CreatedAt: createdAt, Handle: "alice.lemmy.world.ap.tidepool.test",
	})
	require.NotNil(t, frame)
	require.NotNil(t, frame.RepoIdentity, "expected #identity before serialization")
	encoded, err := serializeFrame(frame)
	require.NoError(t, err)
	var decoded events.XRPCStreamEvent
	require.NoError(t, decoded.Deserialize(bytes.NewReader(encoded)))
	require.NotNil(t, decoded.RepoIdentity, "decoded frame must be #identity")
	require.Nil(t, decoded.RepoCommit)
	require.Nil(t, decoded.RepoAccount)
	require.Equal(t, "did:plc:ewvi7nxzyoun6zhxrhs64oiz", decoded.RepoIdentity.Did)
	require.Equal(t, int64(42), decoded.RepoIdentity.Seq)
	require.NotNil(t, decoded.RepoIdentity.Handle)
	require.Equal(t, "alice.lemmy.world.ap.tidepool.test", *decoded.RepoIdentity.Handle)
	require.Equal(t, "2026-10-09T12:30:05.123Z", decoded.RepoIdentity.Time)
}
