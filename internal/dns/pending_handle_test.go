package dns

import (
	"testing"

	miekgdns "github.com/miekg/dns"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/identity"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

func TestPendingHandle_ResolverAndDNS(t *testing.T) {
	database := testutil.DB(t)
	testutil.Truncate(t, database, "bridged_actors", "handle_claims", "firehose_events")
	actors := store.NewBridgedActors(database)
	const (
		did     = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
		old     = "alice.lemmy-world.tdpl.example"
		pending = "alice.lemmy.world.ap.tdpl.example"
		apID    = "https://lemmy.world/u/alice"
	)
	_, err := actors.UpsertActor(t.Context(), store.BridgedActor{
		APActorID: apID, ActorType: store.ActorTypePerson, DID: did,
		Handle: old, ConsentState: store.ConsentStateOK,
	})
	require.NoError(t, err)
	require.NoError(t, actors.SetPendingHandle(t.Context(), did, pending))
	resolver := identity.NewStoreResolver(actors, "tdpl.example", "")
	options := handlerOptions(nil)
	options.Resolver = resolver
	options.HandleZoneRoot = "ap.tdpl.example"
	handler, err := NewHandler(options)
	require.NoError(t, err)

	lookup := func(t *testing.T, handle string, expected bool) {
		t.Helper()
		got, err := resolver.ResolveHandle(t.Context(), handle)
		if expected {
			require.NoError(t, err)
			require.Equal(t, did, got)
		} else {
			require.True(t, errors.IsNotFound(err), "handle %s: %v", handle, err)
		}
		request := new(miekgdns.Msg)
		request.SetQuestion("_atproto."+handle+".", miekgdns.TypeTXT)
		writer := &recordingDNSWriter{}
		handler.ServeDNS(writer, request)
		require.NotNil(t, writer.response)
		require.Equal(t, miekgdns.RcodeSuccess, writer.response.Rcode)
		if expected {
			require.Len(t, writer.response.Answer, 1)
			txt, ok := writer.response.Answer[0].(*miekgdns.TXT)
			require.True(t, ok)
			require.Equal(t, []string{"did=did:plc:ewvi7nxzyoun6zhxrhs64oiz"}, txt.Txt)
		} else {
			require.Empty(t, writer.response.Answer)
		}
	}
	t.Run("both handles resolve while pending", func(t *testing.T) {
		lookup(t, old, true)
		lookup(t, pending, true)
	})
	require.NoError(t, actors.SetConsentState(t.Context(), apID, store.ConsentStateDeleted))
	t.Run("neither resolves after tombstone", func(t *testing.T) {
		lookup(t, old, false)
		lookup(t, pending, false)
	})
}
