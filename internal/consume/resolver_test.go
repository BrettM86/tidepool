package consume

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
)

// Task 14 cycle F1: DID → handle resolution with BIDIRECTIONAL verification.
//
// The stake is unusually high for a lookup. personas freezes the local part at
// creation, so whatever handle reaches the first mint is the name that user
// wears on the fediverse permanently. alsoKnownAs alone is a claim a stranger
// writes in their own DID document; believing it one-way would let anyone mint
// @alice@coves.social by naming alice's handle in their doc, and freezing
// would make the theft irreversible.

const (
	resolveDID         = "did:plc:7iza6de2dwap2sbkpav7c6c6"
	resolveHandle      = "alice.coves.social"
	resolveOtherDID    = "did:plc:44ybard66vv44zksje25o7dz"
	resolveOtherHandle = "mallory.coves.social"
)

func TestHandleResolver_VerifiesBothDirections(t *testing.T) {
	fake := newFakeIdentity(t)
	fake.claim(resolveDID, resolveHandle)

	handle, err := fake.resolver(t).ResolveDIDHandle(context.Background(), resolveDID)
	require.NoError(t, err)
	assert.Equal(t, resolveHandle, handle)

	assert.Equal(t, 1, fake.PLCHits(), "the DID document is fetched")
	assert.Equal(t, 1, fake.WellKnownHits(),
		"and the handle is asked to claim the DID BACK — a resolver that skipped this "+
			"would accept any handle a DID document names, including somebody else's")
}

func TestHandleResolver_ToleratesTheTrailingNewlineRealPDSesServe(t *testing.T) {
	fake := newFakeIdentity(t)
	fake.claim(resolveDID, resolveHandle)

	// The fake serves "did\n", as real PDSes do. A byte-exact comparison would
	// reject every genuine handle on the network.
	handle, err := fake.resolver(t).ResolveDIDHandle(context.Background(), resolveDID)
	require.NoError(t, err)
	assert.Equal(t, resolveHandle, handle)
}

func TestHandleResolver_ImpersonationIsRefused(t *testing.T) {
	fake := newFakeIdentity(t)
	// Mallory's DID document claims alice's handle...
	fake.claimOneWay(resolveOtherDID, resolveHandle)
	// ...but the handle belongs to alice, and says so.
	fake.wellKnownReturns(resolveHandle, resolveDID)

	handle, err := fake.resolver(t).ResolveDIDHandle(context.Background(), resolveOtherDID)

	require.Error(t, err,
		"a DID claiming a handle that names a DIFFERENT DID must be refused: the local "+
			"part is frozen at creation, so accepting this would hand mallory alice's "+
			"fediverse name permanently")
	assert.Empty(t, handle, "no handle may be returned alongside the error")
	assert.ErrorIs(t, err, ErrPermanentEvent,
		"a false claim cannot become true by retrying, so it is dead-lettered exhausted "+
			"rather than redriven")
}

func TestHandleResolver_UnverifiableHandleIsRefused(t *testing.T) {
	fake := newFakeIdentity(t)
	// The document names a handle that serves no well-known at all.
	fake.claimOneWay(resolveDID, resolveHandle)

	handle, err := fake.resolver(t).ResolveDIDHandle(context.Background(), resolveDID)

	require.Error(t, err,
		"an unverifiable handle is refused rather than trusted: one-way is exactly the "+
			"claim an attacker can forge")
	assert.Empty(t, handle)
	assert.Equal(t, 1, fake.WellKnownHits(), "the reverse check was actually attempted")
}

func TestHandleResolver_TransientFailuresStayRedrivable(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*fakeIdentity)
		why   string
	}{
		{
			name:  "directory 503",
			setup: func(f *fakeIdentity) { f.plcFails(resolveDID, http.StatusServiceUnavailable) },
			why:   "a directory outage says nothing about the DID; asking again later is the fix",
		},
		{
			name:  "directory 500",
			setup: func(f *fakeIdentity) { f.plcFails(resolveDID, http.StatusInternalServerError) },
			why:   "same",
		},
		{
			name: "directory 404",
			setup: func(f *fakeIdentity) {
				// No claim registered at all: the DID is simply not there yet.
				// PLC propagation lags account creation, and this event only
				// exists because the repo committed, so the DID does exist.
			},
			why: "a DID that just committed but is not in the directory yet is a propagation " +
				"lag, not a nonexistent identity",
		},
		{
			name: "well-known 500",
			setup: func(f *fakeIdentity) {
				f.claim(resolveDID, resolveHandle)
				f.wellKnownFails(resolveHandle, http.StatusInternalServerError)
			},
			why: "the handle's server being down is not the handle disowning the DID",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeIdentity(t)
			if tc.name != "directory 404" {
				fake.claim(resolveDID, resolveHandle)
			}
			tc.setup(fake)

			handle, err := fake.resolver(t).ResolveDIDHandle(context.Background(), resolveDID)

			require.Error(t, err)
			assert.Empty(t, handle, "no mint may proceed on an unresolved handle")
			assert.NotErrorIs(t, err, ErrPermanentEvent,
				"%s — a permanent classification would exhaust the redrive budget "+
					"immediately and strand the event with no automatic recovery", tc.why)
		})
	}
}

func TestHandleResolver_MissingHandleIsPermanent(t *testing.T) {
	fake := newFakeIdentity(t)
	// A valid DID document with no alsoKnownAs at all.
	fake.plcServes(resolveDID, `{"@context":["https://www.w3.org/ns/did/v1"],
		"id":"`+resolveDID+`","alsoKnownAs":[],"verificationMethod":[],"service":[]}`)

	handle, err := fake.resolver(t).ResolveDIDHandle(context.Background(), resolveDID)

	require.Error(t, err, "a DID with no handle cannot be given a frozen local part")
	assert.Empty(t, handle)
	assert.ErrorIs(t, err, ErrPermanentEvent,
		"RULED PERMANENT: the document states the DID has no handle, which is an answer "+
			"rather than a failure. See the cycle F report — permanent events are "+
			"excluded from redrive by design, so the recovery path is manual")

	assert.Zero(t, fake.WellKnownHits(),
		"and nothing is fetched from the network on a claim that does not exist")
}

func TestHandleResolver_NonATProtoAlsoKnownAsIsPermanent(t *testing.T) {
	fake := newFakeIdentity(t)
	// alsoKnownAs is a general-purpose field; only at:// entries are handles.
	fake.plcServes(resolveDID, `{"id":"`+resolveDID+`",
		"alsoKnownAs":["https://alice.example/profile","mailto:alice@example.com"]}`)

	handle, err := fake.resolver(t).ResolveDIDHandle(context.Background(), resolveDID)

	require.Error(t, err)
	assert.Empty(t, handle,
		"a non-atproto alsoKnownAs entry is not a handle and must never be read as one")
	assert.ErrorIs(t, err, ErrPermanentEvent)
}

func TestHandleResolver_UnsupportedDIDMethods(t *testing.T) {
	fake := newFakeIdentity(t)
	resolver := fake.resolver(t)

	for _, did := range []string{
		"did:web:alice.coves.social",
		"did:key:zQ3shokFTS3brHcDQrn82RUDfCZESWL1ZdCEJwekUDPQiYBme",
		"not-a-did",
		"",
	} {
		t.Run(did, func(t *testing.T) {
			handle, err := resolver.ResolveDIDHandle(context.Background(), did)
			require.Error(t, err, "only did:plc is resolvable this task")
			assert.Empty(t, handle)
			assert.ErrorIs(t, err, ErrPermanentEvent,
				"an unsupported DID method never becomes supported by retrying")
		})
	}

	assert.Zero(t, fake.PLCHits(),
		"an unsupported method is rejected before any network call — a did:web sent to "+
			"a PLC directory is a wasted request at best")
}

func TestNewHandleResolver_RequiresGuardedEgress(t *testing.T) {
	_, err := NewHandleResolver(ResolverOptions{PLCDirectoryURL: "https://plc.directory"})
	require.Error(t, err,
		"the HTTP client is required: the well-known host comes from a DID document a "+
			"STRANGER controls, so this is the most SSRF-exposed egress in the bridge "+
			"and must not fall back to an unguarded default")
	assert.True(t, errors.IsValidation(err), "want validation error, got %v", err)
	assert.Contains(t, err.Error(), "NewGuardedHTTPClient",
		"the message must name the constructor a caller is supposed to use")

	_, err = NewHandleResolver(ResolverOptions{
		PLCDirectoryURL: "notaurl",
		HTTPClient:      http.DefaultClient,
	})
	require.Error(t, err, "the directory URL must be an absolute http(s) URL")
	assert.True(t, errors.IsValidation(err), "want validation error, got %v", err)
}
