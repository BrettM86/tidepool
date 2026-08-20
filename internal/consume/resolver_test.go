package consume

import (
	"context"
	"net"
	"net/http"
	"strings"
	"testing"
	"unicode/utf8"

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

func TestHandleResolver_MissingHandleIsTransient(t *testing.T) {
	fake := newFakeIdentity(t)
	// A valid DID document with no alsoKnownAs at all.
	fake.plcServes(resolveDID, `{"@context":["https://www.w3.org/ns/did/v1"],
		"id":"`+resolveDID+`","alsoKnownAs":[],"verificationMethod":[],"service":[]}`)

	handle, err := fake.resolver(t).ResolveDIDHandle(context.Background(), resolveDID)

	require.Error(t, err, "a DID with no handle cannot be given a frozen local part")
	assert.Empty(t, handle)
	assert.NotErrorIs(t, err, ErrPermanentEvent,
		"RE-RULED TRANSIENT. The document is a complete answer TODAY, but it is an "+
			"answer about a mutable external world: a user who publishes their handle "+
			"minutes after their first comment must get that comment federated. Under "+
			"our shipped semantics a permanent failure is dead-lettered with its "+
			"budget already spent and the redriver never touches it again, so "+
			"'permanent' here would mean 'lost until a human intervenes' — while "+
			"transient costs ten cheap retries and reaches the same terminal state "+
			"if the handle never appears")

	assert.Zero(t, fake.WellKnownHits(),
		"and nothing is fetched from the network on a claim that does not exist")
	assert.Zero(t, fake.TXTHits(), "nor from DNS")
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

// ---------------------------------------------------------------------------
// FF2 — the DNS half of handle verification
// ---------------------------------------------------------------------------
//
// The atproto convention offers two ways for a handle to claim a DID back:
// a _atproto.{handle} TXT record, and the HTTPS well-known. DNS is tried
// first, which is both the spec's order and the safer one — see the
// contradiction test below.

func TestHandleResolver_DNSAloneVerifiesAndSkipsTheWellKnown(t *testing.T) {
	fake := newFakeIdentity(t)
	// The document claims the handle; the handle claims the DID back over DNS
	// ONLY — it serves no well-known at all, like most self-hosted handles.
	fake.claimOneWay(resolveDID, resolveHandle)
	fake.txtClaims(resolveHandle, resolveDID)

	handle, err := fake.resolver(t).ResolveDIDHandle(context.Background(), resolveDID)
	require.NoError(t, err,
		"a TXT-verified handle must resolve: DNS-only handles are the majority on the "+
			"network, and refusing them would silently drop those users' comments")
	assert.Equal(t, resolveHandle, handle)

	assert.Equal(t, 1, fake.TXTHits())
	assert.Zero(t, fake.WellKnownHits(),
		"DNS answered, so the well-known is never fetched — one round-trip, not two, "+
			"on the path in front of every first-time commenter")
}

func TestHandleResolver_ContradictingTXTIsPermanentAndTheWellKnownCannotOverrideIt(t *testing.T) {
	fake := newFakeIdentity(t)
	// Mallory's document claims alice's handle.
	fake.claimOneWay(resolveOtherDID, resolveHandle)
	// DNS — which alice controls — says the handle is alice's.
	fake.txtClaims(resolveHandle, resolveDID)
	// The well-known says otherwise. An attacker who can serve HTTP for the
	// handle's host, but cannot change its DNS, would win if a contradicting
	// TXT fell through to the well-known.
	fake.wellKnownReturns(resolveHandle, resolveOtherDID)

	handle, err := fake.resolver(t).ResolveDIDHandle(context.Background(), resolveOtherDID)

	require.Error(t, err)
	assert.Empty(t, handle)
	assert.ErrorIs(t, err, ErrPermanentEvent,
		"DNS answered and the answer names a different DID: that is a refusal, not a "+
			"missing record")
	assert.Zero(t, fake.WellKnownHits(),
		"and the well-known must NOT be consulted after a contradicting TXT — falling "+
			"through would let whoever controls the handle's web server overrule the "+
			"DNS its real owner published")
}

func TestHandleResolver_MissingTXTFallsThroughToTheWellKnown(t *testing.T) {
	fake := newFakeIdentity(t)
	fake.claim(resolveDID, resolveHandle) // well-known only; no TXT registered

	handle, err := fake.resolver(t).ResolveDIDHandle(context.Background(), resolveDID)
	require.NoError(t, err)
	assert.Equal(t, resolveHandle, handle)

	assert.Equal(t, 1, fake.TXTHits(), "DNS is asked first")
	assert.Equal(t, 1, fake.WellKnownHits(),
		"NXDOMAIN is 'this handle does not use DNS', not 'this handle disowns the DID', "+
			"so the well-known is the answer")
}

func TestHandleResolver_TXTWithoutAnATProtoRecordFallsThrough(t *testing.T) {
	fake := newFakeIdentity(t)
	fake.claim(resolveDID, resolveHandle)
	// DNS answers, but says nothing about atproto. Almost every domain has
	// TXT records; only the did= one is a claim.
	fake.txtRecords(resolveHandle, "v=spf1 -all", "google-site-verification=abc123")

	handle, err := fake.resolver(t).ResolveDIDHandle(context.Background(), resolveDID)
	require.NoError(t, err)
	assert.Equal(t, resolveHandle, handle)

	assert.Equal(t, 1, fake.TXTHits())
	assert.Equal(t, 1, fake.WellKnownHits(),
		"an unrelated TXT record set is not an atproto answer, so verification "+
			"continues rather than failing")
}

// ---------------------------------------------------------------------------
// Second-opinion C5: a DNS OUTAGE must not turn into a permanent verdict
// ---------------------------------------------------------------------------
//
// Impersonation is permanent ONLY when DNS authoritatively said the handle has
// no record and the well-known then named a different DID. If DNS was
// UNREACHABLE (SERVFAIL, timeout) we never learned what the real owner's DNS
// claims, so a mismatched well-known cannot be trusted as impersonation — an
// attacker who controls the handle's web server but not its DNS would win a
// permanent verdict exactly during a DNS blip. The result must be TRANSIENT so
// the redrive re-checks once DNS recovers.

func TestHandleResolver_DNSOutageWithMismatchedWellKnownIsTransient(t *testing.T) {
	fake := newFakeIdentity(t)
	fake.claimOneWay(resolveOtherDID, resolveHandle) // doc claims the handle
	fake.txtFails(resolveHandle, &net.DNSError{Err: "server misbehaving", Name: "_atproto." + resolveHandle, IsTemporary: true})
	fake.wellKnownReturns(resolveHandle, resolveDID) // well-known names a DIFFERENT DID

	handle, err := fake.resolver(t).ResolveDIDHandle(context.Background(), resolveOtherDID)

	require.Error(t, err)
	assert.Empty(t, handle)
	assert.NotErrorIs(t, err, ErrPermanentEvent,
		"DNS was unreachable, so we never learned the owner's authoritative claim — a "+
			"mismatched well-known during a DNS outage cannot be trusted as "+
			"impersonation, and burning the redrive budget on it would strand a "+
			"legitimate mint whenever DNS blips")
	assert.Positive(t, fake.TXTHits(), "DNS was actually consulted")
}

func TestHandleResolver_NXDOMAINWithMismatchedWellKnownStaysPermanent(t *testing.T) {
	fake := newFakeIdentity(t)
	fake.claimOneWay(resolveOtherDID, resolveHandle)
	// No TXT registered → the fake returns an NXDOMAIN (IsNotFound) error:
	// DNS authoritatively has no record, so the well-known is the answer.
	fake.wellKnownReturns(resolveHandle, resolveDID)

	handle, err := fake.resolver(t).ResolveDIDHandle(context.Background(), resolveOtherDID)

	require.Error(t, err)
	assert.Empty(t, handle)
	assert.ErrorIs(t, err, ErrPermanentEvent,
		"NXDOMAIN is authoritative — the handle publishes no DNS claim — so a "+
			"well-known naming a different DID is a real impersonation and stays "+
			"permanent. Only a DNS OUTAGE downgrades the verdict")
}

func TestHandleResolver_WellKnown404IsTransient(t *testing.T) {
	fake := newFakeIdentity(t)
	fake.claimOneWay(resolveDID, resolveHandle) // no TXT, no well-known body
	fake.wellKnownFails(resolveHandle, http.StatusNotFound)

	handle, err := fake.resolver(t).ResolveDIDHandle(context.Background(), resolveDID)

	require.Error(t, err)
	assert.Empty(t, handle)
	assert.NotErrorIs(t, err, ErrPermanentEvent,
		"a 404 well-known is NOT a disavowal: the handle may be mid-setup, or publish "+
			"its claim only over DNS. Treating it as permanent would strand a user who "+
			"finishes configuring their PDS a minute later")
}

// ---------------------------------------------------------------------------
// The remote claim is EVIDENCE, not a transcript
// ---------------------------------------------------------------------------
//
// verifyWellKnown embeds the body served by https://{handle}/... — a host named
// in a stranger's DID document — into its error. That error is not just read by
// an operator: it becomes a dead letter's last_error, a postgres TEXT column
// that rejects NUL outright. Echoing the body verbatim hands an attacker a
// string that can fail the very write meant to capture the failure. The claim
// must be quoted (control bytes become printable escapes) and length-capped
// before it goes anywhere near an error message.

// poisonWellKnownBody is what a hostile — or merely broken — server can return:
// NULs, invalid UTF-8, and far more bytes than a DID could ever need.
var poisonWellKnownBody = "did:plc:\x00\x00evil\xff\xfe" + strings.Repeat("A", 4096)

func TestHandleResolver_PoisonWellKnownClaimIsQuotedAndBounded(t *testing.T) {
	assertSafeEvidence := func(t *testing.T, err error) {
		t.Helper()
		message := err.Error()
		assert.False(t, strings.ContainsRune(message, 0),
			"the error must carry no NUL: it lands in the last_error TEXT column, and a "+
				"NUL there fails the dead-letter write — the fallback that must never "+
				"itself fail")
		assert.True(t, utf8.ValidString(message),
			"and must be valid UTF-8, so the operator triaging the DLQ can read it")
		assert.LessOrEqual(t, len(message), 512,
			"and must be BOUNDED: the well-known read is capped at 1 KiB, but none of "+
				"that belongs in an error message verbatim — the claim is evidence, not "+
				"a transcript")
		assert.Contains(t, message, resolveHandle,
			"while still naming the handle whose claim disagreed")
	}

	t.Run("transient, DNS unreachable", func(t *testing.T) {
		fake := newFakeIdentity(t)
		fake.claimOneWay(resolveDID, resolveHandle)
		fake.txtFails(resolveHandle, &net.DNSError{
			Err: "server misbehaving", Name: atprotoTXTPrefix + resolveHandle, IsTemporary: true})
		fake.wellKnownReturns(resolveHandle, poisonWellKnownBody)

		_, err := fake.resolver(t).ResolveDIDHandle(context.Background(), resolveDID)

		require.Error(t, err)
		assert.NotErrorIs(t, err, ErrPermanentEvent,
			"DNS was unreachable, so this stays redrivable — which is exactly why the "+
				"error string matters: it will be written to last_error again on every "+
				"redrive pass")
		assertSafeEvidence(t, err)
	})

	t.Run("permanent, NXDOMAIN", func(t *testing.T) {
		fake := newFakeIdentity(t)
		fake.claimOneWay(resolveDID, resolveHandle) // no TXT registered → NXDOMAIN
		fake.wellKnownReturns(resolveHandle, poisonWellKnownBody)

		_, err := fake.resolver(t).ResolveDIDHandle(context.Background(), resolveDID)

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrPermanentEvent)
		assertSafeEvidence(t, err)
	})

	t.Run("permanent, contradicting DNS TXT", func(t *testing.T) {
		// DNS TXT is remote-supplied too: whoever runs the handle's zone writes
		// those bytes, and the resolver echoes them the same way.
		fake := newFakeIdentity(t)
		fake.claimOneWay(resolveDID, resolveHandle)
		fake.txtRecords(resolveHandle, "did="+poisonWellKnownBody)

		_, err := fake.resolver(t).ResolveDIDHandle(context.Background(), resolveDID)

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrPermanentEvent)
		assertSafeEvidence(t, err)
	})
}
