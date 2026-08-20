package echo

import (
	"context"
	"database/sql"
	stderrors "errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/personas"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// The serving surface these pins mirror. internal/personas answers 200 for
// exactly three routes, and Identify must ask each route's exact question:
//
//	/ap/actor/{did}                       ap_actors row AND NormalizedOrigin == host
//	/ap/object/{did}/{collection}/{rkey}  ap_objects (origin=bridge) OR outbound_objects
//	/ap/activity/{hash}                   outbound_activities
//
// An id is ours IFF that surface would answer for it: ENTITY EXISTENCE, never
// path shape. Shape-only matching would classify every well-formed URL on a
// host that merely looks like ours as our own content and DROP it — and dropped
// Lemmy content is gone for good, where a duplicate is recoverable.
const (
	ecOrigin = "https://coves.social"
	ecHost   = "coves.social"

	// The native author whose persona coves.social serves.
	ecAuthorDID   = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	ecAuthorLocal = "alice"
	ecActorID     = ecOrigin + "/ap/actor/" + ecAuthorDID

	// A persona minted under a VANITY origin (decision 10). Its DID exists in
	// ap_actors, but it does not live on coves.social.
	ecVanityDID    = "did:plc:7iza6de2dwap2sbkpav7c6c6"
	ecVanityOrigin = "https://vanity.example"
	ecVanityHost   = "vanity.example"

	// A post the bridge federated into a Lemmy community, mapped in ap_objects
	// with origin=bridge (the v1 write-side half).
	ecMappedRKey  = "3lzmapped1111"
	ecMappedATURI = "at://" + ecAuthorDID + "/social.coves.community.postv2/" + ecMappedRKey
	ecMappedAPID  = ecOrigin + "/ap/object/" + ecAuthorDID +
		"/social.coves.community.postv2/" + ecMappedRKey

	// A native author-owned post, tracked only in outbound_objects (the
	// acceptance-engine half: nothing writes an ap_objects row for it).
	ecNativeRKey  = "3lznative2222"
	ecNativeATURI = "at://" + ecAuthorDID + "/social.coves.community.postv2/" + ecNativeRKey
	ecNativeAPID  = ecOrigin + "/ap/object/" + ecAuthorDID +
		"/social.coves.community.postv2/" + ecNativeRKey

	// A native post we later DELETED: its outbound row is tombstoned (personas
	// answers 410, not 404 — the entity still exists).
	ecGoneRKey  = "3lzgone333333"
	ecGoneATURI = "at://" + ecAuthorDID + "/social.coves.community.postv2/" + ecGoneRKey
	ecGoneAPID  = ecOrigin + "/ap/object/" + ecAuthorDID +
		"/social.coves.community.postv2/" + ecGoneRKey

	// A bridged post whose ap_objects mapping was SOFT-DELETED.
	ecSoftRKey  = "3lzsoft444444"
	ecSoftATURI = "at://" + ecAuthorDID + "/social.coves.community.comment/" + ecSoftRKey
	ecSoftAPID  = ecOrigin + "/ap/object/" + ecAuthorDID +
		"/social.coves.community.comment/" + ecSoftRKey

	// An activity the bridge minted and delivered.
	ecActivityID = ecOrigin + "/ap/activity/" +
		"9f2c1e6b6d5b4a3f8c7d0e1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e"

	// Genuine Lemmy content the bridge materialized: it has an ap_objects row,
	// origin=fediverse. Classifying THIS as ours is the data-loss bug.
	ecLemmyAPID  = "https://lemmy.world/post/49131386"
	ecLemmyRKey  = "3lzlemmy555555"
	ecLemmyATURI = "at://" + ecCommunityDID + "/social.coves.community.post/" + ecLemmyRKey

	ecCommunityDID = "did:plc:44ybard66vv44zksje25o7dz"
	ecCID          = "bafyreib2rxk3rybk3aobmv5cjuql3bm2twh4jo5uxgf5kpqrsqxi3jgxte"
)

// newWorld seeds the serving surface's backing tables and returns a Classifier
// reading them, plus the stores so a test can swap one for a failing double.
func newWorld(t *testing.T) (*Classifier, Options, *sql.DB) {
	t.Helper()
	ctx := context.Background()
	database := testutil.DB(t)
	testutil.Truncate(t, database,
		"ap_objects", "ap_actors", "bridged_actors",
		"outbound_deliveries", "outbound_activities", "outbound_objects")

	actors := store.NewAPActors(database)
	objects := store.NewAPObjects(database)
	outboundObjects := store.NewOutboundObjects(database)
	activities := store.NewOutboundActivities(database)

	_, err := actors.Create(ctx, store.APActor{
		DID:              ecAuthorDID,
		Kind:             store.ActorTypePerson,
		ActorID:          ecActorID,
		NormalizedOrigin: ecHost,
		LocalPart:        ecAuthorLocal,
		RSAKeySealed:     []byte{0x01, 0x02, 0x03},
		RSAKeyVersion:    1,
		PublicKeyPEM:     "-----BEGIN PUBLIC KEY-----\nMIIB\n-----END PUBLIC KEY-----\n",
	})
	require.NoError(t, err, "seed the coves.social persona")

	_, err = actors.Create(ctx, store.APActor{
		DID:              ecVanityDID,
		Kind:             store.ActorTypePerson,
		ActorID:          ecVanityOrigin + "/ap/actor/" + ecVanityDID,
		NormalizedOrigin: ecVanityHost,
		LocalPart:        ecAuthorLocal,
		RSAKeySealed:     []byte{0x04, 0x05, 0x06},
		RSAKeyVersion:    1,
		PublicKeyPEM:     "-----BEGIN PUBLIC KEY-----\nMIIB\n-----END PUBLIC KEY-----\n",
	})
	require.NoError(t, err, "seed the vanity-origin persona")

	_, err = objects.PutMapping(ctx, store.APObjectMapping{
		APID:           ecMappedAPID,
		APType:         "Page",
		OriginInstance: ecHost,
		Origin:         store.OriginBridge,
		DID:            ecAuthorDID,
		Collection:     "social.coves.community.postv2",
		RKey:           ecMappedRKey,
		CID:            ecCID,
	})
	require.NoError(t, err, "seed the bridge-origin object mapping")

	_, err = objects.PutMapping(ctx, store.APObjectMapping{
		APID:           ecSoftAPID,
		APType:         "Note",
		OriginInstance: ecHost,
		Origin:         store.OriginBridge,
		DID:            ecAuthorDID,
		Collection:     "social.coves.community.comment",
		RKey:           ecSoftRKey,
		CID:            ecCID,
	})
	require.NoError(t, err, "seed the soon-to-be-soft-deleted mapping")
	require.NoError(t, objects.SoftDelete(ctx, ecSoftAPID))

	_, err = objects.PutMapping(ctx, store.APObjectMapping{
		APID:           ecLemmyAPID,
		APType:         "Page",
		OriginInstance: "lemmy.world",
		Origin:         store.OriginFediverse,
		DID:            ecCommunityDID,
		Collection:     "social.coves.community.post",
		RKey:           ecLemmyRKey,
		CID:            ecCID,
	})
	require.NoError(t, err, "seed genuine Lemmy content")

	for _, native := range []struct{ atURI, apID string }{
		{ecNativeATURI, ecNativeAPID},
		{ecGoneATURI, ecGoneAPID},
	} {
		_, err = outboundObjects.Upsert(ctx, store.OutboundObject{
			ATURI:              native.atURI,
			APObjectID:         native.apID,
			LastCID:            ecCID,
			LastRev:            "3lzrev00000001",
			CommunityDID:       ecCommunityDID,
			CommunityAPID:      "https://lemmy.world/c/technology",
			TranslatedSnapshot: []byte(`{"type":"Page","name":"native post"}`),
		})
		require.NoError(t, err, "seed native outbound object %s", native.atURI)
	}
	_, err = outboundObjects.Tombstone(ctx, ecGoneATURI)
	require.NoError(t, err, "tombstone the deleted native post")

	// The payload is the FULL canonical activity, the way the enqueuer stores it
	// (byte-stable for replay). It is what a node claiming this id can be
	// corroborated against: the id alone is public and derivable.
	_, err = activities.Insert(ctx, store.OutboundActivity{
		ActivityID: ecActivityID,
		ActorDID:   ecAuthorDID,
		Kind:       "Create",
		Payload: []byte(`{"@context":"https://www.w3.org/ns/activitystreams",` +
			`"id":"` + ecActivityID + `","type":"Create","actor":"` + ecActorID + `",` +
			`"to":["https://www.w3.org/ns/activitystreams#Public"],` +
			`"cc":["https://lemmy.world/c/technology"],"audience":"https://lemmy.world/c/technology",` +
			`"object":{"id":"` + ecNativeAPID + `","type":"Page","attributedTo":"` + ecActorID + `"}}`),
	})
	require.NoError(t, err, "seed the delivered activity")

	opts := Options{
		Objects:         objects,
		OutboundObjects: outboundObjects,
		Activities:      activities,
		Actors:          actors,
	}
	classifier, err := New(opts)
	require.NoError(t, err)
	return classifier, opts, database
}

// TestIdentifyResolvesOurServingSurface pins the POSITIVE half: every id
// personas would answer for resolves to its class AND carries the entity
// behind it. The identity — not a bool — is what the later sub-tasks act on:
// the ancestor short-circuit needs the at-uri, moderation needs the DID.
func TestIdentifyResolvesOurServingSurface(t *testing.T) {
	classifier, _, _ := newWorld(t)
	ctx := context.Background()

	cases := []struct {
		name      string
		apID      string
		wantClass Class
		wantDID   string
		wantATURI string
		why       string
	}{
		{
			name:      "P1 actor route on its own origin",
			apID:      ecActorID,
			wantClass: ClassLocalActor,
			wantDID:   ecAuthorDID,
			why: "an Announce{Like}'s object is the LEMMY subject; the inner ACTOR is the " +
				"only handle on an echoed vote, so the actor route must resolve to a DID",
		},
		{
			name:      "P2 object route backed by a bridge-origin ap_objects row",
			apID:      ecMappedAPID,
			wantClass: ClassMappedObject,
			wantDID:   ecAuthorDID,
			wantATURI: ecMappedATURI,
			why:       "the ap_objects origin=bridge lookup is the belt (v1's write-side mapping)",
		},
		{
			name:      "P3 object route backed only by outbound_objects",
			apID:      ecNativeAPID,
			wantClass: ClassMappedObject,
			wantDID:   ecAuthorDID,
			wantATURI: ecNativeATURI,
			why: "an author-owned post has no ap_objects row at all — outbound_objects is " +
				"the half personas actually serves the body from",
		},
		{
			name:      "P4 activity route",
			apID:      ecActivityID,
			wantClass: ClassLocalActivity,
			wantDID:   ecAuthorDID,
			why: "outbound_activities is the id a peer re-fetches an activity by; the row " +
				"carries the persona that sent it",
		},
		{
			name:      "P5 tombstoned native object is still ours",
			apID:      ecGoneAPID,
			wantClass: ClassMappedObject,
			wantATURI: ecGoneATURI,
			why: "personas answers 410 for it, not 404: an echo of something we later " +
				"deleted is still an echo, and treating it as remote content would " +
				"re-materialize the record we just removed",
		},
		{
			name:      "P6 a persona minted under a VANITY origin, on that origin",
			apID:      ecVanityOrigin + "/ap/actor/" + ecVanityDID,
			wantClass: ClassLocalActor,
			wantDID:   ecVanityDID,
			why: "decision 10 stated POSITIVELY: the origin set is unenumerable, so the host " +
				"test is the actor's OWN stored NormalizedOrigin. An implementation comparing " +
				"against one configured origin passes every negative case and fails this one — " +
				"and it would take every vanity-origin user's traffic with it",
		},
		{
			name:      "P5 soft-deleted bridge mapping is still ours",
			apID:      ecSoftAPID,
			wantClass: ClassMappedObject,
			wantDID:   ecAuthorDID,
			wantATURI: ecSoftATURI,
			why:       "GetByAPID includes soft-deleted rows precisely so this stays recognizable",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			identity, err := classifier.Identify(ctx, tc.apID)
			require.NoError(t, err, "a resolvable id is not an error")
			assert.Equal(t, tc.wantClass, identity.Class, tc.why)
			if tc.wantDID != "" {
				assert.Equal(t, tc.wantDID, identity.DID,
					"the resolved identity carries the repo behind the id")
			}
			if tc.wantATURI != "" {
				assert.Equal(t, tc.wantATURI, identity.ATURI,
					"the resolved identity carries the record behind the id")
			}
		})
	}
}

// TestIdentifyRefusesWhatWeDoNotServe pins the NEGATIVE half — the data-loss
// guards. Every case here is genuine remote content (or nothing at all):
// classifying any of them as ours drops real Lemmy content PERMANENTLY, where
// the opposite mistake only duplicates.
func TestIdentifyRefusesWhatWeDoNotServe(t *testing.T) {
	classifier, _, _ := newWorld(t)
	ctx := context.Background()

	cases := []struct {
		name string
		apID string
		why  string
	}{
		{
			name: "N1 empty id",
			apID: "",
			why:  "the empty string must never match a route — a missing id is not our id",
		},
		{
			name: "N2 well-shaped object route with no row behind it",
			apID: ecOrigin + "/ap/object/" + ecAuthorDID + "/social.coves.community.postv2/3lznotmine11",
			why: "existence over shape: personas 404s this, so an inbound object wearing " +
				"our URL shape is not ours",
		},
		{
			name: "N2 well-shaped activity route with no row behind it",
			apID: ecOrigin + "/ap/activity/0000000000000000000000000000000000000000000000000000000000000000",
			why:  "an activity id we never minted is 404 at the surface and not ours here",
		},
		{
			name: "N2 actor route for a DID we never minted",
			apID: ecOrigin + "/ap/actor/did:plc:strangerstrangerxx",
			why:  "no ap_actors row means no persona, whatever the path looks like",
		},
		{
			name: "N3 actor DID that exists but was minted on another origin",
			apID: ecOrigin + "/ap/actor/" + ecVanityDID,
			why: "the DID is global, the actor is not (decision 10): personas requires " +
				"NormalizedOrigin == the routed host, and so must this",
		},
		{
			name: "N4 our origin appearing in the PATH of a foreign host",
			apID: "https://evil.example/coves.social/ap/object/" + ecAuthorDID +
				"/social.coves.community.postv2/" + ecMappedRKey,
			why: "the route prefix must be anchored at the start of the path, not found " +
				"anywhere in the URL",
		},
		{
			name: "N4 a host our host is a suffix of",
			apID: "https://notcoves.social/ap/object/" + ecAuthorDID +
				"/social.coves.community.postv2/" + ecMappedRKey,
			why: "host comparison is label-boundary-safe, never substring: notcoves.social " +
				"is a different authority that can host the same path",
		},
		{
			name: "N4 a foreign host carrying an at-uri we DO serve (the spoof that reaches the check)",
			apID: "https://notcoves.social/ap/object/" + ecAuthorDID +
				"/social.coves.community.postv2/" + ecNativeRKey,
			why: "outbound_objects is keyed by AT-URI, so this path finds a REAL row — the " +
				"only thing standing between it and a mapped-object verdict is comparing the " +
				"row's own ap id against the id we were asked about. Without that comparison " +
				"any authority can wear our path shape and have its content dropped as our echo",
		},
		{
			name: "N4 a foreign host carrying an activity hash we minted",
			apID: "https://notcoves.social/ap/activity/" +
				"9f2c1e6b6d5b4a3f8c7d0e1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e",
			why: "the activity route is keyed by the FULL id for this reason: a lookup keyed " +
				"on the hash alone would answer for whoever hosts it",
		},
		{
			name: "N4 a host that merely starts with ours",
			apID: "https://coves.social.evil.example/ap/actor/" + ecAuthorDID,
			why:  "same boundary rule in the other direction: ours as a prefix label is not ours",
		},
		{
			name: "N5 a non-/ap/ path on our own authority",
			apID: ecOrigin + "/u/" + ecAuthorLocal,
			why:  "our authority is not the test; the ROUTE plus the row is",
		},
		{
			name: "N5 a truncated object route on our own authority",
			apID: ecOrigin + "/ap/object/" + ecAuthorDID + "/social.coves.community.postv2",
			why:  "did/collection/rkey is three parts or nothing (personas 404s a short one)",
		},
		{
			name: "N6 genuine Lemmy content we materialized",
			apID: ecLemmyAPID,
			why: "it HAS an ap_objects row — origin=fediverse. Reading the row without the " +
				"origin test would classify every bridged Lemmy post as our own echo and " +
				"drop the whole community's content",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			identity, err := classifier.Identify(ctx, tc.apID)
			require.NoError(t, err, "an id that is simply not ours is not an error")
			assert.Equal(t, ClassNone, identity.Class, tc.why)
			assert.Empty(t, identity.DID, "a non-identity carries no entity")
			assert.Empty(t, identity.ATURI, "a non-identity carries no record")
		})
	}
}

// TestNormalizeHostMatchesPersonas pins the hand-copy to its original
// DIRECTLY, not just through classification outcomes. echo's normalizeHost is a
// duplicate of personas.NormalizeHost, and the two are the read side and the
// write side of one question — "is this authority ours?" — asked about rows
// whose normalized_origin was frozen at mint. If they ever disagree, echo
// either fails to recognize our own actor ids (duplicating content) or claims
// ids that are not ours (dropping genuine content), and no test in either
// package would otherwise notice.
func TestNormalizeHostMatchesPersonas(t *testing.T) {
	for _, host := range []string{
		"", "coves.social", "COVES.SOCIAL", "coves.social.", " coves.social ",
		"coves.social:443", "coves.social:80", "coves.social:8091",
		"COVES.SOCIAL:443", "coves.social:443.", "localhost", "localhost:8091",
		"[::1]", "[::1]:443", "[::1]:8091", "127.0.0.1:80", "tdpl.io:8443",
	} {
		assert.Equal(t, personas.NormalizeHost(host), normalizeHost(host),
			"echo and personas must reduce %q to the same authority", host)
	}
}

// TestIdentifyNormalizesTheHostTheWayServingDoes pins echo.normalizeHost, the
// read-side twin of personas' own. It is a hand-copy, and a hand-copy that
// drifts is invisible: replacing its body with `return host` leaves every other
// test in this package green while every actor id a peer spells slightly
// differently stops being recognized as ours — and an unrecognized echo is a
// duplicate, an over-normalized one is dropped genuine content.
func TestIdentifyNormalizesTheHostTheWayServingDoes(t *testing.T) {
	classifier, _, _ := newWorld(t)
	ctx := context.Background()

	cases := []struct {
		name      string
		apID      string
		wantClass Class
		why       string
	}{
		{
			name:      "the default port is the same origin",
			apID:      "https://coves.social:443/ap/actor/" + ecAuthorDID,
			wantClass: ClassLocalActor,
			why:       "https://host:443 and https://host name the same authority",
		},
		{
			name:      "the host is case-insensitive",
			apID:      "https://COVES.SOCIAL/ap/actor/" + ecAuthorDID,
			wantClass: ClassLocalActor,
			why:       "DNS is case-insensitive and normalized_origin is stored lowercased",
		},
		{
			name:      "a fully-qualified trailing dot is the same origin",
			apID:      "https://coves.social./ap/actor/" + ecAuthorDID,
			wantClass: ClassLocalActor,
			why:       "the root label is implicit; a peer that spells it out names the same host",
		},
		{
			name:      "a NON-default port is a DIFFERENT origin",
			apID:      "https://coves.social:8091/ap/actor/" + ecAuthorDID,
			wantClass: ClassNone,
			why: "the dev origin runs on :8091 and is genuinely another origin — stripping " +
				"ports wholesale would let it answer for production's actors",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			identity, err := classifier.Identify(ctx, tc.apID)
			require.NoError(t, err)
			assert.Equal(t, tc.wantClass, identity.Class, tc.why)
		})
	}
}

// TestIdentifyPropagatesStoreFailures pins the FAIL-SAFE DIRECTION. A database
// hiccup must surface as a RETRYABLE error so the event is redelivered:
//
//   - swallowed into ClassNone → the echo re-materializes (a duplicate, and
//     possibly a self-moderation loop);
//   - reported as ours → genuine Lemmy content is dropped, permanently.
//
// Neither is acceptable, and only one of them is even recoverable. This is the
// shape ingest/handler.go's existing echo check already has: NotFound means
// "not ours", anything else propagates.
func TestIdentifyPropagatesStoreFailures(t *testing.T) {
	ctx := context.Background()
	// Every probed id is one NO store has a row for, so the lookup must reach
	// the failing store whatever order the routes are consulted in.
	missingObject := ecOrigin + "/ap/object/" + ecAuthorDID + "/social.coves.community.postv2/3lzmissing11"
	missingActivity := ecOrigin + "/ap/activity/" +
		"1111111111111111111111111111111111111111111111111111111111111111"
	missingActor := ecOrigin + "/ap/actor/did:plc:neverminted00000"

	cases := []struct {
		name string
		apID string
		fail func(opts *Options, boom error)
	}{
		{
			name: "ap_objects unavailable",
			apID: missingObject,
			fail: func(opts *Options, boom error) {
				opts.Objects = failingObjects{APObjects: opts.Objects, err: boom}
			},
		},
		{
			name: "outbound_objects unavailable",
			apID: missingObject,
			fail: func(opts *Options, boom error) {
				opts.OutboundObjects = failingOutboundObjects{OutboundObjects: opts.OutboundObjects, err: boom}
			},
		},
		{
			name: "outbound_activities unavailable",
			apID: missingActivity,
			fail: func(opts *Options, boom error) {
				opts.Activities = failingActivities{OutboundActivities: opts.Activities, err: boom}
			},
		},
		{
			name: "ap_actors unavailable",
			apID: missingActor,
			fail: func(opts *Options, boom error) {
				opts.Actors = failingActors{APActors: opts.Actors, err: boom}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, opts, _ := newWorld(t)
			boom := stderrors.New("connection reset by peer")
			tc.fail(&opts, boom)
			classifier, err := New(opts)
			require.NoError(t, err)

			identity, err := classifier.Identify(ctx, tc.apID)
			require.Error(t, err,
				"a store failure must NOT be swallowed into a classification: "+
					"silently 'not ours' duplicates, silently 'ours' loses data")
			assert.ErrorIs(t, err, boom, "the transient cause must be preserved, not flattened")
			assert.False(t, errors.IsNotFound(err),
				"a failed lookup is not a miss: NotFound would read as 'not ours' upstream")
			assert.False(t, errors.IsValidation(err),
				"a transient store failure must stay RETRYABLE — a validation error poisons "+
					"the event and drops the activity for good")
			assert.Equal(t, ClassNone, identity.Class,
				"an errored classification must not also claim the id is ours")
		})
	}
}

// ---------------------------------------------------------------------------
// Failing store doubles: each embeds the real repository and breaks exactly the
// one read the route depends on.
// ---------------------------------------------------------------------------

type failingObjects struct {
	store.APObjects
	err error
}

func (f failingObjects) GetByAPID(context.Context, string) (*store.APObjectMapping, error) {
	return nil, f.err
}

type failingOutboundObjects struct {
	store.OutboundObjects
	err error
}

func (f failingOutboundObjects) GetByATURI(context.Context, string) (*store.OutboundObject, error) {
	return nil, f.err
}

type failingActivities struct {
	store.OutboundActivities
	err error
}

func (f failingActivities) Get(context.Context, string) (*store.OutboundActivity, error) {
	return nil, f.err
}

type failingActors struct {
	store.APActors
	err error
}

func (f failingActors) GetByDID(context.Context, string) (*store.APActor, error) {
	return nil, f.err
}
