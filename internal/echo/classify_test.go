package echo

import (
	"context"
	"database/sql"
	stderrors "errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// Classify is the ENVELOPE contract. What comes back from a community is never
// a bare id: it is Announce{Create{Page}}, Announce{Like}, Announce{Undo{Like}},
// or an Announce carrying nothing but an IRI — and the identity that proves the
// activity is ours can sit at any level of that nesting.
//
// The walk runs OUTER→INNER and returns the FIRST identity that is ours (outer
// activity id, inner activity id, inner actor, inner object id). The order is
// load-bearing, not cosmetic: each class has its own counter, and a drop
// attributed to the wrong class hides exactly the false positive the split
// exists to expose.

// Genuine Lemmy actors: content the bridge MIRRORS into atproto. A mirrored
// Lemmy person has a bridged_actors row and a real DID, and is still NOT ours
// for echo purposes — misreading one as ours would drop every vote and comment
// that user ever sends.
const (
	ecLemmyPersonAPID = "https://lemmy.world/u/LeftLeaningFreedomFighters"
	ecLemmyPersonDID  = "did:plc:mirroredlemmyuser01"
	ecLemmyGroupAPID  = "https://lemmy.world/c/technology"
	ecLemmyNoteAPID   = "https://lemmy.world/comment/27485395"
	ecLemmyActivityID = "https://lemmy.world/activities/create/6a91b0d9-c1e5-45d6-be8b-ce248306867e"
	ecLemmyAnnounceID = "https://lemmy.world/activities/announce/create/6a91b0d9"
)

// envelope parses wire JSON the way the inbox does, so these tests exercise
// exactly the shapes ap.ParseObject produces — including a bare-IRI object,
// which arrives as an Object carrying nothing but an id.
func envelope(t *testing.T, body string) *ap.Object {
	t.Helper()
	parsed, err := ap.ParseObject([]byte(body))
	require.NoError(t, err, "fixture must parse as an AP object")
	return parsed
}

// seedMirroredLemmyUser gives the Lemmy person a real bridged atproto identity,
// so "not ours" is asserted against a database where that user genuinely EXISTS.
func seedMirroredLemmyUser(t *testing.T, database *sql.DB) {
	t.Helper()
	_, err := store.NewBridgedActors(database).UpsertActor(context.Background(), store.BridgedActor{
		APActorID:    ecLemmyPersonAPID,
		ActorType:    store.ActorTypePerson,
		DID:          ecLemmyPersonDID,
		Handle:       "leftleaningfreedomfighters.lemmy-world.bridge.test",
		ConsentState: store.ConsentStateOK,
	})
	require.NoError(t, err, "seed the mirrored Lemmy person")
}

// TestClassifyWalksTheEnvelopeOuterIn pins the walk itself: where the identity
// sits, which one wins, and that a nested shape is resolved from stored state
// alone (the Classifier holds no fetcher — an echo must be recognized without
// dereferencing anything).
func TestClassifyWalksTheEnvelopeOuterIn(t *testing.T) {
	classifier, _, database := newWorld(t)
	seedMirroredLemmyUser(t, database)
	ctx := context.Background()

	cases := []struct {
		name      string
		body      string
		wantClass Class
		wantDID   string
		wantATURI string
		why       string
	}{
		{
			name: "W1 depth: Announce{Undo{Like}} decided by the innermost actor",
			body: `{
				"id": "` + ecLemmyAnnounceID + `/undo",
				"type": "Announce",
				"actor": "` + ecLemmyGroupAPID + `",
				"object": {
					"id": "https://lemmy.world/activities/undo/aaa",
					"type": "Undo",
					"object": {
						"id": "https://lemmy.world/activities/like/bbb",
						"type": "Like",
						"actor": "` + ecActorID + `",
						"object": "` + ecLemmyAPID + `"
					}
				}
			}`,
			wantClass: ClassLocalActor,
			wantDID:   ecAuthorDID,
			why: "handleUndo hands the INNER object to RetractVote, so the identity that " +
				"decides an echoed Undo lives two levels down — a walk that stops at the " +
				"announce's object retracts our own vote off our own echo",
		},
		{
			name: "W2 precedence: inner activity id AND inner object id are both ours",
			body: `{
				"id": "` + ecLemmyAnnounceID + `",
				"type": "Announce",
				"actor": "` + ecLemmyGroupAPID + `",
				"object": {
					"id": "` + ecActivityID + `",
					"type": "Create",
					"actor": "` + ecActorID + `",
					"object": {
						"id": "` + ecMappedAPID + `",
						"type": "Page",
						"attributedTo": "` + ecActorID + `"
					}
				}
			}`,
			wantClass: ClassLocalActivity,
			wantDID:   ecAuthorDID,
			why: "outer-in: the inner Create's id is an activity we sent, and it is reached " +
				"before the Page it carries — classifying this as mapped-object would file " +
				"a delivered-activity echo under the object counter",
		},
		{
			name: "W3 inner actor is decisive for an echoed vote",
			body: `{
				"id": "` + ecLemmyAnnounceID + `/like",
				"type": "Announce",
				"actor": "` + ecLemmyGroupAPID + `",
				"object": {
					"id": "https://lemmy.world/activities/like/ccc",
					"type": "Like",
					"actor": "` + ecActorID + `",
					"object": "` + ecLemmyAPID + `"
				}
			}`,
			wantClass: ClassLocalActor,
			wantDID:   ecAuthorDID,
			why: "an Announce{Like}'s object is the LEMMY subject and its activity id is " +
				"Lemmy's; only the inner ACTOR says the vote is ours, and without it the " +
				"aggregator counts our own upvote twice",
		},
		{
			name: "W4 bare-IRI object naming an activity we sent",
			body: `{
				"id": "` + ecLemmyAnnounceID + `/bare",
				"type": "Announce",
				"actor": "` + ecLemmyGroupAPID + `",
				"object": "` + ecActivityID + `"
			}`,
			wantClass: ClassLocalActivity,
			wantDID:   ecAuthorDID,
			why: "a bare IRI must be classified from stored state BEFORE anything " +
				"dereferences it: fetching our own URL to find out whether it is ours is " +
				"a round trip that answers a question we already hold the answer to",
		},
		{
			name: "W4 bare-IRI object naming one of our objects",
			body: `{
				"id": "` + ecLemmyAnnounceID + `/bare-object",
				"type": "Announce",
				"actor": "` + ecLemmyGroupAPID + `",
				"object": "` + ecNativeAPID + `"
			}`,
			wantClass: ClassMappedObject,
			wantATURI: ecNativeATURI,
			why:       "the same for an object IRI: an author-owned post's URL resolves from outbound_objects",
		},
		{
			name: "W2 outer activity id is ours (a redelivery of our own activity)",
			body: `{
				"id": "` + ecActivityID + `",
				"type": "Create",
				"actor": "` + ecActorID + `",
				"object": {
					"id": "` + ecMappedAPID + `",
					"type": "Page"
				}
			}`,
			wantClass: ClassLocalActivity,
			wantDID:   ecAuthorDID,
			why:       "the OUTER activity id is the first thing the walk asks about",
		},
		{
			name:      "W1 the walk reaches the deepest admissible level",
			body:      nestedEnvelope(MaxDepth, ecActivityID),
			wantClass: ClassLocalActivity,
			wantDID:   ecAuthorDID,
			why: "MaxDepth is the bound, not the reach: everything within it is still walked, " +
				"or a legal-but-deep envelope would echo straight through",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			identity, err := classifier.Classify(ctx, envelope(t, tc.body))
			require.NoError(t, err)
			assert.Equal(t, tc.wantClass, identity.Class, tc.why)
			if tc.wantDID != "" {
				assert.Equal(t, tc.wantDID, identity.DID,
					"the resolved identity carries the entity the drop site acts on")
			}
			if tc.wantATURI != "" {
				assert.Equal(t, tc.wantATURI, identity.ATURI,
					"an object identity carries the record behind the id")
			}
		})
	}
}

// TestClassifyPassesGenuineRemoteContentThrough is the data-loss guard for the
// whole inbound pipeline. Every envelope here is real community traffic: if any
// of them classifies as ours it is dropped, silently, forever.
func TestClassifyPassesGenuineRemoteContentThrough(t *testing.T) {
	classifier, _, database := newWorld(t)
	seedMirroredLemmyUser(t, database)
	ctx := context.Background()

	cases := []struct {
		name string
		body string
		why  string
	}{
		{
			name: "W5 a Lemmy user's post announced by the community",
			body: `{
				"id": "` + ecLemmyAnnounceID + `",
				"type": "Announce",
				"actor": "` + ecLemmyGroupAPID + `",
				"object": {
					"id": "` + ecLemmyActivityID + `",
					"type": "Create",
					"actor": "` + ecLemmyPersonAPID + `",
					"object": {
						"id": "` + ecLemmyAPID + `",
						"type": "Page",
						"attributedTo": "` + ecLemmyPersonAPID + `"
					}
				}
			}`,
			why: "the post is MATERIALIZED (it has an ap_objects row, origin=fediverse) and " +
				"its author is MIRRORED (a bridged_actors row with a real DID) — neither " +
				"makes it ours, and reading either as ours drops the community's content",
		},
		{
			name: "W5 a mirrored Lemmy user's vote",
			body: `{
				"id": "` + ecLemmyAnnounceID + `/like",
				"type": "Announce",
				"actor": "` + ecLemmyGroupAPID + `",
				"object": {
					"id": "https://lemmy.world/activities/like/ddd",
					"type": "Like",
					"actor": "` + ecLemmyPersonAPID + `",
					"object": "` + ecLemmyAPID + `"
				}
			}`,
			why: "a bridged actor is a fediverse user we MIRROR, never a persona we SPEAK AS: " +
				"classifying their vote as our echo silently zeroes the community's tallies",
		},
		{
			name: "W5 a Lemmy reply to one of our posts",
			body: `{
				"id": "` + ecLemmyAnnounceID + `/reply",
				"type": "Announce",
				"actor": "` + ecLemmyGroupAPID + `",
				"object": {
					"id": "https://lemmy.world/activities/create/eee",
					"type": "Create",
					"actor": "` + ecLemmyPersonAPID + `",
					"object": {
						"id": "` + ecLemmyNoteAPID + `",
						"type": "Note",
						"attributedTo": "` + ecLemmyPersonAPID + `",
						"inReplyTo": "` + ecMappedAPID + `"
					}
				}
			}`,
			why: "inReplyTo naming OUR object is the whole point of bridging — the reply is " +
				"the Lemmy user's, and the ancestor link is resolved later by the ancestor " +
				"short-circuit, not suppressed here",
		},
		{
			name: "W5 an Undo of a Lemmy user's own vote",
			body: `{
				"id": "` + ecLemmyAnnounceID + `/undo-remote",
				"type": "Announce",
				"actor": "` + ecLemmyGroupAPID + `",
				"object": {
					"id": "https://lemmy.world/activities/undo/fff",
					"type": "Undo",
					"actor": "` + ecLemmyPersonAPID + `",
					"object": {
						"id": "https://lemmy.world/activities/like/ggg",
						"type": "Like",
						"actor": "` + ecLemmyPersonAPID + `",
						"object": "` + ecLemmyAPID + `"
					}
				}
			}`,
			why: "depth must not make the walk trigger-happy: three levels of genuine remote " +
				"traffic is still genuine remote traffic",
		},
		{
			name: "W5 a vanity-origin actor id that is not this origin's",
			body: `{
				"id": "` + ecLemmyAnnounceID + `/vanity",
				"type": "Announce",
				"actor": "` + ecLemmyGroupAPID + `",
				"object": {
					"id": "https://lemmy.world/activities/like/hhh",
					"type": "Like",
					"actor": "` + ecOrigin + `/ap/actor/` + ecVanityDID + `",
					"object": "` + ecLemmyAPID + `"
				}
			}`,
			why: "the per-id rules still apply inside the walk: a DID minted on another " +
				"origin does not become ours by being nested",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			identity, err := classifier.Classify(ctx, envelope(t, tc.body))
			require.NoError(t, err, "genuine remote traffic is not an error")
			assert.Equal(t, ClassNone, identity.Class, tc.why)
			assert.Empty(t, identity.DID, "a non-identity carries no entity")
			assert.Empty(t, identity.ATURI, "a non-identity carries no record")
		})
	}
}

// TestClassifyToleratesMalformedEnvelopes pins the degenerate shapes. An
// envelope the bridge cannot make sense of is not ours — but it must also not
// take the process down, and a nesting chain must not be walkable without end.
func TestClassifyToleratesMalformedEnvelopes(t *testing.T) {
	classifier, _, _ := newWorld(t)
	ctx := context.Background()

	t.Run("W6 nil envelope", func(t *testing.T) {
		identity, err := classifier.Classify(ctx, nil)
		require.NoError(t, err, "a nil envelope is nothing to classify, not a failure")
		assert.Equal(t, ClassNone, identity.Class)
	})

	t.Run("W6 envelope with no object", func(t *testing.T) {
		identity, err := classifier.Classify(ctx, envelope(t, `{
			"id": "`+ecLemmyAnnounceID+`/empty",
			"type": "Announce",
			"actor": "`+ecLemmyGroupAPID+`"
		}`))
		require.NoError(t, err)
		assert.Equal(t, ClassNone, identity.Class)
	})

	t.Run("W6 ids are empty throughout", func(t *testing.T) {
		identity, err := classifier.Classify(ctx, envelope(t, `{
			"type": "Announce",
			"object": {"type": "Create", "object": {"type": "Page"}}
		}`))
		require.NoError(t, err)
		assert.Equal(t, ClassNone, identity.Class,
			"an empty id must never match a route — every level of it")
	})

	t.Run("W6 nesting deeper than the bound", func(t *testing.T) {
		// The ONLY ours-identity sits one level past MaxDepth, so a bounded
		// walk cannot reach it. Refusing to classify it is the recoverable
		// direction (a duplicate, not a drop) — and it is what keeps an
		// attacker from choosing how much work an inbound activity costs.
		identity, err := classifier.Classify(ctx,
			envelope(t, nestedEnvelope(MaxDepth+1, ecActivityID)))
		require.NoError(t, err)
		assert.Equal(t, ClassNone, identity.Class,
			"the walk stops at MaxDepth (%d) levels", MaxDepth)
	})

	t.Run("W6 self-referential object graph", func(t *testing.T) {
		// ap.ParseObject cannot build this from the wire, but a caller can hand
		// it to us: the bound must be structural, not a property of the parser.
		cyclic := &ap.Object{ID: ecLemmyAnnounceID + "/cycle", Type: "Announce"}
		cyclic.Object = cyclic
		identity, err := classifier.Classify(ctx, cyclic)
		require.NoError(t, err)
		assert.Equal(t, ClassNone, identity.Class,
			"a cycle must terminate at the bound instead of walking until the stack dies")
	})
}

// TestClassifyPropagatesStoreFailuresMidWalk extends E1's fail-safe direction to
// the walk: a database hiccup at ANY level must surface as a retryable error.
// Swallowing it into ClassNone re-materializes our own content; reporting ours
// on a failed read drops genuine content permanently.
func TestClassifyPropagatesStoreFailuresMidWalk(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name string
		body string
		fail func(opts *Options, boom error)
		why  string
	}{
		{
			name: "activity store fails on the INNER activity id",
			body: `{
				"id": "` + ecLemmyAnnounceID + `/probe",
				"type": "Announce",
				"actor": "` + ecLemmyGroupAPID + `",
				"object": {
					"id": "` + ecOrigin + `/ap/activity/` + strings.Repeat("a", 64) + `",
					"type": "Create",
					"actor": "` + ecLemmyPersonAPID + `",
					"object": {"id": "` + ecLemmyAPID + `", "type": "Page"}
				}
			}`,
			fail: func(opts *Options, boom error) {
				opts.Activities = failingActivities{OutboundActivities: opts.Activities, err: boom}
			},
			why: "the outer announce id is Lemmy's, so the failure is reached one level in",
		},
		{
			name: "object store fails on the INNER object id",
			body: `{
				"id": "` + ecLemmyAnnounceID + `/probe-object",
				"type": "Announce",
				"actor": "` + ecLemmyGroupAPID + `",
				"object": {
					"id": "` + ecLemmyActivityID + `",
					"type": "Create",
					"actor": "` + ecLemmyPersonAPID + `",
					"object": {
						"id": "` + ecOrigin + `/ap/object/` + ecAuthorDID + `/social.coves.community.postv2/3lzprobe0001",
						"type": "Page"
					}
				}
			}`,
			fail: func(opts *Options, boom error) {
				opts.Objects = failingObjects{APObjects: opts.Objects, err: boom}
			},
			why: "two levels in, on the last identity the walk asks about",
		},
		{
			name: "actor store fails on the INNER actor",
			body: `{
				"id": "` + ecLemmyAnnounceID + `/probe-actor",
				"type": "Announce",
				"actor": "` + ecLemmyGroupAPID + `",
				"object": {
					"id": "https://lemmy.world/activities/like/iii",
					"type": "Like",
					"actor": "` + ecOrigin + `/ap/actor/did:plc:neverminted00000",
					"object": "` + ecLemmyAPID + `"
				}
			}`,
			fail: func(opts *Options, boom error) {
				opts.Actors = failingActors{APActors: opts.Actors, err: boom}
			},
			why: "the vote path's decisive lookup — failing OPEN here double-counts votes",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, opts, _ := newWorld(t)
			boom := stderrors.New("connection reset by peer")
			tc.fail(&opts, boom)
			classifier, err := New(opts)
			require.NoError(t, err)

			identity, err := classifier.Classify(ctx, envelope(t, tc.body))
			require.Error(t, err, "a store failure mid-walk must not be swallowed: %s", tc.why)
			assert.ErrorIs(t, err, boom, "the transient cause must be preserved, not flattened")
			assert.False(t, errors.IsNotFound(err),
				"a failed lookup is not a miss: NotFound would read as 'not ours' upstream")
			assert.False(t, errors.IsValidation(err),
				"a transient store failure must stay RETRYABLE — a validation error poisons "+
					"the event, and the activity is gone")
			assert.Equal(t, ClassNone, identity.Class,
				"an errored classification must not also claim the id is ours")
		})
	}
}

// nestedEnvelope builds an Announce chain `levels` deep whose ONLY ours-identity
// is the id at the deepest level; every level above it is genuine Lemmy traffic.
func nestedEnvelope(levels int, deepestID string) string {
	body := `{"id": "` + deepestID + `", "type": "Create"}`
	for level := levels - 1; level >= 1; level-- {
		body = `{
			"id": "https://lemmy.world/activities/announce/nested-` + string(rune('a'+level%26)) + `",
			"type": "Announce",
			"actor": "` + ecLemmyGroupAPID + `",
			"object": ` + body + `
		}`
	}
	return body
}
