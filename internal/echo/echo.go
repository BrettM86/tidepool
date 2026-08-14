// Package echo classifies inbound ActivityPub ids against the bridge's own
// serving surface: an activity Tidepool SENT to a community comes back as a
// community Announce, and re-materializing it would duplicate content,
// double-count votes, or — worst — read as MODERATION of our own content.
//
// It is a LEAF package on purpose (ap + store + errors only, no
// ingest/materialize import): the ingest dispatcher, the vote aggregator and
// the ancestor walk all have to ask the same question, and none of them may
// import each other.
//
// An id is "ours" IFF the serving surface (internal/personas) would answer 200
// for it — ENTITY EXISTENCE, never path shape alone, because vanity origins
// (decision 10) make authority alone far too broad. The three routes are
// /ap/actor/{did}, /ap/object/{did}/{collection}/{rkey} and
// /ap/activity/{hash}.
//
// FAIL-SAFE DIRECTION: a store failure must surface as a RETRYABLE error, never
// as a silent "ours" (which would drop genuine Lemmy content permanently) and
// never as a silent "not ours" (which would duplicate — recoverable).
package echo

import (
	"context"
	"expvar"
	"fmt"
	"net/url"
	"strings"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// The three route prefixes personas.Service answers for. They are repeated
// here rather than exported from personas because this package must stay a
// leaf: the classifier is the READ-SIDE MIRROR of the serving surface, and the
// prefixes are the wire contract both sides implement.
const (
	objectPathPrefix   = "/ap/object/"
	activityPathPrefix = "/ap/activity/"
	actorPathPrefix    = "/ap/actor/"
)

// Class names WHY an id was recognized as the bridge's own. The classes are
// counted separately because false-positive detection depends on the split: a
// spike in one class is a different bug from a spike in another.
type Class string

const (
	// ClassNone means the id is not ours: genuine remote content.
	ClassNone Class = "none"
	// ClassMappedObject: the id resolves to an object the bridge federated
	// (ap_objects origin=bridge, or an outbound_objects row).
	ClassMappedObject Class = "mapped-object"
	// ClassLocalActivity: the id is an activity the bridge sent
	// (outbound_activities) — the outer or inner activity id of the echo.
	ClassLocalActivity Class = "local-activity"
	// ClassLocalActor: the id is one of our minted personas (ap_actors). An
	// Announce{Like}'s object is the LEMMY subject, so an echoed vote is only
	// ever identifiable by its inner ACTOR.
	ClassLocalActor Class = "local-actor"
	// ClassAncestorShortCircuit: an ancestor walk reached one of our own
	// object URLs and must resolve to the existing at-uri instead of fetching
	// and re-materializing it.
	ClassAncestorShortCircuit Class = "ancestor-short-circuit"
)

// Identity is the RESOLVED ENTITY behind a classified id — not a bool: the
// ancestor short-circuit needs the at-uri, and moderation needs the DID.
type Identity struct {
	// Class is the reason this id is ours; ClassNone means it is not.
	Class Class
	// DID is the repo the id belongs to (the actor's DID, or the object's).
	DID string
	// ATURI is the record the id maps to, when the id names an object.
	ATURI string
}

// Options are the Classifier's dependencies — the four tables that back the
// serving surface's three routes.
type Options struct {
	// Objects backs the ap_objects origin=bridge half of the object route.
	Objects store.APObjects
	// OutboundObjects backs the native (author-owned) half of the object route.
	OutboundObjects store.OutboundObjects
	// Activities backs /ap/activity/{hash}.
	Activities store.OutboundActivities
	// Actors backs /ap/actor/{did}, including the NormalizedOrigin match that
	// keeps a vanity origin's actor distinct from another origin's.
	Actors store.APActors
}

// Classifier answers "is this ours?" against the serving surface.
type Classifier struct {
	objects         store.APObjects
	outboundObjects store.OutboundObjects
	activities      store.OutboundActivities
	actors          store.APActors
}

// New wires a Classifier. All four stores are REQUIRED: this classifier's whole
// contract is that an unanswerable question surfaces as a retryable error, and
// a missing store answers it with a nil dereference on the first inbound
// activity instead.
func New(opts Options) (*Classifier, error) {
	if opts.Objects == nil {
		return nil, errors.NewValidationError("objects", "must not be nil")
	}
	if opts.OutboundObjects == nil {
		return nil, errors.NewValidationError("outbound_objects", "must not be nil")
	}
	if opts.Activities == nil {
		return nil, errors.NewValidationError("activities", "must not be nil")
	}
	if opts.Actors == nil {
		return nil, errors.NewValidationError("actors", "must not be nil")
	}
	return &Classifier{
		objects:         opts.Objects,
		outboundObjects: opts.OutboundObjects,
		activities:      opts.Activities,
		actors:          opts.Actors,
	}, nil
}

// Identify classifies ONE id against the serving surface.
//
// The id must be an absolute URL whose PATH starts with one of the three route
// prefixes; the route then decides by asking the exact question personas asks,
// which is always an entity read. Nothing here compares the id's authority
// against a configured origin: vanity origins (decision 10) make that set
// unenumerable, so the host test is folded into the row itself — the actor's
// stored NormalizedOrigin, or the stored id the object/activity rows are keyed
// by. Anything else is remote content and stays remote.
//
// An id ALONE is all this can weigh, and our ids are public and derivable. When
// the id arrives inside a node the walk can read, Classify corroborates it
// against the activity we actually sent; see identifyActivity.
func (c *Classifier) Identify(ctx context.Context, apID string) (Identity, error) {
	return c.identify(ctx, apID, nil)
}

// identify is Identify with the NODE the id was read from, when there is one.
// node is corroboration material, never the decision: an id that names nothing
// of ours stays not-ours whatever the node claims.
func (c *Classifier) identify(ctx context.Context, apID string, node *ap.Object) (Identity, error) {
	if apID == "" {
		return Identity{Class: ClassNone}, nil
	}
	parsed, err := url.Parse(apID)
	// An id we cannot even parse into an absolute URL is not something this
	// origin ever minted. Refusing it is the recoverable direction.
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return Identity{Class: ClassNone}, nil
	}

	// HasPrefix on the PATH, never on the whole URL: our route sitting
	// somewhere inside a foreign host's path names that host's resource.
	switch {
	case strings.HasPrefix(parsed.Path, objectPathPrefix):
		return c.identifyObject(ctx, apID, strings.TrimPrefix(parsed.Path, objectPathPrefix))
	case strings.HasPrefix(parsed.Path, activityPathPrefix):
		return c.identifyActivity(ctx, apID, strings.TrimPrefix(parsed.Path, activityPathPrefix), node)
	case strings.HasPrefix(parsed.Path, actorPathPrefix):
		return c.identifyActor(ctx, apID, parsed.Scheme, normalizeHost(parsed.Host),
			strings.TrimPrefix(parsed.Path, actorPathPrefix))
	}
	return Identity{Class: ClassNone}, nil
}

// identifyObject mirrors personas.handleObject: rest is did/collection/rkey,
// and the body is served from OUTBOUND_OBJECTS (personas' handleObject reads
// GetByATURI) — so that table is the route's real oracle, and the ap_objects
// read is defence in depth.
//
// Both are consulted because the two rows are written by different halves of
// the bridge and neither implies the other: the enqueuer records a
// bridge-origin ap_objects mapping (outbound's Enqueuer.EnqueueActivity, via
// objectMapping) alongside the outbound row, but a legacy v1 write has only the
// mapping, and a state where
// the outbound row is missing must not make our own object answer "remote".
//
// The ap_objects read is keyed by the requested id, but a row alone is NOT
// enough: genuine Lemmy content the bridge materialized has an ap_objects row
// too (origin=fediverse), and treating that as our own echo would drop a whole
// community's content. Only origin=bridge is ours.
//
// RESIDUAL (accepted, not closed here): like the actor route, this weighs the
// ID and not the node it was read from — our object ids are public, so a peer
// can paint one onto a node wrapping different content and have that node
// dropped. There is no stored per-object payload to corroborate against the way
// identifyActivity has one, and the reachable harm is bounded: the announce
// path runs behind the followed-community gate, so the peer must be a community
// we follow suppressing content it chose to announce, and the bare paths
// authorize separately. Closing it needs the outbound snapshot, which belongs
// with the fetch-binding belt, not here.
//
// outbound_objects is keyed by AT-URI, so a row found under the at-uri this
// path spells is only ours if the row's own ap id is the id we were asked
// about — otherwise any authority could borrow our path shape and have its
// content silently attributed to us.
func (c *Classifier) identifyObject(ctx context.Context, apID, rest string) (Identity, error) {
	parts := strings.SplitN(rest, "/", 3)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return Identity{Class: ClassNone}, nil
	}
	did := parts[0]
	atURI := "at://" + did + "/" + parts[1] + "/" + parts[2]

	mapping, err := c.objects.GetByAPID(ctx, apID)
	switch {
	case err == nil:
		// Soft-deleted mappings are included by GetByAPID on purpose: an echo
		// of something we later deleted is still an echo.
		if mapping.Origin == store.OriginBridge {
			return Identity{
				Class: ClassMappedObject,
				DID:   mapping.DID,
				ATURI: mapping.ATURI,
			}, nil
		}
	case !errors.IsNotFound(err):
		return Identity{Class: ClassNone}, fmt.Errorf("echo: object mapping for %s: %w", apID, err)
	}

	object, err := c.outboundObjects.GetByATURI(ctx, atURI)
	switch {
	case err == nil:
		// Tombstoned rows count: personas answers 410 for them, not 404.
		if object.APObjectID == apID {
			return Identity{Class: ClassMappedObject, DID: did, ATURI: object.ATURI}, nil
		}
	case !errors.IsNotFound(err):
		return Identity{Class: ClassNone}, fmt.Errorf("echo: outbound object for %s: %w", atURI, err)
	}

	return Identity{Class: ClassNone}, nil
}

// identifyActivity mirrors personas.handleActivity: the stored id carries its
// own origin, so looking the FULL requested id up is both the existence test
// and the authority test in one read.
//
// When the id arrives inside a node, that node is CORROBORATED against the
// activity we stored. Our activity ids are public and derivable, so the id
// alone lets a peer paint one onto a node wrapping somebody else's content and
// have it dropped as "our echo" — suppression turned into a deletion primitive.
// The stored payload is kept for byte-stable replay, which makes it exactly the
// witness this needs.
//
// The rule is CONTRADICTION DISQUALIFIES, ABSENCE DOES NOT, on IDENTIFYING
// FIELDS only — never on bytes. A community re-serializes what it announces:
// keys are reordered, addressing is added, names are re-rendered. Comparing
// bytes (or any field a re-render may touch) would refuse every genuine echo
// and re-materialize all of them, which is the bug this package exists to
// prevent. So a bare IRI, carrying neither type nor object, still classifies.
func (c *Classifier) identifyActivity(ctx context.Context, apID, hash string, node *ap.Object) (Identity, error) {
	if hash == "" || strings.Contains(hash, "/") {
		return Identity{Class: ClassNone}, nil
	}
	activity, err := c.activities.Get(ctx, apID)
	if err != nil {
		if errors.IsNotFound(err) {
			return Identity{Class: ClassNone}, nil
		}
		return Identity{Class: ClassNone}, fmt.Errorf("echo: outbound activity for %s: %w", apID, err)
	}
	corroborated, err := c.corroborates(ctx, node, activity)
	if err != nil || !corroborated {
		return Identity{Class: ClassNone}, err
	}
	return Identity{Class: ClassLocalActivity, DID: activity.ActorDID}, nil
}

// corroborates reports whether node can be the activity we stored under that
// id. A nil node (a bare id probe) corroborates trivially — there is nothing to
// contradict.
func (c *Classifier) corroborates(ctx context.Context, node *ap.Object, activity *store.OutboundActivity) (bool, error) {
	if node == nil {
		return true, nil
	}
	// The VERB. We recorded what we sent; a node calling itself something else
	// is not it — and honouring the id alone would let a peer suppress any
	// Delete it likes by wearing the id of a Create we sent.
	if node.Type != "" && activity.Kind != "" && !strings.EqualFold(node.Type, activity.Kind) {
		return false, nil
	}

	// The CARRIED OBJECT. Same id, same verb, different content is the forgery
	// the verb check cannot see. The comparison is against the object our
	// stored payload names, and it disqualifies only when the substituted
	// object is NOT ours: swapping one of our objects for another cannot lose
	// anybody's content, while swapping in a remote human's note is precisely
	// the content loss dressed as echo suppression.
	carried := refObjectID(node)
	if carried == "" {
		return true, nil
	}
	stored := refObjectID(parsePayload(activity.Payload))
	if stored == "" || stored == carried {
		return true, nil
	}
	identity, err := c.identify(ctx, carried, nil)
	if err != nil {
		return false, err
	}
	return identity.Class != ClassNone, nil
}

// parsePayload reads a stored activity back into its object form. A payload
// that will not parse yields no corroboration material rather than a verdict —
// absence, like any other missing field.
func parsePayload(payload []byte) *ap.Object {
	if len(payload) == 0 {
		return nil
	}
	parsed, err := ap.ParseObject(payload)
	if err != nil {
		return nil
	}
	return parsed
}

// refObjectID is the id of the object an activity carries, or "" when it names
// none (including a nil activity).
func refObjectID(activity *ap.Object) string {
	if activity == nil || activity.Object == nil {
		return ""
	}
	return activity.Object.ID
}

// identifyActor mirrors personas.lookupResource: the DID is global but the
// actor is not, so a row alone is not the answer — the actor must have been
// minted on the very origin the id names. The comparison is against the stored
// NormalizedOrigin in full, which makes it label-boundary-safe by
// construction: neither a host we are a suffix of nor one we are a prefix of
// can equal it.
//
// The SCHEME is compared too, against the one the actor was actually minted
// under. The other two routes match a stored id exactly and so cannot be
// spoofed by re-spelling it; without this, http://our.host/ap/actor/{did} — an
// id we never mint and never serve — would classify as ours.
//
// RESIDUAL (accepted, as for identifyObject): an actor id is public, and a node
// can name one of our personas as its actor without our persona having done
// anything. An actor has no per-activity payload to corroborate against, and a
// forged actor claim is already a signature failure at the inbox for the
// top-level activity; what remains is an inner node inside an announce from a
// community we follow.
func (c *Classifier) identifyActor(ctx context.Context, apID, scheme, host, rest string) (Identity, error) {
	if rest == "" || strings.Contains(rest, "/") {
		return Identity{Class: ClassNone}, nil
	}
	actor, err := c.actors.GetByDID(ctx, rest)
	if err != nil {
		if errors.IsNotFound(err) {
			return Identity{Class: ClassNone}, nil
		}
		return Identity{Class: ClassNone}, fmt.Errorf("echo: actor for %s: %w", apID, err)
	}
	if actor.NormalizedOrigin != host || !strings.EqualFold(scheme, schemeOf(actor.ActorID)) {
		return Identity{Class: ClassNone}, nil
	}
	return Identity{Class: ClassLocalActor, DID: actor.DID}, nil
}

// schemeOf is the URL scheme a stored actor id was minted under, or "" if the
// stored id will not parse — which fails closed, since no requested id's scheme
// can equal "".
func schemeOf(actorID string) string {
	parsed, err := url.Parse(actorID)
	if err != nil {
		return ""
	}
	return parsed.Scheme
}

// normalizeHost reduces a URL authority to the authority it names — lowercase,
// no trailing dot, no default port — so it can be compared with the
// normalized_origin an actor was minted under. It is the read-side twin of
// personas' own normalizeHost; a NON-default port still carries meaning,
// because the dev origin runs on :8091 and that is a different origin.
func normalizeHost(host string) string {
	normalized := strings.ToLower(strings.TrimSpace(host))
	for _, defaultPort := range []string{":443", ":80"} {
		if trimmed, found := strings.CutSuffix(normalized, defaultPort); found {
			normalized = trimmed
			break
		}
	}
	return strings.TrimSuffix(normalized, ".")
}

// MaxDepth bounds the envelope walk, counted in NODES: the envelope handed to
// Classify is level 1, its object is level 2, and so on — a walk that reaches
// level MaxDepth still classifies it, and nothing below that level is read.
//
// The deepest shape that carries meaning is
// Announce{Undo{Like{subject}}} — four levels — so this admits every real
// envelope with slack to spare, while refusing to follow a nesting chain a
// hostile (or merely broken) peer can make arbitrarily long. A bound is not an
// optimization here: without one, a self-referential object graph walks until
// the stack dies.
const MaxDepth = 8

// Classify walks the nested chain of an inbound envelope — outer activity id,
// inner activity id, inner actor, inner object id — and returns the first
// identity that is ours.
//
// The walk is OUTER→INNER because the outermost proof is the most specific
// one: an Announce carrying an activity WE sent is a redelivery of that
// activity, and filing it under the object it happens to carry would report
// the wrong class — and each class is counted separately precisely so a spike
// in one is legible as its own bug.
//
// At each node it asks about the node's own id, then its actor, then descends
// into its object — but ONLY through the verbs whose `object` is a PAYLOAD.
//
// This is the difference between a payload and a TARGET, and getting it wrong
// loses content in the most common interaction the product has. For Announce,
// Create, Update and Undo, `object` is what the activity carries: keep asking.
// For Like, Dislike, Delete, Flag, Block, Remove — and for anything else,
// because an unknown verb must fail toward NOT condemning an envelope — it is
// what somebody else's activity is being done TO. A Lemmy human's Like on a
// post we federated out, or a Lemmy moderator's Delete of a native postv2, has
// OUR id as its target; descending would answer "ours" for the whole envelope
// and discard every vote and every moderation action on native content, with no
// error and a counter that says the bridge is working.
//
// Nothing is lost by stopping there: the echoes these guards exist for are
// identified by the target-bearing node ITSELF — an echoed Delete by its own
// activity id, an echoed vote by its actor — never by what sits below it.
//
// A VOTE node inverts the first two: the voter is asked about before the
// activity id. Lemmy 0.19 reconstructs the inner vote of an Announce{Undo{Like}}
// with a FRESHLY GENERATED id (and types it "Like" even when the live vote is a
// Dislike), so our own id survives the round trip only sometimes — and a class
// that flipped between local-activity and local-actor depending on whether the
// peer happened to preserve an id would make the per-class split unreadable on
// exactly the path where double-counting hides. The voter is the stable handle
// (LOOP_STATE's decision-16 amendment: voter identity supersedes the id probe).
//
// Everything is answered from stored state — the Classifier holds no fetcher on
// purpose. A bare IRI must be recognized as ours BEFORE anything dereferences
// it: dialing our own origin to learn whether something is ours is a round trip
// for an answer we already hold, and a trust inversion besides.
//
// Only ap_actors — the personas the bridge SPEAKS AS — makes an actor ours. A
// mirrored fediverse user (bridged_actors) holds a DID we minted and is still a
// genuine remote participant; reading one as ours would silently zero their
// community's tallies and drop its content.
func (c *Classifier) Classify(ctx context.Context, envelope *ap.Object) (Identity, error) {
	// Bounded in NODES, not in recursion: the depth counter is what makes a
	// cyclic graph terminate, and refusing to classify past the bound is the
	// recoverable direction (a duplicate, never a drop).
	node := envelope
	for depth := 1; node != nil && depth <= MaxDepth; depth++ {
		// The node travels with its OWN id — that id claims to name this very
		// activity, so the node is what corroborates it. The actor id names a
		// different entity entirely and carries no such claim.
		probes := [2]probe{{id: node.ID, node: node}, {id: actorIDOf(node)}}
		if isVote(node) {
			probes[0], probes[1] = probes[1], probes[0]
		}
		for _, p := range probes {
			identity, err := c.identify(ctx, p.id, p.node)
			if err != nil || identity.Class != ClassNone {
				return identity, err
			}
		}
		if !carriesPayload(node) {
			// node.Object is this activity's TARGET, not its payload: whatever
			// it names belongs to whoever the activity is being done TO.
			return Identity{Class: ClassNone}, nil
		}
		node = node.Object
	}
	return Identity{Class: ClassNone}, nil
}

// carriesPayload reports whether node.Object is the thing the activity CARRIES
// (walk on) rather than the thing it acts UPON (stop). It is an allowlist, so
// an unrecognized verb stops — the recoverable direction, since descending into
// a target can drop genuine content permanently while declining to descend can
// at worst let a duplicate through.
func carriesPayload(node *ap.Object) bool {
	switch node.Type {
	case ap.TypeAnnounce, ap.TypeCreate, ap.TypeUpdate, ap.TypeUndo:
		return true
	default:
		return false
	}
}

// probe is one question the walk asks: an id, plus the node that id was read
// FROM when the node is a claim about the id itself.
type probe struct {
	id   string
	node *ap.Object
}

// isVote reports whether the node is a vote, whose VOTER identifies it.
func isVote(node *ap.Object) bool {
	return node.Type == ap.TypeLike || node.Type == ap.TypeDislike
}

// actorIDOf is the node's actor id, or "" when it names none — which Identify
// refuses like any other empty id, so an absent actor needs no special case.
func actorIDOf(node *ap.Object) string {
	if node.Actor == nil {
		return ""
	}
	return node.Actor.ID
}

// dropMetricPrefix names the per-class drop counters. The "tidepool_" prefix
// is load-bearing: ingest.scopedMetrics serves ONLY that prefix, so a counter
// named without it is published to expvar and then filtered out of the admin
// endpoint — which looks exactly like a drop site that never fires.
const dropMetricPrefix = "tidepool_echo_drops_"

// dropMetricName is the metric a class is counted under. The class STRING is
// the wire and log spelling and stays hyphenated; the metric suffix is
// underscored, because a hyphen is not legal in a Prometheus metric name and
// every other counter in this repo is underscored.
func dropMetricName(class Class) string {
	return dropMetricPrefix + strings.ReplaceAll(string(class), "-", "_")
}

// dropCounters is one expvar per class that MEANS "ours". ClassNone is absent
// on purpose: it is the answer "this is genuine remote content", and remote
// content is processed, never dropped — a counter for it could only ever
// report zero while implying something was lost.
var dropCounters = func() map[Class]*expvar.Int {
	counters := make(map[Class]*expvar.Int)
	for _, class := range []Class{
		ClassMappedObject,
		ClassLocalActivity,
		ClassLocalActor,
		ClassAncestorShortCircuit,
	} {
		counters[class] = expvar.NewInt(dropMetricName(class))
	}
	return counters
}()

// CountDrop records that one activity was dropped as an echo of class. Drop
// sites call it so a suppressed activity is visible: an echo nobody counts is
// indistinguishable from genuine content silently lost.
func CountDrop(class Class) {
	if counter, ok := dropCounters[class]; ok {
		counter.Add(1)
	}
}

// Drops reports how many activities have been dropped as echoes of class. It
// reads the per-class expvar counter the drop sites increment.
func Drops(class Class) int64 {
	if counter, ok := dropCounters[class]; ok {
		return counter.Value()
	}
	return 0
}
