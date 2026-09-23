package outbound

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/html"

	"tidepool/internal/ap"
	"tidepool/internal/apobject"
	"tidepool/internal/consume"
	"tidepool/internal/identity"
	"tidepool/internal/personas"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// The world this acceptance test builds.
const (
	outUserOrigin = "https://coves.social"
	outCovesHost  = "coves.social"

	// The bridged community, hosted on a fake Lemmy. Its Group actor document
	// advertises endpoints.sharedInbox pointing back at itself.
	outCommunityAPID = "https://lemmy.world/c/technology"
	outLemmyHost     = "lemmy.world"
	outCommunityName = "technology"
	outCommunityDID  = "did:plc:44ybard66vv44zksje25o7dz"
	outSharedInbox   = "https://lemmy.world/c/technology/inbox"

	// The parent post the comment replies to — a BRIDGE-origin object (Tidepool
	// federated it), so it causally gates its child until it is accepted. Its
	// inReplyTo is our coves.social object URL.
	outRootAuthorDID = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	outRootRKey      = "3lzroot2222aa"
	outRootCID       = "bafyreib2rxk3rybk3aobmv5cjuql3bm2twh4jo5uxgf5kpqrsqxi3jgxte"
	outRootATURI     = "at://" + outRootAuthorDID + "/social.coves.community.postv2/" + outRootRKey
	outRootAPID      = outUserOrigin + "/ap/object/" + outRootAuthorDID + "/social.coves.community.postv2/" + outRootRKey

	// The commenter — a native Coves user minted lazily on first interaction.
	outCommenterDID    = "did:plc:7iza6de2dwap2sbkpav7c6c6"
	outCommenterHandle = "alice.coves.social"
	outCommentRKey     = "3lzcmnt3333bb"
	outCommentCID      = "bafyreievgu2ty7qbiaaom5zhmkznsnajuzideek3lo7e65dwqlrvrxnmo4"
	outCommentRev      = "3lzcmntrev001"
	outCommentATURI    = "at://" + outCommenterDID + "/social.coves.community.comment/" + outCommentRKey
	outCommentAPID     = outUserOrigin + "/ap/object/" + outCommenterDID + "/social.coves.community.comment/" + outCommentRKey
)

// outKEK seals minted actors' AP RSA keys (32 bytes, AES-256).
var outKEK = []byte("0123456789abcdef0123456789abcdef")

// TestOutboundDeliversASignedNativeComment is the OUTER acceptance test for
// task 15.
//
// GIVEN a bridged community (its Group doc served by a fake Lemmy that verifies
// the HTTP signatures it receives), a native commenter minted as a coves.social
// Person actor, and a CommentIntent enqueued THROUGH THE REAL ENQUEUER inside a
// transaction, WHEN the delivery worker runs, THEN:
//
//  1. the tx contract holds: an enqueue whose tx rolls back writes NO
//     outbound_activities / outbound_deliveries rows; a committed one writes
//     exactly one of each, pending;
//  2. the fake Lemmy receives EXACTLY ONE Create{Note} whose HTTP signature
//     verifies against the actor document coves.social serves;
//  3. the inner Note is addressed the way Lemmy 0.19.20 demands: to ⊇ Public,
//     cc ⊇ the community, audience = the community, attributedTo = the actor id
//     as a SINGLE STRING, inReplyTo = the parent AP id, content AND source both
//     carried;
//  4. the outbound_deliveries row reaches 'delivered';
//  5. a REPLAY (redeliver) is deduped by activity id — the fake Lemmy's
//     duplicate-activity response leaves the delivery 'delivered', never
//     poisoned.
//
// No network: the only hosts the AP clients may dial (coves.social and
// lemmy.world) are rewritten onto the two httptest listeners.
//
// SEAMS this outer test drives through test doubles (production shapes GREEN
// must supply / wire): outbound.SignerProvider (a sealed-key unseal, mirroring
// personas.actorSigner), outbound.InboxResolver (reads endpoints.sharedInbox
// off the Group doc), outbound.ActivitySender (ap.Client.SendActivityAs), and
// the real outbound.Enqueuer / outbound.Translator / outbound.Worker.
func TestOutboundDeliversASignedNativeComment(t *testing.T) {
	conn := outboundAcceptanceDB(t)
	ctx := context.Background()

	// --- The user origin: personas.Service mints + serves the actor. ---
	custodian, err := identity.NewCustodian(outKEK)
	require.NoError(t, err, "build custodian")
	svc, err := personas.New(personas.Options{DB: conn, Custodian: custodian, UserOrigin: outUserOrigin})
	require.NoError(t, err, "build personas service")

	actor, err := svc.CreateActorForDID(ctx, outCommenterDID, outCommenterHandle)
	require.NoError(t, err, "mint the commenter's actor")
	actorID := actor.ActorID
	require.Equal(t, outUserOrigin+"/ap/actor/"+outCommenterDID, actorID)

	personasServer := httptest.NewServer(svc)
	t.Cleanup(personasServer.Close)

	// --- The bridged community's home: a fake Lemmy. ---
	lemmy := &fakeLemmy{
		host:         outLemmyHost,
		communityAPI: outCommunityAPID,
		sharedInbox:  outSharedInbox,
		seen:         map[string]bool{},
	}
	lemmyServer := httptest.NewServer(lemmy)
	t.Cleanup(lemmyServer.Close)

	// --- One AP client, both hosts rewritten onto the httptest listeners. ---
	routes := map[string]string{
		outCovesHost: personasServer.Listener.Addr().String(),
		outLemmyHost: lemmyServer.Listener.Addr().String(),
	}
	client := ap.NewClient(ap.ClientOptions{
		HTTPClient: &http.Client{Transport: hostRewrite{routes: routes}},
	})
	// The fake Lemmy verifies inbound signatures by fetching the signer's actor
	// document off coves.social through this same client.
	lemmy.verifier = ap.NewVerifier(client)

	// --- The parent post: a bridge-origin object, already accepted so its child
	//     is causally eligible. ---
	seedAcceptedParent(t, conn)

	// --- Wire the real enqueuer + worker. ---
	enqueuer, err := NewEnqueuer(EnqueuerOptions{
		DB:         conn,
		Translator: NewTranslator(outUserOrigin),
		Inboxes:    inboxResolver{client: client},
		Actors:     store.NewAPActors(conn),
		UserOrigin: outUserOrigin,
	})
	require.NoError(t, err, "build enqueuer")

	worker, err := NewWorker(WorkerOptions{
		DB:      conn,
		Actors:  store.NewAPActors(conn),
		Signers: sealedSigners{actors: store.NewAPActors(conn), custodian: custodian},
		Inboxes: inboxResolver{client: client},
		Sender:  client,
		Lease:   time.Minute,
	})
	require.NoError(t, err, "build worker")

	intent := consume.CommentIntent{
		Op:            "create",
		ATURI:         outCommentATURI,
		ID:            consume.ActivityID(outUserOrigin, outCommentATURI, "create", 0),
		CommunityAPID: outCommunityAPID,
		ParentAPID:    outRootAPID,
		Snapshot:      commentSnapshot(t),
	}

	// -------------------------------------------------------------------
	// 1a. Tx contract: a rolled-back enqueue leaves NOTHING behind.
	// -------------------------------------------------------------------
	tx, err := conn.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, enqueuer.EnqueueActivity(ctx, tx, outCommenterDID, outCommunityAPID, outRootATURI, intent),
		"enqueue must write its activity + delivery on the given tx")
	require.NoError(t, tx.Rollback())

	assert.Zero(t, countRows(t, conn, "outbound_activities"),
		"an enqueue whose gate tx rolls back must leave NO activity row — a replay could not "+
			"reproduce it under an advanced gate")
	assert.Zero(t, countRows(t, conn, "outbound_deliveries"),
		"...and no delivery row either")

	// -------------------------------------------------------------------
	// 1b. Tx contract: a committed enqueue writes exactly one of each.
	// -------------------------------------------------------------------
	tx, err = conn.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, enqueuer.EnqueueActivity(ctx, tx, outCommenterDID, outCommunityAPID, outRootATURI, intent))
	require.NoError(t, tx.Commit())

	require.Equal(t, 1, countRows(t, conn, "outbound_activities"),
		"a committed comment enqueue writes exactly one canonical activity")
	require.Equal(t, 1, countRows(t, conn, "outbound_deliveries"),
		"...and exactly one per-inbox delivery (v2 targets are single-community)")

	activities := store.NewOutboundActivities(conn)
	storedActivity, err := activities.Get(ctx, intent.ID)
	require.NoError(t, err, "the activity is keyed by the deterministic id the intent carries")
	require.NotNil(t, storedActivity)
	assert.Equal(t, outCommenterDID, storedActivity.ActorDID, "the signing persona rides the activity")

	deliveries := store.NewOutboundDeliveries(conn)
	pending, err := deliveries.Get(ctx, intent.ID, outSharedInbox)
	require.NoError(t, err,
		"the delivery is addressed to the community's sharedInbox, resolved from its Group doc")
	require.NotNil(t, pending)
	assert.Equal(t, store.DeliveryStatePending, pending.State)
	assert.Equal(t, outCommunityAPID, pending.OrderingKey,
		"the ordering key is the community AP id — the per-community serial line")

	// -------------------------------------------------------------------
	// 2-4. Run the worker: one signed Create{Note} is delivered.
	// -------------------------------------------------------------------
	drainWorker(t, ctx, worker)

	require.Equal(t, 1, lemmy.postCount(),
		"exactly one activity must be POSTed to the community inbox, got %d", lemmy.postCount())

	rec := lemmy.lastActivity(t)
	assert.NoError(t, rec.verifyErr,
		"the delivery's HTTP signature must verify against the actor document coves.social serves")
	assert.Equal(t, actorID, rec.verifiedActorID,
		"Verify attributes the signature to the minted persona")
	assert.Equal(t, "Create", rec.body["type"], "a comment create federates as a Create activity")

	note := asMap(t, rec.body["object"], "Create.object")
	assert.Equal(t, "Note", note["type"], "a comment renders as a Note")

	to := asStringSet(t, note["to"], "Note.to")
	assert.Contains(t, to, ap.PublicAudience,
		"Lemmy REQUIRES a Note's `to` to include as:Public (verify_is_public rejects otherwise)")

	cc := asStringSet(t, note["cc"], "Note.cc")
	assert.Contains(t, cc, outCommunityAPID, "the community is cc'd (the announce_create_note shape)")

	assert.Equal(t, outCommunityAPID, note["audience"],
		"audience = the community AP id, or Lemmy fetches every to/cc URL hunting for the community")

	attributedTo, isString := note["attributedTo"].(string)
	assert.True(t, isString,
		"attributedTo MUST be a single string — an array parses as the Peertube variant")
	assert.Equal(t, actorID, attributedTo, "attributedTo is the persona's actor id")

	assert.Equal(t, outRootAPID, note["inReplyTo"],
		"inReplyTo is the parent's coves.social object URL (a bridge-origin parent)")

	assert.NotEmpty(t, note["content"], "the HTML content is carried")
	source := asMap(t, note["source"], "Note.source")
	assert.NotEmpty(t, source["content"], "the markdown source is carried alongside the HTML")
	assert.Equal(t, "text/markdown", source["mediaType"],
		"Lemmy uses source verbatim when present (content/source duality)")

	delivered, err := deliveries.Get(ctx, intent.ID, outSharedInbox)
	require.NoError(t, err)
	require.NotNil(t, delivered)
	assert.Equal(t, store.DeliveryStateDelivered, delivered.State,
		"a delivered activity's row reaches the delivered state")

	// -------------------------------------------------------------------
	// 5. Replay: a redelivery of the same activity id is deduped, not poisoned.
	// -------------------------------------------------------------------
	resetDeliveryToPending(t, conn, intent.ID, outSharedInbox)
	drainWorker(t, ctx, worker)

	assert.Equal(t, 2, lemmy.postCount(),
		"the redelivery POSTs the same activity again (dedupe is the peer's job, keyed on our id)")

	redelivered, err := deliveries.Get(ctx, intent.ID, outSharedInbox)
	require.NoError(t, err)
	require.NotNil(t, redelivered)
	assert.Equal(t, store.DeliveryStateDelivered, redelivered.State,
		"Lemmy's duplicate-activity response must classify as DELIVERED, never poisoned — a crash "+
			"between deliver and mark is expected and safe under at-least-once")
}

func TestOutboundFederatesNativeCovesFacetsAsSafeLemmyMarkdown(t *testing.T) {
	const plaintext = "Literal *stars* [brackets] # hash > marker and café\n\n" +
		"Roadmap\n\n" +
		"quoted > marker\n\n" +
		"Make bold visit docs and run a*b.\n\n" +
		"secret ending"
	const expectedMarkdown = "Literal \\*stars\\* \\[brackets\\] \\# hash \\> marker and café\n\n" +
		"## Roadmap\n\n" +
		"> quoted \\> marker\n\n" +
		"Make **bold** visit [docs](https://example.com/docs) and run `a*b`.\n\n" +
		"::: spoiler Ending\nsecret ending\n:::"

	facet := func(text string, feature map[string]any) map[string]any {
		t.Helper()
		require.Equal(t, 1, strings.Count(plaintext, text), "facet text must be unique in the fixture")
		start := strings.Index(plaintext, text)
		require.NotEqual(t, -1, start)
		return map[string]any{
			"index":    map[string]any{"byteStart": start, "byteEnd": start + len(text)},
			"features": []any{feature},
		}
	}

	headingStart := strings.Index(plaintext, "Roadmap")
	require.Greater(t, headingStart, len([]rune(plaintext[:headingStart])),
		"a multibyte character must precede a facet so rune offsets cannot satisfy the contract")

	snapshot, err := json.Marshal(map[string]any{
		"atUri":      outCommentATURI,
		"cid":        outCommentCID,
		"rev":        outCommentRev,
		"collection": "social.coves.community.comment",
		"record": map[string]any{
			"$type":   "social.coves.community.comment",
			"content": plaintext,
			"facets": []any{
				facet("Roadmap", map[string]any{
					"$type": "social.coves.richtext.facet#heading", "level": 2,
				}),
				facet("quoted > marker", map[string]any{
					"$type": "social.coves.richtext.facet#blockquote", "level": 1,
				}),
				facet("bold", map[string]any{
					"$type": "social.coves.richtext.facet#bold",
				}),
				facet("docs", map[string]any{
					"$type": "social.coves.richtext.facet#link", "uri": "https://example.com/docs",
				}),
				facet("a*b", map[string]any{
					"$type": "social.coves.richtext.facet#code",
				}),
				facet("secret ending", map[string]any{
					"$type": "social.coves.richtext.facet#spoiler", "reason": "Ending",
				}),
			},
			"reply": map[string]any{
				"root":   map[string]any{"uri": outRootATURI, "cid": outRootCID},
				"parent": map[string]any{"uri": outRootATURI, "cid": outRootCID},
			},
			"createdAt": "2026-08-12T10:00:00.000Z",
		},
		"parentAtUri":   outRootATURI,
		"parentApId":    outRootAPID,
		"communityApId": outCommunityAPID,
	})
	require.NoError(t, err)

	conn := outboundAcceptanceDB(t)
	ctx := context.Background()
	custodian, err := identity.NewCustodian(outKEK)
	require.NoError(t, err)
	personasService, err := personas.New(personas.Options{
		DB: conn, Custodian: custodian, UserOrigin: outUserOrigin,
	})
	require.NoError(t, err)
	actor, err := personasService.CreateActorForDID(ctx, outCommenterDID, outCommenterHandle)
	require.NoError(t, err)

	personasServer := httptest.NewServer(personasService)
	t.Cleanup(personasServer.Close)
	lemmy := &fakeLemmy{
		host: outLemmyHost, communityAPI: outCommunityAPID, sharedInbox: outSharedInbox,
		seen: map[string]bool{},
	}
	lemmyServer := httptest.NewServer(lemmy)
	t.Cleanup(lemmyServer.Close)
	client := ap.NewClient(ap.ClientOptions{HTTPClient: &http.Client{Transport: hostRewrite{routes: map[string]string{
		outCovesHost: personasServer.Listener.Addr().String(),
		outLemmyHost: lemmyServer.Listener.Addr().String(),
	}}}})
	lemmy.verifier = ap.NewVerifier(client)
	seedAcceptedParent(t, conn)

	enqueuer, err := NewEnqueuer(EnqueuerOptions{
		DB: conn, Translator: NewTranslator(outUserOrigin), Inboxes: inboxResolver{client: client},
		Actors: store.NewAPActors(conn), UserOrigin: outUserOrigin,
	})
	require.NoError(t, err)
	worker, err := NewWorker(WorkerOptions{
		DB: conn, Actors: store.NewAPActors(conn),
		Signers: sealedSigners{actors: store.NewAPActors(conn), custodian: custodian},
		Inboxes: inboxResolver{client: client}, Sender: client, Lease: time.Minute,
	})
	require.NoError(t, err)

	intent := consume.CommentIntent{
		Op: "create", ATURI: outCommentATURI,
		ID:            consume.ActivityID(outUserOrigin, outCommentATURI, "create", 0),
		CommunityAPID: outCommunityAPID, ParentAPID: outRootAPID, Snapshot: snapshot,
	}
	tx, err := conn.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, enqueuer.EnqueueActivity(ctx, tx, outCommenterDID, outCommunityAPID, outRootATURI, intent))
	require.NoError(t, tx.Commit())
	drainWorker(t, ctx, worker)

	require.Equal(t, 1, lemmy.postCount(), "the real worker must deliver one activity")
	received := lemmy.lastActivity(t)
	require.NoError(t, received.verifyErr, "the delivered activity must carry a valid HTTP signature")
	require.Equal(t, actor.ActorID, received.verifiedActorID)
	note := asMap(t, received.body["object"], "Create.object")
	source := asMap(t, note["source"], "Note.source")
	assert.Equal(t, "text/markdown", source["mediaType"])
	require.Equal(t, expectedMarkdown, source["content"],
		"canonical plaintext must be escaped before Coves facets become Lemmy Markdown")

	content, ok := note["content"].(string)
	require.True(t, ok, "Note.content must be structural HTML")
	document, err := html.Parse(strings.NewReader(content))
	require.NoError(t, err)
	paragraphs := findHTMLElements(document, "p")
	require.Len(t, paragraphs, 4)
	assert.Equal(t, "Literal *stars* [brackets] # hash > marker and café", htmlText(paragraphs[0]),
		"unannotated Markdown metacharacters must render as literal text")
	assertHTMLElement(t, document, "h2", "Roadmap")
	assertHTMLElement(t, document, "blockquote", "quoted > marker")
	assertHTMLElement(t, document, "strong", "bold")
	link := assertHTMLElement(t, document, "a", "docs")
	assert.Equal(t, "https://example.com/docs", htmlAttribute(link, "href"))
	assertHTMLElement(t, document, "code", "a*b")
	detailsElements := findHTMLElements(document, "details")
	require.Len(t, detailsElements, 1, "the spoiler must render as one <details> element")
	details := detailsElements[0]
	assert.False(t, hasHTMLAttribute(details, "open"), "the spoiler must be hidden until explicitly opened")
	assertHTMLElement(t, details, "summary", "Ending")
	assertHTMLElement(t, details, "p", "secret ending")
	assert.Empty(t, findHTMLElements(document, "em"), "unannotated stars must not create emphasis")

	wireObject, err := json.Marshal(note)
	require.NoError(t, err)
	for _, metadata := range []string{"facets", "byteStart", "byteEnd", "social.coves.richtext.facet"} {
		assert.NotContains(t, string(wireObject), metadata, "Coves facet metadata must not leak into ActivityPub")
	}

	served, err := apobject.RenderObject(outUserOrigin, snapshot)
	require.NoError(t, err)
	assert.Equal(t, source, served["source"], "served source must match the delivered durable snapshot rendering")
	assert.Equal(t, content, served["content"], "served HTML must match the delivered durable snapshot rendering")
}

func assertHTMLElement(t *testing.T, root *html.Node, tag, text string) *html.Node {
	t.Helper()
	elements := findHTMLElements(root, tag)
	require.Len(t, elements, 1, "expected exactly one <%s> element", tag)
	assert.Equal(t, text, htmlText(elements[0]), "unexpected <%s> text", tag)
	return elements[0]
}

func findHTMLElements(root *html.Node, tag string) []*html.Node {
	var elements []*html.Node
	if root.Type == html.ElementNode && root.Data == tag {
		elements = append(elements, root)
	}
	for child := root.FirstChild; child != nil; child = child.NextSibling {
		elements = append(elements, findHTMLElements(child, tag)...)
	}
	return elements
}

func htmlText(root *html.Node) string {
	var text strings.Builder
	var walk func(*html.Node)
	walk = func(node *html.Node) {
		if node.Type == html.TextNode {
			text.WriteString(node.Data)
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(root)
	return strings.TrimSpace(text.String())
}

func htmlAttribute(node *html.Node, key string) string {
	for _, attribute := range node.Attr {
		if attribute.Key == key {
			return attribute.Val
		}
	}
	return ""
}

func hasHTMLAttribute(node *html.Node, key string) bool {
	for _, attribute := range node.Attr {
		if attribute.Key == key {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

func outboundAcceptanceDB(t *testing.T) *sql.DB {
	t.Helper()
	database := testutil.DB(t)
	testutil.Truncate(t, database,
		"outbound_deliveries", "outbound_activities", "outbound_objects",
		"ap_actors", "communities")
	return database
}

// seedAcceptedParent writes the parent post's outbound state and stamps its
// accepted_at directly (SetAccepted is a task-15 seam under test), so the child
// comment is causally eligible: only a BRIDGE-origin parent gates, and only
// until it is accepted.
func seedAcceptedParent(t *testing.T, conn *sql.DB) {
	t.Helper()
	ctx := context.Background()
	_, err := store.NewOutboundObjects(conn).Upsert(ctx, store.OutboundObject{
		ATURI:              outRootATURI,
		APObjectID:         outRootAPID,
		LastCID:            outRootCID,
		LastRev:            "3lzhead000001",
		CommunityDID:       outCommunityDID,
		CommunityAPID:      outCommunityAPID,
		TranslatedSnapshot: []byte(`{"type":"Page","name":"parent post"}`),
	})
	require.NoError(t, err, "seed parent outbound object")
	_, err = conn.ExecContext(ctx,
		`UPDATE outbound_objects SET accepted_at = now() WHERE at_uri = $1`, outRootATURI)
	require.NoError(t, err, "stamp the parent accepted (raw, so the causal gate opens)")
}

// commentSnapshot is the durable state the consumer stored for the comment (the
// commentSnapshot shape from consume/comments.go): the record plus the resolved
// thread context the Translator renders the Note from.
func commentSnapshot(t *testing.T) []byte {
	t.Helper()
	snap, err := json.Marshal(map[string]any{
		"atUri":      outCommentATURI,
		"cid":        outCommentCID,
		"rev":        outCommentRev,
		"collection": "social.coves.community.comment",
		"record": map[string]any{
			"$type": "social.coves.community.comment",
			"reply": map[string]any{
				"root":   map[string]any{"uri": outRootATURI, "cid": outRootCID},
				"parent": map[string]any{"uri": outRootATURI, "cid": outRootCID},
			},
			"content":   "first reply from atproto",
			"createdAt": "2026-08-12T10:00:00.000Z",
		},
		"parentAtUri":   outRootATURI,
		"parentApId":    outRootAPID,
		"communityApId": outCommunityAPID,
	})
	require.NoError(t, err)
	return snap
}

// drainWorker runs DeliverNext until the queue is empty.
func drainWorker(t *testing.T, ctx context.Context, worker *Worker) {
	t.Helper()
	for i := 0; i < 20; i++ {
		worked, err := worker.DeliverNext(ctx)
		require.NoError(t, err, "DeliverNext must not error on a healthy delivery")
		if !worked {
			return
		}
	}
	t.Fatal("worker did not drain within 20 iterations")
}

func resetDeliveryToPending(t *testing.T, conn *sql.DB, activityID, inbox string) {
	t.Helper()
	_, err := conn.ExecContext(context.Background(), `
		UPDATE outbound_deliveries
		SET state = 'pending', delivered_at = NULL, claimed_until = NULL,
		    next_attempt_at = now()
		WHERE activity_id = $1 AND target_inbox = $2`, activityID, inbox)
	require.NoError(t, err, "reset the delivery to pending to force a redelivery")
}

func countRows(t *testing.T, conn *sql.DB, table string) int {
	t.Helper()
	var n int
	require.NoError(t, conn.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM `+table).Scan(&n))
	return n
}

// ---------------------------------------------------------------------------
// Test doubles for the outbound seams
// ---------------------------------------------------------------------------

// hostRewrite sends requests for known hosts to their httptest listeners while
// preserving the request URL and Host, so the AP client — and the signatures it
// produces and verifies — believe they are talking to the real origins. It is
// deliberately NOT an *http.Transport, so ap.NewClient's guardedTransport passes
// it through unchanged. Anything not routed is refused: this test never touches
// the network.
type hostRewrite struct {
	routes map[string]string
}

func (rt hostRewrite) RoundTrip(req *http.Request) (*http.Response, error) {
	target, ok := rt.routes[strings.ToLower(req.URL.Hostname())]
	if !ok {
		return nil, fmt.Errorf("refusing outbound request to %s: no route", req.URL)
	}
	clone := req.Clone(req.Context())
	clone.Host = req.URL.Host // preserve the authority the server verifies against
	clone.URL.Scheme = "http"
	clone.URL.Host = target
	return http.DefaultTransport.RoundTrip(clone)
}

// sealedSigners is the SignerProvider: it unseals the persona's AP RSA key,
// mirroring personas.actorSigner (the exported signer accessor is a task-13/15
// seam GREEN wires — the test unseals directly to stay self-contained).
type sealedSigners struct {
	actors    store.APActors
	custodian *identity.Custodian
}

func (s sealedSigners) SignerFor(ctx context.Context, did string) (*ap.Signer, error) {
	actor, err := s.actors.GetByDID(ctx, did)
	if err != nil {
		return nil, err
	}
	key, err := s.custodian.DecryptActorRSAKey(did, actor.RSAKeySealed)
	if err != nil {
		return nil, err
	}
	return ap.NewSigner(actor.ActorID+"#main-key", key), nil
}

// inboxResolver reads endpoints.sharedInbox off the community's served Group
// document (the InboxResolver seam; a production one caches with a TTL).
type inboxResolver struct {
	client *ap.Client
}

func (r inboxResolver) ResolveInbox(ctx context.Context, communityAPID string) (string, error) {
	doc, err := r.client.FetchActor(ctx, communityAPID)
	if err != nil {
		return "", err
	}
	inbox := doc.SharedInboxOrInbox()
	if inbox == "" {
		return "", fmt.Errorf("community %s advertises no inbox", communityAPID)
	}
	return inbox, nil
}

// ---------------------------------------------------------------------------
// The fake Lemmy
// ---------------------------------------------------------------------------

type receivedActivity struct {
	body            map[string]any
	verifiedActorID string
	verifyErr       error
}

type fakeLemmy struct {
	host         string
	communityAPI string
	sharedInbox  string

	verifier *ap.Verifier

	mu       sync.Mutex
	received []receivedActivity
	seen     map[string]bool // activity ids already accepted (dedupe)
}

func (l *fakeLemmy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/c/"+outCommunityName:
		l.serveGroup(w)
	case r.Method == http.MethodPost && r.URL.Path == "/c/"+outCommunityName+"/inbox":
		l.serveInbox(w, r)
	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

func (l *fakeLemmy) serveGroup(w http.ResponseWriter) {
	doc := map[string]any{
		"@context":          "https://www.w3.org/ns/activitystreams",
		"id":                l.communityAPI,
		"type":              "Group",
		"preferredUsername": outCommunityName,
		"inbox":             l.sharedInbox,
		"endpoints":         map[string]any{"sharedInbox": l.sharedInbox},
	}
	w.Header().Set("Content-Type", ap.ContentTypeActivityJSON)
	_ = json.NewEncoder(w).Encode(doc)
}

func (l *fakeLemmy) serveInbox(w http.ResponseWriter, r *http.Request) {
	body := make([]byte, r.ContentLength)
	if r.ContentLength > 0 {
		_, _ = readFull(r, body)
	}

	actorID, verifyErr := l.verifier.Verify(r.Context(), r, body)

	var parsed map[string]any
	_ = json.Unmarshal(body, &parsed)

	l.mu.Lock()
	l.received = append(l.received, receivedActivity{body: parsed, verifiedActorID: actorID, verifyErr: verifyErr})
	activityID, _ := parsed["id"].(string)
	duplicate := l.seen[activityID]
	if activityID != "" {
		l.seen[activityID] = true
	}
	l.mu.Unlock()

	if duplicate {
		// Lemmy's received_activity dedupe rejects an activity id it already
		// processed. The exact wire code/message is a GREEN classification
		// detail; the pinned CONTRACT is that the sender treats it as DELIVERED.
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"activity was already received"}`))
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (l *fakeLemmy) postCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.received)
}

func (l *fakeLemmy) lastActivity(t *testing.T) receivedActivity {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	require.NotEmpty(t, l.received, "the fake Lemmy received no activity")
	return l.received[len(l.received)-1]
}

// readFull reads exactly len(buf) bytes from the request body.
func readFull(r *http.Request, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Body.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// ---------------------------------------------------------------------------
// JSON assertion helpers
// ---------------------------------------------------------------------------

func asMap(t *testing.T, v any, what string) map[string]any {
	t.Helper()
	m, ok := v.(map[string]any)
	require.True(t, ok, "%s must be a JSON object, got %T", what, v)
	return m
}

// asStringSet coerces an AP addressing field (a string or an array of strings)
// into a set for ⊇ assertions.
func asStringSet(t *testing.T, v any, what string) []string {
	t.Helper()
	switch typed := v.(type) {
	case string:
		return []string{typed}
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			s, ok := item.(string)
			require.True(t, ok, "%s array must hold strings, got %T", what, item)
			out = append(out, s)
		}
		return out
	default:
		require.Failf(t, "bad addressing shape", "%s must be a string or array of strings, got %T", what, v)
		return nil
	}
}
