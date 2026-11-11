package outbound

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/consume"
	"tidepool/internal/identity"
	"tidepool/internal/labels"
	"tidepool/internal/labels/labelstest"
	"tidepool/internal/personas"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// The shared takedown harness (chunk 03). One world: a native author minted as
// a coves.social persona that signs for real, a fake Lemmy hosting the bridged
// community outCommunityAPID that verifies every signature it receives, the
// real Enqueuer and Worker, Takedowns wired to that Enqueuer, and a Postgres
// label store whose appliers include Takedowns.LabelApplier.

const (
	// tdLabelerDID is the Coves labeler every takedown label names as src.
	tdLabelerDID = "did:web:coves.social"
	// tdLabelTakedown is the label value that takes a record down.
	tdLabelTakedown = "!takedown"

	// tdAuthorDID is the native author of every takedown fixture; its persona
	// signs the Create, the takedown Delete and the Undo{Delete}.
	tdAuthorDID     = outCommenterDID
	tdAuthorHandle  = outCommenterHandle
	tdAuthorActorID = outUserOrigin + "/ap/actor/" + tdAuthorDID

	// tdPost is a native post by tdAuthorDID in outCommunityAPID.
	tdPostRKey  = "3lztkdnpost01"
	tdPostCID   = "bafyreib2rxk3rybk3aobmv5cjuql3bm2twh4jo5uxgf5kpqrsqxi3jgxte"
	tdPostRev   = "3lztkdnrev0001"
	tdPostATURI = "at://" + tdAuthorDID + "/social.coves.community.postv2/" + tdPostRKey
	tdPostAPID  = outUserOrigin + "/ap/object/" + tdAuthorDID + "/social.coves.community.postv2/" + tdPostRKey

	// tdOtherAuthorDID is a second native author writing to the same
	// community, so its work shares tdPost's ordering key.
	tdOtherAuthorDID     = "did:plc:4kq7vbk2zjhyqtmn6w3xoh5d"
	tdOtherAuthorHandle  = "bob.coves.social"
	tdOtherAuthorActorID = outUserOrigin + "/ap/actor/" + tdOtherAuthorDID
)

// tdNativePost is a native post fixture in outCommunityAPID.
type tdNativePost struct {
	authorDID, rkey, rev, title string
}

func (p tdNativePost) atURI() string {
	return "at://" + p.authorDID + "/social.coves.community.postv2/" + p.rkey
}

func (p tdNativePost) apID() string {
	return outUserOrigin + "/ap/object/" + p.authorDID + "/social.coves.community.postv2/" + p.rkey
}

var (
	// tdPost is the fixture behind the tdPost* constants.
	tdPost = tdNativePost{authorDID: tdAuthorDID, rkey: tdPostRKey, rev: tdPostRev,
		title: "A native post a Coves admin takes down"}
	// tdSecondPost is another post by tdAuthorDID, a different object on the
	// same ordering key.
	tdSecondPost = tdNativePost{authorDID: tdAuthorDID, rkey: "3lztkdnpost02", rev: "3lztkdnrev0002",
		title: "The same author's other post"}
	// tdOtherPost is a post by the second author on the same ordering key.
	tdOtherPost = tdNativePost{authorDID: tdOtherAuthorDID, rkey: "3lztkdnpost03", rev: "3lztkdnrev0003",
		title: "Another author's post"}
)

// tdTakedownTables are the planned chunk-03 tables (migration 055). They have
// no FK to outbound_objects, so no existing helper's CASCADE clears them.
var tdTakedownTables = []string{"outbound_takedown_sources", "outbound_takedowns"}

// takedownWorld is the harness one takedown test drives.
type takedownWorld struct {
	conn       *sql.DB
	personas   *personas.Service
	lemmy      *fakeLemmy
	faults     *inboxFaults
	enqueuer   *Enqueuer
	worker     *Worker
	takedowns  *Takedowns
	labelStore *labels.PostgresStore
	labelKey   atcrypto.PrivateKey
	labelSeq   int64
	client     *ap.Client
	signers    sealedSigners
}

// takedownDB truncates every table a takedown test reads, including the label
// tables and the planned takedown tables (when the migration has created them).
func takedownDB(t *testing.T) *sql.DB {
	t.Helper()
	database := testutil.DB(t)
	testutil.Truncate(t, database,
		"outbound_deliveries", "outbound_activities", "outbound_objects", "outbound_votes",
		"ap_actors", "ap_objects", "communities", "federation_prefs",
		"coves_labels", "coves_label_cursor", "repo_state")
	truncateTakedownTables(t, database)
	return database
}

// truncateTakedownTables truncates whichever of tdTakedownTables exist; a
// missing one is left for the test to fail on.
func truncateTakedownTables(t *testing.T, conn *sql.DB) {
	t.Helper()
	for _, table := range tdTakedownTables {
		var exists bool
		require.NoError(t, conn.QueryRow(`SELECT to_regclass($1) IS NOT NULL`, table).Scan(&exists))
		if exists {
			testutil.Truncate(t, conn, table)
		}
	}
}

// newTakedownWorld builds the world: persona, fake Lemmy, enqueuer, worker,
// takedowns and the label store with the takedowns applier.
func newTakedownWorld(t *testing.T) *takedownWorld {
	t.Helper()
	conn := takedownDB(t)
	ctx := context.Background()

	custodian, err := identity.NewCustodian(outKEK)
	require.NoError(t, err)
	svc, err := personas.New(personas.Options{DB: conn, Custodian: custodian, UserOrigin: outUserOrigin})
	require.NoError(t, err)
	actor, err := svc.CreateActorForDID(ctx, tdAuthorDID, tdAuthorHandle)
	require.NoError(t, err, "mint the native author's persona")
	require.Equal(t, tdAuthorActorID, actor.ActorID)
	personasServer := httptest.NewServer(svc)
	t.Cleanup(personasServer.Close)

	lemmy := &fakeLemmy{
		host:         outLemmyHost,
		communityAPI: outCommunityAPID,
		sharedInbox:  outSharedInbox,
		seen:         map[string]bool{},
	}
	faults := &inboxFaults{next: lemmy}
	lemmyServer := httptest.NewServer(faults)
	t.Cleanup(lemmyServer.Close)
	client := ap.NewClient(ap.ClientOptions{
		HTTPClient: &http.Client{Transport: hostRewrite{routes: map[string]string{
			outCovesHost: personasServer.Listener.Addr().String(),
			outLemmyHost: lemmyServer.Listener.Addr().String(),
		}}},
	})
	lemmy.verifier = ap.NewVerifier(client)

	enqueuer, err := NewEnqueuer(EnqueuerOptions{
		DB:         conn,
		Translator: NewTranslator(outUserOrigin, ""),
		Inboxes:    inboxResolver{client: client},
		Actors:     store.NewAPActors(conn),
		UserOrigin: outUserOrigin,
	})
	require.NoError(t, err)
	signers := sealedSigners{actors: store.NewAPActors(conn), custodian: custodian}
	worker := newTakedownWorker(t, conn, signers, client, client)
	takedowns, err := NewTakedowns(TakedownsOptions{
		DB:       conn,
		Enqueuer: enqueuer,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	require.NotNil(t, takedowns)

	return &takedownWorld{
		conn:       conn,
		personas:   svc,
		lemmy:      lemmy,
		faults:     faults,
		enqueuer:   enqueuer,
		worker:     worker,
		takedowns:  takedowns,
		labelStore: labels.NewPostgresStore(conn, takedowns.LabelApplier()),
		labelKey:   labelstest.NewKey(t),
		client:     client,
		signers:    signers,
	}
}

// newTakedownWorker is the world's worker shape, POSTing through sender.
func newTakedownWorker(t *testing.T, conn *sql.DB, signers sealedSigners, client *ap.Client,
	sender ActivitySender) *Worker {
	t.Helper()
	worker, err := NewWorker(WorkerOptions{
		DB:          conn,
		Actors:      store.NewAPActors(conn),
		Signers:     signers,
		Inboxes:     inboxResolver{client: client},
		Sender:      sender,
		Lease:       time.Minute,
		BackoffBase: time.Millisecond,
	})
	require.NoError(t, err)
	return worker
}

// workerWithSender is a second worker on the world's queue whose POSTs go
// through sender.
func (w *takedownWorld) workerWithSender(t *testing.T, sender ActivitySender) *Worker {
	t.Helper()
	return newTakedownWorker(t, w.conn, w.signers, w.client, sender)
}

// gatedSender POSTs through inner, then announces the POST on posted and
// blocks until release is closed, so a test can act while a claimed delivery's
// POST is in flight.
type gatedSender struct {
	inner   ActivitySender
	posted  chan struct{}
	release chan struct{}
}

func newGatedSender(inner ActivitySender) *gatedSender {
	return &gatedSender{inner: inner, posted: make(chan struct{}, 1), release: make(chan struct{})}
}

func (s *gatedSender) SendActivityAs(ctx context.Context, signer *ap.Signer, inbox string, activity any) error {
	err := s.inner.SendActivityAs(ctx, signer, inbox, activity)
	s.posted <- struct{}{}
	<-s.release
	return err
}

// inboxFaults fronts the fake Lemmy: while unavailable is above zero, each
// inbox POST is answered 503 (a retryable failure) without reaching it, and
// counted in rejected.
type inboxFaults struct {
	next http.Handler

	mu          sync.Mutex
	unavailable int
	rejected    int
}

func (f *inboxFaults) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		f.mu.Lock()
		reject := f.unavailable > 0
		if reject {
			f.unavailable--
			f.rejected++
		}
		f.mu.Unlock()
		if reject {
			http.Error(w, "service unavailable", http.StatusServiceUnavailable)
			return
		}
	}
	f.next.ServeHTTP(w, r)
}

// answerUnavailable makes the fake Lemmy answer the next n inbox POSTs 503.
func (w *takedownWorld) answerUnavailable(n int) {
	w.faults.mu.Lock()
	defer w.faults.mu.Unlock()
	w.faults.unavailable = n
}

// rejectedPosts counts the inbox POSTs the fake Lemmy answered 503.
func (w *takedownWorld) rejectedPosts() int {
	w.faults.mu.Lock()
	defer w.faults.mu.Unlock()
	return w.faults.rejected
}

// commitLabelWhilePOSTInFlight has a second worker claim the next delivery
// and, while its POST is in flight, commits the `!takedown` label on subject,
// then lets the delivery finish.
func (w *takedownWorld) commitLabelWhilePOSTInFlight(t *testing.T, subject string) {
	t.Helper()
	sender := newGatedSender(w.client)
	gated := w.workerWithSender(t, sender)
	returned := make(chan error, 1)
	go func() {
		_, err := gated.DeliverNext(context.Background())
		returned <- err
	}()
	select {
	case <-sender.posted:
	case <-time.After(5 * time.Second):
		t.Fatal("the delivery was not POSTed within 5s")
	}
	w.commitLabel(t, subject, false)
	close(sender.release)
	select {
	case err := <-returned:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the in-flight delivery did not finish within 5s")
	}
}

// makeDue makes activityID's delivery to the shared inbox due now.
func (w *takedownWorld) makeDue(t *testing.T, activityID string) {
	t.Helper()
	_, err := w.conn.Exec(`UPDATE outbound_deliveries SET next_attempt_at = now()
		WHERE activity_id = $1 AND target_inbox = $2`, activityID, outSharedInbox)
	require.NoError(t, err)
}

// failWrites installs a test trigger named name that raises on timing (e.g.
// "BEFORE UPDATE") of table for every row matching when, and returns the func
// that drops it; the drop also runs at cleanup.
func failWrites(t *testing.T, conn *sql.DB, name, timing, table, when string) (drop func()) {
	t.Helper()
	_, err := conn.Exec(`CREATE OR REPLACE FUNCTION ` + name + `() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'test trigger ` + name + `'; END $$`)
	require.NoError(t, err, "create trigger function %s", name)
	_, err = conn.Exec(`CREATE TRIGGER ` + name + ` ` + timing + ` ON ` + table +
		` FOR EACH ROW WHEN (` + when + `) EXECUTE FUNCTION ` + name + `()`)
	require.NoError(t, err, "create trigger %s", name)
	dropped := false
	drop = func() {
		if dropped {
			return
		}
		dropped = true
		_, err := conn.Exec(`DROP TRIGGER IF EXISTS ` + name + ` ON ` + table)
		require.NoError(t, err, "drop trigger %s", name)
		_, err = conn.Exec(`DROP FUNCTION IF EXISTS ` + name + `()`)
		require.NoError(t, err, "drop trigger function %s", name)
	}
	t.Cleanup(drop)
	return drop
}

// tdReply is a native comment by tdAuthorDID replying to the post at
// parentATURI (AP id parentAPID) in outCommunityAPID.
type tdReply struct {
	rkey, rev, parentATURI, parentAPID string
}

func (r tdReply) atURI() string {
	return "at://" + tdAuthorDID + "/social.coves.community.comment/" + r.rkey
}

func (r tdReply) apID() string {
	return outUserOrigin + "/ap/object/" + tdAuthorDID + "/social.coves.community.comment/" + r.rkey
}

func (r tdReply) createID() string {
	return consume.ActivityID(outUserOrigin, r.atURI(), "create", 0)
}

const (
	// tdBridgedParentATURI is a bridged post: its repo is a Tidepool-hosted
	// Lemmy identity, so it has no outbound row and is already on Lemmy at
	// tdBridgedParentAPID.
	tdBridgedParentATURI = "at://did:plc:n3wq5hbrbz7ydnkxlpqegrty/social.coves.community.postv2/3lztkdnbrdg01"
	tdBridgedParentAPID  = "https://lemmy.world/post/4242"
)

var (
	// tdReplyToPost replies to the native post tdPost.
	tdReplyToPost = tdReply{rkey: "3lztkdncmnt01", rev: "3lztkdncrev001",
		parentATURI: tdPostATURI, parentAPID: tdPostAPID}
	// tdReplyToBridged replies to the bridged post tdBridgedParentATURI.
	tdReplyToBridged = tdReply{rkey: "3lztkdncmnt02", rev: "3lztkdncrev002",
		parentATURI: tdBridgedParentATURI, parentAPID: tdBridgedParentAPID}
)

// enqueueReply enqueues reply's Create{Note} through the real enqueuer with
// its parent at-uri, the way the consumer does for a native comment.
func (w *takedownWorld) enqueueReply(t *testing.T, reply tdReply) {
	t.Helper()
	snapshot, intent := replyCreate(t, reply)
	w.enqueueNative(t, store.OutboundObject{
		ATURI:              reply.atURI(),
		APObjectID:         reply.apID(),
		LastCID:            outCommentCID,
		LastRev:            reply.rev,
		CommunityDID:       outCommunityDID,
		CommunityAPID:      outCommunityAPID,
		TranslatedSnapshot: snapshot,
		Depth:              1,
	}, reply.parentATURI, intent)
}

// replyCreate is reply's durable snapshot and its Create intent.
func replyCreate(t *testing.T, reply tdReply) ([]byte, consume.CommentIntent) {
	t.Helper()
	snapshot, err := json.Marshal(map[string]any{
		"atUri":      reply.atURI(),
		"cid":        outCommentCID,
		"rev":        reply.rev,
		"collection": "social.coves.community.comment",
		"record": map[string]any{
			"$type": "social.coves.community.comment",
			"reply": map[string]any{
				"root":   map[string]any{"uri": reply.parentATURI, "cid": tdPostCID},
				"parent": map[string]any{"uri": reply.parentATURI, "cid": tdPostCID},
			},
			"content":   "a native reply held while its parent is down",
			"createdAt": "2026-11-01T11:00:00.000Z",
		},
		"parentAtUri":   reply.parentATURI,
		"parentApId":    reply.parentAPID,
		"communityApId": outCommunityAPID,
	})
	require.NoError(t, err)
	return snapshot, consume.CommentIntent{
		Op:            "create",
		ATURI:         reply.atURI(),
		ID:            reply.createID(),
		CommunityAPID: outCommunityAPID,
		ParentAPID:    reply.parentAPID,
		Snapshot:      snapshot,
	}
}

// tdNativePostSnapshot is the durable snapshot of post at rev with title.
func tdNativePostSnapshot(t *testing.T, post tdNativePost, rev, title string) []byte {
	t.Helper()
	snap, err := json.Marshal(map[string]any{
		"atUri":      post.atURI(),
		"cid":        tdPostCID,
		"rev":        rev,
		"collection": "social.coves.community.postv2",
		"record": map[string]any{
			"$type":     "social.coves.community.postv2",
			"community": outCommunityDID,
			"title":     title,
			"content":   "the takedown deletes it on Lemmy and the restore undoes that",
			"createdAt": "2026-11-01T10:00:00.000Z",
		},
		"communityApId": outCommunityAPID,
	})
	require.NoError(t, err)
	return snap
}

// mintOtherAuthor mints tdOtherAuthorDID's persona.
func (w *takedownWorld) mintOtherAuthor(t *testing.T) {
	t.Helper()
	actor, err := w.personas.CreateActorForDID(context.Background(), tdOtherAuthorDID, tdOtherAuthorHandle)
	require.NoError(t, err, "mint the second native author's persona")
	require.Equal(t, tdOtherAuthorActorID, actor.ActorID)
}

// enqueueNative writes the object's outbound state and enqueues its intent in
// one transaction, the way the consumer's rev gate does.
func (w *takedownWorld) enqueueNative(t *testing.T, object store.OutboundObject, parentATURI string, intent consume.Intent) {
	t.Helper()
	w.enqueueNativeAs(t, tdAuthorDID, object, parentATURI, intent)
}

// enqueueNativeAs is enqueueNative for an object by actorDID.
func (w *takedownWorld) enqueueNativeAs(t *testing.T, actorDID string, object store.OutboundObject,
	parentATURI string, intent consume.Intent) {
	t.Helper()
	ctx := context.Background()
	tx, err := w.conn.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, err = store.NewOutboundObjects(w.conn).UpsertTx(ctx, tx, object)
	require.NoError(t, err, "write outbound state for %s", object.ATURI)
	require.NoError(t, w.enqueuer.EnqueueActivity(ctx, tx, actorDID, outCommunityAPID, parentATURI, intent),
		"enqueue %s", intent.ActivityID())
	require.NoError(t, tx.Commit())
}

// enqueuePostCreate enqueues tdPost's Create{Page} through the real enqueuer.
func (w *takedownWorld) enqueuePostCreate(t *testing.T) {
	t.Helper()
	w.enqueuePost(t, tdPost, "create", 0)
}

// enqueuePost enqueues post's create (seq 0) or its seq-th update through the
// real enqueuer, as the post's author; an update carries an edited title.
func (w *takedownWorld) enqueuePost(t *testing.T, post tdNativePost, op string, seq int) {
	t.Helper()
	rev, title := post.rev, post.title
	if op == "update" {
		rev, title = fmt.Sprintf("%s-edit%d", post.rev, seq), fmt.Sprintf("%s (edit %d)", post.title, seq)
	}
	snapshot := tdNativePostSnapshot(t, post, rev, title)
	w.enqueueNativeAs(t, post.authorDID, store.OutboundObject{
		ATURI:              post.atURI(),
		APObjectID:         post.apID(),
		LastCID:            tdPostCID,
		LastRev:            rev,
		CommunityDID:       outCommunityDID,
		CommunityAPID:      outCommunityAPID,
		TranslatedSnapshot: snapshot,
		LastActivitySeq:    seq,
	}, "", consume.PostIntent{
		Op:            op,
		ATURI:         post.atURI(),
		ID:            consume.ActivityID(outUserOrigin, post.atURI(), op, seq),
		CommunityAPID: outCommunityAPID,
		CommunityDID:  outCommunityDID,
		Snapshot:      snapshot,
	})
}

// enqueueCommentCreate enqueues the native comment outCommentATURI (a reply to
// the bridge parent outRootATURI) through the real enqueuer.
func (w *takedownWorld) enqueueCommentCreate(t *testing.T) {
	t.Helper()
	w.enqueueNative(t, store.OutboundObject{
		ATURI:              outCommentATURI,
		APObjectID:         outCommentAPID,
		LastCID:            outCommentCID,
		LastRev:            outCommentRev,
		CommunityDID:       outCommunityDID,
		CommunityAPID:      outCommunityAPID,
		TranslatedSnapshot: commentSnapshot(t),
		Depth:              1,
	}, outRootATURI, commentEnqueueIntent(t))
}

// deliverPost enqueues tdPost's Create and drains the worker, so the post is
// delivered and accepted_at stamped the real way.
func (w *takedownWorld) deliverPost(t *testing.T) {
	t.Helper()
	w.enqueuePostCreate(t)
	w.drain(t)
	require.Equal(t, []string{"Create"}, w.receivedTypes(), "precondition: Lemmy received the post's Create")
	require.True(t, rawAccepted(t, w.conn, tdPostATURI), "precondition: the delivered post is accepted")
}

// commitLabel commits a signed label on subject at the next seq through the
// store with the takedowns applier; negate commits its negation.
func (w *takedownWorld) commitLabel(t *testing.T, subject string, negate bool) {
	t.Helper()
	w.labelSeq++
	commitTakedownLabel(t, w.labelStore, w.labelKey, w.labelSeq, subject, negate)
}

// commitTakedownLabel commits one signed `!takedown` (or its negation) at seq
// through store.
func commitTakedownLabel(t *testing.T, labelStore *labels.PostgresStore, key atcrypto.PrivateKey,
	seq int64, subject string, negate bool) {
	t.Helper()
	commitTakedownLabelExp(t, labelStore, key, seq, subject, negate, nil)
}

// changeSource adds (add true) or removes src on atURI through Takedowns in
// one committed transaction.
func (w *takedownWorld) changeSource(t *testing.T, atURI string, src store.TakedownSource, add bool) {
	t.Helper()
	ctx := context.Background()
	tx, err := w.conn.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	if add {
		require.NoError(t, w.takedowns.AddSourceTx(ctx, tx, atURI, src))
	} else {
		require.NoError(t, w.takedowns.RemoveSourceTx(ctx, tx, atURI, src))
	}
	require.NoError(t, tx.Commit())
}

// drain makes every pending delivery due and runs the worker until the queue
// is empty.
func (w *takedownWorld) drain(t *testing.T) {
	t.Helper()
	_, err := w.conn.Exec(`UPDATE outbound_deliveries SET next_attempt_at = now() WHERE state = 'pending'`)
	require.NoError(t, err)
	drainWorker(t, context.Background(), w.worker)
}

// received is every activity the fake Lemmy received, in arrival order.
func (w *takedownWorld) received() []receivedActivity {
	w.lemmy.mu.Lock()
	defer w.lemmy.mu.Unlock()
	return append([]receivedActivity(nil), w.lemmy.received...)
}

// receivedTypes is the type of every activity the fake Lemmy received, in order.
func (w *takedownWorld) receivedTypes() []string {
	types := []string{}
	for _, activity := range w.received() {
		kind, _ := activity.body["type"].(string)
		types = append(types, kind)
	}
	return types
}

// receivedSince is what the fake Lemmy received after its first n activities.
func (w *takedownWorld) receivedSince(n int) []receivedActivity {
	all := w.received()
	if n >= len(all) {
		return nil
	}
	return all[n:]
}

// receivedLines is one "Type object" line per activity the fake Lemmy
// received, in order: the object is the AP id a Create/Update carries or a
// Delete names, and an Undo reads "Undo{Delete} <object>".
func (w *takedownWorld) receivedLines(t *testing.T) []string {
	t.Helper()
	lines := []string{}
	for _, activity := range w.received() {
		lines = append(lines, receivedLine(t, activity.body))
	}
	return lines
}

func receivedLine(t *testing.T, body map[string]any) string {
	t.Helper()
	kind, _ := body["type"].(string)
	switch object := body["object"].(type) {
	case string:
		return kind + " " + object
	case map[string]any:
		if inner, _ := object["type"].(string); kind == "Undo" {
			return "Undo{" + inner + "} " + strings.TrimPrefix(receivedLine(t, object), inner+" ")
		}
		id, _ := object["id"].(string)
		return kind + " " + id
	default:
		return kind
	}
}

// deliveryOf is the delivery of the single outbound activity of kind to the
// community's shared inbox.
func (w *takedownWorld) deliveryOf(t *testing.T, kind string) *store.OutboundDelivery {
	t.Helper()
	var activityID string
	require.NoError(t, w.conn.QueryRow(`SELECT activity_id FROM outbound_activities WHERE kind = $1`, kind).
		Scan(&activityID), "one outbound %s activity", kind)
	delivery, err := store.NewOutboundDeliveries(w.conn).Get(context.Background(), activityID, outSharedInbox)
	require.NoError(t, err)
	return delivery
}

// deliveryStateOf is the state of activityID's delivery to the shared inbox.
func (w *takedownWorld) deliveryStateOf(t *testing.T, activityID string) store.DeliveryState {
	t.Helper()
	delivery, err := store.NewOutboundDeliveries(w.conn).Get(context.Background(), activityID, outSharedInbox)
	require.NoError(t, err)
	return delivery.State
}

// activityCount counts the outbound activities of kind.
func activityCount(t *testing.T, conn *sql.DB, kind string) int {
	t.Helper()
	var n int
	require.NoError(t, conn.QueryRow(`SELECT count(*) FROM outbound_activities WHERE kind = $1`, kind).Scan(&n))
	return n
}

// takedownResolver resolves the labeler to a labelstest server and key.
type takedownResolver struct {
	endpoint string
	key      atcrypto.PublicKey
}

func (r takedownResolver) Resolve(_ context.Context, did string) (labels.Labeler, error) {
	return labels.Labeler{DID: did, Endpoint: r.endpoint, Key: r.key}, nil
}

// runLabelConsumer runs a labels.Consumer on labelStore against server until
// the server sees a subscription at cursor, then stops it.
func runLabelConsumer(t *testing.T, labelStore labels.Store, server *labelstest.Server,
	key atcrypto.PrivateKey, cursor int64) {
	t.Helper()
	consumer, err := labels.NewConsumer(labels.ConsumerOptions{
		LabelerDID: tdLabelerDID,
		Resolver:   takedownResolver{endpoint: server.URL, key: labelstest.PublicKey(t, key)},
		Store:      labelStore,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		MinBackoff: 10 * time.Millisecond,
		MaxBackoff: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		consumer.Run(ctx)
	}()
	defer func() {
		cancel()
		select {
		case <-returned:
		case <-time.After(5 * time.Second):
			t.Fatal("the label consumer did not stop within 5s")
		}
	}()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case seen := <-server.Connections():
			if seen == cursor {
				return
			}
		case <-returned:
			t.Fatal("the label consumer returned before subscribing")
		case <-deadline:
			t.Fatalf("the label server saw no subscription at cursor %d within 5s", cursor)
		}
	}
}

// deletePost is post's author deleting it: its outbound state is tombstoned
// and its Delete{Page} enqueued in one transaction, the way the consumer's
// author-delete path does.
func (w *takedownWorld) deletePost(t *testing.T, post tdNativePost) {
	t.Helper()
	w.deleteNative(t, post.authorDID, post.atURI(), "", func(dead *store.OutboundObject) consume.Intent {
		return consume.PostIntent{
			Op:            "delete",
			ATURI:         dead.ATURI,
			ID:            consume.ActivityID(outUserOrigin, dead.ATURI, "delete", dead.LastActivitySeq),
			CommunityAPID: dead.CommunityAPID,
			Snapshot:      dead.TranslatedSnapshot,
		}
	})
}

// deleteReply is reply's author deleting it: its outbound state is tombstoned
// and its Delete{Note} enqueued with its parent, in one transaction, the way
// the consumer's comment-delete path does.
func (w *takedownWorld) deleteReply(t *testing.T, reply tdReply) {
	t.Helper()
	w.deleteNative(t, tdAuthorDID, reply.atURI(), reply.parentATURI, func(dead *store.OutboundObject) consume.Intent {
		return consume.CommentIntent{
			Op:            "delete",
			ATURI:         dead.ATURI,
			ID:            consume.ActivityID(outUserOrigin, dead.ATURI, "delete", dead.LastActivitySeq),
			CommunityAPID: dead.CommunityAPID,
			CommunityDID:  dead.CommunityDID,
			ParentAPID:    reply.parentAPID,
			Snapshot:      dead.TranslatedSnapshot,
		}
	})
}

// deleteNative tombstones atURI and enqueues the intent built from the
// tombstoned state, in one committed transaction.
func (w *takedownWorld) deleteNative(t *testing.T, actorDID, atURI, parentATURI string,
	intent func(dead *store.OutboundObject) consume.Intent) {
	t.Helper()
	ctx := context.Background()
	tx, err := w.conn.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	dead, err := store.NewOutboundObjects(w.conn).TombstoneTx(ctx, tx, atURI)
	require.NoError(t, err, "tombstone %s", atURI)
	built := intent(dead)
	require.NoError(t, w.enqueuer.EnqueueActivity(ctx, tx, actorDID, outCommunityAPID, parentATURI, built),
		"enqueue %s", built.ActivityID())
	require.NoError(t, tx.Commit())
}

// restoreConsent commits restore (a write that gives did's persona back its
// consent: an unpause or an opt-in) and the consent release in one
// transaction, the shape of the consumer's account-status and opt-in hooks.
func (w *takedownWorld) restoreConsent(t *testing.T, did string, restore func(tx *sql.Tx)) {
	t.Helper()
	ctx := context.Background()
	tx, err := w.conn.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	restore(tx)
	_, err = store.NewOutboundDeliveries(w.conn).ReleaseConsentHeldTx(ctx, tx, did)
	require.NoError(t, err, "release %s's consent-held deliveries", did)
	require.NoError(t, tx.Commit())
}

// heldReasonOf is the held_reason of activityID's delivery to the shared inbox.
func (w *takedownWorld) heldReasonOf(t *testing.T, activityID string) string {
	t.Helper()
	var reason string
	require.NoError(t, w.conn.QueryRow(`SELECT held_reason FROM outbound_deliveries
		WHERE activity_id = $1 AND target_inbox = $2`, activityID, outSharedInbox).Scan(&reason))
	return reason
}
