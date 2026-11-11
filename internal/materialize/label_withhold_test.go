package materialize

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/labeling"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/blobwithhold"
	"tidepool/internal/errors"
	"tidepool/internal/labels"
	"tidepool/internal/labels/labelstest"
	"tidepool/internal/repo"
	"tidepool/internal/testutil"
)

// labelSourceDID is the Coves labeler every label in these tests names.
const labelSourceDID = "did:web:coves.social"

// imagePostPublished is the `published` materializeImagePost gives a page.
const imagePostPublished = "2026-10-22T10:00:00.000000Z"

// labelRig commits signed Coves labels straight into the Postgres label
// store, with the materializer's applier, as the consumer would.
type labelRig struct {
	store *labels.PostgresStore
	key   atcrypto.PrivateKey
}

// newLabelRig clears the label tables and builds a store on the harness's
// materializer applier.
func newLabelRig(t *testing.T, h *harness) *labelRig {
	t.Helper()
	database := testutil.DB(t)
	testutil.Truncate(t, database, "coves_labels", "coves_label_cursor")
	testutil.TruncateIfExists(t, database, "community_removal_suppressions")
	return &labelRig{
		store: labels.NewPostgresStore(database, h.m.LabelApplier()),
		key:   labelstest.NewKey(t),
	}
}

// signed is l with src = labelSourceDID, signed by the rig's key.
func (r *labelRig) signed(t *testing.T, l labeling.Label) labeling.Label {
	t.Helper()
	l.SourceDID = labelSourceDID
	return labelstest.Sign(t, r.key, l)
}

// commit signs l (src = labelSourceDID) and commits it at (0, seq).
func (r *labelRig) commit(t *testing.T, seq int64, l labeling.Label) {
	t.Helper()
	require.NoError(t, r.store.Commit(context.Background(), labels.Cursor{Epoch: 0, Seq: seq}, r.signed(t, l)),
		"commit label seq %d", seq)
}

func negated() *bool {
	neg := true
	return &neg
}

// imagePostPage is the Page materializeImagePost builds, with its image at
// mediaPath.
func imagePostPage(t *testing.T, postID, mediaPath string) map[string]any {
	t.Helper()
	post := page(postID, personID, groupID, "an image post "+postID, imagePostPublished)
	post["attachment"] = []any{map[string]any{
		"type": "Image", "href": "https://lemmy.world" + mediaPath,
		"mediaType": "image/png", "name": "a picture",
	}}
	return post
}

// serveImage serves imageBytes at mediaPath on the fixture server.
func serveImage(h *harness, mediaPath string, imageBytes []byte) {
	h.mux.HandleFunc("GET "+mediaPath, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(imageBytes)
	})
}

// futurePostURI is the at-uri a bridged postv2 by authorDID for postID will
// have once materialized (deterministic rkey from published and the AP id).
func futurePostURI(t *testing.T, authorDID, postID string) string {
	t.Helper()
	published, err := time.Parse(time.RFC3339Nano, imagePostPublished)
	require.NoError(t, err)
	rkey, err := repo.DeterministicTID(published, postID)
	require.NoError(t, err)
	return "at://" + authorDID + "/" + CollectionPostV2 + "/" + rkey.String()
}

// subjectURIOf is the hosted record's at-uri for postID.
func subjectURIOf(t *testing.T, h *harness, postID string) string {
	t.Helper()
	mapping, err := h.objects.GetByAPID(context.Background(), postID)
	require.NoError(t, err)
	require.NotEmpty(t, mapping.ATURI)
	return mapping.ATURI
}

// covered is Covers(did, cid) with the error required away.
func covered(t *testing.T, withholds *blobwithhold.Store, did, cidStr string) bool {
	t.Helper()
	ok, err := withholds.Covers(context.Background(), did, cidStr)
	require.NoError(t, err)
	return ok
}

// labelRowState is one label-source withhold row of a subject.
type labelRowState struct {
	CID    string
	Lifted bool
}

func labelRowsOfSubject(t *testing.T, subjectURI string) []labelRowState {
	t.Helper()
	rows, err := testutil.DB(t).Query(`
		SELECT cid, lifted_at IS NOT NULL FROM blob_withholds
		WHERE source = 'label' AND subject_uri = $1 ORDER BY cid`, subjectURI)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var states []labelRowState
	for rows.Next() {
		var s labelRowState
		require.NoError(t, rows.Scan(&s.CID, &s.Lifted))
		states = append(states, s)
	}
	require.NoError(t, rows.Err())
	return states
}

func activeSourcesOfCID(t *testing.T, cidStr string) []string {
	t.Helper()
	rows, err := testutil.DB(t).Query(`
		SELECT source FROM blob_withholds
		WHERE cid = $1 AND lifted_at IS NULL ORDER BY source`, cidStr)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var sources []string
	for rows.Next() {
		var source string
		require.NoError(t, rows.Scan(&source))
		sources = append(sources, source)
	}
	require.NoError(t, rows.Err())
	return sources
}

// TestLabelTakedown_WithholdsTheSubjectsBlobsUntilTheNegation (B7): a
// `!takedown` on a hosted bridged record withholds, for the record's repo DID
// only, every blob CID the record references, and an edit's new image too; an
// `nsfw` label withholds nothing; the negation lifts only that subject's
// label rows, leaving operator and community-removal rows on the same CID.
func TestLabelTakedown_WithholdsTheSubjectsBlobsUntilTheNegation(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	rig := newLabelRig(t, h)
	withholds := blobwithhold.NewStore(testutil.DB(t))
	ctx := context.Background()
	authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")
	communityDID := testDIDFor("technology", "lemmy.world")
	otherDID := testDIDFor("someoneelse", "lemmy.world")
	const postID = "https://lemmy.world/post/b7-label-takedown"

	materializeImagePost(t, h, postID, "/media/b7-a.png", []byte("b7-label-takedown-image-a"))
	imageA := embedImageRef(t, h.recordFor(t, postID)).CID
	subject := subjectURIOf(t, h, postID)

	rig.commit(t, 1, labeling.Label{URI: subject, Val: "nsfw"})
	assert.False(t, covered(t, withholds, authorDID, imageA), "an nsfw label withholds nothing")

	rig.commit(t, 2, labeling.Label{URI: subject, Val: "!takedown"})
	assert.True(t, covered(t, withholds, authorDID, imageA), "the takedown withholds the image for the record's repo DID")
	assert.False(t, covered(t, withholds, otherDID, imageA), "the takedown is subject scope, not every owner")

	serveImage(h, "/media/b7-b.png", []byte("b7-label-takedown-image-b"))
	_, err := h.m.HandleUpdate(ctx, mustObject(t, imagePostPage(t, postID, "/media/b7-b.png")), groupID)
	require.NoError(t, err)
	imageB := embedImageRef(t, h.recordFor(t, postID)).CID
	require.NotEqual(t, imageA, imageB, "setup: the edit swapped the image")
	assert.True(t, covered(t, withholds, authorDID, imageB), "an edit under a standing takedown withholds its new image")

	operatorWithhold(t, withholds, imageA)
	_, err = withholds.Insert(ctx, blobwithhold.Withhold{
		CID:          imageA,
		Source:       blobwithhold.SourceCommunityRemoval,
		Scope:        blobwithhold.ScopeSubject,
		OwnerDID:     authorDID,
		SubjectURI:   subject,
		CommunityDID: communityDID,
		ReasonCode:   "moderator-discretion",
		CreatedBy:    "b7-community-moderator",
	})
	require.NoError(t, err)

	rig.commit(t, 3, labeling.Label{URI: subject, Val: "!takedown", Negated: negated()})
	wantLifted := []labelRowState{{CID: imageA, Lifted: true}, {CID: imageB, Lifted: true}}
	if imageB < imageA {
		wantLifted = []labelRowState{{CID: imageB, Lifted: true}, {CID: imageA, Lifted: true}}
	}
	assert.Equal(t, wantLifted, labelRowsOfSubject(t, subject), "the negation lifts every label row of the subject")
	assert.False(t, covered(t, withholds, authorDID, imageB), "after the negation the edit's image is no longer withheld")
	assert.True(t, covered(t, withholds, authorDID, imageA), "the operator and community-removal rows still withhold image A")
	assert.Equal(t, []string{blobwithhold.SourceCommunityRemoval, blobwithhold.SourceOperator}, activeSourcesOfCID(t, imageA),
		"the negation leaves the operator and community-removal rows on image A active")
}

// TestLabelTakedown_MaterializedAfterTheLabelIsWithheldInItsOwnCommit (B8):
// a record first materialized while a `!takedown` stands for its URI is
// withheld by its own commit: covered when MaterializePost returns, and the
// record is not committed when the withhold write fails.
func TestLabelTakedown_MaterializedAfterTheLabelIsWithheldInItsOwnCommit(t *testing.T) {
	authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")

	t.Run("the image is covered when MaterializePost returns", func(t *testing.T) {
		h := newHarness(t)
		h.serveLemmyWorldFixtures()
		rig := newLabelRig(t, h)
		withholds := blobwithhold.NewStore(testutil.DB(t))
		const postID = "https://lemmy.world/post/b8-label-before-record"
		subject := futurePostURI(t, authorDID, postID)

		rig.commit(t, 1, labeling.Label{URI: subject, Val: "!takedown"})
		materializeImagePost(t, h, postID, "/media/b8-before.png", []byte("b8-label-before-record-image"))
		require.Equal(t, subject, subjectURIOf(t, h, postID), "setup: the label named the record's at-uri")
		image := embedImageRef(t, h.recordFor(t, postID)).CID

		assert.True(t, covered(t, withholds, authorDID, image), "the record's image is withheld by its own commit")
	})

	t.Run("a failed withhold write leaves the record uncommitted", func(t *testing.T) {
		h := newHarness(t)
		h.serveLemmyWorldFixtures()
		rig := newLabelRig(t, h)
		ctx := context.Background()
		const postID = "https://lemmy.world/post/b8-label-withhold-fails"
		subject := futurePostURI(t, authorDID, postID)

		rig.commit(t, 1, labeling.Label{URI: subject, Val: "!takedown"})

		database := testutil.DB(t)
		_, err := database.Exec(`
			CREATE FUNCTION b8_refuse_label_withhold() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN
				RAISE EXCEPTION 'b8: label withhold refused';
			END $$;
			CREATE TRIGGER b8_refuse_label_withhold BEFORE INSERT ON blob_withholds
				FOR EACH ROW WHEN (NEW.source = 'label') EXECUTE FUNCTION b8_refuse_label_withhold();`)
		require.NoError(t, err)
		t.Cleanup(func() {
			if _, err := database.Exec(`
				DROP TRIGGER IF EXISTS b8_refuse_label_withhold ON blob_withholds;
				DROP FUNCTION IF EXISTS b8_refuse_label_withhold();`); err != nil {
				t.Errorf("drop b8 trigger: %v", err)
			}
		})

		serveImage(h, "/media/b8-fails.png", []byte("b8-label-withhold-fails-image"))
		_, err = h.m.MaterializePost(ctx, mustObject(t, imagePostPage(t, postID, "/media/b8-fails.png")), groupID)
		assert.Error(t, err, "the record's commit fails with its withhold write")

		_, _, rkey, ok := splitATURI(subject)
		require.True(t, ok)
		_, _, err = h.manager.GetRecord(ctx, authorDID, CollectionPostV2, rkey)
		assert.True(t, errors.IsNotFound(err), "the record is not committed without its withhold; err=%v", err)
	})
}

// TestLabelTakedown_RefusesStoresAndServesAgainAfterTheNegation (B9): while a
// takedown stands the image's bytes are refused (PutBlob returns the ref
// with IsBlobWithheld, a refresh keeps the record's ref, GetBlob answers
// absent); after the negation GetBlob serves the bytes with no upstream edit.
func TestLabelTakedown_RefusesStoresAndServesAgainAfterTheNegation(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	rig := newLabelRig(t, h)
	ctx := context.Background()
	authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")
	const postID = "https://lemmy.world/post/b9-label-refuses"
	imageBytes := []byte("b9-label-refuses-then-serves-image")

	materializeImagePost(t, h, postID, "/media/b9.png", imageBytes)
	stored := embedImageRef(t, h.recordFor(t, postID))
	subject := subjectURIOf(t, h, postID)

	rig.commit(t, 1, labeling.Label{URI: subject, Val: "!takedown"})

	ref, err := h.manager.PutBlob(ctx, authorDID, "image/png", imageBytes)
	assert.True(t, errors.IsBlobWithheld(err), "a store of the withheld bytes is refused, got %v", err)
	if assert.NotNil(t, ref, "the refused store still returns the ref") {
		assert.Equal(t, stored.CID, cid.Cid(ref.Ref).String())
	}

	post := imagePostPage(t, postID, "/media/b9.png")
	post["source"] = map[string]any{"content": "b9 refreshed body", "mediaType": "text/markdown"}
	_, err = h.m.HandleUpdate(ctx, mustObject(t, post), groupID)
	require.NoError(t, err)
	record := h.recordFor(t, postID)
	require.Equal(t, "b9 refreshed body", record["content"], "the refresh landed")
	assert.Equal(t, stored, embedImageRef(t, record), "the refreshed record keeps the withheld image's ref")

	_, _, err = h.manager.GetBlob(ctx, authorDID, stored.CID)
	assert.True(t, errors.IsNotFound(err), "while the takedown stands the image answers as absent, got %v", err)

	rig.commit(t, 2, labeling.Label{URI: subject, Val: "!takedown", Negated: negated()})
	data, mimeType, err := h.manager.GetBlob(ctx, authorDID, stored.CID)
	require.NoError(t, err, "after the negation the image is served again")
	assert.Equal(t, imageBytes, data)
	assert.Equal(t, "image/png", mimeType)
}

// storedLabel is a labels.Stored reduced to what B15 and B16 compare.
type storedLabel struct {
	Epoch, Seq int64
	Src, URI   string
	Val        string
	Neg        bool
	Cts        string
	Exp        *string
}

func standingLabels(t *testing.T, rig *labelRig, val string) []storedLabel {
	t.Helper()
	standing, err := rig.store.Standing(context.Background(), val)
	require.NoError(t, err)
	out := make([]storedLabel, 0, len(standing))
	for _, s := range standing {
		out = append(out, storedLabel{Epoch: s.Epoch, Seq: s.Seq, Src: s.Src, URI: s.URI, Val: s.Val, Neg: s.Neg, Cts: s.Cts, Exp: s.Exp})
	}
	return out
}

// TestLabelTakedown_StoredOnlySubjects (B15): a `!takedown` on a native
// record (its repo is not hosted here) and on a bare DID carrying `exp`
// commit and advance the cursor, are stored with their exp, and add no
// withhold rows.
func TestLabelTakedown_StoredOnlySubjects(t *testing.T) {
	h := newHarness(t)
	rig := newLabelRig(t, h)
	ctx := context.Background()
	const nativeRecord = "at://did:plc:ewvi7nxzyoun6zhxrhs64oiz/social.coves.community.post/3m2abcdefgh2a"
	const bareDID = "did:plc:yk4dd2qkboz2yv6tpubpc6co"
	exp := "2026-12-01T00:00:00.000Z"

	rig.commit(t, 1, labeling.Label{URI: nativeRecord, Val: "!takedown"})
	rig.commit(t, 2, labeling.Label{URI: bareDID, Val: "!takedown", ExpiresAt: &exp})

	cursor, err := rig.store.Cursor(ctx)
	require.NoError(t, err)
	assert.Equal(t, labels.Cursor{Epoch: 0, Seq: 2}, cursor, "the cursor advances past stored-only subjects")
	assert.ElementsMatch(t, []storedLabel{
		{Epoch: 0, Seq: 1, Src: labelSourceDID, URI: nativeRecord, Val: "!takedown", Cts: labelstest.DefaultCreatedAt},
		{Epoch: 0, Seq: 2, Src: labelSourceDID, URI: bareDID, Val: "!takedown", Cts: labelstest.DefaultCreatedAt, Exp: &exp},
	}, standingLabels(t, rig, "!takedown"), "both labels are stored, the DID's with its exp")

	var withholdRows int
	require.NoError(t, testutil.DB(t).QueryRow(`SELECT count(*) FROM blob_withholds`).Scan(&withholdRows))
	assert.Zero(t, withholdRows, "stored-only subjects add no withhold rows")
}

// TestLabelTakedown_SeqOrdersCtsNever (B16): a negation at a later seq
// supersedes the takedown even though its cts is earlier, so the subject is
// not standing and materializing it afterwards withholds nothing. A second
// subject's takedown, still standing, is the control.
func TestLabelTakedown_SeqOrdersCtsNever(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	rig := newLabelRig(t, h)
	withholds := blobwithhold.NewStore(testutil.DB(t))
	authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")
	const negatedPostID = "https://lemmy.world/post/b16-negated-by-seq"
	const standingPostID = "https://lemmy.world/post/b16-still-standing"
	negatedSubject := futurePostURI(t, authorDID, negatedPostID)
	standingSubject := futurePostURI(t, authorDID, standingPostID)

	rig.commit(t, 10, labeling.Label{URI: negatedSubject, Val: "!takedown", CreatedAt: "2026-11-08T13:00:00.000Z"})
	rig.commit(t, 11, labeling.Label{URI: negatedSubject, Val: "!takedown", Negated: negated(), CreatedAt: "2026-11-08T11:00:00.000Z"})
	rig.commit(t, 12, labeling.Label{URI: standingSubject, Val: "!takedown", CreatedAt: "2026-11-08T12:00:00.000Z"})

	assert.Equal(t, []storedLabel{
		{Epoch: 0, Seq: 12, Src: labelSourceDID, URI: standingSubject, Val: "!takedown", Cts: "2026-11-08T12:00:00.000Z"},
	}, standingLabels(t, rig, "!takedown"), "the later-seq negation wins over the later-cts takedown")

	materializeImagePost(t, h, negatedPostID, "/media/b16-negated.png", []byte("b16-negated-by-seq-image"))
	materializeImagePost(t, h, standingPostID, "/media/b16-standing.png", []byte("b16-still-standing-image"))
	require.Equal(t, negatedSubject, subjectURIOf(t, h, negatedPostID), "setup: the labels named the record's at-uri")
	require.Equal(t, standingSubject, subjectURIOf(t, h, standingPostID), "setup: the labels named the record's at-uri")
	assert.False(t, covered(t, withholds, authorDID, embedImageRef(t, h.recordFor(t, negatedPostID)).CID),
		"the negated subject's record withholds nothing")
	assert.True(t, covered(t, withholds, authorDID, embedImageRef(t, h.recordFor(t, standingPostID)).CID),
		"control: the standing subject's record is withheld")
}

// staticLabeler resolves every DID to one endpoint and key.
type staticLabeler struct {
	endpoint string
	key      atcrypto.PublicKey
}

func (r staticLabeler) Resolve(_ context.Context, did string) (labels.Labeler, error) {
	return labels.Labeler{DID: did, Endpoint: r.endpoint, Key: r.key}, nil
}

// labelWaitBound bounds every wait on the consumer in these tests.
const labelWaitBound = 10 * time.Second

// startLabelConsumer runs a consumer of labelSourceDID on store and returns
// a stop that cancels it and waits (bounded) for Run to return, plus a
// channel closed when Run returns.
func startLabelConsumer(t *testing.T, store labels.Store, resolver labels.KeyResolver) (<-chan struct{}, func()) {
	t.Helper()
	consumer, err := labels.NewConsumer(labels.ConsumerOptions{
		LabelerDID: labelSourceDID,
		Resolver:   resolver,
		Store:      store,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		MinBackoff: 10 * time.Millisecond,
		MaxBackoff: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	require.NotNil(t, consumer)
	ctx, cancel := context.WithCancel(context.Background())
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		consumer.Run(ctx)
	}()
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case <-returned:
		case <-time.After(labelWaitBound):
			t.Fatalf("Run did not return within %s of cancel", labelWaitBound)
		}
	}
	t.Cleanup(stop)
	return returned, stop
}

// awaitLabelConnection returns the cursor of the server's next connection;
// ok is false when Run returned or the bound passed first.
func awaitLabelConnection(server *labelstest.Server, returned <-chan struct{}) (cursor int64, ok bool) {
	select {
	case cursor = <-server.Connections():
		return cursor, true
	case <-returned:
		return 0, false
	case <-time.After(labelWaitBound):
		return 0, false
	}
}

// awaitLabelCursor polls the store until its cursor is want, Run returns, or
// the bound passes; the caller asserts on what was stored.
func awaitLabelCursor(store labels.Store, returned <-chan struct{}, want labels.Cursor) {
	deadline := time.Now().Add(labelWaitBound)
	for time.Now().Before(deadline) {
		if at, err := store.Cursor(context.Background()); err == nil && at == want {
			return
		}
		select {
		case <-returned:
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// storedLabelPositions is every stored label's (epoch, seq), in stream order.
func storedLabelPositions(t *testing.T) []labels.Cursor {
	t.Helper()
	rows, err := testutil.DB(t).Query(`SELECT reset_epoch, seq FROM coves_labels ORDER BY reset_epoch, seq`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	positions := []labels.Cursor{}
	for rows.Next() {
		var at labels.Cursor
		require.NoError(t, rows.Scan(&at.Epoch, &at.Seq))
		positions = append(positions, at)
	}
	require.NoError(t, rows.Err())
	return positions
}

// TestLabelConsumer_FutureCursorResetKeepsEarlierEpochLabels (B11): two
// hosted image posts are withheld by `!takedown` labels at (0,90) and (0,91).
// Phase 1: the labeler answers the stored cursor 91 with FutureCursor, so the
// consumer resets to (1,0) and reconnects from 0. Phase 2: after a restart
// the consumer resumes from 0 and the replayed stream's only label, a
// negation of record 1 at seq 1, lifts record 1 while the epoch-0 takedown of
// record 2 still stands. Phase 3: a further restart resumes from seq 1, never
// from the pre-reset 91.
func TestLabelConsumer_FutureCursorResetKeepsEarlierEpochLabels(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	rig := newLabelRig(t, h)
	withholds := blobwithhold.NewStore(testutil.DB(t))
	ctx := context.Background()
	authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")
	const firstPostID = "https://lemmy.world/post/b11-reset-first"
	const secondPostID = "https://lemmy.world/post/b11-reset-second"

	materializeImagePost(t, h, firstPostID, "/media/b11-first.png", []byte("b11-reset-first-image"))
	materializeImagePost(t, h, secondPostID, "/media/b11-second.png", []byte("b11-reset-second-image"))
	firstImage := embedImageRef(t, h.recordFor(t, firstPostID)).CID
	secondImage := embedImageRef(t, h.recordFor(t, secondPostID)).CID
	firstSubject := subjectURIOf(t, h, firstPostID)
	secondSubject := subjectURIOf(t, h, secondPostID)

	rig.commit(t, 90, labeling.Label{URI: firstSubject, Val: "!takedown"})
	rig.commit(t, 91, labeling.Label{URI: secondSubject, Val: "!takedown"})
	assert.True(t, covered(t, withholds, authorDID, firstImage), "setup: record 1 is withheld")
	assert.True(t, covered(t, withholds, authorDID, secondImage), "setup: record 2 is withheld")

	negation := rig.signed(t, labeling.Label{URI: firstSubject, Val: "!takedown", Negated: negated()})
	var phase atomic.Int32
	phase.Store(1)
	server := labelstest.NewServer(t, func(cursor int64) labelstest.Reply {
		switch {
		case cursor == 91:
			return labelstest.Reply{Frames: []labelstest.Frame{labelstest.FutureCursor(t)}}
		case cursor == 0 && phase.Load() == 2:
			return labelstest.Reply{Frames: []labelstest.Frame{labelstest.Labels(t, 1, negation)}, Hold: true}
		default:
			return labelstest.Reply{Hold: true}
		}
	})
	resolver := staticLabeler{endpoint: server.URL, key: labelstest.PublicKey(t, rig.key)}

	// Phase 1: FutureCursor at 91 resets the epoch; the cursor-0 connection
	// stays open with nothing on it.
	returned, stop := startLabelConsumer(t, rig.store, resolver)
	firstCursor, ok := awaitLabelConnection(server, returned)
	assert.True(t, ok, "phase 1: the consumer connects")
	assert.Equal(t, int64(91), firstCursor, "phase 1: the consumer resumes from the stored cursor 91")
	replayCursor, ok := awaitLabelConnection(server, returned)
	assert.True(t, ok, "phase 1: the consumer reconnects after FutureCursor")
	assert.Equal(t, int64(0), replayCursor, "phase 1: the reconnect replays from 0")
	awaitLabelCursor(rig.store, returned, labels.Cursor{Epoch: 1, Seq: 0})
	stop()

	cursor, err := rig.store.Cursor(ctx)
	require.NoError(t, err)
	assert.Equal(t, labels.Cursor{Epoch: 1, Seq: 0}, cursor, "phase 1: the reset is durable")

	// Phase 2: the restarted consumer replays from 0 and commits the negation.
	phase.Store(2)
	returned, stop = startLabelConsumer(t, rig.store, resolver)
	restartCursor, ok := awaitLabelConnection(server, returned)
	assert.True(t, ok, "phase 2: the consumer connects")
	assert.Equal(t, int64(0), restartCursor, "phase 2: the restart resumes from the reset cursor")
	awaitLabelCursor(rig.store, returned, labels.Cursor{Epoch: 1, Seq: 1})
	stop()

	assert.Equal(t, []labelRowState{{CID: firstImage, Lifted: true}}, labelRowsOfSubject(t, firstSubject),
		"phase 2: the negation lifts record 1's label rows")
	assert.False(t, covered(t, withholds, authorDID, firstImage), "phase 2: record 1's image is served again")
	assert.True(t, covered(t, withholds, authorDID, secondImage), "phase 2: record 2's epoch-0 takedown still withholds")
	assert.Equal(t, []labels.Cursor{{Epoch: 0, Seq: 90}, {Epoch: 0, Seq: 91}, {Epoch: 1, Seq: 1}}, storedLabelPositions(t),
		"phase 2: both epoch-0 labels and the replayed (1,1) are stored")
	assert.Equal(t, []storedLabel{
		{Epoch: 0, Seq: 91, Src: labelSourceDID, URI: secondSubject, Val: "!takedown", Cts: labelstest.DefaultCreatedAt},
	}, standingLabels(t, rig, "!takedown"), "phase 2: only record 2's takedown stands")
	cursor, err = rig.store.Cursor(ctx)
	require.NoError(t, err)
	assert.Equal(t, labels.Cursor{Epoch: 1, Seq: 1}, cursor, "phase 2: the cursor is (1,1)")

	// Phase 3: a further restart resumes from seq 1 of the new epoch.
	phase.Store(3)
	returned, stop = startLabelConsumer(t, rig.store, resolver)
	lastCursor, ok := awaitLabelConnection(server, returned)
	stop()
	assert.True(t, ok, "phase 3: the consumer connects")
	assert.Equal(t, int64(1), lastCursor, "phase 3: the restart resumes from 1, not the pre-reset 91")
	assert.Equal(t, []int64{91, 0, 0, 1}, server.Cursors(), "every connection's cursor, in order")
}

// labelRaceGate is what the test holds to line the label commit and the edit
// up: the repo commit lock, or a SHARE lock on blob_withholds that stops a
// label transaction at its first withhold write, after it has read the
// subject record (and, in production, taken the commit lock).
type labelRaceGate int

const (
	gateCommitLock labelRaceGate = iota
	gateWithholdTable
)

// holdLabelRaceGate takes gate in a transaction of its own and returns its
// release; the release is also registered for cleanup.
func holdLabelRaceGate(t *testing.T, gate labelRaceGate) func() {
	t.Helper()
	tx, err := testutil.DB(t).BeginTx(context.Background(), nil)
	require.NoError(t, err)
	switch gate {
	case gateCommitLock:
		require.NoError(t, repo.LockCommitsTx(context.Background(), tx))
	case gateWithholdTable:
		_, err = tx.Exec(`LOCK TABLE blob_withholds IN SHARE MODE`)
		require.NoError(t, err)
	}
	released := false
	release := func() {
		if released {
			return
		}
		released = true
		require.NoError(t, tx.Rollback())
	}
	t.Cleanup(func() {
		if !released {
			released = true
			_ = tx.Rollback()
		}
	})
	return release
}

// raceSide runs fn in a goroutine; done closes when fn returns, err is its
// result.
type raceSide struct {
	done chan struct{}
	err  error
}

func startRaceSide(fn func() error) *raceSide {
	s := &raceSide{done: make(chan struct{})}
	go func() {
		defer close(s.done)
		s.err = fn()
	}()
	return s
}

func (s *raceSide) returned() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// awaitSidesBlocked polls until every side still running waits on a lock in
// this database (one waiting backend per running side) or the bound passes.
// A side that returned instead of blocking counts as settled: the production
// code took no lock there, and the final state decides.
func awaitSidesBlocked(t *testing.T, sides ...*raceSide) {
	t.Helper()
	deadline := time.Now().Add(labelWaitBound)
	for {
		running := 0
		for _, s := range sides {
			if !s.returned() {
				running++
			}
		}
		var waiting int
		require.NoError(t, testutil.DB(t).QueryRow(`
			SELECT count(DISTINCT l.pid) FROM pg_locks l
			JOIN pg_database d ON d.oid = l.database
			WHERE NOT l.granted AND d.datname = current_database()`).Scan(&waiting))
		if waiting >= running {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d running sides blocked within %s", waiting, running, labelWaitBound)
		}
		<-time.After(5 * time.Millisecond)
	}
}

// awaitSide waits (bounded) for s to return and requires its success.
func awaitSide(t *testing.T, s *raceSide, what string) {
	t.Helper()
	select {
	case <-s.done:
		require.NoError(t, s.err, what)
	case <-time.After(labelWaitBound):
		t.Fatalf("%s did not return within %s", what, labelWaitBound)
	}
}

// TestLabelTakedown_RacesAnEditOfTheSubject (B13): a `!takedown` (or its
// negation) committing through the label store concurrently with an edit
// that swaps the subject's image A for B ends, in either commit order, with
// every blob of the committed record withheld for the author (takedown) or
// with no active label row of the subject (negation).
//
// The gate lines the two transactions up deterministically:
//   - label first (gateWithholdTable): the label transaction has inserted its
//     label row, taken the commit lock and read the record (still on A) when
//     it stops at its withhold write; the edit then starts and must wait on
//     the commit lock until the label commits, so it reads the label as
//     standing (takedown) or negated (negation). Without the label taking the
//     commit lock the edit reads the uncommitted label as absent, commits B
//     unwithheld (or re-adds a row for B after the negation lifted A's), and
//     the final state is wrong: this is the interleaving the lock prevents.
//   - edit first (gateCommitLock): the edit queues on the commit lock first,
//     then the label transaction inserts its row and queues behind it; on
//     release the edit commits B without seeing the uncommitted label, and
//     the label, reading the record under the lock, then sees B.
func TestLabelTakedown_RacesAnEditOfTheSubject(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	rig := newLabelRig(t, h)
	withholds := blobwithhold.NewStore(testutil.DB(t))
	authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")
	var seq int64

	tests := []struct {
		name       string
		slug       string
		negation   bool
		gate       labelRaceGate
		labelFirst bool
	}{
		{name: "takedown, label transaction first", slug: "b13-takedown-label-first", gate: gateWithholdTable, labelFirst: true},
		{name: "takedown, edit first", slug: "b13-takedown-edit-first", gate: gateCommitLock},
		{name: "negation, label transaction first", slug: "b13-negation-label-first", negation: true, gate: gateWithholdTable, labelFirst: true},
		{name: "negation, edit first", slug: "b13-negation-edit-first", negation: true, gate: gateCommitLock},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*labelWaitBound)
			defer cancel()
			postID := "https://lemmy.world/post/" + tc.slug
			materializeImagePost(t, h, postID, "/media/"+tc.slug+"-a.png", []byte(tc.slug+"-image-a"))
			imageA := embedImageRef(t, h.recordFor(t, postID)).CID
			subject := subjectURIOf(t, h, postID)
			if tc.negation {
				seq++
				rig.commit(t, seq, labeling.Label{URI: subject, Val: "!takedown"})
				require.True(t, covered(t, withholds, authorDID, imageA), "setup: the standing takedown withholds image A")
			}
			label := labeling.Label{URI: subject, Val: "!takedown"}
			if tc.negation {
				label.Negated = negated()
			}
			seq++
			at := labels.Cursor{Epoch: 0, Seq: seq}
			signed := rig.signed(t, label)
			serveImage(h, "/media/"+tc.slug+"-b.png", []byte(tc.slug+"-image-b"))
			edited := mustObject(t, imagePostPage(t, postID, "/media/"+tc.slug+"-b.png"))

			commitLabel := func() error { return rig.store.Commit(ctx, at, signed) }
			edit := func() error {
				_, err := h.m.HandleUpdate(ctx, edited, groupID)
				return err
			}

			release := holdLabelRaceGate(t, tc.gate)
			var labelSide, editSide *raceSide
			if tc.labelFirst {
				labelSide = startRaceSide(commitLabel)
				awaitSidesBlocked(t, labelSide)
				require.False(t, labelSide.returned(), "setup: the label transaction is held at its withhold write")
				editSide = startRaceSide(edit)
			} else {
				editSide = startRaceSide(edit)
				awaitSidesBlocked(t, editSide)
				require.False(t, editSide.returned(), "setup: the edit is held on the commit lock")
				labelSide = startRaceSide(commitLabel)
			}
			awaitSidesBlocked(t, labelSide, editSide)
			release()
			awaitSide(t, labelSide, "the label commit")
			awaitSide(t, editSide, "the edit")

			imageB := embedImageRef(t, h.recordFor(t, postID)).CID
			require.NotEqual(t, imageA, imageB, "setup: the edit swapped the image")
			if tc.negation {
				for _, row := range labelRowsOfSubject(t, subject) {
					assert.True(t, row.Lifted, "no label row of the subject stays active after the negation: %s", row.CID)
				}
				assert.False(t, covered(t, withholds, authorDID, imageB), "the edit's image is not withheld after the negation")
				assert.False(t, covered(t, withholds, authorDID, imageA), "image A is not withheld after the negation")
				return
			}
			assert.True(t, covered(t, withholds, authorDID, imageB), "the committed record's image B is withheld for the author")
		})
	}
}

// TestLabelTakedown_UnusableSubjectURIIsStoredAndSkipped: a signed
// `!takedown` whose at-uri is not a record a repo can hold (a handle
// authority, an invalid NSID, an invalid rkey) is stored, counted and
// skipped: its commit advances the cursor and the label after it commits
// too, so one bad label cannot stop the stream.
func TestLabelTakedown_UnusableSubjectURIIsStoredAndSkipped(t *testing.T) {
	for _, tc := range []struct{ name, uri string }{
		{"handle authority", "at://handle.example/coll/rkey"},
		{"invalid collection", "at://did:plc:ewvi7nxzyoun6zhxrhs64oiz/coll/3m2abcdefgh2a"},
		{"invalid rkey", "at://did:plc:ewvi7nxzyoun6zhxrhs64oiz/social.coves.community.post/bad%20key"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			rig := newLabelRig(t, h)
			ctx := context.Background()
			const nextSubject = "at://did:plc:ewvi7nxzyoun6zhxrhs64oiz/social.coves.community.post/3m2abcdefgh2a"
			skippedBefore := LabelsSkippedUnusableSubject.Value()

			rig.commit(t, 1, labeling.Label{URI: tc.uri, Val: "!takedown"})
			rig.commit(t, 2, labeling.Label{URI: nextSubject, Val: "!takedown"})

			cursor, err := rig.store.Cursor(ctx)
			require.NoError(t, err)
			assert.Equal(t, labels.Cursor{Epoch: 0, Seq: 2}, cursor, "both labels commit and the cursor advances past the unusable one")
			assert.Equal(t, skippedBefore+1, LabelsSkippedUnusableSubject.Value(), "the skipped label is counted")
			var stored int
			require.NoError(t, testutil.DB(t).QueryRow(`SELECT count(*) FROM coves_labels WHERE uri = $1`, tc.uri).Scan(&stored))
			assert.Equal(t, 1, stored, "the unusable label is stored")
		})
	}
}

// TestLabelTakedown_AuthorDeleteUnderAStandingTakedownKeepsTheBytes: the
// 2026-11-08 rule for a standing removal applies to a standing `!takedown`:
// the author's delete keeps the image's bytes and getBlob keeps refusing
// them; a later negation finds no record, so the label rows stay and the
// bytes stay refused until an operator's delete-bytes removes them.
func TestLabelTakedown_AuthorDeleteUnderAStandingTakedownKeepsTheBytes(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	rig := newLabelRig(t, h)
	withholds := blobwithhold.NewStore(testutil.DB(t))
	ctx := context.Background()
	authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")
	const postID = "https://lemmy.world/post/label-author-delete-keeps-bytes"

	materializeImagePost(t, h, postID, "/media/label-author-delete.png", []byte("label-author-delete-keeps-bytes-image"))
	image := embedImageRef(t, h.recordFor(t, postID)).CID
	subject := subjectURIOf(t, h, postID)
	mapping, err := h.objects.GetByAPID(ctx, postID)
	require.NoError(t, err)
	require.Equal(t, []string{authorDID}, blobHoldersOfCID(t, image), "setup: the image is stored under the author")

	rig.commit(t, 1, labeling.Label{URI: subject, Val: "!takedown"})
	require.NoError(t, h.m.HandleAuthorDeleteRecord(ctx, postID))

	_, _, err = h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
	require.True(t, errors.IsNotFound(err), "setup: the author's delete removed the record, got %v", err)
	assert.Equal(t, []string{authorDID}, blobHoldersOfCID(t, image), "the delete under a standing takedown keeps the bytes")
	var reasonCode string
	require.NoError(t, testutil.DB(t).QueryRowContext(ctx, `
		SELECT reason_code FROM blob_withholds WHERE source = 'label' AND subject_uri = $1`, subject).Scan(&reasonCode))
	assert.Equal(t, blobwithhold.ReasonLabelTakedownAuthorDeleted, reasonCode,
		"the author's delete marks the subject's label row so a negation keeps it")
	assert.True(t, covered(t, withholds, authorDID, image), "the kept bytes stay withheld")
	_, _, err = h.manager.GetBlob(ctx, authorDID, image)
	assert.True(t, errors.IsNotFound(err), "getBlob refuses the kept bytes, got %v", err)

	rig.commit(t, 2, labeling.Label{URI: subject, Val: "!takedown", Negated: negated()})
	assert.Equal(t, []labelRowState{{CID: image, Lifted: false}}, labelRowsOfSubject(t, subject),
		"a negation of a deleted subject keeps its label rows")
	assert.Equal(t, []string{authorDID}, blobHoldersOfCID(t, image), "the bytes stay stored after the negation")
	_, _, err = h.manager.GetBlob(ctx, authorDID, image)
	assert.True(t, errors.IsNotFound(err), "getBlob still refuses the bytes after the negation, got %v", err)
}

// TestLabelTakedown_AuthorDeleteOfAnUnlabeledSharerKeepsTheLabeledBytes: two
// records of one author share an image; the labeled one is deleted first (its
// label row is kept, author-deleted), then the unlabeled one. The second
// delete finds no remaining reference to the CID, but the first record's kept
// label row still claims the bytes, so they stay stored.
func TestLabelTakedown_AuthorDeleteOfAnUnlabeledSharerKeepsTheLabeledBytes(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	rig := newLabelRig(t, h)
	ctx := context.Background()
	authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")
	const labeledID = "https://lemmy.world/post/label-shared-cid-labeled"
	const sharerID = "https://lemmy.world/post/label-shared-cid-sharer"
	imageBytes := []byte("label-shared-cid-author-delete-image")

	materializeImagePost(t, h, labeledID, "/media/label-shared-cid-a.png", imageBytes)
	materializeImagePost(t, h, sharerID, "/media/label-shared-cid-b.png", imageBytes)
	image := embedImageRef(t, h.recordFor(t, labeledID)).CID
	require.Equal(t, image, embedImageRef(t, h.recordFor(t, sharerID)).CID, "setup: both records reference one CID")
	require.Equal(t, []string{authorDID}, blobHoldersOfCID(t, image), "setup: the image is stored under the author")

	rig.commit(t, 1, labeling.Label{URI: subjectURIOf(t, h, labeledID), Val: "!takedown"})
	require.NoError(t, h.m.HandleAuthorDeleteRecord(ctx, labeledID))
	require.NoError(t, h.m.HandleAuthorDeleteRecord(ctx, sharerID))

	assert.Equal(t, []string{authorDID}, blobHoldersOfCID(t, image),
		"the unlabeled sharer's delete keeps the bytes the labeled record's kept row claims")
}
