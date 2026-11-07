package votes

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/repo"
	"tidepool/internal/store"
)

// Bridged vote records (PLAN.md decision 21): an eligible bridged voter's
// Like/Dislike becomes a social.coves.feed.vote record in their own repo and
// leaves the served aggregate; every other vote stays aggregate.

const (
	voteCollection = "social.coves.feed.vote"

	// The record's subject: bridgeSubject(subjectPost, subjectRKey) maps it to
	// at://testDID/testCollection/subjectRKey with cid testCID.
	subjectRKey  = "3jzfcijpj2z2a"
	subjectATURI = "at://" + testDID + "/" + testCollection + "/" + subjectRKey

	aliceDID = "did:plc:recordsalice000000000000"
	bobDID   = "did:plc:recordsbob00000000000000"
)

// staticSigningKeys signs every repo with one key (internal/repo's staticKeys
// shape): this package tests what is committed, not key custody.
type staticSigningKeys struct{ key *atcrypto.PrivateKeyK256 }

func (s *staticSigningKeys) SigningKey(context.Context, string, repo.KeyUse) (atcrypto.PrivateKey, error) {
	return s.key, nil
}

// nopSubjectRefresher repins nothing.
type nopSubjectRefresher struct{}

func (nopSubjectRefresher) RefreshSubject(context.Context, string) error { return nil }

// recordsWorld is an Aggregator with a RecordWriter over a real repo.Manager,
// and one bridged subject (subjectPost).
type recordsWorld struct {
	db      *sql.DB
	agg     *Aggregator
	objects store.APObjects
	manager *repo.Manager
}

// recordsBoth is the default writer configuration.
var recordsBoth = RecordWriterOptions{Mode: RecordsBoth, Interval: time.Minute, Batch: 20}

func newRecordsWorld(t *testing.T, opts RecordWriterOptions) *recordsWorld {
	t.Helper()
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	key, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	manager, err := repo.NewManager(database, &staticSigningKeys{key: key}, nil)
	require.NoError(t, err)
	writer, err := NewRecordWriter(database, manager, objects, nopSubjectRefresher{}, opts, slog.Default())
	require.NoError(t, err)
	agg.SetRecordWriter(writer)
	require.Equal(t, subjectATURI, bridgeSubject(t, objects, subjectPost, subjectRKey))
	return &recordsWorld{db: database, agg: agg, objects: objects, manager: manager}
}

// bridgedVoter registers a bridged person with the given consent and, when
// withRepo, gives it a repo through a genesis actor.profile commit.
func (w *recordsWorld) bridgedVoter(t *testing.T, apID, did string, consent store.ConsentState, withRepo bool) {
	t.Helper()
	ctx := context.Background()
	_, err := store.NewBridgedActors(w.db).UpsertActor(ctx, store.BridgedActor{
		APActorID:    apID,
		ActorType:    store.ActorTypePerson,
		DID:          did,
		ConsentState: consent,
	})
	require.NoError(t, err)
	if withRepo {
		_, err = w.manager.PutRecord(ctx, did, "social.coves.actor.profile", "self", map[string]any{
			"$type":       "social.coves.actor.profile",
			"displayName": "voter",
		})
		require.NoError(t, err)
	}
}

// eligibleVoter is a bridged person, consent ok, with a repo.
func (w *recordsWorld) eligibleVoter(t *testing.T, apID, did string) {
	t.Helper()
	w.bridgedVoter(t, apID, did, store.ConsentStateOK, true)
}

// voteRecords lists the feed.vote records in a voter's repo.
func (w *recordsWorld) voteRecords(t *testing.T, did string) []repo.RecordEntry {
	t.Helper()
	entries, err := w.manager.ListRecords(context.Background(), did)
	require.NoError(t, err)
	var out []repo.RecordEntry
	for _, entry := range entries {
		if entry.Collection == voteCollection {
			out = append(out, entry)
		}
	}
	return out
}

// voteRow is one vote_events row with the record columns.
type voteRow struct {
	undone      bool
	createdAt   time.Time
	recordATURI sql.NullString
	recordState sql.NullString
}

func readVoteRow(t *testing.T, database *sql.DB, activity string) voteRow {
	t.Helper()
	var row voteRow
	require.NoError(t, database.QueryRow(`
		SELECT undone, created_at, record_at_uri, record_state
		FROM vote_events WHERE activity_id = $1`, activity).Scan(
		&row.undone, &row.createdAt, &row.recordATURI, &row.recordState))
	return row
}

// firehoseEvents counts the commit events for one DID.
func firehoseEvents(t *testing.T, database *sql.DB, did string) int {
	t.Helper()
	var n int
	require.NoError(t, database.QueryRow(`SELECT COUNT(*) FROM firehose_events WHERE did = $1`, did).Scan(&n))
	return n
}

// firehoseOpsSince returns the ops of every commit for did with seq > after,
// one slice per commit, in seq order.
func firehoseOpsSince(t *testing.T, database *sql.DB, did string, after int64) [][]repo.Op {
	t.Helper()
	rows, err := database.Query(`
		SELECT ops FROM firehose_events WHERE did = $1 AND seq > $2 ORDER BY seq`, did, after)
	require.NoError(t, err)
	defer rows.Close()
	var out [][]repo.Op
	for rows.Next() {
		var raw []byte
		require.NoError(t, rows.Scan(&raw))
		var ops []repo.Op
		require.NoError(t, json.Unmarshal(raw, &ops))
		out = append(out, ops)
	}
	require.NoError(t, rows.Err())
	return out
}

func latestSeq(t *testing.T, database *sql.DB) int64 {
	t.Helper()
	var seq int64
	require.NoError(t, database.QueryRow(`SELECT COALESCE(MAX(seq), 0) FROM firehose_events`).Scan(&seq))
	return seq
}

func tableCount(t *testing.T, database *sql.DB, table string) int {
	t.Helper()
	var n int
	require.NoError(t, database.QueryRow(`SELECT COUNT(*) FROM `+table).Scan(&n))
	return n
}

// B1.
func TestEligibleBridgedVoterLikeBecomesFeedVoteRecord(t *testing.T) {
	w := newRecordsWorld(t, recordsBoth)
	ctx := context.Background()
	w.eligibleVoter(t, voterAlice, aliceDID)

	aliceLike := activityID(t, 1)
	require.NoError(t, w.agg.ApplyVote(ctx, like(aliceLike, voterAlice, subjectPost), ""))
	// Bob has no bridged actor: his vote stays aggregate.
	require.NoError(t, w.agg.ApplyVote(ctx, like(activityID(t, 2), voterBob, subjectPost), ""))

	records := w.voteRecords(t, aliceDID)
	require.Len(t, records, 1, "alice's Like must be a feed.vote record in her repo")
	rkey := records[0].Rkey

	record, _, err := w.manager.GetRecord(ctx, aliceDID, voteCollection, rkey)
	require.NoError(t, err)
	assert.Equal(t, voteCollection, record["$type"])
	assert.Equal(t, "up", record["direction"])
	assert.Equal(t, map[string]any{"uri": subjectATURI, "cid": testCID}, record["subject"])

	row := readVoteRow(t, w.db, aliceLike)
	assert.False(t, row.undone)
	assert.Equal(t, sql.NullString{String: "written", Valid: true}, row.recordState)
	assert.Equal(t, sql.NullString{String: "at://" + aliceDID + "/" + voteCollection + "/" + rkey, Valid: true},
		row.recordATURI)

	// rkey: a TID whose timestamp is the time the bridge first recorded the vote.
	tid, err := syntax.ParseTID(rkey)
	require.NoError(t, err, "rkey %q must be a TID", rkey)
	assert.True(t, tid.Time().Equal(row.createdAt.Truncate(time.Microsecond)),
		"TID time %s, row created_at %s", tid.Time().UTC(), row.createdAt.UTC())

	createdAt, ok := record["createdAt"].(string)
	require.True(t, ok, "createdAt must be a string, got %T", record["createdAt"])
	assert.True(t, strings.HasSuffix(createdAt, "Z"), "createdAt %q must be UTC", createdAt)
	parsed, err := time.Parse(time.RFC3339Nano, createdAt)
	require.NoError(t, err)
	assert.True(t, parsed.Truncate(time.Second).Equal(row.createdAt.Truncate(time.Second)),
		"record createdAt %s, row created_at %s", parsed, row.createdAt.UTC())

	up, down, found := counts(t, w.db, subjectPost)
	require.True(t, found)
	assert.Equal(t, 1, up, "served counts only bob's unrecorded Like")
	assert.Equal(t, 0, down)
}

// B2.
func TestVotesThatStayAggregate(t *testing.T) {
	const personaVoter = "https://coves.social/ap/actor/did:plc:recordspersona0000000000"

	tests := []struct {
		name  string
		opts  RecordWriterOptions
		setup func(t *testing.T, w *recordsWorld)
		voter string
		did   string // the voter's DID when bridged; "" when not
		vote  func(id, voter, subject string) *ap.Object
		// wantUp/wantDown are the served counts after the vote; wantRow is
		// whether a vote_events row exists.
		wantUp, wantDown int
		wantRow          bool
	}{
		{
			name:  "no bridged actor",
			opts:  recordsBoth,
			setup: func(*testing.T, *recordsWorld) {},
			voter: voterBob,
			vote:  like, wantUp: 1, wantRow: true,
		},
		{
			name: "bridged person without a repo",
			opts: recordsBoth,
			setup: func(t *testing.T, w *recordsWorld) {
				w.bridgedVoter(t, voterAlice, aliceDID, store.ConsentStateOK, false)
			},
			voter: voterAlice, did: aliceDID,
			vote: like, wantUp: 1, wantRow: true,
		},
		{
			name: "consent nobridge with a repo",
			opts: recordsBoth,
			setup: func(t *testing.T, w *recordsWorld) {
				w.bridgedVoter(t, voterAlice, aliceDID, store.ConsentStateNoBridge, true)
			},
			voter: voterAlice, did: aliceDID,
			vote: like, wantUp: 1, wantRow: true,
		},
		{
			name: "consent deleted with a repo",
			opts: recordsBoth,
			setup: func(t *testing.T, w *recordsWorld) {
				w.bridgedVoter(t, voterAlice, aliceDID, store.ConsentStateDeleted, true)
			},
			voter: voterAlice, did: aliceDID,
			vote: like, wantUp: 1, wantRow: true,
		},
		{
			name:  "mode up and a Dislike",
			opts:  RecordWriterOptions{Mode: RecordsUp, Interval: time.Minute, Batch: 20},
			setup: func(t *testing.T, w *recordsWorld) { w.eligibleVoter(t, voterAlice, aliceDID) },
			voter: voterAlice, did: aliceDID,
			vote: dislike, wantDown: 1, wantRow: true,
		},
		{
			name:  "dry run with mode both",
			opts:  RecordWriterOptions{Mode: RecordsBoth, DryRun: true, Interval: time.Minute, Batch: 20},
			setup: func(t *testing.T, w *recordsWorld) { w.eligibleVoter(t, voterAlice, aliceDID) },
			voter: voterAlice, did: aliceDID,
			vote: like, wantUp: 1, wantRow: true,
		},
		{
			name: "our own persona",
			opts: recordsBoth,
			setup: func(t *testing.T, w *recordsWorld) {
				_, err := store.NewAPActors(w.db).Create(context.Background(), store.APActor{
					DID:              "did:plc:recordspersona0000000000",
					Kind:             store.ActorTypePerson,
					ActorID:          personaVoter,
					NormalizedOrigin: "coves.social",
					LocalPart:        "recordspersona",
					RSAKeySealed:     []byte{0x01},
					RSAKeyVersion:    1,
					PublicKeyPEM:     "-----BEGIN PUBLIC KEY-----\nMIIB\n-----END PUBLIC KEY-----\n",
				})
				require.NoError(t, err)
			},
			voter: personaVoter,
			vote:  like, wantRow: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := newRecordsWorld(t, tc.opts)
			ctx := context.Background()
			tc.setup(t, w)
			actorsBefore := tableCount(t, w.db, "bridged_actors")
			reposBefore := tableCount(t, w.db, "repo_state")
			seqBefore := latestSeq(t, w.db)

			id := activityID(t, 1)
			require.NoError(t, w.agg.ApplyVote(ctx, tc.vote(id, tc.voter, subjectPost), ""))

			assert.Equal(t, actorsBefore, tableCount(t, w.db, "bridged_actors"), "the vote path never mints")
			assert.Equal(t, reposBefore, tableCount(t, w.db, "repo_state"), "the vote path never creates a repo")
			assert.Equal(t, seqBefore, latestSeq(t, w.db), "no commit")
			if tc.did != "" {
				if _, _, err := w.manager.Head(ctx, tc.did); err == nil {
					assert.Empty(t, w.voteRecords(t, tc.did))
				}
			}

			if !tc.wantRow {
				assert.Equal(t, 0, allVotes(t, w.db, tc.voter))
				_, _, found := counts(t, w.db, subjectPost)
				assert.False(t, found, "a dropped vote leaves no aggregate")
				return
			}
			row := readVoteRow(t, w.db, id)
			assert.False(t, row.recordATURI.Valid, "record_at_uri must be NULL")
			assert.False(t, row.recordState.Valid, "record_state must be NULL")
			up, down, found := counts(t, w.db, subjectPost)
			require.True(t, found)
			assert.Equal(t, tc.wantUp, up)
			assert.Equal(t, tc.wantDown, down)
		})
	}
}

// B3.
func TestRecordedVoteFlipIsOneCommit(t *testing.T) {
	w := newRecordsWorld(t, recordsBoth)
	ctx := context.Background()
	w.eligibleVoter(t, voterAlice, aliceDID)

	likeID, dislikeID := activityID(t, 1), activityID(t, 2)
	require.NoError(t, w.agg.ApplyVote(ctx, like(likeID, voterAlice, subjectPost), ""))
	before := w.voteRecords(t, aliceDID)
	require.Len(t, before, 1, "the Like must be recorded before the flip")
	oldPath := voteCollection + "/" + before[0].Rkey
	seq := latestSeq(t, w.db)

	require.NoError(t, w.agg.ApplyVote(ctx, dislike(dislikeID, voterAlice, subjectPost), ""))

	after := w.voteRecords(t, aliceDID)
	require.Len(t, after, 1)
	assert.Equal(t, "down", after[0].Value["direction"])
	newPath := voteCollection + "/" + after[0].Rkey
	require.NotEqual(t, oldPath, newPath)

	commits := firehoseOpsSince(t, w.db, aliceDID, seq)
	require.Len(t, commits, 1, "a flip is exactly one commit")
	actions := map[string]repo.OpAction{}
	for _, op := range commits[0] {
		actions[op.Path] = op.Action
	}
	assert.Equal(t, map[string]repo.OpAction{oldPath: repo.OpActionDelete, newPath: repo.OpActionCreate}, actions)

	oldRow, newRow := readVoteRow(t, w.db, likeID), readVoteRow(t, w.db, dislikeID)
	assert.Equal(t, sql.NullString{String: "removed", Valid: true}, oldRow.recordState)
	assert.Equal(t, sql.NullString{String: "written", Valid: true}, newRow.recordState)

	up, down, found := counts(t, w.db, subjectPost)
	require.True(t, found)
	assert.Equal(t, 0, up)
	assert.Equal(t, 0, down)
}

// B4.
func TestUndoOfRecordedLikeDeletesTheRecord(t *testing.T) {
	tests := []struct {
		name              string
		softDeleteSubject bool
	}{
		{name: "subject mapped"},
		{name: "subject mapping soft-deleted before the undo", softDeleteSubject: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := newRecordsWorld(t, recordsBoth)
			ctx := context.Background()
			w.eligibleVoter(t, voterAlice, aliceDID)

			likeID := activityID(t, 1)
			require.NoError(t, w.agg.ApplyVote(ctx, like(likeID, voterAlice, subjectPost), ""))
			require.Len(t, w.voteRecords(t, aliceDID), 1, "the Like must be recorded before the undo")

			if tc.softDeleteSubject {
				require.NoError(t, w.objects.SoftDelete(ctx, subjectPost))
			}
			// Lemmy's Undo reconstructs the inner Like with a fresh id.
			require.NoError(t, w.agg.RetractVote(ctx, like(activityID(t, 99), voterAlice, subjectPost), ""))

			assert.Empty(t, w.voteRecords(t, aliceDID), "the undone vote's record must be deleted")
			row := readVoteRow(t, w.db, likeID)
			assert.True(t, row.undone)
			assert.Equal(t, sql.NullString{String: "removed", Valid: true}, row.recordState)
			up, down, found := counts(t, w.db, subjectPost)
			require.True(t, found)
			assert.Equal(t, 0, up)
			assert.Equal(t, 0, down)
		})
	}
}

// B5.
func TestRedeliveredRecordedLikeCommitsNothing(t *testing.T) {
	w := newRecordsWorld(t, recordsBoth)
	ctx := context.Background()
	w.eligibleVoter(t, voterAlice, aliceDID)

	id := activityID(t, 1)
	require.NoError(t, w.agg.ApplyVote(ctx, like(id, voterAlice, subjectPost), ""))
	require.Len(t, w.voteRecords(t, aliceDID), 1, "the first delivery must be recorded")
	commits := firehoseEvents(t, w.db, aliceDID)

	require.NoError(t, w.agg.ApplyVote(ctx, like(id, voterAlice, subjectPost), ""))

	assert.Len(t, w.voteRecords(t, aliceDID), 1)
	assert.Equal(t, commits, firehoseEvents(t, w.db, aliceDID), "re-delivery commits nothing")
}

// B6.
func TestSeedNetsRecordedVotesOutOfTheBaseline(t *testing.T) {
	w := newRecordsWorld(t, recordsBoth)
	ctx := context.Background()
	w.eligibleVoter(t, voterAlice, aliceDID)

	require.NoError(t, w.agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))
	require.NoError(t, w.agg.ApplyVote(ctx, like(activityID(t, 2), voterBob, subjectPost), ""))

	// The API total 5 contains both live Likes: alice's is a record (Coves
	// counts it natively), bob's is live aggregate.
	require.NoError(t, w.agg.SeedAggregates(ctx, subjectPost, 5, 0))

	up, _, found := counts(t, w.db, subjectPost)
	require.True(t, found)
	assert.Equal(t, 4, up, "served = 3 seeded + bob's live Like")
	var seeded int
	require.NoError(t, w.db.QueryRow(`
		SELECT seeded_upvotes FROM vote_aggregates WHERE subject_ap_id = $1`, subjectPost).Scan(&seeded))
	assert.Equal(t, 3, seeded)
}
