package acceptrec

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/repo"
	"tidepool/internal/testutil"
)

// The world these pins run in.
const (
	arCommunityDID = "did:plc:44ybard66vv44zksje25o7dz"
	arAuthorDID    = "did:plc:7iza6de2dwap2sbkpav7c6c6"
	arPostRKey     = "3lzpostaaaa11"
	arPostURI      = "at://" + arAuthorDID + "/social.coves.community.postv2/" + arPostRKey
	arPostCID      = "bafyreievgu2ty7qbiaaom5zhmkznsnajuzideek3lo7e65dwqlrvrxnmo4"
)

var arPublishedAt = time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)

// staticKeys signs every DID with one fixed key — acceptrec only needs the repo
// layer to be able to sign the community's acceptance commit; key custody has
// its own tests.
type staticKeys struct{ key *atcrypto.PrivateKeyK256 }

func (s staticKeys) SigningKey(context.Context, string, repo.KeyUse) (atcrypto.PrivateKey, error) {
	return s.key, nil
}

func newRepos(t *testing.T) (*repo.Manager, *sql.DB) {
	t.Helper()
	database := testutil.DB(t)
	testutil.Truncate(t, database, "blocks", "repo_state", "firehose_events")
	key, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	manager, err := repo.NewManager(database, staticKeys{key: key}, nil)
	require.NoError(t, err)
	return manager, database
}

// marker is a scratch table a side effect writes into, so a test can prove the
// side effect ran (and committed) or did not.
func newMarker(t *testing.T, database *sql.DB) {
	t.Helper()
	_, err := database.Exec(`CREATE TABLE IF NOT EXISTS acceptrec_marker (note TEXT NOT NULL)`)
	require.NoError(t, err)
	_, err = database.Exec(`TRUNCATE acceptrec_marker`)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = database.Exec(`DROP TABLE IF EXISTS acceptrec_marker`) })
}

func markerCount(t *testing.T, database *sql.DB) int {
	t.Helper()
	var n int
	require.NoError(t, database.QueryRow(`SELECT COUNT(*) FROM acceptrec_marker`).Scan(&n))
	return n
}

func writeMarker(ctx context.Context) repo.TxSideEffect {
	return func(_ context.Context, tx *sql.Tx, _ *repo.CommitResult) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO acceptrec_marker (note) VALUES ('enqueued')`)
		return err
	}
}

// ---------------------------------------------------------------------------
// Golden: acceptrec.SubjectRKey must byte-match Coves and the materializer.
// ---------------------------------------------------------------------------

// The golden values are copied verbatim from the materializer's
// subject_rkey_test.go (which copied them from Coves' rkey_test.go, computed
// OUTSIDE Go). If acceptrec's derivation forks from either, a post acquires two
// acceptance keys and neither engine can see the other's record.
func TestSubjectRKey_GoldenVectorMatchesCoves(t *testing.T) {
	t.Parallel()
	assert.Equal(t,
		"xxdmibjaexx43drostplutjbp7g4oaw3uriugf5twafpldfkupca",
		SubjectRKey("at://did:plc:abc123/social.coves.community.postv2/3kjzl5kcb2s2v"))
	assert.Equal(t,
		"iyhgczhg7xsbrayzrrs2qa4fks6amctx7ghyjakqyrlhxztbbl5a",
		SubjectRKey("at://did:plc:abc123/social.coves.community.postv2/3kjzl5kcb2s2w"))
	// The long-subject vector: a truncating implementation would pass a shape
	// check but collide two long subjects. Pinned to a golden value (copied from
	// the materializer's longDIDWeb vector).
	assert.Equal(t,
		"fktiwazbhqfsqjg5e7yypm3ukbalk72bfplcr2d2ukm7iuwqeyqa",
		SubjectRKey("at://"+longDIDWeb()+"/social.coves.community.postv2/3kjzl5kcb2s2v"))
	assert.Len(t, SubjectRKey(arPostURI), 52, "the key is always a fixed 52 characters")
}

func longDIDWeb() string {
	label := ""
	for i := 0; i < 63; i++ {
		label += "a"
	}
	authority := ""
	for i := 0; i < 8; i++ {
		if i > 0 {
			authority += "."
		}
		authority += label
	}
	return "did:web:" + authority + ".example.com"
}

// ---------------------------------------------------------------------------
// AcceptSubject
// ---------------------------------------------------------------------------

// readAcceptance returns the acceptance record + CID at the subject's rkey, or
// fails the lookup with IsNotFound when none stands.
func readAcceptance(t *testing.T, repos *repo.Manager, communityDID, subjectURI string) (map[string]any, string, error) {
	t.Helper()
	return repos.GetRecord(context.Background(), communityDID, CollectionAcceptance, SubjectRKey(subjectURI))
}

// TestAcceptSubject_WritesRecordAndRunsSideEffectAtomically: a fresh accept
// writes the acceptance record pinning the post's uri+cid at SubjectRKey, and
// runs the side effect inside the same commit.
func TestAcceptSubject_WritesRecordAndRunsSideEffectAtomically(t *testing.T) {
	repos, database := newRepos(t)
	newMarker(t, database)
	ctx := context.Background()

	res, err := AcceptSubject(ctx, repos, arCommunityDID, arPostURI, arPostCID, arPublishedAt, writeMarker(ctx))
	require.NoError(t, err, "a fresh accept with no standing removal must succeed")
	require.NotNil(t, res)
	assert.False(t, res.NoOp, "the first acceptance is a real commit")

	record, _, err := readAcceptance(t, repos, arCommunityDID, arPostURI)
	require.NoError(t, err,
		"the acceptance record must exist in the COMMUNITY repo at SubjectRKey(%s)", arPostURI)
	assert.Equal(t, CollectionAcceptance, record["$type"])
	subject, ok := record["subject"].(map[string]any)
	require.True(t, ok, "the acceptance pins the subject as a strongRef, got %v", record["subject"])
	assert.Equal(t, arPostURI, subject["uri"], "the strongRef pins the post at-uri")
	assert.Equal(t, arPostCID, subject["cid"], "the strongRef pins the evaluated post CID")

	assert.Equal(t, 1, markerCount(t, database),
		"the side effect (the outbound enqueue) must run in the acceptance commit: "+
			"acceptance and enqueue land together or not at all")
}

// TestAcceptSubject_RemovalStandsRefusesAndSkipsSideEffect: a removal is
// terminal, so an accept over one must be refused — no acceptance written, and
// the side effect NOT run (a removal means the community decided the post is
// out; re-firing the enqueue would federate a post that must stay hidden).
func TestAcceptSubject_RemovalStandsRefusesAndSkipsSideEffect(t *testing.T) {
	repos, database := newRepos(t)
	newMarker(t, database)
	ctx := context.Background()

	// A standing removal at the shared rkey.
	_, err := repos.PutRecord(ctx, arCommunityDID, CollectionRemoval, SubjectRKey(arPostURI),
		map[string]any{"$type": CollectionRemoval, "subject": map[string]any{"uri": arPostURI, "cid": arPostCID}, "code": "moderator-discretion", "createdAt": "2026-08-12T10:00:00.000Z"})
	require.NoError(t, err)

	_, err = AcceptSubject(ctx, repos, arCommunityDID, arPostURI, arPostCID, arPublishedAt, writeMarker(ctx))
	require.ErrorIs(t, err, ErrRemovalStands,
		"an accept over a standing removal must refuse with ErrRemovalStands (removal terminality)")

	_, _, aerr := readAcceptance(t, repos, arCommunityDID, arPostURI)
	assert.True(t, errors.IsNotFound(aerr), "no acceptance may be written while a removal stands")
	assert.Zero(t, markerCount(t, database),
		"the side effect must NOT run when the accept is refused: the removal-guard fails the "+
			"batch BEFORE the side effect, so no enqueue fires for a decided-out post")
}

// TestAcceptSubject_RedeliveryRePutsNoOpButStillRunsSideEffect: re-accepting the
// same post produces a byte-identical record (createdAt derived, not stamped),
// so the repo layer takes the NoOp path — but the side effect STILL runs, so
// the at-least-once outbound enqueue re-fires (matching task 15's idempotent
// activity dedup).
func TestAcceptSubject_RedeliveryRePutsNoOpButStillRunsSideEffect(t *testing.T) {
	repos, database := newRepos(t)
	newMarker(t, database)
	ctx := context.Background()

	first, err := AcceptSubject(ctx, repos, arCommunityDID, arPostURI, arPostCID, arPublishedAt, writeMarker(ctx))
	require.NoError(t, err)
	require.NotNil(t, first)
	assert.False(t, first.NoOp)
	require.Equal(t, 1, markerCount(t, database))

	// The redelivery: same subject, same CID, same publishedAt.
	second, err := AcceptSubject(ctx, repos, arCommunityDID, arPostURI, arPostCID, arPublishedAt, writeMarker(ctx))
	require.NoError(t, err)
	require.NotNil(t, second)
	assert.True(t, second.NoOp,
		"an identical re-accept must produce a NoOp record: createdAt is derived, not stamped, "+
			"so the bytes match and the repo layer's no-op path absorbs it")
	assert.Equal(t, 2, markerCount(t, database),
		"the side effect must run AGAIN on the redelivery even though the record did not change: "+
			"the outbound enqueue is at-least-once and dedupe is the peer's job")
}
