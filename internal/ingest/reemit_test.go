package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/repo"
)

// fakeReemitter records the delete/put sequence the re-emit core drives.
type fakeReemitter struct {
	dids    []string
	records map[string][]repo.RecordEntry
	// calls logs "delete did coll/rkey" / "put did coll/rkey" in order.
	calls []string
	// failPut, when matching "coll/rkey", fails that PutRecord.
	failPut string
	// missingDelete, when matching "coll/rkey", makes DeleteRecord NotFound.
	missingDelete string
}

func (f *fakeReemitter) ListDIDs(context.Context) ([]string, error) { return f.dids, nil }

func (f *fakeReemitter) ListRecords(_ context.Context, did string) ([]repo.RecordEntry, error) {
	entries, ok := f.records[did]
	if !ok {
		return nil, errors.NewNotFoundError("repo", did)
	}
	return entries, nil
}

func (f *fakeReemitter) DeleteRecord(_ context.Context, did, collection, rkey string) (*repo.CommitResult, error) {
	if collection+"/"+rkey == f.missingDelete {
		return nil, errors.NewNotFoundError("record", rkey)
	}
	f.calls = append(f.calls, fmt.Sprintf("delete %s %s/%s", did, collection, rkey))
	return &repo.CommitResult{}, nil
}

func (f *fakeReemitter) PutRecord(_ context.Context, did, collection, rkey string, _ map[string]any) (*repo.CommitResult, error) {
	if collection+"/"+rkey == f.failPut {
		return nil, fmt.Errorf("boom")
	}
	f.calls = append(f.calls, fmt.Sprintf("put %s %s/%s", did, collection, rkey))
	return &repo.CommitResult{}, nil
}

func entry(collection, rkey string) repo.RecordEntry {
	return repo.RecordEntry{Collection: collection, Rkey: rkey, Value: map[string]any{"$type": collection}}
}

func TestReemitRepo(t *testing.T) {
	logger := slog.Default()

	t.Run("emits profiles before posts before comments, delete then put each", func(t *testing.T) {
		f := &fakeReemitter{records: map[string][]repo.RecordEntry{
			"did:plc:c": {
				entry("social.coves.community.comment", "c1"),
				entry("social.coves.community.post", "p1"),
				entry("social.coves.community.profile", "self"),
			},
		}}
		res := reemitRepo(t.Context(), f, "did:plc:c", logger)
		require.Empty(t, res.Error)
		assert.Equal(t, 3, res.Records)
		assert.Equal(t, 3, res.Reemited)
		assert.Equal(t, []string{
			"delete did:plc:c social.coves.community.profile/self",
			"put did:plc:c social.coves.community.profile/self",
			"delete did:plc:c social.coves.community.post/p1",
			"put did:plc:c social.coves.community.post/p1",
			"delete did:plc:c social.coves.community.comment/c1",
			"put did:plc:c social.coves.community.comment/c1",
		}, f.calls)
	})

	t.Run("a record vanished mid-run is skipped, not fatal", func(t *testing.T) {
		f := &fakeReemitter{
			records: map[string][]repo.RecordEntry{
				"did:plc:c": {entry("social.coves.community.post", "p1"), entry("social.coves.community.post", "p2")},
			},
			missingDelete: "social.coves.community.post/p1",
		}
		res := reemitRepo(t.Context(), f, "did:plc:c", logger)
		require.Empty(t, res.Error)
		assert.Equal(t, 1, res.Reemited, "the vanished record is skipped, the other re-emits")
	})

	t.Run("recreate failure aborts loudly", func(t *testing.T) {
		f := &fakeReemitter{
			records: map[string][]repo.RecordEntry{
				"did:plc:c": {entry("social.coves.community.post", "p1"), entry("social.coves.community.post", "p2")},
			},
			failPut: "social.coves.community.post/p1",
		}
		res := reemitRepo(t.Context(), f, "did:plc:c", logger)
		require.Contains(t, res.Error, "RECORD DROPPED")
		assert.Equal(t, 0, res.Reemited)
	})

	t.Run("unknown repo reports the error", func(t *testing.T) {
		f := &fakeReemitter{records: map[string][]repo.RecordEntry{}}
		res := reemitRepo(t.Context(), f, "did:plc:nope", logger)
		assert.NotEmpty(t, res.Error)
	})
}

// putIndex finds where a record's re-create landed in the call sequence.
func putIndex(t *testing.T, calls []string, did, collection, rkey string) int {
	t.Helper()
	want := fmt.Sprintf("put %s %s/%s", did, collection, rkey)
	for i, call := range calls {
		if call == want {
			return i
		}
	}
	t.Fatalf("no re-emit of %s/%s in %v", collection, rkey, calls)
	return -1
}

// TestReemitRepoOrdersBothPostEras pins the re-emit ordering across the two
// post eras and the community-side moderation records.
//
// The rank exists so an indexer never sees a record before the thing it
// references. postv2 falling into the default bucket puts an author's
// comments ahead of the root posts they reply to — the exact dangling-
// reference the ordering was written to prevent, reintroduced by the flip
// because the rank matched on the deprecated collection name only.
//
// Acceptance and removal rank AFTER content: reemitRepo iterates the repo's
// RECORDS (ListRecords), not ap_objects mappings, so these are re-emitted
// like anything else — and each strongRef-pins a post, so they must not
// precede one. (Their subject lives in the AUTHOR's repo while they live in
// the COMMUNITY's, and cross-repo order is relay-dependent regardless; this
// is the within-repo half the bridge can actually control.)
func TestReemitRepoOrdersBothPostEras(t *testing.T) {
	const did = "did:plc:community"
	// Deliberately adversarial input order: every record before the one it
	// should follow.
	f := &fakeReemitter{records: map[string][]repo.RecordEntry{
		did: {
			entry("social.coves.community.acceptance", "a1"),
			entry("social.coves.community.comment", "c1"),
			entry("social.coves.community.postv2", "p2"),
			entry("social.coves.community.removal", "r1"),
			entry("social.coves.community.post", "p1"),
			entry("social.coves.community.profile", "self"),
		},
	}}

	res := reemitRepo(t.Context(), f, did, slog.Default())
	require.Empty(t, res.Error)
	require.Equal(t, 6, res.Reemited)

	profile := putIndex(t, f.calls, did, "social.coves.community.profile", "self")
	legacyPost := putIndex(t, f.calls, did, "social.coves.community.post", "p1")
	postV2 := putIndex(t, f.calls, did, "social.coves.community.postv2", "p2")
	comment := putIndex(t, f.calls, did, "social.coves.community.comment", "c1")
	acceptance := putIndex(t, f.calls, did, "social.coves.community.acceptance", "a1")
	removal := putIndex(t, f.calls, did, "social.coves.community.removal", "r1")

	assert.Less(t, postV2, comment,
		"a postv2 must be re-emitted BEFORE comments, exactly as a legacy post is: a comment "+
			"reply.root-pins its post, and an indexer that sees the comment first has a dangling ref")
	assert.Less(t, profile, postV2, "profiles still precede content of either era")
	assert.Less(t, profile, legacyPost)
	assert.Less(t, comment, acceptance,
		"acceptance records strongRef a post and must follow all content")
	assert.Less(t, comment, removal,
		"removal records strongRef a post and must follow all content")
	assert.Less(t, postV2, acceptance)
	assert.Less(t, postV2, removal)
}
