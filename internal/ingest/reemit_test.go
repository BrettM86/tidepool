package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/repo"
	"tidepool/internal/store"
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

func (f *fakeReemitter) GetRecord(_ context.Context, did, collection, rkey string) (map[string]any, string, error) {
	for _, e := range f.records[did] {
		if e.Collection == collection && e.Rkey == rkey {
			return e.Value, "", nil
		}
	}
	return nil, "", errors.NewNotFoundError("record", rkey)
}

// noBans is a ban reader with no ban standing anywhere.
type noBans struct{}

func (noBans) StandingBanRecord(context.Context, string, string) (*materialize.BanRecord, error) {
	return nil, nil
}

// fakeBans serves standing ban records keyed "community subject", and fails
// every read in failCommunity.
type fakeBans struct {
	records       map[string]*materialize.BanRecord
	failCommunity string
	reads         int
}

func (f *fakeBans) StandingBanRecord(_ context.Context, communityDID, subjectDID string) (*materialize.BanRecord, error) {
	f.reads++
	if communityDID == f.failCommunity {
		return nil, fmt.Errorf("ban store unavailable")
	}
	return f.records[communityDID+" "+subjectDID], nil
}

// fakeMappings serves stored ap_objects mappings by at-uri, answers NotFound
// for every other at-uri, and fails every read of failATURI.
type fakeMappings struct {
	byATURI   map[string]*store.APObjectMapping
	failATURI string
}

func (f fakeMappings) GetByATURI(_ context.Context, atURI string) (*store.APObjectMapping, error) {
	if atURI == f.failATURI {
		return nil, fmt.Errorf("mapping store unavailable")
	}
	if mapping, ok := f.byATURI[atURI]; ok {
		return mapping, nil
	}
	return nil, errors.NewNotFoundError("ap_object", atURI)
}

func entry(collection, rkey string) repo.RecordEntry {
	return repo.RecordEntry{Collection: collection, Rkey: rkey, Value: map[string]any{"$type": collection}}
}

func TestReemitRepo(t *testing.T) {
	logger := slog.Default()

	t.Run("emits profiles before posts before comments, delete then put each", func(t *testing.T) {
		f := &fakeReemitter{records: map[string][]repo.RecordEntry{
			"did:plc:c": {
				commentIn("c1", "at://did:plc:c/social.coves.community.post/p1"),
				entry("social.coves.community.post", "p1"),
				entry("social.coves.community.profile", "self"),
			},
		}}
		res := reemitRepo(t.Context(), f, noBans{}, fakeMappings{}, "did:plc:c", logger)
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
		res := reemitRepo(t.Context(), f, noBans{}, fakeMappings{}, "did:plc:c", logger)
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
		res := reemitRepo(t.Context(), f, noBans{}, fakeMappings{}, "did:plc:c", logger)
		require.Contains(t, res.Error, "RECORD DROPPED")
		assert.Equal(t, 0, res.Reemited)
	})

	t.Run("unknown repo reports the error", func(t *testing.T) {
		f := &fakeReemitter{records: map[string][]repo.RecordEntry{}}
		res := reemitRepo(t.Context(), f, noBans{}, fakeMappings{}, "did:plc:nope", logger)
		assert.NotEmpty(t, res.Error)
	})
}

// commentIn is a comment record whose thread root is the given post uri.
func commentIn(rkey, rootURI string) repo.RecordEntry {
	return repo.RecordEntry{Collection: materialize.CollectionComment, Rkey: rkey, Value: map[string]any{
		"$type": materialize.CollectionComment,
		"reply": map[string]any{
			"root":   map[string]any{"uri": rootURI, "cid": "bafyroot"},
			"parent": map[string]any{"uri": rootURI, "cid": "bafyroot"},
		},
	}}
}

// TestReemitRepoSkipsCommentsUnderStandingBan pins that re-emission never
// deletes and recreates a comment whose author is banned in force in the
// comment's community. Coves records a permanent community_ban hide on every
// comment it indexes while a ban stands, re-creations included, and that hide
// outlives the unban; re-emitting such a comment would hide it for good.
func TestReemitRepoSkipsCommentsUnderStandingBan(t *testing.T) {
	const (
		author          = "did:plc:author"
		bannedCommunity = "did:plc:banned"
		openCommunity   = "did:plc:open"
		lapsedCommunity = "did:plc:lapsed"
		v2Author        = "did:plc:v2author"
	)
	past := time.Now().Add(-time.Hour)
	future := time.Now().Add(time.Hour)
	newRecords := func() map[string][]repo.RecordEntry {
		return map[string][]repo.RecordEntry{
			author: {
				commentIn("banned-legacy", "at://"+bannedCommunity+"/social.coves.community.post/p1"),
				commentIn("banned-v2", "at://"+v2Author+"/social.coves.community.postv2/p2"),
				commentIn("open", "at://"+openCommunity+"/social.coves.community.post/p3"),
				commentIn("lapsed", "at://"+lapsedCommunity+"/social.coves.community.post/p4"),
				entry("social.coves.actor.profile", "self"),
			},
			v2Author: {{
				Collection: materialize.CollectionPostV2, Rkey: "p2",
				Value: map[string]any{"$type": materialize.CollectionPostV2, "community": bannedCommunity},
			}},
		}
	}

	t.Run("banned comments are skipped and counted, others re-emitted", func(t *testing.T) {
		f := &fakeReemitter{records: newRecords()}
		bans := &fakeBans{records: map[string]*materialize.BanRecord{
			bannedCommunity + " " + author: {Subject: author, ExpiresAt: &future},
			lapsedCommunity + " " + author: {Subject: author, ExpiresAt: &past},
		}}
		res := reemitRepo(t.Context(), f, bans, fakeMappings{}, author, slog.Default())
		require.Empty(t, res.Error)
		assert.Equal(t, 5, res.Records)
		assert.Equal(t, 2, res.SkippedBanned, "both comments in the banned community are skipped")
		assert.Equal(t, 3, res.Reemited, "the profile, the open-community comment and the lapsed-ban comment re-emit")
		for _, call := range f.calls {
			assert.NotContains(t, call, "/banned-", "a comment under a standing ban is neither deleted nor recreated")
		}
		putIndex(t, f.calls, author, materialize.CollectionComment, "open")
		putIndex(t, f.calls, author, materialize.CollectionComment, "lapsed")
		putIndex(t, f.calls, author, "social.coves.actor.profile", "self")
	})

	t.Run("a permanent ban skips too", func(t *testing.T) {
		f := &fakeReemitter{records: newRecords()}
		bans := &fakeBans{records: map[string]*materialize.BanRecord{
			bannedCommunity + " " + author: {Subject: author},
		}}
		res := reemitRepo(t.Context(), f, bans, fakeMappings{}, author, slog.Default())
		require.Empty(t, res.Error)
		assert.Equal(t, 2, res.SkippedBanned)
		assert.Equal(t, 3, res.Reemited)
	})

	t.Run("a failed ban read aborts the repo without touching the comment", func(t *testing.T) {
		f := &fakeReemitter{records: newRecords()}
		bans := &fakeBans{failCommunity: openCommunity}
		res := reemitRepo(t.Context(), f, bans, fakeMappings{}, author, slog.Default())
		require.Contains(t, res.Error, "ban store unavailable")
		for _, call := range f.calls {
			assert.NotContains(t, call, "comment/open", "the comment whose ban could not be read is left alone")
		}
	})

	// A bridged comment replying into a native Coves postv2 whose repo
	// Tidepool does not host: reply.root cannot be read, so only the stored
	// mapping's CommunityDID names the community.
	const unhostedRoot = "at://did:plc:native/social.coves.community.postv2/p9"
	unhostedCommentURI := "at://" + author + "/" + materialize.CollectionComment + "/unhosted"
	unhostedRecords := func() map[string][]repo.RecordEntry {
		return map[string][]repo.RecordEntry{
			author: {
				commentIn("unhosted", unhostedRoot),
				entry("social.coves.actor.profile", "self"),
			},
		}
	}

	t.Run("the stored mapping's community is used when the thread root is not hosted", func(t *testing.T) {
		f := &fakeReemitter{records: unhostedRecords()}
		bans := &fakeBans{records: map[string]*materialize.BanRecord{
			bannedCommunity + " " + author: {Subject: author, ExpiresAt: &future},
		}}
		mappings := fakeMappings{byATURI: map[string]*store.APObjectMapping{
			unhostedCommentURI: {
				DID: author, Collection: materialize.CollectionComment, RKey: "unhosted",
				CommunityDID: bannedCommunity,
			},
		}}
		res := reemitRepo(t.Context(), f, bans, mappings, author, slog.Default())
		require.Empty(t, res.Error)
		assert.Equal(t, 1, res.SkippedBanned, "the stored community's standing ban skips the comment")
		assert.Equal(t, 0, res.SkippedUnknownCommunity)
		assert.Equal(t, 1, res.Reemited, "only the profile re-emits")
		for _, call := range f.calls {
			assert.NotContains(t, call, "comment/unhosted", "a comment under a standing ban is neither deleted nor recreated")
		}
	})

	t.Run("a comment whose community cannot be determined is skipped and counted", func(t *testing.T) {
		f := &fakeReemitter{records: unhostedRecords()}
		res := reemitRepo(t.Context(), f, noBans{}, fakeMappings{}, author, slog.Default())
		require.Empty(t, res.Error)
		assert.Equal(t, 1, res.SkippedUnknownCommunity, "no stored community and an unreadable root leave the community unknown")
		assert.Equal(t, 0, res.SkippedBanned)
		assert.Equal(t, 1, res.Reemited, "only the profile re-emits")
		for _, call := range f.calls {
			assert.NotContains(t, call, "comment/unhosted", "a comment of unknown community is neither deleted nor recreated")
		}
	})

	t.Run("a failed mapping read aborts the repo without touching the comment", func(t *testing.T) {
		f := &fakeReemitter{records: unhostedRecords()}
		mappings := fakeMappings{failATURI: unhostedCommentURI}
		res := reemitRepo(t.Context(), f, noBans{}, mappings, author, slog.Default())
		require.Contains(t, res.Error, "mapping store unavailable")
		for _, call := range f.calls {
			assert.NotContains(t, call, "comment/unhosted", "the comment whose mapping could not be read is left alone")
		}
	})

	t.Run("a ban published mid-walk skips the community's later comments", func(t *testing.T) {
		f := &fakeReemitter{records: map[string][]repo.RecordEntry{
			author: {
				commentIn("first", "at://"+bannedCommunity+"/social.coves.community.post/p1"),
				commentIn("second", "at://"+bannedCommunity+"/social.coves.community.post/p2"),
			},
		}}
		bans := &banPublishedAfterFirstRead{ban: &materialize.BanRecord{Subject: author}}
		res := reemitRepo(t.Context(), f, bans, fakeMappings{}, author, slog.Default())
		require.Empty(t, res.Error)
		assert.Equal(t, 1, res.Reemited, "the comment checked before the ban re-emits")
		assert.Equal(t, 1, res.SkippedBanned, "the comment checked after the ban is skipped")
		for _, call := range f.calls {
			assert.NotContains(t, call, "comment/second", "a comment checked after the ban is neither deleted nor recreated")
		}
	})
}

// banPublishedAfterFirstRead answers no ban on its first read and ban on
// every read after, as if the ban were published during the repo walk.
type banPublishedAfterFirstRead struct {
	ban   *materialize.BanRecord
	reads int
}

func (b *banPublishedAfterFirstRead) StandingBanRecord(context.Context, string, string) (*materialize.BanRecord, error) {
	b.reads++
	if b.reads == 1 {
		return nil, nil
	}
	return b.ban, nil
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
			commentIn("c1", "at://"+did+"/social.coves.community.post/p1"),
			entry("social.coves.community.postv2", "p2"),
			entry("social.coves.community.removal", "r1"),
			entry("social.coves.community.post", "p1"),
			entry("social.coves.community.profile", "self"),
		},
	}}

	res := reemitRepo(t.Context(), f, noBans{}, fakeMappings{}, did, slog.Default())
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
