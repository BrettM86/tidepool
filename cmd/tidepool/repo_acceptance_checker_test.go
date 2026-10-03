package main

import (
	"context"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/acceptrec"
	"tidepool/internal/repo"
	"tidepool/internal/testutil"
)

// checkerKeys signs every repo commit with one key; the checker only reads.
type checkerKeys struct{ key *atcrypto.PrivateKeyK256 }

func (keys checkerKeys) SigningKey(context.Context, string, repo.KeyUse) (atcrypto.PrivateKey, error) {
	return keys.key, nil
}

func TestRepoAcceptanceCheckerReadsCommunityRepo(t *testing.T) {
	const (
		communityDID = "did:plc:44ybard66vv44zksje25o7dz"
		subjectURI   = "at://did:plc:7iza6de2dwap2sbkpav7c6c6/social.coves.community.postv2/3lzpostaaaa11"
		subjectCID   = "bafyreievgu2ty7qbiaaom5zhmkznsnajuzideek3lo7e65dwqlrvrxnmo4"
	)
	publishedAt := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	ctx := context.Background()

	database := testutil.DB(t)
	testutil.Truncate(t, database, "blocks", "repo_state", "firehose_events")
	key, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	manager, err := repo.NewManager(database, checkerKeys{key: key}, nil)
	require.NoError(t, err)
	checker := repoAcceptanceChecker{repos: manager}

	_, err = acceptrec.AcceptSubject(ctx, manager, communityDID, subjectURI, subjectCID, publishedAt, nil)
	require.NoError(t, err)
	stands, err := checker.AcceptanceStands(ctx, communityDID, subjectURI)
	require.NoError(t, err)
	assert.True(t, stands, "the community repo holds an acceptance for the subject")

	_, err = acceptrec.Remove(ctx, manager, communityDID, subjectURI, subjectCID,
		"moderator-discretion", "", publishedAt, nil)
	require.NoError(t, err)
	stands, err = checker.AcceptanceStands(ctx, communityDID, subjectURI)
	require.NoError(t, err)
	assert.False(t, stands, "a removed subject's acceptance no longer stands")
}
