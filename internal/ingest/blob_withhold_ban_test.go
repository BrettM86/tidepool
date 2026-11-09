package ingest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/blobwithhold"
)

// TestASiteBanKeepsTheBytesOfAnOperatorWithheldImage (B10a).
//
// GIVEN bridged user U with two pre-ban postv2s, each with its own image, and
// an active operator withhold on the first image's CID; WHEN U's home admin
// site-bans U with removeData (every selected record goes through the
// shared-blob-aware delete) THEN both posts are deleted as today, the
// unwithheld image's blobs row is gone, and the withheld image's blobs row
// stays under U's DID: an operator withhold keeps the bytes as evidence until
// an operator deletes them.
func TestASiteBanKeepsTheBytesOfAnOperatorWithheldImage(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h
	ctx := context.Background()

	heldPost := world.postWithImageSeed(ubUserID, world.communityC, "ub-wh-held", ubBeforeBan, "ub-wh-seed-held-b10a")
	plainPost := world.postWithImageSeed(ubUserID, world.communityD, "ub-wh-plain", ubBeforeBan2, "ub-wh-seed-plain-b10a")
	heldCID := postBlobCID(t, h, heldPost)
	plainCID := postBlobCID(t, h, plainPost)
	require.NotEqual(t, heldCID, plainCID, "precondition: each post has its own image blob")

	t.Cleanup(func() {
		if _, err := h.db.Exec(`DELETE FROM blob_withholds WHERE cid = $1`, heldCID); err != nil {
			t.Errorf("cleanup blob_withholds: %v", err)
		}
	})
	_, err := blobwithhold.NewStore(h.db).Withhold(ctx, blobwithhold.OperatorRequest{
		CID:        heldCID,
		ReasonCode: blobwithhold.ReasonIllegalContent,
		Actor:      "b10a-operator",
	})
	require.NoError(t, err)

	const banID = ubHomeHost + "/activities/block/ub-wh-site-ban"
	banTime := ubBanTime
	world.deliverAt(world.admin, siteBlock(banID, ubAdminID, ubUserID, ubHomeRoot, nil), &banTime)
	assertProcessed(t, h, banID)

	assertDeleted(t, h, heldPost, "pre-ban post with the withheld image")
	assertDeleted(t, h, plainPost, "pre-ban post with an unwithheld image")
	assert.Empty(t, blobRowOwners(t, h, plainCID), "the unwithheld image's bytes go with its post")
	assert.Equal(t, []string{ubUserDID}, blobRowOwners(t, h, heldCID),
		"the operator-withheld image's blobs row must stay under the banned author's DID")
}

// blobRowOwners returns every DID holding a blobs row for cidStr.
func blobRowOwners(t *testing.T, h *harness, cidStr string) []string {
	t.Helper()
	rows, err := h.db.Query(`SELECT did FROM blobs WHERE cid = $1 ORDER BY did`, cidStr)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var dids []string
	for rows.Next() {
		var did string
		require.NoError(t, rows.Scan(&did))
		dids = append(dids, did)
	}
	require.NoError(t, rows.Err())
	return dids
}
