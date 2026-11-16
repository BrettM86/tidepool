package blobstore_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/blobstore"
)

// TestObjectKey (B1): ObjectKey builds blobs/<did>/<cid> for a canonical
// CID and a DID that cannot escape the blobs/<did>/ prefix, and refuses
// everything else.
func TestObjectKey(t *testing.T) {
	const (
		validDID = "did:plc:abcdefghijklmnopqrstuvwx"
		validCID = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"
		// validCID rendered in base58btc: parses, but re-renders as base32.
		base58CID = "zb2rhmy65F3REf8SZp7De11gxtECBGgUKaLdiDj7MCGCHxbDW"
	)
	for _, tc := range []struct {
		name    string
		did     string
		cid     string
		wantKey string
		wantErr bool
	}{
		{name: "valid pair", did: validDID, cid: validCID,
			wantKey: "blobs/did:plc:abcdefghijklmnopqrstuvwx/bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"},
		{name: "non-canonical cid", did: validDID, cid: base58CID, wantErr: true},
		{name: "unparseable cid", did: validDID, cid: "not-a-cid", wantErr: true},
		{name: "empty cid", did: validDID, cid: "", wantErr: true},
		{name: "did containing a slash", did: "did:plc:abc/../other", cid: validCID, wantErr: true},
		{name: "did dot", did: ".", cid: validCID, wantErr: true},
		{name: "did dot dot", did: "..", cid: validCID, wantErr: true},
		{name: "empty did", did: "", cid: validCID, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key, err := blobstore.ObjectKey(tc.did, tc.cid)
			if tc.wantErr {
				assert.Error(t, err, "key=%q", key)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantKey, key)
		})
	}
}
