package repo

import (
	"context"
	"database/sql/driver"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/blobstore"
	"tidepool/internal/blobwithhold"
	"tidepool/internal/errors"
	"tidepool/internal/testutil"
)

// onObjectGet runs action once, out of band, when the store's first GET of
// key reaches the proxy, before the GET is forwarded to MinIO.
func onObjectGet(env *s3TestEnv, key string, action func()) {
	objectPath := "/" + env.Bucket.Name + "/" + key
	var once sync.Once
	env.Proxy.SetHook(func(r *http.Request) int {
		if r.Method == http.MethodGet && r.URL.Path == objectPath {
			once.Do(action)
		}
		return 0
	})
}

// TestGetBlobS3Mode (B6): in s3 mode GetBlob answers a withheld blob, or one
// whose delete raced the read, as NotFound without logging; a missing,
// unreadable or wrong-length object is an internal error (never NotFound)
// logged with its code; a row still holding Postgres bytes is served from
// Postgres; the mime type always comes from the row.
func TestGetBlobS3Mode(t *testing.T) {
	ctx := context.Background()
	putBlob := func(t *testing.T, env *s3TestEnv, data string) {
		t.Helper()
		_, err := env.Manager.PutBlob(ctx, testDID, "image/png", []byte(data))
		require.NoError(t, err)
	}
	removeObject := func(t *testing.T, env *s3TestEnv, key string) {
		if err := env.Bucket.Client.RemoveObject(ctx, env.Bucket.Name, key, minio.RemoveObjectOptions{}); err != nil {
			t.Errorf("remove object %s: %v", key, err)
		}
	}

	for _, tc := range []struct {
		name  string
		cid   string
		key   string
		setup func(t *testing.T, env *s3TestEnv, cid, key string)
		// wantNotFound rows answer errors.IsNotFound; wantNoLogs rows also
		// log nothing at any level.
		wantNotFound bool
		wantNoLogs   bool
		// wantErrorCode rows fail with an error that is not NotFound and log
		// exactly one Error record with this code.
		wantErrorCode string
		// The other rows serve wantData with wantMime.
		wantData []byte
		wantMime string
	}{
		{
			name: "withheld blob",
			cid:  "bafkreibxhhso3osjsjv4ey52lgmmtyegqvqjakxbx7fhwggzf4y4bmpata",
			key:  "blobs/did:plc:ewvi7nxzyoun6zhxrhs64oiz/bafkreibxhhso3osjsjv4ey52lgmmtyegqvqjakxbx7fhwggzf4y4bmpata",
			setup: func(t *testing.T, env *s3TestEnv, cid, _ string) {
				putBlob(t, env, "b6 s3 get withheld")
				_, err := env.Withhold.Withhold(ctx, blobwithhold.OperatorRequest{
					CID:        cid,
					ReasonCode: blobwithhold.ReasonIllegalContent,
					Actor:      "blob-s3-get-test",
				})
				require.NoError(t, err)
			},
			wantNotFound: true,
		},
		{
			name: "row and object deleted between the row read and the GET",
			cid:  "bafkreifz7x2vwsjw3mcq2xnr7hahkopbmnzqgxu6iqrvoxdnhfudhlf5je",
			key:  "blobs/did:plc:ewvi7nxzyoun6zhxrhs64oiz/bafkreifz7x2vwsjw3mcq2xnr7hahkopbmnzqgxu6iqrvoxdnhfudhlf5je",
			setup: func(t *testing.T, env *s3TestEnv, cid, key string) {
				putBlob(t, env, "b6 s3 get delete races read")
				onObjectGet(env, key, func() {
					if _, err := env.DB.ExecContext(ctx,
						`DELETE FROM blobs WHERE did = $1 AND cid = $2`, testDID, cid); err != nil {
						t.Errorf("delete blobs row: %v", err)
					}
					removeObject(t, env, key)
				})
			},
			wantNotFound: true,
			wantNoLogs:   true,
		},
		{
			name: "withhold committed and object removed between the row read and the GET",
			cid:  "bafkreigawjkxjee25vfprwek5w6q3snguxs6nzmedosdzwd5aagvtqqxta",
			key:  "blobs/did:plc:ewvi7nxzyoun6zhxrhs64oiz/bafkreigawjkxjee25vfprwek5w6q3snguxs6nzmedosdzwd5aagvtqqxta",
			setup: func(t *testing.T, env *s3TestEnv, cid, key string) {
				putBlob(t, env, "b6 s3 get withhold races read")
				onObjectGet(env, key, func() {
					if _, err := env.Withhold.Withhold(ctx, blobwithhold.OperatorRequest{
						CID:        cid,
						ReasonCode: blobwithhold.ReasonIllegalContent,
						Actor:      "blob-s3-get-test",
					}); err != nil {
						t.Errorf("withhold: %v", err)
					}
					removeObject(t, env, key)
				})
			},
			wantNotFound: true,
			wantNoLogs:   true,
		},
		{
			name: "object removed out of band with the row still there",
			cid:  "bafkreihjswbl3h55hsezrez2yk7fv7chjchpb56mymsfzcmfn73bqcda6e",
			key:  "blobs/did:plc:ewvi7nxzyoun6zhxrhs64oiz/bafkreihjswbl3h55hsezrez2yk7fv7chjchpb56mymsfzcmfn73bqcda6e",
			setup: func(t *testing.T, env *s3TestEnv, _, key string) {
				putBlob(t, env, "b6 s3 get object missing")
				removeObject(t, env, key)
			},
			wantErrorCode: "blob_object_missing",
		},
		{
			name: "object overwritten with longer bytes",
			cid:  "bafkreidlzu6t6y6bd3a2vsohtqmcilzhv23l5zufuwyldn4ybawmvlgstm",
			key:  "blobs/did:plc:ewvi7nxzyoun6zhxrhs64oiz/bafkreidlzu6t6y6bd3a2vsohtqmcilzhv23l5zufuwyldn4ybawmvlgstm",
			setup: func(t *testing.T, env *s3TestEnv, _, key string) {
				putBlob(t, env, "b6 s3 get object longer")
				putObject(t, env.Bucket, key, "image/png", []byte("b6 s3 get object longer, then more"))
			},
			wantErrorCode: "blob_object_size_mismatch",
		},
		{
			name: "object overwritten with shorter bytes",
			cid:  "bafkreicyjthit7gt67cebgjbk4mwpe3oi4yiohqxkik7bqdpxhe4ykq7ka",
			key:  "blobs/did:plc:ewvi7nxzyoun6zhxrhs64oiz/bafkreicyjthit7gt67cebgjbk4mwpe3oi4yiohqxkik7bqdpxhe4ykq7ka",
			setup: func(t *testing.T, env *s3TestEnv, _, key string) {
				putBlob(t, env, "b6 s3 get object shorter")
				putObject(t, env.Bucket, key, "image/png", []byte("b6 short"))
			},
			wantErrorCode: "blob_object_size_mismatch",
		},
		{
			name: "row holding Postgres bytes",
			cid:  "bafkreicfvte3c3lkcq57wnfvjv54itaw7bg2tse3lh3d455lbekap4vbeu",
			key:  "blobs/did:plc:ewvi7nxzyoun6zhxrhs64oiz/bafkreicfvte3c3lkcq57wnfvjv54itaw7bg2tse3lh3d455lbekap4vbeu",
			setup: func(t *testing.T, env *s3TestEnv, cid, _ string) {
				_, err := env.DB.ExecContext(ctx, `
					INSERT INTO blobs (did, cid, mime_type, size, bytes)
					VALUES ($1, $2, 'image/jpeg', 24, $3)`, testDID, cid, []byte("b6 s3 get postgres bytes"))
				require.NoError(t, err)
			},
			wantData: []byte("b6 s3 get postgres bytes"),
			wantMime: "image/jpeg",
		},
		{
			name: "object Content-Type differs from the row",
			cid:  "bafkreihqlayuotdbt2k3btuns3stgulcsd7drctecr7g5doebtjzacnb2e",
			key:  "blobs/did:plc:ewvi7nxzyoun6zhxrhs64oiz/bafkreihqlayuotdbt2k3btuns3stgulcsd7drctecr7g5doebtjzacnb2e",
			setup: func(t *testing.T, env *s3TestEnv, _, key string) {
				putBlob(t, env, "b6 s3 get row mime wins")
				putObject(t, env.Bucket, key, "text/html", []byte("b6 s3 get row mime wins"))
			},
			wantData: []byte("b6 s3 get row mime wins"),
			wantMime: "image/png",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := s3Manager(t)
			tc.setup(t, env, tc.cid, tc.key)
			env.Logs.Reset()

			data, mimeType, err := env.Manager.GetBlob(ctx, testDID, tc.cid)
			switch {
			case tc.wantNotFound:
				require.Error(t, err)
				assert.True(t, errors.IsNotFound(err), "err=%v", err)
				assert.Nil(t, data)
				if tc.wantNoLogs {
					assert.Empty(t, logRecords(t, env.Logs), "a delete that raced the read logs nothing")
				}
			case tc.wantErrorCode != "":
				require.Error(t, err)
				assert.False(t, errors.IsNotFound(err), "the row is still there; err=%v", err)
				assert.Nil(t, data)
				assert.Equal(t, 1, errorRecordsWithCode(t, env.Logs, tc.wantErrorCode))
			default:
				require.NoError(t, err)
				assert.Equal(t, tc.wantData, data)
				assert.Equal(t, tc.wantMime, mimeType)
			}
		})
	}

	t.Run("MinIO cut off", func(t *testing.T) {
		const cidStr = "bafkreicoqyyxxvrrm2qukooa2nyp2yhdsps3crl33o7cn37vlg4vo4xrvy"
		manager, database, logs := cutOffS3Manager(t)
		_, err := database.ExecContext(ctx, `
			INSERT INTO blobs (did, cid, mime_type, size, bytes)
			VALUES ($1, $2, 'image/png', 23, NULL)`, testDID, cidStr)
		require.NoError(t, err)

		data, _, err := manager.GetBlob(ctx, testDID, cidStr)
		require.Error(t, err)
		assert.False(t, errors.IsNotFound(err), "an outage never looks like a removal; err=%v", err)
		assert.Nil(t, data)
		assert.Equal(t, 1, errorRecordsWithCode(t, logs, "blob_object_read_failed"))
	})

	t.Run("object body longer than the row size never ends", func(t *testing.T) {
		const (
			cidStr = "bafkreifkybttf2nw74loiod5jwidhzq5c3wnqqhagpykiwvjvdbi3vdjba"
			key    = "blobs/did:plc:ewvi7nxzyoun6zhxrhs64oiz/bafkreifkybttf2nw74loiod5jwidhzq5c3wnqqhagpykiwvjvdbi3vdjba"
		)
		env := s3Manager(t)
		putBlob(t, env, "b6 s3 get unbounded body")
		// Registered after s3Manager, so it runs before the proxy closes
		// (httptest.Server.Close waits for the blocked handler).
		release := make(chan struct{})
		t.Cleanup(func() { close(release) })
		objectPath := "/" + env.Bucket.Name + "/" + key
		env.Proxy.SetResponder(func(w http.ResponseWriter, r *http.Request) bool {
			if r.Method != http.MethodGet || r.URL.Path != objectPath {
				return false
			}
			// No Content-Length: the body is chunked, so only the read
			// bound stops at size+1.
			w.Header().Set("Content-Type", "image/png")
			w.Header().Set("ETag", `"0123456789abcdef0123456789abcdef"`)
			w.Header().Set("Last-Modified", time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC).Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("b6 s3 get unbounded body!"))
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			select {
			case <-release:
			case <-r.Context().Done():
			}
			return true
		})
		env.Logs.Reset()

		type result struct {
			data []byte
			err  error
		}
		done := make(chan result, 1)
		go func() {
			data, _, err := env.Manager.GetBlob(ctx, testDID, cidStr)
			done <- result{data: data, err: err}
		}()
		var got result
		select {
		case got = <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("GetBlob did not return within 5s of reading size+1 bytes")
		}
		require.Error(t, got.err)
		assert.False(t, errors.IsNotFound(got.err), "err=%v", got.err)
		assert.Nil(t, got.data)
		assert.Equal(t, 1, errorRecordsWithCode(t, env.Logs, "blob_object_size_mismatch"))
	})
}

// failingGetStore is a byte store whose Get fails with err, as a row read
// against a dropped database connection does.
type failingGetStore struct {
	blobstore.Store
	err error
}

func (s failingGetStore) Get(context.Context, string, string) ([]byte, string, error) {
	return nil, "", s.err
}

// TestGetBlobLogsObjectReadFailedOnlyForObjectReads: blob_object_read_failed
// names a failed bucket read with the caller still waiting. A byte store
// failure that is not a bucket read (a row read in postgres mode) and a
// bucket read the caller abandoned (its context cancelled) return an error,
// never NotFound, and log no blob_object_read_failed.
func TestGetBlobLogsObjectReadFailedOnlyForObjectReads(t *testing.T) {
	t.Run("postgres mode row read failure", func(t *testing.T) {
		const cidStr = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"
		rowReadErr := fmt.Errorf("blobstore: read blob %s for %s: %w", cidStr, testDID, driver.ErrBadConn)
		database := testutil.DB(t)
		manager, _, logs := loggedManager(t,
			WithBlobStore(failingGetStore{Store: blobstore.NewPostgres(database), err: rowReadErr}))

		data, _, err := manager.GetBlob(context.Background(), testDID, cidStr)
		require.Error(t, err)
		assert.ErrorIs(t, err, driver.ErrBadConn)
		assert.False(t, errors.IsNotFound(err), "err=%v", err)
		assert.Nil(t, data)
		assert.Equal(t, 0, errorRecordsWithCode(t, logs, "blob_object_read_failed"))
	})

	t.Run("s3 mode caller context cancelled during the GET", func(t *testing.T) {
		env := s3Manager(t)
		ref, err := env.Manager.PutBlob(context.Background(), testDID, "image/png", []byte("get blob caller cancelled"))
		require.NoError(t, err)
		cidStr := ref.Ref.String()
		key, err := blobstore.ObjectKey(testDID, cidStr)
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		onObjectGet(env, key, cancel)
		env.Logs.Reset()

		data, _, err := env.Manager.GetBlob(ctx, testDID, cidStr)
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
		assert.False(t, errors.IsNotFound(err), "err=%v", err)
		assert.Nil(t, data)
		assert.Equal(t, 0, errorRecordsWithCode(t, env.Logs, "blob_object_read_failed"))
	})
}
