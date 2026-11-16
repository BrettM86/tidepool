package blobstore_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/blobstore"
	"tidepool/internal/testutil"
)

// cancelAtFirstPut cancels the run's context the moment the first PUT is
// sent, then forwards it, so that upload fails with the cancellation.
type cancelAtFirstPut struct {
	next   http.RoundTripper
	cancel context.CancelFunc
	once   sync.Once
}

func (c *cancelAtFirstPut) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.Method == http.MethodPut {
		c.once.Do(c.cancel)
	}
	return c.next.RoundTrip(request)
}

// TestMigratePostgresBytesStoppedMidPage: a run whose context is cancelled
// at the first upload of a page returns the cancellation, counts no more
// rows failed than were in flight, logs no blob_object_put_failed alarm and
// leaves every row with its bytes.
func TestMigratePostgresBytesStoppedMidPage(t *testing.T) {
	const (
		did         = "did:plc:migratecancelaaaaaaaaaa"
		rowCount    = 10
		concurrency = 1
	)
	database := migrationDB(t)
	bucket := testutil.NewS3Bucket(t)
	cids := make([]string, rowCount)
	payloads := make([][]byte, rowCount)
	for i := range rowCount {
		payloads[i] = []byte(fmt.Sprintf("migrate cancel row %d", i))
		cids[i] = testutil.RawBlobCID(t, payloads[i])
		insertPostgresBytesBlob(t, database, did, cids[i], payloads[i])
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var log bytes.Buffer
	store, err := blobstore.NewS3(database, blobstore.S3Config{
		Endpoint:              bucket.Endpoint,
		Region:                bucket.Region,
		Bucket:                bucket.Name,
		AccessKeyID:           bucket.AccessKeyID,
		SecretAccessKey:       bucket.SecretAccessKey,
		AllowPrivateAddresses: true,
		Logger:                slog.New(slog.NewJSONHandler(&log, nil)),
		Transport:             &cancelAtFirstPut{next: testutil.LocalS3Transport(), cancel: cancel},
	})
	require.NoError(t, err)

	report, err := store.MigratePostgresBytes(ctx, blobstore.MigrationOptions{Concurrency: concurrency, PageSize: 100})

	require.ErrorIs(t, err, context.Canceled)
	assert.LessOrEqual(t, report.Failed, concurrency, "only rows in flight at the cancel may count failed")
	assert.Zero(t, report.Copied, "nothing was copied")
	assert.NotContains(t, log.String(), "blob_object_put_failed", "a cancelled upload is not an upload alarm")
	for i := range rowCount {
		var held []byte
		require.NoError(t, database.QueryRowContext(context.Background(),
			`SELECT bytes FROM blobs WHERE did = $1 AND cid = $2`, did, cids[i]).Scan(&held))
		assert.Equal(t, payloads[i], held, "row %d keeps its bytes", i)
		_, found := objectBytes(t, bucket, did, cids[i])
		assert.False(t, found, "row %d has no object", i)
	}
}
