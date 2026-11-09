package ingest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"tidepool/internal/materialize"
)

// AN AUTHOR'S DELETE REMOVES THE RECORD AND ITS UNSHARED BYTES TOGETHER.
//
// The bytes go in the record delete's own commit, with the references read
// under the commit lock against the tree after the delete: a failure anywhere
// leaves the record and its bytes both present (a retry converges), and a
// record committed to the same repo before that commit, reusing the CID,
// keeps the bytes.

// TestAnAuthorDeleteWhoseRecordDeleteFailsKeepsTheRecordAndItsImage.
//
// GIVEN U's live postv2 in C with its own image and a fault that fails every
// write of U's repo head; WHEN U's delete is applied THEN it fails and the
// record and its bytes are both still there; WHEN the fault is gone and the
// delete is applied again THEN the record and the bytes are gone.
func TestAnAuthorDeleteWhoseRecordDeleteFailsKeepsTheRecordAndItsImage(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h
	ctx := context.Background()

	const slug = "ad-fault-record"
	postID := world.imagePost(world.communityC, slug, slug+"-image")
	imageCID := recordBlobCIDs(t, h, postID)[0]

	dropFault := installFaultTrigger(t, h, "ad_fault_record", "repo_state", "INSERT OR UPDATE",
		fmt.Sprintf("NEW.did = '%s'", ubUserDID), "RAISE EXCEPTION 'injected record delete fault'")
	require.Error(t, h.mat.HandleAuthorDeleteRecord(ctx, postID), "the record delete fails")
	assertLive(t, h, postID, "the failed delete leaves the record")
	assertBlobStored(t, h, ubUserDID, imageCID, "the failed delete leaves the bytes with the record")

	dropFault()
	require.NoError(t, h.mat.HandleAuthorDeleteRecord(ctx, postID), "the retry")
	assertDeleted(t, h, postID, "the retry deletes the record")
	assertBlobGone(t, h, ubUserDID, imageCID, "the retry deletes the image")
}

// TestAnAuthorDeleteWhoseByteDeleteFailsKeepsTheRecordAndItsImage.
//
// GIVEN U's live postv2 in C with its own image and a fault that fails every
// delete of U's stored bytes; WHEN U's delete is applied THEN it fails and the
// record and its bytes are both still there; WHEN the fault is gone and the
// delete is applied again THEN the record and the bytes are gone. Deleting the
// record before the bytes would leave a retry that finds the record deleted
// and keeps the image served.
func TestAnAuthorDeleteWhoseByteDeleteFailsKeepsTheRecordAndItsImage(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h
	ctx := context.Background()

	const slug = "ad-fault-bytes"
	postID := world.imagePost(world.communityC, slug, slug+"-image")
	imageCID := recordBlobCIDs(t, h, postID)[0]

	dropFault := installFaultTrigger(t, h, "ad_fault_bytes", "blobs", "DELETE",
		fmt.Sprintf("OLD.did = '%s'", ubUserDID), "RAISE EXCEPTION 'injected byte delete fault'")
	require.Error(t, h.mat.HandleAuthorDeleteRecord(ctx, postID), "the byte delete fails")
	assertLive(t, h, postID, "the failed delete leaves the record")
	assertBlobStored(t, h, ubUserDID, imageCID, "the failed delete leaves the bytes")

	dropFault()
	require.NoError(t, h.mat.HandleAuthorDeleteRecord(ctx, postID), "the retry")
	assertDeleted(t, h, postID, "the retry deletes the record")
	assertBlobGone(t, h, ubUserDID, imageCID, "the retry deletes the image")
}

// TestAnAuthorDeleteKeepsTheBytesOfARecordCommittedWhileItWaited.
//
// GIVEN U's live postv2 in C with its own image, and a second record of U
// reusing that image whose commit holds the commit lock while U's delete
// starts; WHEN the second commit lands and the delete then runs THEN the
// post is deleted and the image stays stored and served: the references are
// read after the second commit, not before it.
func TestAnAuthorDeleteKeepsTheBytesOfARecordCommittedWhileItWaited(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h
	ctx := context.Background()

	const slug = "ad-concurrent"
	postID := world.imagePost(world.communityC, slug, slug+"-image")
	imageCID := recordBlobCIDs(t, h, postID)[0]
	copied := liveRecord(t, h, postID)

	// The second commit stalls on stallKey after taking the commit lock, at its
	// write of U's repo head, until the test releases the key.
	const stallKey int64 = 0x61640001
	stall, err := h.db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stall.Close() })
	_, err = stall.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, stallKey)
	require.NoError(t, err)
	released := false
	release := func() {
		if !released {
			released = true
			_, unlockErr := stall.ExecContext(ctx, `SELECT pg_advisory_unlock($1)`, stallKey)
			require.NoError(t, unlockErr)
		}
	}
	t.Cleanup(release)
	installFaultTrigger(t, h, "ad_stall_head", "repo_state", "INSERT OR UPDATE",
		fmt.Sprintf("NEW.did = '%s'", ubUserDID),
		fmt.Sprintf("PERFORM pg_advisory_lock(%d); PERFORM pg_advisory_unlock(%d)", stallKey, stallKey))

	secondCommit := make(chan error, 1)
	go func() {
		_, putErr := h.manager.PutRecord(ctx, ubUserDID, materialize.CollectionPostV2, "ad-concurrent-copy", copied)
		secondCommit <- putErr
	}()
	waitForAdvisoryWaiters(t, h, 1, 5*time.Second, true)

	authorDelete := make(chan error, 1)
	go func() { authorDelete <- h.mat.HandleAuthorDeleteRecord(ctx, postID) }()
	// The delete waits on the commit lock the second commit holds; an
	// implementation that waits elsewhere is given the same time to take any
	// reading it takes before its lock.
	waitForAdvisoryWaiters(t, h, 2, 2*time.Second, false)

	release()
	require.NoError(t, <-secondCommit, "the second record commits")
	require.NoError(t, <-authorDelete, "the author's delete")

	assertDeleted(t, h, postID, "the author deleted it")
	assertBlobStored(t, h, ubUserDID, imageCID, "the second record references the bytes")
	assertBlobServed(t, h, ubUserDID, imageCID, "the second record's image")
}

// installFaultTrigger creates a BEFORE event trigger on table, for rows
// matching when, whose body runs action and then lets the row through. It
// returns a func dropping the trigger, which also runs at cleanup.
func installFaultTrigger(t *testing.T, h *harness, name, table, event, when, action string) func() {
	t.Helper()
	ctx := context.Background()
	_, err := h.db.ExecContext(ctx, fmt.Sprintf(`
		CREATE FUNCTION %[1]s() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			%[2]s;
			IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
			RETURN NEW;
		END $$`, name, action))
	require.NoError(t, err)
	_, err = h.db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TRIGGER %[1]s BEFORE %[2]s ON %[3]s
		FOR EACH ROW WHEN (%[4]s) EXECUTE FUNCTION %[1]s()`, name, event, table, when))
	require.NoError(t, err)
	dropped := false
	drop := func() {
		if dropped {
			return
		}
		dropped = true
		if _, err := h.db.ExecContext(ctx, fmt.Sprintf(`DROP TRIGGER IF EXISTS %s ON %s`, name, table)); err != nil {
			t.Errorf("drop trigger %s: %v", name, err)
		}
		if _, err := h.db.ExecContext(ctx, fmt.Sprintf(`DROP FUNCTION IF EXISTS %s()`, name)); err != nil {
			t.Errorf("drop function %s: %v", name, err)
		}
	}
	t.Cleanup(drop)
	return drop
}

// waitForAdvisoryWaiters polls until at least count sessions wait on a
// one-argument advisory lock other than testutil's package lock (other test
// binaries wait on that one). Past timeout it fails the test when required,
// and otherwise returns.
// testutilPackageLockKey is testutil's advisoryLockKey.
const testutilPackageLockKey int64 = 0x7469646570

func waitForAdvisoryWaiters(t *testing.T, h *harness, count int, timeout time.Duration, required bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		var waiting int
		require.NoError(t, h.db.QueryRowContext(context.Background(),
			`SELECT count(*) FROM pg_locks
			 WHERE locktype = 'advisory' AND NOT granted AND objsubid = 1
			   AND (classid::bigint << 32 | objid::bigint) <> $1`, testutilPackageLockKey).Scan(&waiting))
		if waiting >= count {
			return
		}
		if time.Now().After(deadline) {
			if required {
				t.Fatalf("%d sessions wait on an advisory lock after %s; want %d", waiting, timeout, count)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}
