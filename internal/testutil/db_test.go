package testutil

import (
	"strings"
	"testing"
)

func TestPackageDatabaseName(t *testing.T) {
	// FNV-1a of the directory, computed outside Go (docs/E2E.md derives the
	// repo package's database the same way).
	if got, want := packageDatabaseName("tidepool_test", "/src/tidepool/internal/repo"), "tidepool_test_p3c6d9aba"; got != want {
		t.Errorf("packageDatabaseName = %q, want %q", got, want)
	}

	repo := packageDatabaseName("tidepool_test", "/src/tidepool/internal/repo")
	store := packageDatabaseName("tidepool_test", "/src/tidepool/internal/store")
	worktree := packageDatabaseName("tidepool_test", "/src/tidepool-branch/internal/repo")
	if repo == store || repo == worktree {
		t.Errorf("packages and checkouts must get different databases: repo %q, store %q, worktree %q", repo, store, worktree)
	}

	// A branch database name at Postgres' 63-byte limit still yields a name
	// that fits, keeping its prefix.
	long := "tidepool_test_" + strings.Repeat("x", 49)
	name := packageDatabaseName(long, "/src/tidepool/internal/repo")
	if len(name) > 63 {
		t.Errorf("packageDatabaseName(%d-byte base) is %d bytes, want at most 63", len(long), len(name))
	}
	if !strings.HasPrefix(name, long[:53]+"_p") {
		t.Errorf("packageDatabaseName(%q) = %q, want the base cut to 53 bytes plus _p", long, name)
	}
}
