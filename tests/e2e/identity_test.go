//go:build e2e

package e2e

import (
	"net/http"
	"testing"
	"time"
)

func TestBridgeIdentityPrecedesFirstCommit(t *testing.T) {
	h := newHarness(t)
	community, _ := setupSubscribedCommunity(t, h, "ident")
	username := h.uniqueName(t, "ida")
	user := h.registerUser(t, username)
	user.createPost(t, community.ID, "Identity "+username, "first bridged post")

	conn := h.dialBridgeFirehoseCursor(t, "0")
	deadline := time.Now().Add(eventTimeout)
	wantHandle := bridgedHandle(username)
	firstCommitByDID := make(map[string]int64)
	var identityDID string
	var identitySeq int64
	for {
		frame := readBridgeFrame(t, conn, deadline)
		if frame.Error != nil {
			t.Fatalf("bridge firehose error before identity and profile: %s", frame.Error.Error)
		}
		if frame.RepoInfo != nil && frame.RepoInfo.Name == "OutdatedCursor" {
			t.Fatal("bridge firehose cursor 0 was pruned; cannot prove identity-before-first-commit")
		}
		if frame.RepoCommit != nil {
			commit := frame.RepoCommit
			if _, seen := firstCommitByDID[commit.Repo]; !seen {
				firstCommitByDID[commit.Repo] = commit.Seq
			}
			if identityDID != "" && commit.Repo == identityDID {
				if commit.Seq <= identitySeq {
					t.Fatalf("first #commit for %s at seq %d did not follow #identity at seq %d", identityDID, commit.Seq, identitySeq)
				}
				return
			}
		}
		if frame.RepoIdentity == nil || frame.RepoIdentity.Handle == nil || *frame.RepoIdentity.Handle != wantHandle {
			continue
		}
		identityDID = frame.RepoIdentity.Did
		identitySeq = frame.RepoIdentity.Seq
		if identityDID == "" {
			t.Fatal("#identity frame has no DID")
		}
		if firstCommit, seen := firstCommitByDID[identityDID]; seen {
			t.Fatalf("first #commit for %s at seq %d preceded #identity at seq %d", identityDID, firstCommit, identitySeq)
		}
		resolvedDID, result := h.bridgeResolveHandle(t, wantHandle)
		if result.status != http.StatusOK || resolvedDID != identityDID {
			t.Fatalf("#identity DID %q differs from resolveHandle(%s): status %d, DID %q",
				identityDID, wantHandle, result.status, resolvedDID)
		}
	}
}
