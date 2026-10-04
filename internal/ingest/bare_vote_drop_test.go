package ingest

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/votes"
)

// voterEvents is what vote_events holds for one voter: every row, and the
// live (not undone) ones.
type voterEvents struct {
	total, live int
}

// TestBareVotesNeverCount: a vote counts only when it arrives inside the
// announcing community's own Announce. The inbox binds a bare activity's actor
// to the signer's HOST, never to the signer, and voter actors are never
// fetched — so one signing key on an instance can sign Likes as any number of
// made-up users there. A bare Like/Dislike, and a bare Undo of one, are
// therefore dropped as processed skips: never counted, never retried.
//
// That includes Mastodon/Misskey-style Likes sent straight to a persona's
// inbox on a post a Coves user wrote: the community's Announce is the only
// vouching a vote has.
func TestBareVotesNeverCount(t *testing.T) {
	rows := []struct {
		name string
		// arrange builds the world and returns the voted-on subject.
		arrange func(t *testing.T, h *harness) string
		// send delivers the bare activities and returns their ids.
		send          func(t *testing.T, h *harness, subject string) []string
		wantAggregate bool
		wantUp        int
		wantDown      int
		wantEvents    map[string]voterEvents
		// wantDropped is how far tidepool_vote_bare_dropped rises.
		wantDropped int64
	}{
		{
			name: "bare Likes and a Dislike from fabricated voters on a Lemmy post",
			arrange: func(t *testing.T, h *harness) string {
				bridgeLemmyPost(t, h)
				aggregator, ok := h.votes.delegate.(*votes.Aggregator)
				require.True(t, ok, "the real aggregator must be behind the dispatcher")
				require.NoError(t, aggregator.SeedAggregates(context.Background(), pageID, 7, 1))
				return pageID
			},
			send: func(t *testing.T, h *harness, subject string) []string {
				// One key on evil.example; the voters have no actor documents.
				signer := h.newRemoteActor("https://evil.example/u/signer",
					person("https://evil.example/u/signer", "signer", nil))
				votesSent := []struct{ id, kind, voter string }{
					{"https://evil.example/activities/like/1", "Like", "https://evil.example/u/1"},
					{"https://evil.example/activities/like/2", "Like", "https://evil.example/u/2"},
					{"https://evil.example/activities/dislike/3", "Dislike", "https://evil.example/u/3"},
				}
				ids := make([]string, 0, len(votesSent))
				for _, vote := range votesSent {
					require.Equal(t, http.StatusAccepted, h.deliver(signer, map[string]any{
						"id":     vote.id,
						"type":   vote.kind,
						"actor":  vote.voter,
						"object": subject,
					}))
					ids = append(ids, vote.id)
				}
				return ids
			},
			wantAggregate: true,
			wantUp:        7,
			wantDown:      1,
			wantEvents: map[string]voterEvents{
				"https://evil.example/u/1": {total: 0, live: 0},
				"https://evil.example/u/2": {total: 0, live: 0},
				"https://evil.example/u/3": {total: 0, live: 0},
			},
			wantDropped: 3,
		},
		{
			name: "bare Like to the persona inbox on a Coves-authored post",
			arrange: func(t *testing.T, h *harness) string {
				setupTargetWorld(t, h)
				return tgPostAPID
			},
			send: func(t *testing.T, h *harness, subject string) []string {
				const alice = "https://mastodon.example/users/alice"
				voter := h.newRemoteActor(alice, person(alice, "alice", nil))
				const id = alice + "#likes/1"
				require.Equal(t, http.StatusAccepted, h.deliverToUserInbox(voter, map[string]any{
					"id":     id,
					"type":   "Like",
					"actor":  alice,
					"object": subject,
				}))
				return []string{id}
			},
			wantAggregate: false,
			wantEvents: map[string]voterEvents{
				"https://mastodon.example/users/alice": {total: 0, live: 0},
			},
			wantDropped: 1,
		},
		{
			name: "bare Undo of an announced Like",
			arrange: func(t *testing.T, h *harness) string {
				group := bridgeLemmyPost(t, h)
				require.Equal(t, http.StatusAccepted, h.deliver(group,
					echoAnnounce("https://lemmy.world/activities/announce/like/counted", map[string]any{
						"id":       "https://lemmy.world/activities/like/counted",
						"type":     "Like",
						"actor":    personID,
						"object":   pageID,
						"audience": groupID,
					})))
				h.drain()
				up, down, found := aggregateOf(t, h, pageID)
				require.True(t, found, "precondition: the announced Like is counted")
				require.Equal(t, 1, up)
				require.Equal(t, 0, down)
				return pageID
			},
			send: func(t *testing.T, h *harness, subject string) []string {
				voter := h.newRemoteActor(personID, person(personID, "LeftLeaningFreedomFighters", nil))
				const id = "https://lemmy.world/activities/undo/bare-counted"
				require.Equal(t, http.StatusAccepted, h.deliver(voter, map[string]any{
					"id":    id,
					"type":  "Undo",
					"actor": personID,
					"object": map[string]any{
						"id":     "https://lemmy.world/activities/like/counted",
						"type":   "Like",
						"actor":  personID,
						"object": subject,
					},
				}))
				return []string{id}
			},
			wantAggregate: true,
			wantUp:        1,
			wantDown:      0,
			wantEvents: map[string]voterEvents{
				personID: {total: 1, live: 1},
			},
			wantDropped: 1,
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			h := newHarness(t)
			withRealAggregator(t, h)
			subject := row.arrange(t, h)

			droppedBefore := VoteBareDropped.Value()
			ids := row.send(t, h, subject)
			h.drain()
			assert.Equal(t, row.wantDropped, VoteBareDropped.Value()-droppedBefore,
				"tidepool_vote_bare_dropped rises once per dropped bare activity")

			up, down, found := aggregateOf(t, h, subject)
			assert.Equal(t, row.wantAggregate, found, "aggregate row present")
			assert.Equal(t, row.wantUp, up, "upvotes")
			assert.Equal(t, row.wantDown, down, "downvotes")
			for voter, want := range row.wantEvents {
				assert.Equal(t, want.total, voteRows(t, h, voter), "vote_events rows for %s", voter)
				assert.Equal(t, want.live, liveVoteRows(t, h, voter), "live vote_events rows for %s", voter)
			}
			for _, id := range ids {
				event, err := h.events.GetEvent(context.Background(), id)
				require.NoError(t, err)
				assert.NotNil(t, event.ProcessedAt, "%s is a processed skip, never left pending", id)
				assert.Nil(t, event.FailedAt, "%s is never poisoned", id)
			}
		})
	}
}
