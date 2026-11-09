package votes

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// Coves sends a vote flip as delete of record A plus create of record B under a
// NEW rkey on the same subject. A's row lives until its Undo settles, so B must
// be recordable while it does.

const tpFlipRKey = "3lztemporalflip"

func TestVoteFlipToNewRKeyFederatesUndoThenReplacement(t *testing.T) {
	tests := []struct {
		name          string
		deliverFirstA bool
		wantSentKinds []string
	}{
		{"A delivered before its delete", true, []string{"Like", "Undo", "Dislike"}},
		{"A not yet delivered when deleted", false, []string{"Like", "Undo", "Dislike"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			l := newLifecycle(t, 5)

			l.castVote(t, "3lztprev00001", directionUp)
			if tc.deliverFirstA {
				l.deliver(t)
				require.Equal(t, string(store.DeliveredStateDelivered), l.state(t))
			}
			l.deleteVote(t, "3lztprev00002")
			l.castVoteRKey(t, "3lztprev00003", tpFlipRKey, directionDown)
			require.NotEmpty(t, l.stateOf(t, tpFlipRKey), "B's row recorded")

			l.deliver(t)

			assert.Equal(t, tc.wantSentKinds, l.sender.kinds())
			assert.Equal(t, "", l.state(t), "A's row is gone")
			assert.Equal(t, string(store.DeliveredStateDelivered), l.stateOf(t, tpFlipRKey), "B delivered")
		})
	}
}

// A poisoned Undo must not block the actor's next vote on the subject.
func TestVoteFlipToNewRKeyAfterPoisonedUndo(t *testing.T) {
	l := newLifecycle(t, 1)

	l.castVote(t, "3lztprev00001", directionUp)
	l.deliver(t)
	require.Equal(t, string(store.DeliveredStateDelivered), l.state(t))

	l.deleteVote(t, "3lztprev00002")
	l.sender.fail(fmt.Errorf("lemmy rejected the undo"))
	l.deliver(t)
	var poisoned int
	require.NoError(t, l.db.QueryRow(
		`SELECT COUNT(*) FROM outbound_deliveries WHERE state = 'poisoned'`).Scan(&poisoned))
	require.Equal(t, 1, poisoned, "precondition: the Undo poisoned")

	l.castVoteRKey(t, "3lztprev00003", tpFlipRKey, directionDown)
	require.Equal(t, string(store.DeliveredStatePending), l.stateOf(t, tpFlipRKey), "B's row recorded")

	l.sender.fail(nil)
	l.deliver(t)
	assert.Equal(t, string(store.DeliveredStateDelivered), l.stateOf(t, tpFlipRKey), "B delivered")
}
