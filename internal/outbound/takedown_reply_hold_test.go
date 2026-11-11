package outbound

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// TestTakedown_AReplyUnderARemovedParentIsHeldUntilTheParentClears (B13): a
// native reply whose parent is taken down, either an accepted native post
// (native record label) or a bridged post with no outbound row (bridged record
// label), and queued either before the takedown (held by the worker at
// delivery time) or after it (held at enqueue time), is not delivered and is
// held. When the parent's label is negated, the reply is released to the tail
// of its key: Lemmy receives it after another delivery queued on that key
// during the hold.
func TestTakedown_AReplyUnderARemovedParentIsHeldUntilTheParentClears(t *testing.T) {
	type parent struct {
		name  string
		reply tdReply
		// deliver puts the parent on Lemmy: lines is what Lemmy then has.
		deliver func(t *testing.T, w *takedownWorld) (lines []string)
		// downLines and restoreLines are what Lemmy receives for the parent
		// at its takedown and its restore.
		downLines, restoreLines []string
		// hostedParentDID, when set, gets a repo_state row: the parent's repo
		// is hosted by Tidepool, as a bridged post's author repo is.
		hostedParentDID string
	}
	parents := []parent{
		{
			name:  "accepted native parent",
			reply: tdReplyToPost,
			deliver: func(t *testing.T, w *takedownWorld) []string {
				w.deliverPost(t)
				return []string{"Create " + tdPostAPID}
			},
			downLines:    []string{"Delete " + tdPostAPID},
			restoreLines: []string{"Undo{Delete} " + tdPostAPID},
		},
		{
			name:  "bridged parent with no outbound row",
			reply: tdReplyToBridged,
			deliver: func(*testing.T, *takedownWorld) []string {
				return []string{}
			},
		},
		{
			name:            "bridged parent in a hosted repo",
			reply:           tdReplyToBridged,
			hostedParentDID: "did:plc:n3wq5hbrbz7ydnkxlpqegrty",
			deliver: func(*testing.T, *takedownWorld) []string {
				return []string{}
			},
		},
	}
	timings := []struct {
		name       string
		replyFirst bool
	}{
		{name: "reply queued before the takedown", replyFirst: true},
		{name: "reply enqueued after the takedown", replyFirst: false},
	}

	for _, p := range parents {
		for _, timing := range timings {
			t.Run(p.name+"/"+timing.name, func(t *testing.T) {
				w := newTakedownWorld(t)
				if p.hostedParentDID != "" {
					_, err := w.conn.Exec(`INSERT INTO repo_state (did, head_cid, rev) VALUES ($1, $2, $3)`,
						p.hostedParentDID, "bafyreihostedbridgedparenthead", "3lztkdnbrrev01")
					require.NoError(t, err, "seed repo_state for the hosted bridged parent's repo")
				}
				lines := p.deliver(t, w)
				parentATURI := p.reply.parentATURI

				if timing.replyFirst {
					w.enqueueReply(t, p.reply)
					w.commitLabel(t, parentATURI, false)
				} else {
					w.commitLabel(t, parentATURI, false)
					w.enqueueReply(t, p.reply)
					assert.Equal(t, store.DeliveryStateHeld, w.deliveryStateOf(t, p.reply.createID()),
						"a reply enqueued under a removed parent is held at enqueue, before any delivery")
				}
				w.drain(t)
				lines = append(lines, p.downLines...)
				assert.Equal(t, lines, w.receivedLines(t), "nothing is delivered for the reply while its parent is down")
				assert.Equal(t, store.DeliveryStateHeld, w.deliveryStateOf(t, p.reply.createID()), "the reply is held")

				// During the hold: another delivery on the reply's key.
				w.enqueuePost(t, tdSecondPost, "create", 0)

				w.commitLabel(t, parentATURI, true)
				w.drain(t)
				lines = append(lines, "Create "+tdSecondPost.apID())
				lines = append(lines, p.restoreLines...)
				lines = append(lines, "Create "+p.reply.apID())
				assert.Equal(t, lines, w.receivedLines(t),
					"the released reply is delivered after the work queued during the hold and the parent's restore")
				assert.Equal(t, store.DeliveryStateDelivered, w.deliveryStateOf(t, p.reply.createID()),
					"the released reply is delivered")
				assert.Equal(t, len(p.downLines), activityCount(t, w.conn, "Delete"),
					"a Delete is minted only for a native parent")
				assert.Equal(t, len(p.restoreLines), activityCount(t, w.conn, "Undo"),
					"an Undo is minted only for a native parent")
			})
		}
	}
}

// TestTakedown_AReplyDownItselfStaysHeldWhenOnlyItsParentClears (B13 extra):
// a native reply never delivered and its native parent are both taken down.
// Negating the parent's label restores the parent and leaves the reply held;
// negating the reply's own label releases it and it is delivered, with no
// Delete or Undo for the never-delivered reply.
func TestTakedown_AReplyDownItselfStaysHeldWhenOnlyItsParentClears(t *testing.T) {
	w := newTakedownWorld(t)
	reply := tdReplyToPost
	w.deliverPost(t)

	w.commitLabel(t, tdPostATURI, false)
	w.enqueueReply(t, reply)
	w.commitLabel(t, reply.atURI(), false)
	w.drain(t)
	require.Equal(t, []string{"Create " + tdPostAPID, "Delete " + tdPostAPID}, w.receivedLines(t),
		"the parent's takedown sends its Delete and nothing for the reply")
	assert.Equal(t, store.DeliveryStateHeld, w.deliveryStateOf(t, reply.createID()), "the reply is held")

	w.commitLabel(t, tdPostATURI, true)
	w.drain(t)
	assert.Equal(t, []string{
		"Create " + tdPostAPID,
		"Delete " + tdPostAPID,
		"Undo{Delete} " + tdPostAPID,
	}, w.receivedLines(t), "clearing the parent restores it and does not deliver the reply")
	assert.Equal(t, store.DeliveryStateHeld, w.deliveryStateOf(t, reply.createID()),
		"the reply's own label still holds it")

	w.commitLabel(t, reply.atURI(), true)
	w.drain(t)
	assert.Equal(t, []string{
		"Create " + tdPostAPID,
		"Delete " + tdPostAPID,
		"Undo{Delete} " + tdPostAPID,
		"Create " + reply.apID(),
	}, w.receivedLines(t), "clearing the reply's own label delivers it, with no Delete or Undo of it")
	assert.Equal(t, store.DeliveryStateDelivered, w.deliveryStateOf(t, reply.createID()))
	assert.Equal(t, 1, activityCount(t, w.conn, "Delete"), "the only Delete is the parent's")
	assert.Equal(t, 1, activityCount(t, w.conn, "Undo"), "the only Undo is the parent's")
}
