// Package sync serves the com.atproto.sync.* XRPC surface — the endpoints a
// relay or Jetstream needs to treat Tidepool as a valid subscribeRepos
// upstream. The repo layer (internal/repo) owns all reads against repo
// tables; this package turns those reads into wire frames and HTTP
// responses, and fans out commit notifications to WebSocket subscribers.
package sync

import (
	"bytes"
	"fmt"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/events"
	lexutil "github.com/bluesky-social/indigo/lex/util"

	"github.com/ipfs/go-cid"

	"tidepool/internal/repo"
)

// frameTimeFormat is the atproto datetime layout used for the #commit `time`
// field: RFC 3339, UTC, millisecond precision (the spec's preferred form).
const frameTimeFormat = "2006-01-02T15:04:05.000Z"

// commitFrame converts a stored firehose event into the subscribeRepos
// #commit message, using indigo's lexicon types so the CBOR encoding is
// exactly what relays and Jetstream expect.
func commitFrame(ev *repo.Event) (*events.XRPCStreamEvent, error) {
	commitCID, err := cid.Parse(ev.CommitCID)
	if err != nil {
		return nil, fmt.Errorf("sync: parse commit cid %q for seq %d: %w", ev.CommitCID, ev.Seq, err)
	}
	var prevData *lexutil.LexLink
	if ev.PrevDataCID != nil {
		c, err := cid.Parse(*ev.PrevDataCID)
		if err != nil {
			return nil, fmt.Errorf("sync: parse prevData cid %q for seq %d: %w", *ev.PrevDataCID, ev.Seq, err)
		}
		prevData = (*lexutil.LexLink)(&c)
	}

	ops := make([]*comatproto.SyncSubscribeRepos_RepoOp, 0, len(ev.Ops))
	for _, op := range ev.Ops {
		frameOp := &comatproto.SyncSubscribeRepos_RepoOp{
			Action: string(op.Action),
			Path:   op.Path,
		}
		if op.CID != "" {
			c, err := cid.Parse(op.CID)
			if err != nil {
				return nil, fmt.Errorf("sync: parse op cid %q for seq %d: %w", op.CID, ev.Seq, err)
			}
			frameOp.Cid = (*lexutil.LexLink)(&c)
		}
		if op.Prev != "" {
			c, err := cid.Parse(op.Prev)
			if err != nil {
				return nil, fmt.Errorf("sync: parse op prev cid %q for seq %d: %w", op.Prev, ev.Seq, err)
			}
			frameOp.Prev = (*lexutil.LexLink)(&c)
		}
		ops = append(ops, frameOp)
	}

	return &events.XRPCStreamEvent{
		RepoCommit: &comatproto.SyncSubscribeRepos_Commit{
			Seq:      ev.Seq,
			Repo:     ev.DID,
			Commit:   lexutil.LexLink(commitCID),
			Rev:      ev.Rev,
			Since:    ev.SinceRev,
			PrevData: prevData,
			Blocks:   ev.CAR,
			Ops:      ops,
			Blobs:    []lexutil.LexLink{},
			Time:     ev.CreatedAt.UTC().Format(frameTimeFormat),
		},
	}, nil
}

// infoFrame builds a #info message (e.g. OutdatedCursor).
func infoFrame(name, message string) *events.XRPCStreamEvent {
	info := &comatproto.SyncSubscribeRepos_Info{Name: name}
	if message != "" {
		info.Message = &message
	}
	return &events.XRPCStreamEvent{RepoInfo: info}
}

// errorFrame builds a terminal error frame (e.g. FutureCursor); the spec
// requires closing the stream after sending one.
func errorFrame(name, message string) *events.XRPCStreamEvent {
	return &events.XRPCStreamEvent{Error: &events.ErrorFrame{Error: name, Message: message}}
}

// serializeFrame renders header + body as the binary WebSocket payload.
func serializeFrame(evt *events.XRPCStreamEvent) ([]byte, error) {
	var buf bytes.Buffer
	if err := evt.Serialize(&buf); err != nil {
		return nil, fmt.Errorf("sync: serialize stream frame: %w", err)
	}
	return buf.Bytes(), nil
}
