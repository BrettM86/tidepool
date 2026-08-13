// Package consume is Tidepool's Jetstream consumer (task 14): the atproto
// half of the world the bridge does NOT host. It watches native Coves users'
// repos for federation opt-outs, profile changes, posts, comments and votes,
// and persists the durable OUTBOUND STATE that deletes and Undo are later
// built from (tasks 15-17 consume it through the OutboundEnqueuer seam).
//
// The connector, rev gate, cursor/dead-letter store and redriver are a PORT
// of the Coves AppView's own Jetstream consumer
// (~/Code/coves/internal/atproto/jetstream): same discipline, same failure
// taxonomy, same "a dead-letter write failure blocks cursor advance" rule.
// Where the two diverge it is stated in a comment, not left to be inferred.
package consume

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// ErrPermanentEvent marks a handler failure as permanent: the event can never
// succeed no matter how often it is retried (validation rejection, a record
// the lexicon forbids). The connector skips both the in-line retries and the
// redrive budget for these — the event is dead-lettered already exhausted and
// kept only for forensics. Unwrapped errors are treated as transient.
var ErrPermanentEvent = errors.New("permanent event failure")

// ConsumerNative names the single consumer this task ships. It keys the
// persisted rows in consumer_cursors and jetstream_dead_letters, so it MUST
// stay stable across releases: renaming it silently orphans the cursor (the
// consumer restarts at live tail — the exact loss cursors exist to prevent)
// and strands the dead-letter backlog under the old name.
const ConsumerNative = "native"

// CursorSchemaVersion versions the HANDLER CONTRACT the persisted cursor
// belongs to. consumer_cursors is keyed (consumer_name, schema_version) so a
// future incompatible handler can replay the retained store from scratch
// without overwriting the production cursor; the two rows coexist.
const CursorSchemaVersion = 1

// The collections this consumer subscribes to (Jetstream wantedCollections).
const (
	CollectionFederation = "social.coves.bridge.federation"
	CollectionProfile    = "social.coves.actor.profile"
	CollectionPostV2     = "social.coves.community.postv2"
	CollectionComment    = "social.coves.community.comment"
	CollectionVote       = "social.coves.feed.vote"
)

// WantedCollections is the wantedCollections filter this consumer subscribes
// with. It is a function rather than a package var so no caller can append to
// the shared slice and silently widen every future subscription.
func WantedCollections() []string {
	return []string{
		CollectionFederation,
		CollectionProfile,
		CollectionPostV2,
		CollectionComment,
		CollectionVote,
	}
}

// SubscribeURL builds the WebSocket subscribe URL: the configured base with a
// /subscribe path (appended unless already present) and ONE wantedCollections
// parameter per collection.
//
// The filter is not optional in practice: a subscribe URL without
// wantedCollections asks Jetstream for the entire firehose, which this
// consumer would then discard record by record — the collection filter is the
// difference between a few Coves repos' events and the whole network's.
func SubscribeURL(baseURL string, collections []string) (string, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("invalid Jetstream URL %q: %w", baseURL, err)
	}
	if !strings.HasSuffix(parsed.Path, subscribePath) {
		parsed.Path = strings.TrimSuffix(parsed.Path, "/") + subscribePath
	}
	query := parsed.Query()
	for _, collection := range collections {
		query.Add("wantedCollections", collection)
	}
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

// subscribePath is Jetstream's subscribe endpoint.
const subscribePath = "/subscribe"

// JetstreamEvent is one frame off the Jetstream WebSocket.
type JetstreamEvent struct {
	Account  *AccountEvent  `json:"account,omitempty"`
	Identity *IdentityEvent `json:"identity,omitempty"`
	Commit   *CommitEvent   `json:"commit,omitempty"`
	DID      string         `json:"did"`
	Kind     string         `json:"kind"`
	TimeUS   int64          `json:"time_us"`
}

// AccountEvent is a #account status change. Status is parsed and PERSISTED
// (decision 19): "deleted" is the only value that means deletion, every other
// inactive state is transient.
type AccountEvent struct {
	DID    string `json:"did"`
	Time   string `json:"time"`
	Seq    int64  `json:"seq"`
	Active bool   `json:"active"`
	Status string `json:"status,omitempty"`
}

// IdentityEvent is a #identity handle change. The handle here may be stale;
// the local part is frozen at actor creation regardless.
type IdentityEvent struct {
	DID    string `json:"did"`
	Handle string `json:"handle"`
	Time   string `json:"time"`
	Seq    int64  `json:"seq"`
}

// CommitEvent is a record write in a repo. A DELETE carries DID, collection
// and rkey ONLY — no record body and no CID. That absence is the whole reason
// store.OutboundObjects exists.
type CommitEvent struct {
	Rev        string         `json:"rev"`
	Operation  string         `json:"operation"` // create | update | delete
	Collection string         `json:"collection"`
	RKey       string         `json:"rkey"`
	Record     map[string]any `json:"record,omitempty"`
	CID        string         `json:"cid,omitempty"`
}

// EventHandler processes a single Jetstream event. Handlers MUST be
// idempotent: cursor rewinds on reconnect intentionally replay a few seconds
// of already-processed events, and a full replay must change nothing.
type EventHandler interface {
	HandleEvent(ctx context.Context, event *JetstreamEvent) error
}
