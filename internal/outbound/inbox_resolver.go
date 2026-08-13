package outbound

import (
	"context"
	"fmt"
	"sync"
	"time"

	"tidepool/internal/ap"
)

// ActorFetcher fetches an AP actor document by IRI. *ap.Client satisfies it via
// FetchActor (SSRF-guarded, same-authority binding); the resolver depends only
// on this narrow surface.
type ActorFetcher interface {
	FetchActor(ctx context.Context, iri string) (*ap.Object, error)
}

// cachedInboxResolver resolves a community's target inbox from its Group actor
// document (preferring endpoints.sharedInbox), memoized for a TTL. It also
// implements FreshInboxResolver so the worker can bypass the cache once on an
// endpoint rotation before poisoning.
type cachedInboxResolver struct {
	fetcher ActorFetcher
	ttl     time.Duration

	mu    sync.Mutex
	cache map[string]inboxEntry
}

type inboxEntry struct {
	inbox   string
	expires time.Time
}

// NewInboxResolver builds the TTL-cached inbox resolver.
func NewInboxResolver(fetcher ActorFetcher, ttl time.Duration) InboxResolver {
	return &cachedInboxResolver{
		fetcher: fetcher,
		ttl:     ttl,
		cache:   make(map[string]inboxEntry),
	}
}

// ResolveInbox returns the community's inbox, from cache when fresh.
func (r *cachedInboxResolver) ResolveInbox(ctx context.Context, communityAPID string) (string, error) {
	r.mu.Lock()
	entry, ok := r.cache[communityAPID]
	r.mu.Unlock()
	if ok && time.Now().Before(entry.expires) {
		return entry.inbox, nil
	}
	return r.fetchAndCache(ctx, communityAPID)
}

// ResolveInboxFresh re-fetches the Group doc bypassing the cache (rotation): an
// endpoint rotation must not become a poison, so the worker asks for a fresh
// resolution ONCE on a 401/404/410 before giving up.
func (r *cachedInboxResolver) ResolveInboxFresh(ctx context.Context, communityAPID string) (string, error) {
	return r.fetchAndCache(ctx, communityAPID)
}

// fetchAndCache fetches the Group document and reads its delivery inbox. The
// fetch runs through the ap client's SSRF + same-authority guards (ActorFetcher
// is *ap.Client.FetchActor), so a Group doc advertising a cross-authority or
// private-range inbox is refused at fetch time; the worker's POST re-applies the
// egress guard on the resolved inbox.
func (r *cachedInboxResolver) fetchAndCache(ctx context.Context, communityAPID string) (string, error) {
	doc, err := r.fetcher.FetchActor(ctx, communityAPID)
	if err != nil {
		return "", fmt.Errorf("resolve inbox for %s: %w", communityAPID, err)
	}
	inbox := doc.SharedInboxOrInbox()
	if inbox == "" {
		return "", fmt.Errorf("community %s advertises no inbox", communityAPID)
	}
	r.mu.Lock()
	r.cache[communityAPID] = inboxEntry{inbox: inbox, expires: time.Now().Add(r.ttl)}
	r.mu.Unlock()
	return inbox, nil
}
