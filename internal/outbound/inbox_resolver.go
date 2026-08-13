package outbound

import (
	"context"
	"fmt"
	"sync"
	"time"

	"tidepool/internal/ap"
)

// ActorFetcher fetches an AP actor document by IRI. *ap.Client satisfies it via
// FetchActor; the resolver depends only on this narrow surface.
type ActorFetcher interface {
	FetchActor(ctx context.Context, iri string) (*ap.Object, error)
}

// sameAuthorityFetcher is the optional hardened fetch: it pins the redirect
// authority to the requested IRI, so an open redirect on the community's origin
// cannot bounce the Group-doc fetch to an attacker host. *ap.Client satisfies it
// (FetchActorSameAuthority); a plain ActorFetcher falls back to FetchActor.
type sameAuthorityFetcher interface {
	FetchActorSameAuthority(ctx context.Context, iri string) (*ap.Object, error)
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

// fetchAndCache fetches the Group document (with the redirect authority pinned
// when the fetcher supports it) and reads its delivery inbox. It REFUSES a
// resolved inbox whose host is not same-authority with the community: a Group
// doc a stranger controls must not be able to redirect a signed activity to an
// arbitrary origin. The worker's POST re-applies the SSRF egress guard on top.
func (r *cachedInboxResolver) fetchAndCache(ctx context.Context, communityAPID string) (string, error) {
	var (
		doc *ap.Object
		err error
	)
	if hardened, ok := r.fetcher.(sameAuthorityFetcher); ok {
		doc, err = hardened.FetchActorSameAuthority(ctx, communityAPID)
	} else {
		doc, err = r.fetcher.FetchActor(ctx, communityAPID)
	}
	if err != nil {
		return "", fmt.Errorf("resolve inbox for %s: %w", communityAPID, err)
	}
	inbox := doc.SharedInboxOrInbox()
	if inbox == "" {
		return "", fmt.Errorf("community %s advertises no inbox", communityAPID)
	}
	if !ap.SameAuthority(communityAPID, inbox) {
		return "", fmt.Errorf("community %s advertises a cross-authority inbox %q; refusing", communityAPID, inbox)
	}
	r.mu.Lock()
	r.cache[communityAPID] = inboxEntry{inbox: inbox, expires: time.Now().Add(r.ttl)}
	r.mu.Unlock()
	return inbox, nil
}
