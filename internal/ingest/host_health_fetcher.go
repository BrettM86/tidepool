package ingest

import (
	"context"
	"log/slog"

	"tidepool/internal/ap"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// hostHealthFetcher is a materialize.Fetcher that ends a host's host-gone
// streak whenever an object or actor fetch from it succeeds, so a host that
// came back is not later skipped by backfill on a stale streak.
type hostHealthFetcher struct {
	materialize.Fetcher
	instances store.HostGoneStreaks
	logger    *slog.Logger
}

// NewHostHealthFetcher wraps inner so a successful FetchObject or FetchActor
// clears the fetched IRI's host-gone streak (keyed by ap.HostKey). Results
// and errors pass through unchanged; a failing clear is logged, never
// returned. FetchMedia passes through untouched.
func NewHostHealthFetcher(inner materialize.Fetcher, instances store.HostGoneStreaks, logger *slog.Logger) materialize.Fetcher {
	return &hostHealthFetcher{Fetcher: inner, instances: instances, logger: logger}
}

func (f *hostHealthFetcher) FetchObject(ctx context.Context, iri string) (*ap.Object, error) {
	obj, err := f.Fetcher.FetchObject(ctx, iri)
	if err == nil {
		f.clear(ctx, iri, obj)
	}
	return obj, err
}

func (f *hostHealthFetcher) FetchActor(ctx context.Context, iri string) (*ap.Object, error) {
	obj, err := f.Fetcher.FetchActor(ctx, iri)
	if err == nil {
		f.clear(ctx, iri, obj)
	}
	return obj, err
}

func (f *hostHealthFetcher) clear(ctx context.Context, iri string, obj *ap.Object) {
	host, ok := ap.HostKey(iri)
	if !ok {
		return
	}
	// A response naming another host's ID does not prove the requested host is alive.
	if obj != nil && obj.ID != "" {
		if returnedHost, ok := ap.HostKey(obj.ID); !ok || returnedHost != host {
			return
		}
	}
	if err := f.instances.ClearHostGone(ctx, host); err != nil {
		f.logger.Warn("clear host-gone streak after a successful fetch failed", "host", host, "error", err)
	}
}
