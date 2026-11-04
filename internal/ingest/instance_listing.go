package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"tidepool/internal/errors"
)

const (
	// lemmyMaxPage and lemmyMaxLimit are the limits Lemmy 0.19.20 answers 400
	// past.
	lemmyMaxPage  = 100
	lemmyMaxLimit = 50
	// defaultMaxListingPages is how many pages one read takes when
	// LemmyListerOptions leaves MaxPages zero.
	defaultMaxListingPages = 100
	// maxListingBodyBytes bounds one listing response: remote input.
	maxListingBodyBytes = 8 << 20
	listingSortOld      = "Old"
	listingSortNew      = "New"
	// lemmyCommunityListPath is the community list endpoint. It has no cursor,
	// so one sort order reaches lemmyMaxPage pages of lemmyMaxLimit results.
	lemmyCommunityListPath = "/api/v3/community/list"
	// listingReachLimit is how many communities the two sort orders reach.
	listingReachLimit = 2 * lemmyMaxPage * lemmyMaxLimit
)

// ListingPosition is where a read of one host's community list starts.
type ListingPosition struct {
	Sort string
	Page int
	// Boundary is the published time of the last result of a full sort=Old
	// page 100. It is set during the sort=New pass.
	Boundary time.Time
}

// firstListingPosition is where every host starts and wraps back to.
var firstListingPosition = ListingPosition{Sort: listingSortOld, Page: 1}

// CommunityListing is the result of one read. A non-nil Err means the read
// failed; a read stopped by the page cap has not failed.
type CommunityListing struct {
	ActorIDs []string
	Next     ListingPosition
	Err      error
	// ReachedLimit reports a full sort=New page 100 that did not meet the sort=Old
	// end: communities between the two ends may be unreachable.
	ReachedLimit bool
}

// CommunityLister reads the communities local to a host.
type CommunityLister interface {
	ListCommunities(ctx context.Context, host string, start ListingPosition) CommunityListing
}

// LemmyListerOptions configures NewLemmyLister. Client is required. A zero
// MaxPages takes defaultMaxListingPages, a zero MaxPostPages takes
// defaultMaxPostPages, and a zero PostLimit takes lemmyMaxLimit.
type LemmyListerOptions struct {
	Client    *http.Client
	Scheme    string
	UserAgent string
	MaxPages  int
	Logger    *slog.Logger
	// MaxPostPages caps one post walk.
	MaxPostPages int
	// PostLimit is the post list's page size, 1 to lemmyMaxLimit.
	PostLimit int
}

// LemmyLister is the CommunityLister for Lemmy's community list endpoint.
//
// A host is walked with sort=Old first, because new communities land on later
// pages and do not shift earlier ones. An instance with a full sort=Old page 100
// is then walked with sort=New back to where sort=Old ended.
type LemmyLister struct {
	client    *http.Client
	scheme    string
	userAgent string
	maxPages  int
	logger    *slog.Logger
	// maxPostPages and postLimit are the post walk's page cap and page size.
	maxPostPages int
	postLimit    int
}

// NewLemmyLister validates the options and builds a lister.
func NewLemmyLister(opts LemmyListerOptions) (*LemmyLister, error) {
	if opts.Client == nil {
		return nil, errors.NewValidationError("client", "must not be nil")
	}
	maxPages := opts.MaxPages
	if maxPages <= 0 {
		maxPages = defaultMaxListingPages
	}
	maxPostPages := opts.MaxPostPages
	if maxPostPages <= 0 {
		maxPostPages = defaultMaxPostPages
	}
	postLimit := opts.PostLimit
	if postLimit < 0 || postLimit > lemmyMaxLimit {
		return nil, errors.NewValidationError("post_limit", fmt.Sprintf("must be 0 to %d", lemmyMaxLimit))
	}
	if postLimit == 0 {
		postLimit = lemmyMaxLimit
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &LemmyLister{
		client:       opts.Client,
		scheme:       opts.Scheme,
		userAgent:    opts.UserAgent,
		maxPages:     maxPages,
		logger:       logger,
		maxPostPages: maxPostPages,
		postLimit:    postLimit,
	}, nil
}

// ListCommunities implements CommunityLister. One read stays in one sort and
// takes at most the page cap; the listing's Next is where the following read
// starts. A failed read keeps what it has. Its Next is the failed page, or the
// first position when the read's first request failed, so a page that always
// fails cannot hold a host in place.
func (l *LemmyLister) ListCommunities(ctx context.Context, host string, start ListingPosition) CommunityListing {
	var listing CommunityListing
	position := start
	for pagesRead := range l.maxPages {
		page, err := l.readPage(ctx, host, position)
		if err != nil {
			listing.Err = fmt.Errorf("ingest: list communities of %s: %w", host, err)
			listing.Next = position
			if pagesRead == 0 {
				listing.Next = firstListingPosition
			}
			l.logger.Warn("community listing failed", "host", host,
				"sort", position.Sort, "page", position.Page, "error", err)
			return listing
		}
		listing.ActorIDs = append(listing.ActorIDs, l.keepHostActorIDs(host, page)...)
		next, readEnded, reachedLimit := nextPosition(position, page)
		listing.Next = next
		listing.ReachedLimit = reachedLimit
		if readEnded {
			return listing
		}
		position = next
	}
	l.logger.Info("community listing stopped at the page cap", "host", host,
		"next_sort", listing.Next.Sort, "next_page", listing.Next.Page)
	return listing
}

// lemmyCommunityView is the part of a listed community the walk reads.
type lemmyCommunityView struct {
	Community struct {
		ActorID   string    `json:"actor_id"`
		Published time.Time `json:"published"`
	} `json:"community"`
}

// readPage fetches one page. The body is remote input, so it is bounded, and a
// body Lemmy would not send is a failed read: read as a short page it would end
// the walk, and a zero published time would end a sort=New pass, past communities
// not yet read.
func (l *LemmyLister) readPage(ctx context.Context, host string, position ListingPosition) ([]lemmyCommunityView, error) {
	body, err := l.fetchBody(ctx, host, lemmyCommunityListPath, url.Values{
		"type_":     {"Local"},
		"show_nsfw": {"true"},
		"limit":     {strconv.Itoa(lemmyMaxLimit)},
		"page":      {strconv.Itoa(position.Page)},
		"sort":      {position.Sort},
	})
	if err != nil {
		return nil, err
	}
	var document struct {
		Communities []lemmyCommunityView `json:"communities"`
	}
	if err := json.Unmarshal(body, &document); err != nil {
		return nil, fmt.Errorf("decode body: %w", err)
	}
	if document.Communities == nil {
		return nil, fmt.Errorf("body has no communities array")
	}
	if len(document.Communities) > lemmyMaxLimit {
		return nil, fmt.Errorf("page holds %d results, more than the limit of %d", len(document.Communities), lemmyMaxLimit)
	}
	if slices.ContainsFunc(document.Communities, func(view lemmyCommunityView) bool {
		return view.Community.Published.IsZero()
	}) {
		return nil, fmt.Errorf("result has no published time")
	}
	return document.Communities, nil
}

// fetchBody GETs path on host with query and gives the body of a 200, bounded
// to maxListingBodyBytes because it is remote input.
func (l *LemmyLister) fetchBody(ctx context.Context, host, path string, query url.Values) ([]byte, error) {
	target := url.URL{Scheme: l.scheme, Host: host, Path: path, RawQuery: query.Encode()}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	request.Header.Set("User-Agent", l.userAgent)
	response, err := l.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("fetch: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxListingBodyBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	if len(body) > maxListingBodyBytes {
		return nil, fmt.Errorf("body exceeds %d bytes", maxListingBodyBytes)
	}
	return body, nil
}

// keepHostActorIDs gives the page's actor ids that belong to host, and logs the
// rest.
func (l *LemmyLister) keepHostActorIDs(host string, page []lemmyCommunityView) []string {
	actorIDs := make([]string, 0, len(page))
	for _, view := range page {
		if l.onHost(host, view.Community.ActorID, "community listing dropped a result from another host") {
			actorIDs = append(actorIDs, view.Community.ActorID)
		}
	}
	return actorIDs
}

// onHost reports whether actorID belongs to host, and logs dropMessage with
// both when it does not.
func (l *LemmyLister) onHost(host, actorID, dropMessage string) bool {
	if actorIDOnHost(actorID, l.scheme, host) {
		return true
	}
	l.logger.Warn(dropMessage, "host", host, "actor_id", actorID)
	return false
}

// actorIDOnHost reports whether actorID is a URL in scheme whose hostname is host,
// with no user info, query or fragment. The scheme is matched on the raw string:
// resolveCommunity matches it exactly, and sends anything else to WebFinger on the
// host after an @, where url.Parse would pass a mixed-case scheme.
func actorIDOnHost(actorID, scheme, host string) bool {
	if !strings.HasPrefix(actorID, scheme+"://") || strings.ContainsAny(actorID, "?#") {
		return false
	}
	parsed, err := url.Parse(actorID)
	return err == nil && parsed.User == nil && strings.EqualFold(parsed.Hostname(), host)
}

// nextPosition is where the walk goes after a page, and readEnded reports that
// this read stops there. Fullness counts every result Lemmy served, whether or
// not it was kept.
func nextPosition(position ListingPosition, page []lemmyCommunityView) (next ListingPosition, readEnded, reachedLimit bool) {
	if len(page) < lemmyMaxLimit {
		return firstListingPosition, true, false
	}
	if position.Sort == listingSortNew && slices.ContainsFunc(page, func(view lemmyCommunityView) bool {
		return !view.Community.Published.After(position.Boundary)
	}) {
		return firstListingPosition, true, false // The two ends have met.
	}
	if position.Page < lemmyMaxPage {
		position.Page++
		return position, false, false
	}
	if position.Sort == listingSortOld {
		boundary := page[len(page)-1].Community.Published
		return ListingPosition{Sort: listingSortNew, Page: 1, Boundary: boundary}, true, false
	}
	return firstListingPosition, true, true
}
