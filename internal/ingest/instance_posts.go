package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"time"
)

const (
	// defaultMaxPostPages is how many pages one post walk takes when
	// LemmyListerOptions leaves MaxPostPages zero.
	defaultMaxPostPages = 20
	// lemmyPostListEndpoint is the post list path. It pages by a cursor naming
	// the last post read, so posts created during a walk shift nothing.
	lemmyPostListEndpoint = "/api/v3/post/list"
)

// PostMark is a point in a host's local posts, newest first. The zero value
// means no mark.
type PostMark struct {
	Published time.Time
	PostID    int64
}

// after reports whether m is newer than other in Lemmy's sort=New order:
// published later, or published at the same time with a higher post id.
func (m PostMark) after(other PostMark) bool {
	return m.Published.After(other.Published) ||
		(m.Published.Equal(other.Published) && m.PostID > other.PostID)
}

// PostWalk is the result of one walk of a host's recent local posts. A non-nil
// Err means the walk failed.
type PostWalk struct {
	ActorIDs []string
	Newest   PostMark
	Err      error
}

// RecentPostReader reads the communities of a host's local posts newer than a
// mark.
type RecentPostReader interface {
	RecentPostCommunities(ctx context.Context, host string, since PostMark) PostWalk
}

// RecentPostCommunities implements RecentPostReader. The walk reads newest
// first and ends after the first page holding a post that is not featured_local
// and is at or before since, at a page shorter than the limit, or at the page
// cap, which is not a failure. Featured posts lead every walk whatever their age,
// so they neither end it nor move Newest, but their communities are kept. A
// failed walk keeps what earlier pages gave.
func (l *LemmyLister) RecentPostCommunities(ctx context.Context, host string, since PostMark) PostWalk {
	var walk PostWalk
	seen := make(map[string]struct{})
	cursor := ""
	for range l.maxPostPages {
		page, err := l.readPostPage(ctx, host, cursor)
		if err != nil {
			walk.Err = fmt.Errorf("ingest: walk posts of %s: %w", host, err)
			l.logger.Warn("post walk failed", "host", host, "page_cursor", cursor, "error", err)
			return walk
		}
		reachedSince := false
		for _, view := range page.Posts {
			actorID := view.Community.ActorID
			if _, found := seen[actorID]; !found {
				seen[actorID] = struct{}{}
				if l.onHost(host, actorID, "post walk dropped a community from another host") {
					walk.ActorIDs = append(walk.ActorIDs, actorID)
				}
			}
			if view.Post.FeaturedLocal {
				continue
			}
			mark := PostMark{Published: view.Post.Published, PostID: view.Post.ID}
			if mark.after(walk.Newest) {
				walk.Newest = mark
			}
			if !mark.after(since) {
				reachedSince = true
			}
		}
		if reachedSince || len(page.Posts) < l.postLimit {
			return walk
		}
		cursor = page.NextPage
	}
	l.logger.Info("post walk stopped at the page cap", "host", host, "max_pages", l.maxPostPages)
	return walk
}

// lemmyListedPost is the part of a listed post the walk reads.
type lemmyListedPost struct {
	Post struct {
		ID            int64     `json:"id"`
		Published     time.Time `json:"published"`
		FeaturedLocal bool      `json:"featured_local"`
	} `json:"post"`
	Community struct {
		ActorID string `json:"actor_id"`
	} `json:"community"`
}

// lemmyPostListResponse is one post list response.
type lemmyPostListResponse struct {
	Posts    []lemmyListedPost `json:"posts"`
	NextPage string            `json:"next_page"`
}

// readPostPage fetches the page after cursor, or the first page when cursor is
// empty. A body Lemmy would not send is a failed read: a post with no published
// time would read as older than any mark and end the walk early, and a full page
// with no next_page cannot be followed.
func (l *LemmyLister) readPostPage(ctx context.Context, host, cursor string) (lemmyPostListResponse, error) {
	query := url.Values{
		"type_":     {"Local"},
		"sort":      {listingSortNew},
		"show_nsfw": {"true"},
		"limit":     {strconv.Itoa(l.postLimit)},
	}
	if cursor != "" {
		query.Set("page_cursor", cursor)
	}
	body, err := l.fetchBody(ctx, host, lemmyPostListEndpoint, query)
	if err != nil {
		return lemmyPostListResponse{}, err
	}
	var page lemmyPostListResponse
	if err := json.Unmarshal(body, &page); err != nil {
		return lemmyPostListResponse{}, fmt.Errorf("decode body: %w", err)
	}
	if page.Posts == nil {
		return lemmyPostListResponse{}, fmt.Errorf("body has no posts array")
	}
	if len(page.Posts) > l.postLimit {
		return lemmyPostListResponse{}, fmt.Errorf("page holds %d posts, more than the limit of %d", len(page.Posts), l.postLimit)
	}
	if slices.ContainsFunc(page.Posts, func(view lemmyListedPost) bool { return view.Post.ID <= 0 }) {
		return lemmyPostListResponse{}, fmt.Errorf("post has no id")
	}
	if slices.ContainsFunc(page.Posts, func(view lemmyListedPost) bool { return view.Post.Published.IsZero() }) {
		return lemmyPostListResponse{}, fmt.Errorf("post has no published time")
	}
	if len(page.Posts) == l.postLimit && page.NextPage == "" {
		return lemmyPostListResponse{}, fmt.Errorf("full page has no next_page")
	}
	return page, nil
}
