// Package linkpreview resolves a shared link to a thumbnail image URL:
// YouTube links through YouTube's oEmbed endpoint, everything else through
// the page's OpenGraph og:image.
package linkpreview

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"

	"golang.org/x/net/html"
)

const (
	// maxOEmbedBytes caps the oEmbed JSON body; a larger body is an error.
	maxOEmbedBytes = 64 << 10
	// maxPageBytes caps how much of an HTML page is read; og:image lives in
	// <head>, so a longer page is truncated rather than refused.
	maxPageBytes = 1 << 20
)

// youTubeHosts are matched exactly; a suffix match would let
// youtube.com.evil.example through.
var youTubeHosts = map[string]bool{
	"youtube.com":       true,
	"www.youtube.com":   true,
	"m.youtube.com":     true,
	"music.youtube.com": true,
	"youtu.be":          true,
}

// Resolver finds thumbnail URLs for links. Outbound requests go through the
// client it was built with, which in production is the SSRF-guarded client.
type Resolver struct {
	client    *http.Client
	userAgent string
}

// NewResolver returns a Resolver that fetches with client and sends userAgent.
// client must be non-nil.
func NewResolver(client *http.Client, userAgent string) *Resolver {
	return &Resolver{client: client, userAgent: userAgent}
}

// ThumbnailURL returns an absolute http(s) image URL for link, or "" when the
// link has none.
func (r *Resolver) ThumbnailURL(ctx context.Context, link string) (string, error) {
	u, err := url.Parse(link)
	if err != nil {
		return "", fmt.Errorf("parse link: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("link scheme %q is not http(s)", u.Scheme)
	}
	if youTubeHosts[strings.ToLower(u.Hostname())] {
		return r.youTubeThumbnail(ctx, link)
	}
	return r.openGraphThumbnail(ctx, link)
}

func (r *Resolver) youTubeThumbnail(ctx context.Context, link string) (string, error) {
	endpoint := "https://www.youtube.com/oembed?url=" + url.QueryEscape(link) + "&format=json"
	resp, err := r.get(ctx, endpoint, "application/json")
	if err != nil {
		// oEmbed refuses private, removed and embedding-disabled videos with
		// these statuses: the link has nothing to preview.
		var status *statusError
		if errors.As(err, &status) {
			switch status.code {
			case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound:
				return "", nil
			}
		}
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxOEmbedBytes+1))
	if err != nil {
		return "", fmt.Errorf("read oembed: %w", err)
	}
	if len(body) > maxOEmbedBytes {
		return "", fmt.Errorf("oembed body exceeds %d bytes", maxOEmbedBytes)
	}
	var doc struct {
		ThumbnailURL string `json:"thumbnail_url"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return "", fmt.Errorf("decode oembed: %w", err)
	}
	if doc.ThumbnailURL == "" {
		return "", nil
	}
	thumb, err := url.Parse(doc.ThumbnailURL)
	if err != nil {
		return "", fmt.Errorf("parse oembed thumbnail_url: %w", err)
	}
	if !isHTTPURL(thumb) {
		return "", fmt.Errorf("oembed thumbnail_url %q is not an absolute http(s) URL", doc.ThumbnailURL)
	}
	return thumb.String(), nil
}

func (r *Resolver) openGraphThumbnail(ctx context.Context, link string) (string, error) {
	resp, err := r.get(ctx, link, "text/html")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	// A page that is not HTML, or does not say what it is, has nothing to
	// preview.
	contentType := resp.Header.Get("Content-Type")
	if contentType == "" {
		return "", nil
	}
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return "", fmt.Errorf("parse page content type %q: %w", contentType, err)
	}
	if mediaType != "text/html" && mediaType != "application/xhtml+xml" {
		return "", nil
	}
	content, err := findOGImage(io.LimitReader(resp.Body, maxPageBytes))
	if err != nil {
		return "", fmt.Errorf("read page: %w", err)
	}
	if content == "" {
		return "", nil
	}
	// Relative og:image resolves against the page's final URL, after redirects.
	image, err := resp.Request.URL.Parse(strings.TrimSpace(content))
	if err != nil {
		return "", fmt.Errorf("parse og:image: %w", err)
	}
	if !isHTTPURL(image) {
		return "", fmt.Errorf("og:image %q is not an absolute http(s) URL", image.Redacted())
	}
	return image.String(), nil
}

// isHTTPURL reports whether u is an http(s) URL with a non-empty host name,
// so a port-only authority such as "https://:443/x" is rejected.
func isHTTPURL(u *url.URL) bool {
	return (u.Scheme == "http" || u.Scheme == "https") && u.Hostname() != ""
}

// statusError is get's error for a non-2xx response.
type statusError struct {
	target string
	code   int
}

func (e *statusError) Error() string {
	return fmt.Sprintf("fetch %s: status %d", e.target, e.code)
}

// get issues a GET and returns the response only when it is 2xx; any other
// status is a *statusError.
func (r *Resolver) get(ctx context.Context, target, accept string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", accept)
	if r.userAgent != "" {
		req.Header.Set("User-Agent", r.userAgent)
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch %s: %w", req.URL.Redacted(), err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		resp.Body.Close()
		return nil, &statusError{target: req.URL.Redacted(), code: resp.StatusCode}
	}
	return resp, nil
}

// findOGImage returns the content of the first og:image <meta> (by property
// or name), or "" when the document has none. Reaching the end of body,
// including the truncation point of a size limit, is not an error.
func findOGImage(body io.Reader) (string, error) {
	tokenizer := html.NewTokenizer(body)
	for {
		switch tokenizer.Next() {
		case html.ErrorToken:
			if err := tokenizer.Err(); !errors.Is(err, io.EOF) {
				return "", err
			}
			return "", nil
		case html.StartTagToken, html.SelfClosingTagToken:
			token := tokenizer.Token()
			if token.Data != "meta" {
				continue
			}
			var isOGImage bool
			var content string
			for _, attr := range token.Attr {
				switch attr.Key {
				case "property", "name":
					isOGImage = isOGImage || strings.EqualFold(attr.Val, "og:image")
				case "content":
					content = attr.Val
				}
			}
			if isOGImage && content != "" {
				return content, nil
			}
		}
	}
}
