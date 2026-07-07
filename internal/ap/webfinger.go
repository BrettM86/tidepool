package ap

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"tidepool/internal/errors"
)

// WebFingerLink is one entry of a JRD document's links array.
type WebFingerLink struct {
	Rel  string `json:"rel,omitempty"`
	Type string `json:"type,omitempty"`
	Href string `json:"href,omitempty"`
}

// WebFingerResponse is the JRD document served at /.well-known/webfinger.
type WebFingerResponse struct {
	Subject string          `json:"subject,omitempty"`
	Aliases []string        `json:"aliases,omitempty"`
	Links   []WebFingerLink `json:"links,omitempty"`
}

// ParseHandle splits a fediverse handle into username and host. It accepts
// the forms Lemmy and Mastodon users write:
//
//	user@instance, @user@instance, !community@instance, acct:user@instance
//
// The leading ! (Lemmy community sigil) and @ carry no meaning at the
// WebFinger layer — communities and users resolve identically.
func ParseHandle(handle string) (username, host string, err error) {
	trimmed := strings.TrimSpace(handle)
	trimmed = strings.TrimPrefix(trimmed, "acct:")
	trimmed = strings.TrimPrefix(trimmed, "!")
	trimmed = strings.TrimPrefix(trimmed, "@")

	username, host, found := strings.Cut(trimmed, "@")
	// Reject anything that would let the host smuggle a path, query, fragment,
	// or second authority into the WebFinger URL we build from it.
	if !found || username == "" || host == "" || strings.ContainsAny(host, "@/ ?#") {
		return "", "", errors.NewValidationError("handle",
			fmt.Sprintf("%q is not of the form user@instance or !community@instance", handle))
	}
	return username, host, nil
}

// ResolveHandle resolves a fediverse handle (user@instance or
// !community@instance) to the actor's canonical AP id via WebFinger,
// preferring the rel="self" link with an ActivityPub media type.
func (c *Client) ResolveHandle(ctx context.Context, handle string) (actorURL string, err error) {
	username, host, err := ParseHandle(handle)
	if err != nil {
		return "", err
	}

	acct := fmt.Sprintf("acct:%s@%s", username, host)
	query := url.Values{}
	query.Set("resource", acct)
	webfingerURL := fmt.Sprintf("https://%s/.well-known/webfinger?%s", host, query.Encode())

	body, err := c.getDedupedMode(ctx, webfingerURL, fetchModeWebFinger)
	if err != nil {
		// Only a genuine 404 means "no such account". A 401/403 (Cloudflare, a
		// defederating instance) is surfaced as-is so the caller can tell a
		// blocked lookup apart from a missing account.
		if errors.IsNotFound(err) {
			return "", errors.NewNotFoundError("webfinger account", handle)
		}
		return "", fmt.Errorf("ap: webfinger %s: %w", handle, err)
	}

	var jrd WebFingerResponse
	if err := json.Unmarshal(body, &jrd); err != nil {
		return "", fmt.Errorf("ap: webfinger %s: parse JRD: %w", handle, err)
	}

	// Host-confusion guard: a self link must resolve to the same authority we
	// queried (the interoperable WebFinger rule) — otherwise instance A could
	// claim to speak for actors on instance B — OR the JRD subject must be the
	// exact acct we asked for. Reject anything else.
	subjectMatches := strings.EqualFold(jrd.Subject, acct)

	// Prefer rel=self with an AP media type; fall back to any rel=self.
	var fallback string
	for _, link := range jrd.Links {
		if link.Rel != "self" || link.Href == "" {
			continue
		}
		if !hrefAuthorityMatches(link.Href, host) && !subjectMatches {
			continue
		}
		if isActivityJSONType(link.Type) {
			return link.Href, nil
		}
		if fallback == "" {
			fallback = link.Href
		}
	}
	if fallback != "" {
		return fallback, nil
	}
	return "", errors.NewNotFoundError("webfinger self link", handle)
}

// hrefAuthorityMatches reports whether an href's authority (host:port) equals
// the queried WebFinger host. The comparison is case-insensitive.
func hrefAuthorityMatches(href, host string) bool {
	u, err := url.Parse(href)
	if err != nil || u.Host == "" {
		return false
	}
	return strings.EqualFold(u.Host, host)
}

// ActorHandle derives the canonical fediverse handle for an actor document
// (the reverse of ResolveHandle): preferredUsername@host-of-actor-id, with a
// leading ! for Groups (Lemmy community convention).
func ActorHandle(actor *Object) (string, error) {
	if actor == nil {
		return "", errors.NewValidationError("actor", "must not be nil")
	}
	if actor.PreferredUsername == "" {
		return "", errors.NewValidationError("preferredUsername",
			fmt.Sprintf("actor %s has no preferredUsername", actor.ID))
	}
	host := actor.Host()
	if host == "" {
		return "", errors.NewValidationError("id",
			fmt.Sprintf("actor id %q has no usable host", actor.ID))
	}
	handle := actor.PreferredUsername + "@" + host
	if actor.Type == TypeGroup {
		return "!" + handle, nil
	}
	return handle, nil
}

// ResolveActorHandle fetches an actor and returns both its handle and the
// fetched document (so callers don't fetch twice).
func (c *Client) ResolveActorHandle(ctx context.Context, actorURL string) (string, *Object, error) {
	actor, err := c.FetchActor(ctx, actorURL)
	if err != nil {
		return "", nil, err
	}
	handle, err := ActorHandle(actor)
	if err != nil {
		return "", nil, err
	}
	return handle, actor, nil
}

func isActivityJSONType(mediaType string) bool {
	mediaType = strings.ToLower(mediaType)
	return strings.HasPrefix(mediaType, "application/activity+json") ||
		strings.HasPrefix(mediaType, "application/ld+json")
}
