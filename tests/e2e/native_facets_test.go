//go:build e2e

package e2e

// Native Coves richtext facets → Lemmy markdown, through the real stack.
//
// A postv2 carrying social.coves.richtext.facet annotations is written into
// the reference PDS's native repo (an implementation this repo did not
// write), crosses the relay and Jetstream, is admitted by the bridge's
// consumer + acceptance engine into a bridged Lemmy community, and is
// delivered as Create{Page} to the real Lemmy container. The assertion is on
// what LEMMY STORED as the post body (post.body via Lemmy's API): that is
// source.content, the markdown lemmy-ui renders client-side.
//
// Expected bodies are literal strings written here, not produced by the
// renderer under test. Where lemmy-ui's behaviour decided the expected
// shape, it was checked by running lemmy-ui's markdown-it setup
// (src/shared/utils/markdown.ts: markdown-it 14.2.0 + markdown-it-container
// 4.0.0 with its spoiler validate `^spoiler\s+(.*)$`):
//
//   - "::: spoiler\nsecret\n:::" renders as a plain <p> with the colons
//     visible: the container needs a title, so every spoiler carries
//     `::: spoiler <reason or "Spoiler">`.
//   - "::: spoiler Spoiler\n```go\nx := 1\n```\n:::" renders <details> with
//     the <pre><code> inside it; without the title the fence escapes and the
//     `:::` lines show as text.
//   - "> quoted\n>\n> ::: spoiler Spoiler\n> secret\n> :::\n>\n> after"
//     renders <blockquote> containing <details>: a container opens INSIDE a
//     blockquote, so a spoiler in a quote stays nested in it.
//   - "> ## Title\n> body" renders <blockquote><h2>.
//   - "Wow![click](https://example.com/p.png)" renders an <img>;
//     "Wow\\![click](…)" renders a literal "!" followed by a link.

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
)

const (
	facetSpoiler    = "social.coves.richtext.facet#spoiler"
	facetCodeBlock  = "social.coves.richtext.facet#codeBlock"
	facetBlockquote = "social.coves.richtext.facet#blockquote"
	facetHeading    = "social.coves.richtext.facet#heading"
	facetLink       = "social.coves.richtext.facet#link"
	facetBold       = "social.coves.richtext.facet#bold"
)

// facetOver builds one facet covering the first occurrence of span in
// content. Byte offsets come from strings.Index, so a fixture edit cannot
// silently leave a stale hand-counted offset behind.
func facetOver(t *testing.T, content, span string, feature map[string]any) map[string]any {
	t.Helper()
	start := strings.Index(content, span)
	if start < 0 {
		t.Fatalf("fixture error: %q does not occur in %q", span, content)
	}
	return map[string]any{
		"index":    map[string]any{"byteStart": start, "byteEnd": start + len(span)},
		"features": []any{feature},
	}
}

type facetScenario struct {
	name    string
	content string
	facets  func(t *testing.T, content string) []any
	// want is the exact body Lemmy must store; empty when check is used.
	want string
	// check asserts structure where the exact blank-line layout is not part
	// of the decided rule.
	check func(t *testing.T, body string)
}

// TestNativeFacets_LemmyStoresRenderedMarkdown is the real-infrastructure
// acceptance test for rendering native Coves facets into Lemmy markdown.
func TestNativeFacets_LemmyStoresRenderedMarkdown(t *testing.T) {
	h := newHarness(t)

	// A Lemmy community the bridge follows: the acceptance engine admits
	// native posts only into communities with an ACCEPTED Follow.
	owner := h.registerUser(t, h.uniqueName(t, "fct_own"))
	name := h.uniqueName(t, "fct_comm")
	community := owner.createCommunity(t, name, "native facets "+name)
	sub := h.subscribeCommunity(t, "!"+name+"@lemmy")
	if sub.DID == "" {
		t.Fatalf("subscribed %s but the bridge reported no community DID: %+v", name, sub)
	}
	t.Logf("bridged community %s → %s", community.APID, sub.DID)

	pds := &pdsClient{http: h.http}
	if err := pds.createSession(nativeHandle, nativePassword); err != nil {
		t.Fatalf("createSession as the native bootstrap account %q at %s: %v", nativeHandle, pdsURL(), err)
	}

	scenarios := []facetScenario{
		{
			name:    "reasonless spoiler carries a Spoiler title",
			content: "secret",
			facets: func(t *testing.T, c string) []any {
				return []any{facetOver(t, c, "secret", map[string]any{"$type": facetSpoiler})}
			},
			want: "::: spoiler Spoiler\nsecret\n:::",
		},
		{
			name:    "code block inside a spoiler stays inside the container",
			content: "x := 1",
			facets: func(t *testing.T, c string) []any {
				return []any{
					facetOver(t, c, "x := 1", map[string]any{"$type": facetSpoiler}),
					facetOver(t, c, "x := 1", map[string]any{"$type": facetCodeBlock, "language": "go"}),
				}
			},
			want: "::: spoiler Spoiler\n```go\nx := 1\n```\n:::",
		},
		{
			name:    "heading inside a quote",
			content: "Title\nbody",
			facets: func(t *testing.T, c string) []any {
				return []any{
					facetOver(t, c, "Title\nbody", map[string]any{"$type": facetBlockquote}),
					facetOver(t, c, "Title", map[string]any{"$type": facetHeading, "level": 2}),
				}
			},
			want: "> ## Title\n> body",
		},
		{
			name:    "spoiler inside a quote nests in the blockquote",
			content: "quoted\nsecret\nafter",
			facets: func(t *testing.T, c string) []any {
				return []any{
					facetOver(t, c, "quoted\nsecret\nafter", map[string]any{"$type": facetBlockquote}),
					facetOver(t, c, "secret", map[string]any{"$type": facetSpoiler}),
				}
			},
			check: func(t *testing.T, body string) {
				lines := strings.Split(body, "\n")
				for i, line := range lines {
					if !strings.HasPrefix(line, ">") {
						t.Errorf("line %d %q is outside the blockquote; every line of a quoted spoiler must stay quoted", i+1, line)
					}
				}
				if lines[0] != "> quoted" {
					t.Errorf("first line = %q, want %q", lines[0], "> quoted")
				}
				if last := lines[len(lines)-1]; last != "> after" {
					t.Errorf("last line = %q, want %q", last, "> after")
				}
				if !strings.Contains(body, "> ::: spoiler Spoiler\n> secret\n> :::") {
					t.Errorf("body lacks the nested container %q", "> ::: spoiler Spoiler\n> secret\n> :::")
				}
			},
		},
		{
			name:    "literal bang before a link is escaped",
			content: "Wow!click",
			facets: func(t *testing.T, c string) []any {
				return []any{facetOver(t, c, "click", map[string]any{"$type": facetLink, "uri": "https://example.com/p.png"})}
			},
			want: `Wow\![click](https://example.com/p.png)`,
		},
		{
			name:    "bold",
			content: "make it bold now",
			facets: func(t *testing.T, c string) []any {
				return []any{facetOver(t, c, "bold", map[string]any{"$type": facetBold})}
			},
			want: "make it **bold** now",
		},
		{
			name:    "plain link",
			content: "visit the site",
			facets: func(t *testing.T, c string) []any {
				return []any{facetOver(t, c, "the site", map[string]any{"$type": facetLink, "uri": "https://example.com/page"})}
			},
			want: "visit [the site](https://example.com/page)",
		},
	}

	// Write every post first, then poll: federation round trips overlap
	// instead of running back to back.
	titles := make([]string, len(scenarios))
	postURIs := make([]string, len(scenarios))
	for i, sc := range scenarios {
		rkey := syntax.NewTIDNow(uint(i)).String()
		titles[i] = fmt.Sprintf("facets %s %s", h.suffix, rkey)
		record := map[string]any{
			"$type":     colPostV2,
			"community": sub.DID,
			"title":     titles[i],
			"content":   sc.content,
			"facets":    sc.facets(t, sc.content),
			"createdAt": time.Now().UTC().Format(time.RFC3339),
		}
		uri, _, err := pds.createRecord(colPostV2, rkey, record)
		if err != nil {
			t.Fatalf("createRecord for %q: %v", sc.name, err)
		}
		postURIs[i] = uri
		t.Logf("wrote %s (%s)", uri, sc.name)
	}

	for i, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			body := h.awaitLemmyPostBody(t, community.ID, titles[i], sub.DID, postURIs[i])
			t.Logf("lemmy stored body: %q", body)
			if strings.Contains(strings.ReplaceAll(body, `\!`, ""), "![") {
				t.Errorf("stored body %q contains an unescaped \"![\", which lemmy-ui renders as an image", body)
			}
			if strings.Contains(body, ":::") && !strings.Contains(body, "::: spoiler ") {
				t.Errorf("stored body %q has a spoiler container without a title; lemmy-ui then shows the colons as text", body)
			}
			if sc.check != nil {
				sc.check(t, body)
				return
			}
			if body != sc.want {
				t.Errorf("lemmy stored body\n  got:  %q\n  want: %q", body, sc.want)
			}
		})
	}
}

// awaitLemmyPostBody polls the community's post list on Lemmy until the post
// titled title arrives and returns its stored markdown body. On timeout it
// reports the bridge's admission decision for the post, which separates "the
// bridge refused it" from "delivery or Lemmy failed".
func (h *harness) awaitLemmyPostBody(t *testing.T, communityID int, title, communityDID, postURI string) string {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	query := url.Values{
		"community_id": {fmt.Sprint(communityID)},
		"sort":         {"New"},
		"limit":        {"50"},
	}
	var lastErr error
	for {
		var out struct {
			Posts []struct {
				Post struct {
					Name string `json:"name"`
					Body string `json:"body"`
				} `json:"post"`
			} `json:"posts"`
		}
		lastErr = h.admin.do(http.MethodGet, "/api/v3/post/list?"+query.Encode(), nil, &out)
		if lastErr == nil {
			for _, p := range out.Posts {
				if p.Post.Name == title {
					return p.Post.Body
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("post %q (%s) never reached Lemmy community %d within %s (last list error: %v); bridge admission: %s",
				title, postURI, communityID, eventTimeout, lastErr, h.admissionFor(communityDID, postURI))
		}
		time.Sleep(time.Second)
	}
}

// admissionFor describes the bridge's admission ledger row for a post.
func (h *harness) admissionFor(communityDID, postURI string) string {
	var out struct {
		Admissions []struct {
			Post         string `json:"post"`
			Status       string `json:"status"`
			DecisionCode string `json:"decisionCode"`
		} `json:"admissions"`
	}
	if err := h.adminDo(http.MethodGet, "/admin/admissions?community="+url.QueryEscape(communityDID), nil, &out); err != nil {
		return "unavailable: " + err.Error()
	}
	for _, a := range out.Admissions {
		if a.Post == postURI {
			return fmt.Sprintf("status=%q decisionCode=%q", a.Status, a.DecisionCode)
		}
	}
	return "no admission row (the consumer never admitted or rejected it)"
}
