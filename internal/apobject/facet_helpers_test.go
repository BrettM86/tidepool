package apobject

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/extension"
	"golang.org/x/net/html"
)

// testFacet builds a raw facet over the byte range [start, end).
func testFacet(start, end int, features ...any) map[string]any {
	return map[string]any{
		"index":    map[string]any{"byteStart": start, "byteEnd": end},
		"features": features,
	}
}

func emphasisFacet(start, end int, featureNames ...string) map[string]any {
	features := make([]any, 0, len(featureNames))
	for _, name := range featureNames {
		features = append(features, map[string]any{"$type": emphasisFacetTypePrefix + name})
	}
	return testFacet(start, end, features...)
}

// buildTestNote renders plaintext and facets through BuildNote and returns the
// Markdown source and the HTML content.
func buildTestNote(t *testing.T, plaintext string, facets []any) (string, string) {
	t.Helper()
	note := BuildNote("actor", "community", "parent", "object", map[string]any{
		"content": plaintext,
		"facets":  facets,
	})
	source, ok := note["source"].(map[string]any)
	require.True(t, ok)
	markdown, ok := source["content"].(string)
	require.True(t, ok)
	content, ok := note["content"].(string)
	require.True(t, ok)
	return markdown, content
}

func parseRenderedMarkdown(t *testing.T, markdown string) *html.Node {
	t.Helper()
	var rendered bytes.Buffer
	converter := goldmark.New(goldmark.WithExtensions(extension.Strikethrough))
	require.NoError(t, converter.Convert([]byte(markdown), &rendered))
	return parseTestHTML(t, rendered.String())
}

func parseTestHTML(t *testing.T, content string) *html.Node {
	t.Helper()
	document, err := html.Parse(strings.NewReader(content))
	require.NoError(t, err)
	return document
}

func findTestElements(root *html.Node, name string) []*html.Node {
	var elements []*html.Node
	var walk func(*html.Node)
	walk = func(node *html.Node) {
		if node.Type == html.ElementNode && node.Data == name {
			elements = append(elements, node)
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(root)
	return elements
}

func testHTMLText(root *html.Node) string {
	var text strings.Builder
	var walk func(*html.Node)
	walk = func(node *html.Node) {
		if node.Type == html.TextNode {
			text.WriteString(node.Data)
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(root)
	return text.String()
}

func testHTMLAttribute(node *html.Node, name string) string {
	for _, attribute := range node.Attr {
		if attribute.Key == name {
			return attribute.Val
		}
	}
	return ""
}
