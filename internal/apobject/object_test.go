package apobject

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Second-opinion (important): a post's external embed href must pass a scheme
// allowlist before it is rendered as a Link attachment. A javascript:/data: uri
// federated as a clickable Link is a stored-XSS-shaped hazard on every peer that
// renders it.

func buildPageWithEmbedURI(t *testing.T, uri string) map[string]any {
	t.Helper()
	page, err := BuildPage(
		"https://coves.social/ap/actor/did:plc:x",
		"https://lemmy.world/c/tech",
		"https://coves.social/ap/object/did:plc:x/social.coves.community.postv2/rk",
		map[string]any{
			"title": "a post",
			"embed": map[string]any{
				"$type":    "social.coves.embed.external",
				"external": map[string]any{"uri": uri},
			},
		})
	require.NoError(t, err)
	return page
}

func TestBuildPage_RejectsUnsafeEmbedSchemes(t *testing.T) {
	for _, uri := range []string{
		"javascript:alert(1)",
		"data:text/html,<script>alert(1)</script>",
		"vbscript:msgbox(1)",
		"file:///etc/passwd",
	} {
		page := buildPageWithEmbedURI(t, uri)
		_, has := page["attachment"]
		assert.Falsef(t, has,
			"an unsafe embed scheme (%s) must NOT be rendered as a Link attachment", uri)
	}
}

func TestBuildPage_KeepsSafeEmbedLink(t *testing.T) {
	page := buildPageWithEmbedURI(t, "https://example.com/article")
	attach, ok := page["attachment"].([]any)
	require.True(t, ok, "a safe https link is rendered as an attachment")
	require.NotEmpty(t, attach)
	link, ok := attach[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Link", link["type"])
	assert.Equal(t, "https://example.com/article", link["href"])
}
