package personas

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
)

// longHandle is a 253-char handle (the atproto maximum): three 63-char
// labels plus a 61-char TLD label. Derived as a foreign handle it is the
// full 253 chars, which must be truncated to MaxLocalPartLen.
var longHandle = strings.Repeat("a", 63) + "." +
	strings.Repeat("b", 63) + "." +
	strings.Repeat("c", 63) + "." +
	strings.Repeat("d", 61)

// TestDeriveLocalPart is the golden table for the frozen local part.
func TestDeriveLocalPart(t *testing.T) {
	require.Len(t, longHandle, 253, "fixture must sit exactly on the handle length limit")

	tests := []struct {
		name         string
		handle       string
		nativeSuffix string
		want         string
	}{
		{
			name:         "native single label",
			handle:       "alice.coves.social",
			nativeSuffix: "coves.social",
			want:         "alice",
		},
		{
			name:         "native label with hyphen survives",
			handle:       "alice-b.coves.social",
			nativeSuffix: "coves.social",
			// Lemmy's in-text mention regex does not match hyphens in the
			// user part (a display nit); resolution still works, so the
			// hyphen stands rather than being encoded away.
			want: "alice-b",
		},
		{
			name:         "uppercase input normalizes",
			handle:       "Alice.Coves.Social",
			nativeSuffix: "coves.social",
			want:         "alice",
		},
		{
			name:         "deep subdomain is not native",
			handle:       "alice.blog.coves.social",
			nativeSuffix: "coves.social",
			want:         "alice.blog.coves.social",
		},
		{
			name:         "foreign domain keeps the full handle",
			handle:       "bretton.dev",
			nativeSuffix: "coves.social",
			want:         "bretton.dev",
		},
		{
			name:         "foreign handle with many labels",
			handle:       "alice.staging.eu.example.com",
			nativeSuffix: "coves.social",
			want:         "alice.staging.eu.example.com",
		},
		{
			name:         "another PDS's native space is foreign here",
			handle:       "alice.bsky.social",
			nativeSuffix: "coves.social",
			want:         "alice.bsky.social",
		},
		{
			name:         "suffix match must be on a label boundary",
			handle:       "evilcoves.social",
			nativeSuffix: "coves.social",
			// A bare strings.HasSuffix would derive "evil" here and hand an
			// attacker-chosen local part on our own origin.
			want: "evilcoves.social",
		},
		{
			name:         "vanity origin derives its own native space",
			handle:       "alice.vanity.example",
			nativeSuffix: "vanity.example",
			want:         "alice",
		},
		{
			name:         "coves handle is foreign under a vanity origin",
			handle:       "alice.coves.social",
			nativeSuffix: "vanity.example",
			want:         "alice.coves.social",
		},
		{
			name:         "253-char handle truncates to the cap",
			handle:       longHandle,
			nativeSuffix: "coves.social",
			want:         longHandle[:MaxLocalPartLen],
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DeriveLocalPart(tc.handle, tc.nativeSuffix)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
			assert.LessOrEqual(t, len(got), MaxLocalPartLen,
				"a derived local part must leave room for a collision suffix inside Lemmy's varchar(255)")
		})
	}
}

// TestDeriveLocalPart_Rejects covers the inputs that must not produce a
// local part at all. The gate is atproto handle syntax (indigo's
// syntax.ParseHandle), so no IDNA/punycode machinery is needed here:
// non-ASCII never reaches the derivation.
func TestDeriveLocalPart_Rejects(t *testing.T) {
	tests := []struct {
		name         string
		handle       string
		nativeSuffix string
	}{
		{"empty handle", "", "coves.social"},
		{"single label", "alice", "coves.social"},
		{"leading dot", ".alice.coves.social", "coves.social"},
		{"trailing dot", "alice.coves.social.", "coves.social"},
		{"underscore", "alice_bob.coves.social", "coves.social"},
		{"non-ASCII", "álice.coves.social", "coves.social"},
		{"over 253 chars", longHandle + "x", "coves.social"},
		{
			// The apex is the instance actor's own name: an actor claiming
			// it would collide with the origin's identity at webfinger.
			name:         "the origin apex itself",
			handle:       "coves.social",
			nativeSuffix: "coves.social",
		},
		{
			name:         "the origin apex, differently cased",
			handle:       "Coves.Social",
			nativeSuffix: "coves.social",
		},
		{
			// Without a suffix every handle would look native and derive
			// its first label — a silent hijack of the whole namespace.
			name:         "empty native suffix",
			handle:       "alice.coves.social",
			nativeSuffix: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DeriveLocalPart(tc.handle, tc.nativeSuffix)
			require.Error(t, err, "must not derive a local part from %q", tc.handle)
			assert.True(t, errors.IsValidation(err), "want a validation error, got %v", err)
			assert.Empty(t, got, "a rejected handle must not also return a local part")
		})
	}
}

// TestDeriveLocalPart_Deterministic pins that the derivation is pure: the
// local part is FROZEN at creation, so the same inputs must always agree.
func TestDeriveLocalPart_Deterministic(t *testing.T) {
	first, err := DeriveLocalPart("alice.coves.social", "coves.social")
	require.NoError(t, err)
	second, err := DeriveLocalPart("ALICE.coves.social", "coves.social")
	require.NoError(t, err)
	assert.Equal(t, first, second)
	assert.Equal(t, "alice", first)
}
