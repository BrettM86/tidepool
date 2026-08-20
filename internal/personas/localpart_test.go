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

// dottedLongHandle is 253 chars laid out so the MaxLocalPartLen cut lands
// immediately after a dot: 63 + 1 + 63 + 1 + 63 + 1 + 58 = 250 chars, the
// separator at index 250, then a 2-char TLD.
var dottedLongHandle = strings.Repeat("a", 63) + "." +
	strings.Repeat("b", 63) + "." +
	strings.Repeat("c", 63) + "." +
	strings.Repeat("d", 58) + ".ee"

// hyphenLongHandle is 253 chars whose final label carries a hyphen exactly
// at the cut (index 250).
var hyphenLongHandle = strings.Repeat("a", 63) + "." +
	strings.Repeat("b", 63) + "." +
	strings.Repeat("c", 63) + "." +
	strings.Repeat("d", 58) + "-" + strings.Repeat("e", 2)

// TestDeriveLocalPart_TruncationShape: truncation is a blind cut, so it can
// land on a separator. A local part ending in "." or "-" is not a name any
// implementation renders or matches sanely — Lemmy's mention regex needs a
// trailing alphanumeric, and a trailing dot reads as a hostname root.
func TestDeriveLocalPart_TruncationShape(t *testing.T) {
	for _, tc := range []struct {
		name   string
		handle string
	}{
		{"cut lands on a dot", dottedLongHandle},
		{"cut lands on a hyphen", hyphenLongHandle},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Len(t, tc.handle, 253, "fixture must sit on the handle length limit")
			require.Contains(t, ".-", string(tc.handle[MaxLocalPartLen-1]),
				"fixture must make the LAST KEPT character a separator")

			got, err := DeriveLocalPart(tc.handle, "coves.social")
			require.NoError(t, err)
			assert.LessOrEqual(t, len(got), MaxLocalPartLen)
			assert.False(t, strings.HasSuffix(got, "."), "a local part must not end in a dot: %q", got)
			assert.False(t, strings.HasSuffix(got, "-"), "a local part must not end in a hyphen: %q", got)
			assert.Equal(t, strings.TrimRight(tc.handle[:MaxLocalPartLen], ".-"), got,
				"the cut is trimmed of trailing separators, nothing else")
		})
	}
}

// TestDeriveLocalPart_NormalizesSuffix: the suffix arrives from config and
// from a routed Host, which spell the same authority several ways. Failing
// to normalize it silently demotes native handles to foreign ones — they
// would mint as "alice.coves.social" instead of "alice", permanently.
func TestDeriveLocalPart_NormalizesSuffix(t *testing.T) {
	for _, suffix := range []string{"coves.social", "Coves.Social", "coves.social.", "COVES.SOCIAL."} {
		t.Run(suffix, func(t *testing.T) {
			got, err := DeriveLocalPart("alice.coves.social", suffix)
			require.NoError(t, err)
			assert.Equal(t, "alice", got, "suffix %q names the native space", suffix)
		})
	}

	// The apex is refused through every spelling too — it is the instance
	// actor's own name.
	for _, suffix := range []string{"coves.social.", "Coves.Social"} {
		_, err := DeriveLocalPart("coves.social", suffix)
		require.Error(t, err, "the apex must be refused under suffix %q", suffix)
		assert.True(t, errors.IsValidation(err), "got %v", err)
	}
}
