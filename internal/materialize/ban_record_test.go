package materialize

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
)

// TestBanRKey_GoldenVector pins the ban record key to the vector the vendored
// lexicon (social.coves.moderation.ban) and Coves publish: lowercase unpadded
// base32 of SHA-256 over the subject DID. Coves indexes a ban record only at
// this key, so a different value is a ban Coves never sees.
func TestBanRKey_GoldenVector(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "a5uzm6rn7ba5hnqtdjrd22rg53skdtu6ksiksa5eini4ychhrcpa",
		BanRKey("did:plc:troublemaker456"),
		"the ban rkey for did:plc:troublemaker456 is fixed by the ban lexicon; a different "+
			"value means the encoding diverged (uppercase, padded, base32-Hex, or another digest)")
}

// TestWriteBanRecord_OverLongReasonIsTruncatedToTheLexiconLimits: the reason
// is a Block's summary, which Lemmy does not bound to the ban lexicon's
// 2000 graphemes / 20000 bytes. An over-long one is cut to fit both, keeping
// its start, instead of failing validation and leaving the ban unpublished.
func TestWriteBanRecord_OverLongReasonIsTruncatedToTheLexiconLimits(t *testing.T) {
	const (
		maxReasonGraphemes = 2000
		maxReasonBytes     = 20000
	)
	// A family ZWJ emoji: one grapheme of 25 bytes.
	const family = "\U0001F468\u200d\U0001F469\u200d\U0001F467\u200d\U0001F466"
	for _, tc := range []struct {
		name   string
		reason string
	}{
		{name: "over the grapheme limit", reason: strings.Repeat("spam ", 500)},
		{name: "over the byte limit within the grapheme limit", reason: strings.Repeat(family, 1500)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			h.serveLemmyWorldFixtures()
			ctx := context.Background()
			community, err := h.m.EnsureCommunity(ctx, &ap.Object{ID: groupID})
			require.NoError(t, err)
			const subjectDID = "did:plc:troublemaker456"
			require.True(t, graphemeCount(tc.reason) > maxReasonGraphemes || len(tc.reason) > maxReasonBytes,
				"precondition: the reason is over a lexicon limit")

			written, err := h.m.WriteBanRecord(ctx, community.DID, subjectDID, tc.reason, nil,
				time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))
			require.NoError(t, err, "an over-long reason must not keep the ban from being published")
			require.True(t, written)

			record, _, err := h.manager.GetRecord(ctx, community.DID, CollectionBan, BanRKey(subjectDID))
			require.NoError(t, err)
			reason, _ := record["reason"].(string)
			assert.LessOrEqual(t, graphemeCount(reason), maxReasonGraphemes, "within the grapheme limit")
			assert.LessOrEqual(t, len(reason), maxReasonBytes, "within the byte limit")
			assert.NotEmpty(t, reason)
			assert.True(t, strings.HasPrefix(tc.reason, strings.TrimSuffix(reason, "…")),
				"the written reason is the start of the Block's summary")
		})
	}
}
