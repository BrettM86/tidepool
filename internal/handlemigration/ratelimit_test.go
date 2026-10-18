package handlemigration

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRateLimitPause(t *testing.T) {
	now := time.Date(2026, time.October, 18, 21, 58, 0, 0, time.UTC)
	for _, tc := range []struct {
		name       string
		retryAfter string
		want       time.Duration
	}{
		{"missing header waits an hour", "", time.Hour},
		{"delta seconds", "120", 2 * time.Minute},
		{"zero seconds", "0", 0},
		{"thirty seconds", "30", 30 * time.Second},
		{"two days is not clamped", "172800", 48 * time.Hour},
		{"future HTTP date", "Sun, 18 Oct 2026 22:28:00 GMT", 30 * time.Minute},
		{"past HTTP date", "Sun, 18 Oct 2026 21:00:00 GMT", 0},
		{"unparseable", "soon", time.Hour},
		{"negative seconds", "-5", time.Hour},
		{"fractional seconds", "1.5", time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, rateLimitPause(tc.retryAfter, now))
		})
	}
}
