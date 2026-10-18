package handlemigration

import (
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// defaultRateLimitPause is the pause when PLC sends no usable Retry-After.
const defaultRateLimitPause = time.Hour

// rateLimitPause is how long to pause after a PLC 429 carrying retryAfter:
// delta-seconds or an HTTP-date, honoured exactly. A past date pauses for 0.
// A missing, unparseable, negative, fractional or out-of-range value pauses
// for an hour.
func rateLimitPause(retryAfter string, now time.Time) time.Duration {
	retryAfter = strings.TrimSpace(retryAfter)
	if retryAfter == "" {
		return defaultRateLimitPause
	}
	if strings.Trim(retryAfter, "0123456789") == "" {
		seconds, err := strconv.ParseInt(retryAfter, 10, 64)
		if err != nil || seconds > math.MaxInt64/int64(time.Second) {
			return defaultRateLimitPause
		}
		return time.Duration(seconds) * time.Second
	}
	resumeAt, err := http.ParseTime(retryAfter)
	if err != nil {
		return defaultRateLimitPause
	}
	return max(resumeAt.Sub(now), 0)
}
