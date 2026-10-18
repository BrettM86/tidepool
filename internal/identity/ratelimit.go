package identity

import (
	"fmt"
	"net/http"
	"slices"
	"strings"
)

const (
	// maxRateLimitHeaderValue caps each captured header value, after
	// repeated values are joined or Retry-After's first value is taken; the
	// PLC directory controls them.
	maxRateLimitHeaderValue = 128
	// maxRateLimitHeaders caps how many headers other than Retry-After one
	// response contributes.
	maxRateLimitHeaders = 16
)

// PLCRateLimitedError marks a PLC request whose final attempt was answered
// 429. It unwraps to ErrPLCTransient.
type PLCRateLimitedError struct {
	// Headers maps canonical header names to values, holding only
	// Retry-After, Ratelimit, Ratelimit-*, X-Ratelimit and X-Ratelimit-*.
	// Retry-After is a singleton field (RFC 9110), so only its first value
	// is kept; repeated values of the other names are joined with ", ".
	// Each value is truncated to 128 bytes.
	Headers map[string]string
}

func (e *PLCRateLimitedError) Error() string {
	if retryAfter, ok := e.Headers["Retry-After"]; ok {
		return fmt.Sprintf("%v: rate limited, Retry-After %q", ErrPLCTransient, retryAfter)
	}
	return fmt.Sprintf("%v: rate limited", ErrPLCTransient)
}

func (e *PLCRateLimitedError) Unwrap() error { return ErrPLCTransient }

// rateLimitHeaders copies the allowlisted rate-limit headers of a response.
// Every other header is dropped, so cookies and tokens never reach a log.
// Retry-After is always kept, as its first value only; the other names are
// sorted, capped at maxRateLimitHeaders and keep every repeated value.
func rateLimitHeaders(header http.Header) map[string]string {
	headers := make(map[string]string)
	names := make([]string, 0, len(header))
	for name, values := range header {
		if len(values) == 0 {
			continue
		}
		canonical := http.CanonicalHeaderKey(name)
		switch {
		case canonical == "Retry-After":
			headers[canonical] = truncateRateLimitValue(values[0])
		case canonical == "Ratelimit", canonical == "X-Ratelimit",
			strings.HasPrefix(canonical, "Ratelimit-"), strings.HasPrefix(canonical, "X-Ratelimit-"):
			names = append(names, name)
		}
	}
	slices.Sort(names)
	for _, name := range names[:min(len(names), maxRateLimitHeaders)] {
		headers[http.CanonicalHeaderKey(name)] = joinRateLimitValues(header[name])
	}
	return headers
}

// joinRateLimitValues joins a repeated header's values with ", " and
// truncates the result to maxRateLimitHeaderValue bytes.
func joinRateLimitValues(values []string) string {
	return truncateRateLimitValue(strings.Join(values, ", "))
}

// truncateRateLimitValue cuts a header value to maxRateLimitHeaderValue bytes.
func truncateRateLimitValue(value string) string {
	if len(value) > maxRateLimitHeaderValue {
		value = value[:maxRateLimitHeaderValue]
	}
	return value
}

// logPLCRateLimit records one 429 response; request is "get" or "post".
func (m *Minter) logPLCRateLimit(did, request string, headers map[string]string) {
	m.logger.Warn("PLC rate limited", "did", did, "request", request, "headers", headers)
}
