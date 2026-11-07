package votes

import (
	"context"
	"log/slog"
	"time"

	"tidepool/internal/ap"
)

// VoteClaim is what an inbound vote claims: its activity id, voter, subject
// and type (ap.TypeLike or ap.TypeDislike).
type VoteClaim struct {
	ActivityID, Actor, Object, Type string
}

// OriginVerdict is the stored outcome of checking a vote at the voter's origin.
type OriginVerdict string

const (
	OriginVerified    OriginVerdict = "verified"
	OriginMismatch    OriginVerdict = "mismatch"
	OriginNotFound    OriginVerdict = "not_found"
	OriginExpired     OriginVerdict = "expired"
	OriginUnavailable OriginVerdict = "unavailable"
)

// OriginVerifier checks a vote claim at the voter's origin. deferred means no
// fetch slot was free (rate limit or host backoff); the verdict is then "".
type OriginVerifier interface {
	Verify(ctx context.Context, claim VoteClaim) (verdict OriginVerdict, deferred bool)
}

// OriginFetcher fetches an object whose final URL keeps the requested
// authority (*ap.Client).
type OriginFetcher interface {
	FetchObjectSameAuthority(ctx context.Context, iri string) (*ap.Object, error)
}

// OriginVerifierOptions pace the verifier.
type OriginVerifierOptions struct {
	HostRPS   float64
	GlobalRPS float64
	Timeout   time.Duration
	Now       func() time.Time
}

// NewOriginVerifier builds the origin verifier.
func NewOriginVerifier(fetcher OriginFetcher, opts OriginVerifierOptions, logger *slog.Logger) (OriginVerifier, error) {
	return originVerifier{}, nil
}

type originVerifier struct{}

func (originVerifier) Verify(context.Context, VoteClaim) (OriginVerdict, bool) { return "", false }
