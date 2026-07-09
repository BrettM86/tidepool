package ingest

import (
	"context"
	stderrors "errors"
	"fmt"
	"log/slog"

	"golang.org/x/time/rate"

	"tidepool/internal/errors"
	"tidepool/internal/identity"
	"tidepool/internal/materialize"
)

// ErrMintRateExceeded marks a mint refused by the rate gate. It is a plain
// retryable error (not a skip, not a validation error): the queue backs the
// delivery off and the mint succeeds on a later attempt once the bucket
// refills.
var ErrMintRateExceeded = stderrors.New("ingest: DID mint rate exceeded")

// MintGate rate-limits inbound DID minting. Without it, a crafted deep
// comment thread with a distinct fake author per level mints up to the
// materializer's ancestor-depth cap (50) of PLC identities per delivered
// object — and PLC registrations are forever (the amplification vector
// flagged in task 05). The token bucket bounds sustained mint throughput
// while the burst absorbs legitimate spikes (community backfills).
type MintGate struct {
	inner   materialize.ActorMinter
	limiter *rate.Limiter
	logger  *slog.Logger
}

// NewMintGate wraps a minter with a token bucket of perMinute sustained
// mints and the given burst.
func NewMintGate(inner materialize.ActorMinter, perMinute float64, burst int, logger *slog.Logger) (*MintGate, error) {
	if inner == nil {
		return nil, errors.NewValidationError("minter", "must not be nil")
	}
	if perMinute <= 0 {
		return nil, errors.NewValidationError("per_minute", "must be positive")
	}
	if burst <= 0 {
		return nil, errors.NewValidationError("burst", "must be positive")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &MintGate{
		inner:   inner,
		limiter: rate.NewLimiter(rate.Limit(perMinute/60.0), burst),
		logger:  logger,
	}, nil
}

// MintActor implements materialize.ActorMinter. A refused mint fails fast
// (never blocks a queue worker) with ErrMintRateExceeded.
func (g *MintGate) MintActor(ctx context.Context, req identity.MintRequest) (*identity.Identity, error) {
	if !g.limiter.Allow() {
		g.logger.Warn("DID mint refused by rate gate",
			"preferred_username", req.PreferredUsername, "instance", req.Instance)
		return nil, fmt.Errorf("mint identity for %s@%s: %w",
			req.PreferredUsername, req.Instance, ErrMintRateExceeded)
	}
	return g.inner.MintActor(ctx, req)
}
