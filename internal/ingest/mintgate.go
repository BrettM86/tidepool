package ingest

import (
	"context"
	stderrors "errors"
	"fmt"
	"log/slog"

	"tidepool/internal/errors"
	"tidepool/internal/identity"
	"tidepool/internal/materialize"
)

// ErrMintRateExceeded marks a mint refused by the rate gate. It is not a
// failure of the event (not a skip, not a validation error): the queue parks
// the event without spending an attempt, and the mint succeeds on a later
// claim once the budget has a free slot.
var ErrMintRateExceeded = stderrors.New("ingest: DID mint rate exceeded")

// budgetWaitKey is the context key of the withBudgetWait marker.
type budgetWaitKey struct{}

// withBudgetWait marks ctx so a mint under it waits on the budget and through
// a PLC pause. Only callers that may block for an hour set it; a queue worker
// must not.
func withBudgetWait(ctx context.Context) context.Context {
	return context.WithValue(ctx, budgetWaitKey{}, true)
}

// waitsOnBudget reports whether ctx carries the withBudgetWait marker.
func waitsOnBudget(ctx context.Context) bool {
	return ctx.Value(budgetWaitKey{}) != nil
}

// sweepMintKey is the context key of the withSweepMint marker.
type sweepMintKey struct{}

// withSweepMint marks ctx so a mint under it waits for its token but never for
// a pause: a sweep holds its lock, and a pause can last an hour.
func withSweepMint(ctx context.Context) context.Context {
	return context.WithValue(ctx, sweepMintKey{}, true)
}

// isSweepMint reports whether ctx carries the withSweepMint marker.
func isSweepMint(ctx context.Context) bool {
	return ctx.Value(sweepMintKey{}) != nil
}

// MintGate rate-limits inbound DID minting. Without it, a crafted deep
// comment thread with a distinct fake author per level mints up to the
// materializer's ancestor-depth cap (50) of PLC identities per delivered
// object — and PLC registrations are forever (the amplification vector
// flagged in task 05). The PLC write budget, shared with every other PLC
// write, bounds mint throughput.
type MintGate struct {
	inner  materialize.ActorMinter
	budget *identity.PLCWriteBudget
	logger *slog.Logger
}

// NewMintGate wraps a minter so its mints ask budget for a token.
func NewMintGate(inner materialize.ActorMinter, budget *identity.PLCWriteBudget, logger *slog.Logger) (*MintGate, error) {
	if inner == nil {
		return nil, errors.NewValidationError("minter", "must not be nil")
	}
	if budget == nil {
		return nil, errors.NewValidationError("budget", "must not be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &MintGate{inner: inner, budget: budget, logger: logger}, nil
}

// MintActor implements materialize.ActorMinter. It takes no token itself: the
// token is taken in BeforeSubmit, right before the PLC POST, because handle
// preparation can block on the database for any length of time.
//
// An unmarked mint never blocks a queue worker: a refused one fails fast with
// ErrMintRateExceeded and a PLC 429 is returned at once. A mint under
// withBudgetWait waits for its token, and after a PLC 429 waits out the pause
// and mints again, until it succeeds, fails otherwise or its context ends. A
// mint under withSweepMint waits for its token too, but a pause fails it with
// ErrMintRateExceeded and a PLC 429 is returned at once. It reads the pause again
// after Recheck, whose reads can outlast the check in BeforeSubmit.
//
// BeforeSubmit reports waited when a marked mint found no free token, or is the
// attempt after a pause even if a token is free by then, so Recheck can read
// again what that time may have changed. Every other mint reports false and
// pays for no second read.
func (g *MintGate) MintActor(ctx context.Context, req identity.MintRequest) (*identity.Identity, error) {
	wait := waitsOnBudget(ctx)
	sweep := isSweepMint(ctx)
	paused := false
	refuse := func() error {
		g.logger.Warn("DID mint refused by rate gate",
			"preferred_username", req.PreferredUsername, "instance", req.Instance)
		return fmt.Errorf("mint identity for %s@%s: %w",
			req.PreferredUsername, req.Instance, ErrMintRateExceeded)
	}
	req.BeforeSubmit = func(ctx context.Context) (bool, error) {
		if wait {
			if g.budget.Allow() {
				return paused, nil
			}
			return true, g.budget.Wait(ctx)
		}
		if g.budget.Allow() {
			return false, nil
		}
		if !sweep {
			return false, refuse()
		}
		err := g.budget.WaitUnlessPaused(ctx)
		if stderrors.Is(err, identity.ErrPLCWritesPaused) {
			return true, refuse()
		}
		return true, err
	}
	if sweep {
		recheck := req.Recheck
		req.Recheck = func(ctx context.Context, waited bool) error {
			if recheck != nil {
				if err := recheck(ctx, waited); err != nil {
					return err
				}
			}
			if g.budget.Paused() {
				return refuse()
			}
			return nil
		}
	}
	for {
		minted, err := g.inner.MintActor(ctx, req)
		var rateLimited *identity.PLCRateLimitedError
		if !wait || !stderrors.As(err, &rateLimited) {
			return minted, err
		}
		if err := g.budget.WaitForResume(ctx); err != nil {
			return nil, fmt.Errorf("mint identity for %s@%s: wait for PLC pause to end: %w",
				req.PreferredUsername, req.Instance, err)
		}
		paused = true
	}
}
