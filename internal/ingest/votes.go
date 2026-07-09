package ingest

import (
	"context"
	"log/slog"

	"tidepool/internal/ap"
)

// VoteAggregator consumes Like/Dislike activities. Votes never become
// records (PLAN.md locked decision 7): task 07 implements this interface
// with the bridge-side aggregate store behind the
// social.coves.bridge.getVoteAggregates XRPC. Task 06 only defines the
// seam and hands activities over.
type VoteAggregator interface {
	// ApplyVote records one Like or Dislike. vote is the (possibly
	// Announce-unwrapped) activity: Type is Like or Dislike, Actor is the
	// voter, Object is the voted-on AP object. communityIRI is the
	// announcing community's AP id ("" for a bare, un-announced vote).
	ApplyVote(ctx context.Context, vote *ap.Object, communityIRI string) error

	// RetractVote undoes a previously applied vote (Undo{Like|Dislike});
	// vote is the inner activity being undone.
	RetractVote(ctx context.Context, vote *ap.Object, communityIRI string) error
}

// noopVotes is the task-06 placeholder implementation: it logs at debug and
// drops the vote. Task 07 replaces it with the real aggregator.
type noopVotes struct {
	logger *slog.Logger
}

// NewNoopVotes returns a VoteAggregator that discards votes (logged at
// debug). Wired until task 07 lands the aggregate store.
func NewNoopVotes(logger *slog.Logger) VoteAggregator {
	if logger == nil {
		logger = slog.Default()
	}
	return &noopVotes{logger: logger}
}

func (v *noopVotes) ApplyVote(_ context.Context, vote *ap.Object, communityIRI string) error {
	v.logger.Debug("vote dropped (aggregator lands in task 07)",
		"type", vote.Type, "object", refID(vote.Object), "community", communityIRI)
	return nil
}

func (v *noopVotes) RetractVote(_ context.Context, vote *ap.Object, communityIRI string) error {
	v.logger.Debug("vote retraction dropped (aggregator lands in task 07)",
		"type", vote.Type, "object", refID(vote.Object), "community", communityIRI)
	return nil
}
