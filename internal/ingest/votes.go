package ingest

import (
	"context"

	"tidepool/internal/ap"
)

// VoteAggregator consumes Like/Dislike activities. Votes never become
// records (PLAN.md locked decision 7): votes.Aggregator implements this
// interface with the bridge-side aggregate store behind the
// social.coves.bridge.getVoteAggregates XRPC (task 07); ingest only
// defines the seam and hands activities over.
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
