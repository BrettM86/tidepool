package ingest

import (
	"context"

	"tidepool/internal/ap"
)

// VoteAggregator consumes Like/Dislike activities. votes.Aggregator
// implements this interface with the bridge-side aggregate store behind the
// social.coves.bridge.getVoteAggregates XRPC (task 07); a vote by a bridged
// voter who already has a repo can become a social.coves.feed.vote record in
// that repo instead (PLAN.md decision 21). Ingest only defines the seam and
// hands activities over.
type VoteAggregator interface {
	// ApplyVote records one Like or Dislike. vote is the (possibly
	// Announce-unwrapped) activity: Type is Like or Dislike, Actor is the
	// voter, Object is the voted-on AP object. communityIRI is the
	// announcing community's AP id; ingest never passes "" (bare votes are
	// dropped before they reach the aggregator).
	ApplyVote(ctx context.Context, vote *ap.Object, communityIRI string) error

	// RetractVote undoes a previously applied vote (Undo{Like|Dislike});
	// vote is the inner activity being undone. communityIRI is the
	// announcing community's AP id; ingest never passes "" (bare undos of
	// votes are dropped before they reach the aggregator).
	RetractVote(ctx context.Context, vote *ap.Object, communityIRI string) error
}
