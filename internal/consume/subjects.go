package consume

import (
	"context"
	"fmt"

	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// Subject resolution is the one question a comment and a vote both have to
// answer before anything else: does this bridge federate the thing being
// replied to or voted on, and if so, under which community?
//
// It lives here once because the two paths giving different answers would be a
// fork in what gets delivered where — a comment federated into one community
// while its vote goes to another.

// resolvedSubject is what the bridge already knows about a thing a native
// record points at. Every field comes from state the bridge itself wrote,
// never from the pointing record.
type resolvedSubject struct {
	ATURI string
	APID  string
	// CommunityDID and CommunityAPID are the target community on both sides
	// of the bridge.
	CommunityDID  string
	CommunityAPID string
	// Depth is the subject's OWN recorded reply depth (0 for a post or a
	// subject the bridge does not track depth for). Callers that nest below it
	// add one.
	Depth int
}

// resolveSubject looks a subject up in the two places one can live, in order.
// Both are legitimate:
//
//   - ap_objects: content materialized FROM the fediverse (a Lemmy post or
//     comment), or bridge-origin content mapped at write time;
//   - outbound_objects: a NATIVE postv2 the acceptance engine admitted, or an
//     earlier native comment. Nothing maps those into ap_objects — they were
//     never materialized from the fediverse — so their outbound row is the
//     only evidence they federate at all.
//
// A nil subject with a nil error means "not federated here": a skip, not a
// failure. Most native comments and votes land on native content, and
// dead-lettering all of them would bury the queue.
func (d *Dispatcher) resolveSubject(ctx context.Context, atURI string) (*resolvedSubject, error) {
	mapping, err := d.objectMappings.GetByATURI(ctx, atURI)
	if err != nil && !errors.IsNotFound(err) {
		return nil, fmt.Errorf("resolve subject %s: %w", atURI, err)
	}
	// A soft-deleted mapping is NOT live: the content was removed upstream, so
	// a reply to it or a vote on it has nowhere legitimate to go. Skip rather
	// than federate against a tombstone.
	if err == nil && !mapping.IsDeleted() {
		communityDID, err := d.subjectCommunityDID(ctx, mapping)
		if err != nil {
			return nil, err
		}
		if communityDID != "" {
			community, err := d.communities.GetByDID(ctx, communityDID)
			if errors.IsNotFound(err) {
				// Mapped, but its community is not one this bridge federates:
				// there is nowhere to deliver to.
				return nil, nil
			}
			if err != nil {
				return nil, fmt.Errorf("resolve community %s: %w", communityDID, err)
			}
			depth, err := d.recordedDepth(ctx, atURI)
			if err != nil {
				return nil, err
			}
			return &resolvedSubject{
				ATURI:         atURI,
				APID:          mapping.APID,
				CommunityDID:  communityDID,
				CommunityAPID: community.APGroupID,
				Depth:         depth,
			}, nil
		}
	}

	state, err := d.objects.GetByATURI(ctx, atURI)
	if errors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read subject outbound state %s: %w", atURI, err)
	}
	if state.IsTombstoned() {
		// The bridge already withdrew this object: replying to or voting on it
		// again must not resurrect it.
		return nil, nil
	}
	return &resolvedSubject{
		ATURI:         atURI,
		APID:          state.APObjectID,
		CommunityDID:  state.CommunityDID,
		CommunityAPID: state.CommunityAPID,
		Depth:         state.Depth,
	}, nil
}

// subjectCommunityDID answers which community a mapped record belongs to.
//
// materialize.CommunityDIDOf is THE answer to that question bridge-wide —
// ingest's announced-delete authorization and votes' announced-vote binding
// both ask it, and a fork between those answers is a fork in who may moderate
// what. It needs a RecordGetter because rows written BEFORE migration 016 have
// no community_did column filled: for those the answer sits in the record
// itself (a postv2's `community` field, a comment's thread root), where no
// UPDATE statement could reach it.
//
// Without a RecordGetter this falls back to the column alone, which is correct
// for everything materialized since 016 and simply blind to the older rows —
// they resolve to "" and their comments and votes are skipped rather than
// misdirected.
func (d *Dispatcher) subjectCommunityDID(ctx context.Context, mapping *store.APObjectMapping) (string, error) {
	if d.records == nil {
		return mapping.CommunityDID, nil
	}
	communityDID, err := materialize.CommunityDIDOf(ctx, d.records, mapping)
	if err != nil {
		return "", fmt.Errorf("resolve community of %s: %w", mapping.ATURI, err)
	}
	return communityDID, nil
}

// recordedDepth reads a subject's own reply depth, which exists only if the
// bridge federated it OUTWARD too. A mapped subject with no outbound row is a
// post, or a Lemmy object whose depth this bridge does not track, so it counts
// as the top: replies to it are depth 1 (this returns 0).
//
// Only a genuine MISS defaults to 0. A real error — postgres down, a timeout —
// PROPAGATES: recording depth 0 off a dropped connection would federate a
// deeply nested comment at the wrong nesting and, past Lemmy's cap, keep
// federating ones it will reject, silently and with no retry.
func (d *Dispatcher) recordedDepth(ctx context.Context, atURI string) (int, error) {
	state, err := d.objects.GetByATURI(ctx, atURI)
	if errors.IsNotFound(err) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read recorded depth for %s: %w", atURI, err)
	}
	return state.Depth, nil
}
