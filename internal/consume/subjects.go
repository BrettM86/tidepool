package consume

import (
	"context"
	"fmt"
	"log/slog"

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
	// RootATURI is the at-uri of the THREAD this subject hangs in, as the
	// bridge's own outbound state records it. It is empty for a subject that IS
	// a thread root (Depth 0) and for a comment written before the root was
	// recorded — threadRootOf tells those two apart, because they are opposite
	// answers, not one missing value.
	//
	// It comes from state, never from the record: reply.root is written by the
	// author, so trusting it would let anyone reopen a locked thread by naming a
	// different root.
	RootATURI string
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
			depth, rootATURI, tombstoned, err := d.recordedState(ctx, atURI)
			if err != nil {
				return nil, err
			}
			if tombstoned {
				// The bridge already withdrew this object. This branch USED to
				// be unreachable for native content — with no community_did on
				// a bridge-origin mapping the lookup fell through to the
				// outbound_objects branch below, whose IsTombstoned check
				// caught it. Populating that column (17c) moves native parents
				// up here, and without this check replies to and votes on an
				// author-DELETED native post would start federating again.
				return nil, nil
			}
			if rootATURI == "" {
				// No outbound row said which thread this is, so it is not one
				// the bridge federated OUTWARD — it is fediverse content the
				// bridge materialized IN, and the materializer recorded the
				// thread on the mapping (migration 026). Without this a native
				// reply to a LEMMY COMMENT resolves its thread to that comment,
				// and the lock on the post above it never reaches the reply —
				// which is most of Lemmy, since most replies go under comments.
				rootATURI = mapping.ThreadRootATURI
			}
			return &resolvedSubject{
				ATURI:         atURI,
				APID:          mapping.APID,
				CommunityDID:  communityDID,
				CommunityAPID: community.APGroupID,
				Depth:         depth,
				RootATURI:     rootATURI,
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
		RootATURI:     rootFromSnapshot(state.TranslatedSnapshot),
	}, nil
}

// threadRootOf answers which THREAD a new child of subject belongs to — the
// at-uri a lock on the whole conversation is recorded against.
//
// Three answers, and the order matters:
//
//  1. a recorded root is the answer, and the cheapest one;
//  2. a subject at depth 0 IS a thread root — a post, or an object the bridge
//     tracks no thread state for, which is the same boundary recordedState
//     already draws for depth;
//  3. anything else is a comment whose stored state predates the recorded root,
//     and it must be RESOLVED rather than assumed. Answering "itself" there
//     would silently make every pre-existing nested comment its own thread, so
//     a lock on the real root would not reach it — the bypass surviving the fix
//     for exactly the comments most likely to be in an old thread.
//
// The second result names the object the answer stopped at when the root could
// not be established — the only thing an operator can look up when a comment is
// held for an undeterminable thread.
func (d *Dispatcher) threadRootOf(ctx context.Context, subject *resolvedSubject) (root, deadEnd string, err error) {
	if subject.RootATURI != "" {
		return subject.RootATURI, "", nil
	}
	if subject.Depth == 0 {
		return subject.ATURI, "", nil
	}
	return d.walkThreadRoot(ctx, subject.ATURI)
}

// walkThreadRoot climbs the recorded parent chain of a comment whose stored
// state does not name its thread root, and returns the root it reaches.
//
// Cold by construction: every comment written since the root was recorded
// carries it, so this runs only for rows that predate it — and the update path
// re-writes the snapshot on the next successful edit, so a row that walks once
// stops walking.
//
// Reaching content the bridge holds no outbound state for is NOT a failure: that
// is the edge of what this bridge federated, and the last object on the chain is
// the top of the thread as far as any of our state goes — the same boundary
// recordedState draws for depth, and the same answer a direct reply to that
// object would have got.
//
// A chain that DEAD-ENDS — a nested row naming no parent, or one longer than a
// conversation can be — returns an empty root and NAMES the object it stopped
// at. Three things it deliberately does not do. It does not guess the deepest
// object reached, because that answer would be written into the child's snapshot
// and every later comment would inherit the guess. It does not decide the
// outcome, because "we could not determine the thread" is not "the thread is not
// locked" — refuseInLockedThread weighs the empty answer against the locks that
// actually exist. And it does not report the dead end as a bare failure: the
// object it names is the one an operator has to open to see why. A store failure
// IS an error: retrying is the only honest response to a question that was never
// answered.
func (d *Dispatcher) walkThreadRoot(ctx context.Context, atURI string) (root, deadEnd string, err error) {
	climbed := atURI
	// Bounded by Lemmy's nesting cap plus the root itself: every hop is one
	// level up, so a chain longer than that is a cycle, not a conversation.
	for hop := 0; hop <= maxCommentDepth+1; hop++ {
		state, err := d.objects.GetByATURI(ctx, climbed)
		if errors.IsNotFound(err) {
			return climbed, "", nil
		}
		if err != nil {
			return "", "", fmt.Errorf("read thread state for %s: %w", climbed, err)
		}
		if root := rootFromSnapshot(state.TranslatedSnapshot); root != "" {
			return root, "", nil
		}
		if state.Depth == 0 {
			return climbed, "", nil
		}
		parent := d.parentFromSnapshot(state.TranslatedSnapshot)
		if parent.ATURI == "" {
			// State this consumer never wrote: every comment snapshot it has
			// ever written names its parent, and a post is at depth 0.
			d.logger.Warn("thread root is undeterminable: nested outbound state names no parent",
				slog.String("at_uri", atURI), slog.String("dead_end", climbed))
			return "", climbed, nil
		}
		climbed = parent.ATURI
	}
	d.logger.Warn("thread root is undeterminable: the parent chain is longer than a thread can be",
		slog.String("at_uri", atURI), slog.String("dead_end", climbed))
	return "", climbed, nil
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

// recordedState reads what the bridge's OWN outbound row says about a mapped
// subject: its reply depth, the thread it hangs in, and whether it has been
// withdrawn.
//
// depth exists only if the bridge federated the subject outward too. A mapped
// subject with no outbound row is a post, or a Lemmy object whose depth this
// bridge does not track, so it counts as the top: replies to it are depth 1
// (this returns 0), and it is its own thread root (this returns no root, which
// threadRootOf reads together with the depth).
//
// tombstoned is the same fact the outbound_objects branch of resolveSubject
// checks, read HERE because both facts come off one row and the caller needs
// them together — a second read could see a delete land between them and
// federate against a subject the first read had already shown as live.
//
// Only a genuine MISS defaults to zero values. A real error — postgres down, a
// timeout — PROPAGATES: reading depth 0 off a dropped connection would federate
// a deeply nested comment at the wrong nesting and, past Lemmy's cap, keep
// federating ones it will reject, silently and with no retry; and reading
// "not tombstoned" off one would resurrect deleted content.
func (d *Dispatcher) recordedState(ctx context.Context, atURI string) (depth int, rootATURI string, tombstoned bool, err error) {
	state, err := d.objects.GetByATURI(ctx, atURI)
	if errors.IsNotFound(err) {
		return 0, "", false, nil
	}
	if err != nil {
		return 0, "", false, fmt.Errorf("read recorded state for %s: %w", atURI, err)
	}
	return state.Depth, rootFromSnapshot(state.TranslatedSnapshot), state.IsTombstoned(), nil
}
