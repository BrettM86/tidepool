package consume

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strconv"

	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// The profile cache and the #identity handler.
//
// Neither enqueues anything. Lemmy has no Update{Person} handler — verified
// against 0.19.20 and 1.0 main, it 400s — so an outbound profile activity
// would be a guaranteed-failed delivery. Peers refresh through their own lazy
// ≤24h actor refetch, which means this cache only has to be right when it is
// READ.

// handleProfile refreshes the ap_actors profile CACHE from a
// social.coves.actor.profile record.
//
// A DID with no actor row is skipped rather than minted: a profile edit is not
// a federating interaction, and minting here would give an AP identity to
// every Coves user who ever set a display name.
//
// That skip CLAIMS its gate row, unlike the transient skips errSkipUnclaimed
// covers, and the trade is deliberate. The actor could exist tomorrow, so the
// skip is technically recoverable — but what is lost is one cached display
// name, the actor document already falls back to the local part, and the next
// profile write refreshes it. Against that: a profile edit by a user who never
// federates anything is one of the most common events on this stream, and
// releasing the claim would put every one of them in the unclaimed-skip counter
// and an INFO log, drowning the signal that counter exists to carry.
//
// The tx is unused for the same reason handleProfile writes no outbound state:
// this handler only refreshes a cache, and UpdateProfile is last-write-wins.
func (d *Dispatcher) handleProfile(ctx context.Context, _ *sql.Tx, did string, commit *CommitEvent) error {
	if _, err := d.apActors.GetByDID(ctx, did); err != nil {
		if errors.IsNotFound(err) {
			d.logger.Debug("profile record for a DID with no actor", slog.String("did", did))
			return nil
		}
		return fmt.Errorf("look up actor for %s: %w", did, err)
	}

	// Deleting the profile record clears the cache; the ACTOR survives,
	// because a deleted profile record is not a deleted identity and the local
	// part is frozen regardless.
	profile := store.APActorProfile{}
	if commit.Operation != operationDelete {
		profile.DisplayName = stringField(commit.Record, "displayName")
		// The lexicon's description IS the AP summary — same thing, two
		// vocabularies.
		profile.Summary = stringField(commit.Record, "description")
		// avatar is deliberately NOT cached. It is a blob REF (a CID), not a
		// URL, and turning it into one needs the author's PDS host plus a
		// getBlob convention this task has not established; a half-derived URL
		// would serve every peer a broken image.
	}

	// The record is a whole DOCUMENT, not a patch: every field is written,
	// so an omitted one clears the cached value the user removed. Merging
	// would keep a bio its owner deleted.
	if err := d.apActors.UpdateProfile(ctx, did, profile); err != nil {
		if errors.IsNotFound(err) {
			return nil // the actor vanished between the check and the write
		}
		return fmt.Errorf("refresh profile cache for %s: %w", did, err)
	}
	return nil
}

// handleIdentity applies a #identity handle change.
//
// The event's own handle is a HINT about which DID to re-check, never an
// answer: identity events can be stale or replayed, so the handle is
// re-resolved and re-verified from scratch.
//
// The local part is untouched. It was frozen at actor creation, and
// re-deriving it would strand every federated mention of the old name — a
// rename may refresh the profile CACHE and nothing else.
//
// NO ORDERING GUARD, AND THAT IS THE RULING RATHER THAN AN OVERSIGHT.
// IdentityEvent.Seq is parsed off the wire and deliberately not consulted here,
// where the sibling #account tier claims its seq (applyAccountClaimed) before
// touching anything. The difference is what each handler WRITES. An #account
// frame's payload IS the state applied — active/status go straight into the
// actor row — so a stale replay writes stale facts and a user who came back
// silently stays paused. This handler writes nothing the frame carries: the
// handle is re-resolved from PLC and well-known every time, so a frame from an
// hour ago and a frame from a second ago apply the IDENTICAL current answer.
// Replaying one cannot regress the cache.
//
// The residual, named so a later change does not rediscover it as a surprise:
// two handlers racing for one DID could resolve in one order and write in the
// other, leaving the older handle cached. It is bounded to a single write —
// the fallback below fires only while DisplayName is empty, and the first
// write fills it — so the window closes after the first rename and no seq
// claim is worth holding an advisory lock across two network round-trips for.
// A seq claim becomes REQUIRED the moment this handler starts persisting
// anything the frame itself asserts.
func (d *Dispatcher) handleIdentity(ctx context.Context, event *JetstreamEvent) error {
	if event.Identity == nil {
		return fmt.Errorf("%w: identity event for %s carries no identity", ErrPermanentEvent, event.DID)
	}

	// The envelope DID is authoritative, exactly as it is for #account. A nested
	// payload naming a DIFFERENT DID is malformed or hostile — acting on the
	// inner one lets a frame about DID A resolve, verify and cache DID B — and
	// no retry makes the two agree, so it is rejected as permanent BEFORE the
	// resolver is reached. That last part is the security half: without it a
	// crafted #identity frame aims this bridge's PLC and well-known round trips
	// at any DID the attacker cares to name.
	if event.Identity.DID != "" && event.Identity.DID != event.DID {
		return fmt.Errorf("%w: identity payload DID %s disagrees with the envelope DID %s",
			ErrPermanentEvent, strconv.Quote(event.Identity.DID), strconv.Quote(event.DID))
	}
	// An ABSENT inner DID is not a disagreement: the envelope answers for the
	// frame. Jetstream always fills it, but the DLQ stores raw frames and a
	// redriven or hand-repaired one can be thinner than the wire shape.
	did := event.DID

	// The actor check comes FIRST: every Coves user who renames emits one of
	// these, and resolving would spend two network round-trips updating a
	// cache that does not exist.
	actor, err := d.apActors.GetByDID(ctx, did)
	if err != nil {
		if errors.IsNotFound(err) {
			d.logger.Debug("identity event for a DID with no actor", slog.String("did", did))
			return nil
		}
		return fmt.Errorf("look up actor for %s: %w", did, err)
	}

	handle, err := d.resolver.ResolveDIDHandle(ctx, did)
	if err != nil {
		// The cache keeps its last VERIFIED value. Filling it with the frame's
		// unverified handle would publish a name nobody confirmed, and
		// clearing it would lose a good value over a directory outage.
		return fmt.Errorf("resolve handle for %s: %w", did, err)
	}

	if actor.DisplayName != "" {
		// Any NON-EMPTY display name is left alone. The guard is only
		// DisplayName != "" — it cannot tell a name the user actually set from
		// a handle a PRIOR rename already cached here, so it treats both the
		// same: a rename never overwrites an existing display name.
		//
		// The staleness consequence, accepted deliberately: once a handle has
		// been cached as the display name (the fallback path below), a LATER
		// handle change will not refresh it — the cache keeps showing the old
		// handle until a real actor.profile record sets or clears the field.
		// The profile handler, not this one, is the authority on the display
		// name; a handle is only the fallback when none exists.
		d.logger.Debug("handle change leaves an existing display name alone",
			slog.String("did", did), slog.String("handle", handle))
		return nil
	}

	if err := d.apActors.UpdateProfile(ctx, did, store.APActorProfile{
		DisplayName: handle,
		Summary:     actor.Summary,
		AvatarURL:   actor.AvatarURL,
	}); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("refresh profile cache for %s: %w", did, err)
	}
	return nil
}

// stringField reads an optional string from a decoded record.
func stringField(record map[string]any, name string) string {
	value, _ := record[name].(string)
	return value
}
