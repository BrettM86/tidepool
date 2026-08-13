package consume

import (
	"context"
	"fmt"
	"log/slog"

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
func (d *Dispatcher) handleProfile(ctx context.Context, did string, commit *CommitEvent) error {
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
func (d *Dispatcher) handleIdentity(ctx context.Context, event *JetstreamEvent) error {
	if event.Identity == nil {
		return fmt.Errorf("%w: identity event for %s carries no identity", ErrPermanentEvent, event.DID)
	}
	did := event.Identity.DID
	if did == "" {
		did = event.DID
	}

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
		// A display name the user SET wins over their handle: the profile
		// record is the user speaking about themselves, while the handle is
		// only what the cache falls back to when they have not.
		d.logger.Debug("handle change leaves a user-set display name alone",
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
