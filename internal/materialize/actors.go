package materialize

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/ipfs/go-cid"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/identity"
	"tidepool/internal/store"
)

// nobridgeMarkers are the consent opt-out hashtags (PLAN.md locked decision
// 6, same convention bridgy-fed honors). An actor carrying either in its
// summary or tags is never materialized.
var nobridgeMarkers = []string{"#nobridge", "#nobot"}

// EnsureActor makes sure the AP person behind actorRef is bridged: DID
// minted, bridged_actors row upserted, actor.profile record (rkey "self")
// committed, profile fresher than the refresh TTL. It returns the stored
// row for the caller to place content with.
//
// Consent is enforced here, fail-closed: nobridge and deleted actors return
// a SkipError — their content is dropped by every caller with the reason
// logged.
func (m *Materializer) EnsureActor(ctx context.Context, actorRef *ap.Object) (*store.BridgedActor, error) {
	return m.ensureActor(ctx, actorRef, false)
}

// RefreshActor re-materializes the actor's profile regardless of TTL —
// the Update{Person} path.
func (m *Materializer) RefreshActor(ctx context.Context, actorRef *ap.Object) (*store.BridgedActor, error) {
	return m.ensureActor(ctx, actorRef, true)
}

func (m *Materializer) ensureActor(ctx context.Context, actorRef *ap.Object, force bool) (*store.BridgedActor, error) {
	if actorRef == nil || actorRef.ID == "" {
		return nil, errors.NewValidationError("actor", "must carry an AP actor id")
	}
	apID := actorRef.ID

	stored, err := m.actors.GetByAPActorID(ctx, apID)
	switch {
	case err == nil:
		return m.ensureKnownActor(ctx, stored, actorRef, force)
	case errors.IsNotFound(err):
		return m.bridgeNewActor(ctx, actorRef, store.ActorTypePerson, force)
	default:
		return nil, fmt.Errorf("materialize: look up actor %s: %w", apID, err)
	}
}

// ensureKnownActor applies consent and freshness policy to an actor we have
// bridged (or refused) before.
//
// A row whose id ap_objects maps as content is stranded, and healed first,
// whatever its consent state: a tombstoned or opted-out row on a post's id
// still reads that id as a bridged actor. It is healed before any fetch, too
// (the origin may serve the post itself, which names the same id). The check
// is skipped only for a synced OK row: an OK row is stamped only after its
// profile committed (rematerializeProfile), and a committed profile mapping
// means the id is not content. A non-OK row is checked even when stamped,
// since the nobridge re-check stamps without committing a profile; such rows
// are rare, so the fresh OK path still costs no extra read. This relies on
// every OK-path MarkProfileSynced following a profile commit: a call site that
// stamps an OK row without one would reopen stranded rows.
func (m *Materializer) ensureKnownActor(ctx context.Context, stored *store.BridgedActor, actorRef *ap.Object, force bool) (*store.BridgedActor, error) {
	if stored.ProfileSyncedAt == nil || stored.ConsentState != store.ConsentStateOK {
		if existing, err := m.objects.GetByAPID(ctx, stored.APActorID); err == nil {
			if !isProfileCollection(existing.Collection) {
				return nil, m.healStrandedActorRow(ctx, stored, existing.Collection)
			}
		} else if !errors.IsNotFound(err) {
			return nil, fmt.Errorf("materialize: check mapping for actor %s: %w", stored.APActorID, err)
		}
	}
	switch stored.ConsentState {
	case store.ConsentStateDeleted:
		// Terminal: the repo is tombstoned, nothing is ever materialized.
		return nil, skip(stored.APActorID, "actor is deleted (tombstoned repo)")
	case store.ConsentStateNoBridge:
		// Re-check on the profile TTL: removing the marker upstream restores
		// bridging, so nobridge is sticky only until the next stale check.
		if !force && m.profileFresh(stored) {
			return nil, skip(stored.APActorID, "actor opted out (#nobridge/#nobot)")
		}
		doc, err := m.knownActorDoc(ctx, stored, actorRef, force)
		if err != nil {
			return nil, err
		}
		if hasNobridgeMarker(doc) {
			// Stamps without a profile commit; ensureKnownActor still checks
			// this non-OK row for a stranded id.
			if err := m.actors.MarkProfileSynced(ctx, stored.APActorID, m.now()); err != nil {
				return nil, fmt.Errorf("materialize: mark nobridge re-check for %s: %w", stored.APActorID, err)
			}
			return nil, skip(stored.APActorID, "actor opted out (#nobridge/#nobot)")
		}
		if err := m.actors.SetConsentState(ctx, stored.APActorID, store.ConsentStateOK); err != nil {
			return nil, fmt.Errorf("materialize: restore consent for %s: %w", stored.APActorID, err)
		}
		m.logger.Info("actor removed nobridge marker; bridging restored", "ap_actor_id", stored.APActorID)
		stored.ConsentState = store.ConsentStateOK
		return m.rematerializeProfile(ctx, stored, doc)
	case store.ConsentStateOK:
		if !force && m.profileFresh(stored) {
			return stored, nil
		}
		doc, err := m.knownActorDoc(ctx, stored, actorRef, force)
		if err != nil {
			return nil, err
		}
		if hasNobridgeMarker(doc) {
			// A previously-bridged actor opted out: scrub the content already
			// mirrored (posts, comments, profile) before recording the
			// consent flip, so nothing of theirs keeps being served — same
			// ordering discipline as DeleteActor, but reversible (NoBridge,
			// not the terminal Deleted).
			if _, err := m.scrubActorRecords(ctx, stored.DID); err != nil {
				return nil, err
			}
			if err := m.actors.SetConsentState(ctx, stored.APActorID, store.ConsentStateNoBridge); err != nil {
				return nil, fmt.Errorf("materialize: record nobridge for %s: %w", stored.APActorID, err)
			}
			m.logger.Info("actor added nobridge marker; existing content scrubbed and new materialization stopped",
				"ap_actor_id", stored.APActorID)
			return nil, skip(stored.APActorID, "actor opted out (#nobridge/#nobot)")
		}
		return m.rematerializeProfile(ctx, stored, doc)
	default:
		return nil, fmt.Errorf("materialize: actor %s has invalid consent state %q",
			stored.APActorID, stored.ConsentState)
	}
}

// bridgeNewActor mints an identity for an unseen actor and materializes its
// profile. actorType selects person vs group handling (groups come through
// EnsureCommunity, which also maintains the communities row).
func (m *Materializer) bridgeNewActor(ctx context.Context, actorRef *ap.Object, actorType store.ActorType, allowEmbedded bool) (*store.BridgedActor, error) {
	doc, err := m.actorDoc(ctx, actorRef, allowEmbedded)
	if err != nil {
		return nil, err
	}
	if doc.Type == ap.TypeGroup {
		// A Group can reach this path through attributedTo references too;
		// its bridged row must say group so profile refreshes build
		// community.profile records.
		actorType = store.ActorTypeGroup
	}
	if hasNobridgeMarker(doc) {
		// Never mint for an opted-out actor: no DID, no row, content dropped.
		m.logger.Info("actor opted out (#nobridge/#nobot); not bridging",
			"ap_actor_id", doc.ID, "actor_type", string(actorType))
		return nil, skip(doc.ID, "actor opted out (#nobridge/#nobot)")
	}
	if doc.PreferredUsername == "" {
		return nil, skip(doc.ID, "actor has no preferredUsername (cannot derive a handle)")
	}
	instance := doc.Host()
	if instance == "" {
		return nil, skip(doc.ID, "actor id has no parseable host")
	}
	// The fetched document names its own id, bound only to the fetch
	// authority, so an origin can answer with a Person whose id is one of its
	// posts'. The profile commit would refuse that id, but too late: the DID is
	// minted and the bridged_actors row keyed on it by then, and PLC DIDs are
	// permanent. Live or deleted, a content id is never an actor's.
	profileCollection := CollectionActorProfile
	if actorType == store.ActorTypeGroup {
		profileCollection = CollectionCommunityProfile
	}
	if existing, err := m.objects.GetByAPID(ctx, doc.ID); err == nil {
		if existing.Collection != profileCollection {
			// A retry of an alias naming a content id lands here, never in
			// ensureKnownActor, so a row stranded on that id is healed here.
			if !isProfileCollection(existing.Collection) {
				if row, err := m.actors.GetByAPActorID(ctx, doc.ID); err == nil {
					return nil, m.healStrandedActorRow(ctx, row, existing.Collection)
				} else if !errors.IsNotFound(err) {
					return nil, fmt.Errorf("materialize: look up actor %s: %w", doc.ID, err)
				}
			}
			return nil, skip(doc.ID, fmt.Sprintf(
				"id already maps a %s record; refusing to bridge it as an actor", existing.Collection))
		}
	} else if !errors.IsNotFound(err) {
		return nil, fmt.Errorf("materialize: check mapping for actor %s: %w", doc.ID, err)
	}
	// The same binding lets an unseen alias IRI answer with an already-bridged
	// actor's id. That id has its row, so minting would only orphan a DID, and
	// reusing the row would rewrite that actor's profile from the alias's
	// document. A same-id row is a concurrent bridge, left to mintAndUpsert.
	// Refusing is deliberately fail-closed: content attributed to the alias IRI
	// is dropped rather than attributed to the canonical actor, since nothing
	// but the alias's own document binds the two.
	if doc.ID != actorRef.ID {
		if _, err := m.actors.GetByAPActorID(ctx, doc.ID); err == nil {
			return nil, skip(actorRef.ID, fmt.Sprintf(
				"actor document names already-bridged actor %s; refusing to bridge an alias over it", doc.ID))
		} else if !errors.IsNotFound(err) {
			return nil, fmt.Errorf("materialize: look up actor %s: %w", doc.ID, err)
		}
	}

	stored, inserted, err := m.mintAndUpsert(ctx, doc, actorType, instance)
	if err != nil {
		return nil, err
	}
	if !inserted && doc.ID != actorRef.ID {
		// The alias check above lost a race: doc.ID's row landed after it. The
		// winner is that actor, not a concurrent bridge of this one, and
		// reusing it would rewrite its profile from the alias's document.
		return nil, skip(actorRef.ID, fmt.Sprintf(
			"actor document names already-bridged actor %s; refusing to bridge an alias over it", doc.ID))
	}
	profiled, err := m.rematerializeProfile(ctx, stored, doc)
	if inserted && errors.IsCollectionImmutable(err) {
		// The mapping check above lost a race: another worker mapped doc.ID
		// as content after it, and the profile commit refused. The row this
		// call inserted would leave a content id reading as a bridged actor
		// for good, so it goes; the DID cannot. Detached from processing
		// cancellation, so a cancelled delivery does not leave the row.
		// A row already gone (healed concurrently) is the same outcome.
		if delErr := m.removeActorRow(ctx, stored.APActorID, stored.DID); delErr != nil && !errors.IsNotFound(delErr) {
			return nil, fmt.Errorf("materialize: remove bridged actor %s after refused profile commit: %w", stored.APActorID, delErr)
		}
		m.logger.Error("profile commit refused because the id is mapped as content; removed the just-inserted bridged actor row (freshly minted DID is orphaned on the PLC directory)",
			"ap_actor_id", stored.APActorID, "orphaned_did", stored.DID)
	}
	return profiled, err
}

// mintAndUpsert mints a DID and registers the bridged_actors row, handling
// the two races minting can lose:
//   - another process bridged the same AP actor concurrently → reuse the
//     winner's row (our freshly minted DID is orphaned; logged loudly);
//   - another actor grabbed the same handle between the availability check
//     and the insert → retry the mint once (the suffixer now sees the
//     winner). Never re-mints in a loop beyond that.
//
// The bool is true only when the returned row carries the DID this call
// minted, i.e. this call inserted it; a reused winner is false.
func (m *Materializer) mintAndUpsert(ctx context.Context, doc *ap.Object, actorType store.ActorType, instance string) (*store.BridgedActor, bool, error) {
	for attempt := 0; ; attempt++ {
		minted, err := m.minter.MintActor(ctx, identity.MintRequest{
			ActorType:         actorType,
			PreferredUsername: doc.PreferredUsername,
			Instance:          instance,
		})
		if err != nil {
			return nil, false, fmt.Errorf("materialize: mint identity for %s: %w", doc.ID, err)
		}
		stored, err := m.actors.UpsertActor(ctx, store.BridgedActor{
			APActorID:           doc.ID,
			ActorType:           actorType,
			DID:                 minted.DID,
			Handle:              minted.Handle,
			SigningKeyEncrypted: minted.SigningKeyEncrypted,
			ConsentState:        store.ConsentStateOK,
		})
		if err == nil {
			return stored, stored.DID == minted.DID, nil
		}
		if !errors.IsAlreadyExists(err) {
			return nil, false, fmt.Errorf("materialize: register bridged actor %s: %w", doc.ID, err)
		}
		// Same AP actor already bridged by a concurrent worker? Reuse theirs.
		if winner, getErr := m.actors.GetByAPActorID(ctx, doc.ID); getErr == nil {
			m.logger.Error("lost bridging race for actor; reusing winner's identity (freshly minted DID is orphaned on the PLC directory)",
				"ap_actor_id", doc.ID, "orphaned_did", minted.DID, "winning_did", winner.DID)
			return winner, false, nil
		}
		// Otherwise the collision is on the handle (a different actor won
		// it). Retry once so the suffixer can pick a free handle; the first
		// mint's DID is orphaned (DID reuse via updateHandle is deferred).
		if attempt == 0 {
			m.logger.Error("handle collision while registering bridged actor; re-minting once (previous DID is orphaned on the PLC directory)",
				"ap_actor_id", doc.ID, "orphaned_did", minted.DID, "handle", minted.Handle)
			continue
		}
		return nil, false, fmt.Errorf("materialize: register bridged actor %s after handle-collision retry: %w", doc.ID, err)
	}
}

// rematerializeProfile writes the actor's profile record (rkey "self"),
// refreshes the mapping, and stamps profile_synced_at. Idempotent: an
// unchanged profile is a repo-layer no-op.
//
// Media carry-forward (task 11): a refresh whose avatar/banner fetch FAILS
// while the actor still advertises the image keeps the previously stored
// blob instead of dropping it — a transient 5xx or timeout at the origin
// must not strip profiles bare until the next refresh. An actor that
// REMOVED its image (no icon/image in the doc) still loses the blob, as it
// should.
func (m *Materializer) rematerializeProfile(ctx context.Context, stored *store.BridgedActor, doc *ap.Object) (*store.BridgedActor, error) {
	collection := CollectionActorProfile
	if stored.ActorType == store.ActorTypeGroup {
		collection = CollectionCommunityProfile
	}

	avatar := m.fetchBlobWithCarryForward(ctx, stored.DID, collection, imageURL(doc.Icon), slotAvatar, "avatar")
	banner := m.fetchBlobWithCarryForward(ctx, stored.DID, collection, imageURL(doc.Image), slotBanner, "banner")

	var record map[string]any
	switch stored.ActorType {
	case store.ActorTypeGroup:
		record = m.buildCommunityProfile(doc, stored.CreatedAt, avatar, banner)
	default:
		record = m.buildActorProfile(doc, stored.CreatedAt, avatar, banner)
	}
	if _, err := m.commitRecord(ctx, stored.DID, collection, ProfileRKey, record, doc, stored.DID, ""); err != nil {
		return nil, err
	}
	// Stamped only after the commit: ensureKnownActor skips the stranded-row
	// check for a synced OK row on that basis.
	if err := m.actors.MarkProfileSynced(ctx, stored.APActorID, m.now()); err != nil {
		return nil, fmt.Errorf("materialize: mark profile synced for %s: %w", stored.APActorID, err)
	}
	return stored, nil
}

// actorRowRemovalTimeout bounds the detached delete of a bridged_actors row
// that must not outlive the refusal that found it.
const actorRowRemovalTimeout = 10 * time.Second

// removeActorRow deletes apActorID's row while it still carries did, on a
// context detached from processing cancellation: the refusal that calls it
// has already happened, and a cancelled delivery must not leave the row.
func (m *Materializer) removeActorRow(ctx context.Context, apActorID, did string) error {
	removeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), actorRowRemovalTimeout)
	defer cancel()
	return m.actors.DeleteActorRow(removeCtx, apActorID, did)
}

// healStrandedActorRow removes row, a bridged_actors row keyed on an id that
// ap_objects maps as content (in collection): the leftover of a first bridge
// that lost the race to that mapping, or whose compensation failed. Content
// ids are never actors', so the row goes (the DID stays orphaned) and the id
// is a skip.
func (m *Materializer) healStrandedActorRow(ctx context.Context, row *store.BridgedActor, collection string) error {
	err := m.removeActorRow(ctx, row.APActorID, row.DID)
	switch {
	case err == nil:
		m.logger.Error("removed a bridged actor row keyed on an id mapped as content (its DID is orphaned on the PLC directory)",
			"ap_actor_id", row.APActorID, "did", row.DID)
	case !errors.IsNotFound(err):
		return fmt.Errorf("materialize: remove stranded bridged actor %s: %w", row.APActorID, err)
	}
	return skip(row.APActorID, fmt.Sprintf("id already maps a %s record; it is not an actor", collection))
}

// isProfileCollection reports whether collection holds actor or community
// profiles, the only records an actor's id may map.
func isProfileCollection(collection string) bool {
	return collection == CollectionActorProfile || collection == CollectionCommunityProfile
}

// fetchBlobWithCarryForward fetches profile media like fetchBlob, but when a
// TRANSIENT fetch failure hits AND the actor still advertises an image URL,
// it falls back to the blob already stored in the existing profile record
// under field (avatar/banner) — a temporary 5xx/timeout/dial error must not
// strip the profile bare until the next refresh. A PERMANENT removal
// (IsNotFound 404 / IsTombstoned 410: the image was deleted at origin while
// its URL lingers in a stale doc) is NOT carried forward — the blob is
// dropped, because serving media the origin removed forever is wrong. A
// first-ever materialization has no existing record, so the fallback is
// naturally empty there.
func (m *Materializer) fetchBlobWithCarryForward(ctx context.Context, did, collection, url string, slot blobSlot, field string) *atdata.Blob {
	if url == "" {
		return nil
	}
	blob, fetchErr := m.fetchBlobClassified(ctx, did, url, slot)
	if blob != nil {
		return blob
	}
	if errors.IsNotFound(fetchErr) || errors.IsTombstoned(fetchErr) {
		// Image gone at origin: drop it rather than carrying the stale blob
		// forward forever.
		m.logger.Info("media removed at origin; dropping previously stored blob",
			"did", did, "field", field, "url", url)
		return nil
	}
	existing, _, err := m.repos.GetRecord(ctx, did, collection, ProfileRKey)
	if err != nil {
		if !errors.IsNotFound(err) {
			m.logger.Warn("media carry-forward: read existing profile failed",
				"did", did, "field", field, "error", err)
		}
		return nil
	}
	prev, ok := existing[field].(atdata.Blob)
	if !ok {
		return nil
	}
	m.logger.Info("media fetch failed; carrying forward previously stored blob",
		"did", did, "field", field, "url", url, "cid", cid.Cid(prev.Ref).String())
	return &prev
}

// EnsureCommunity makes sure the AP group behind groupRef is bridged:
// community DID minted, bridged_actors row (which holds the signing key the
// repo layer needs) and communities row upserted, community.profile record
// committed. Returns the communities row.
func (m *Materializer) EnsureCommunity(ctx context.Context, groupRef *ap.Object) (*store.Community, error) {
	return m.ensureCommunity(ctx, groupRef, false)
}

// RefreshCommunity re-materializes the community profile regardless of TTL
// — the Update{Group} path.
func (m *Materializer) RefreshCommunity(ctx context.Context, groupRef *ap.Object) (*store.Community, error) {
	return m.ensureCommunity(ctx, groupRef, true)
}

func (m *Materializer) ensureCommunity(ctx context.Context, groupRef *ap.Object, force bool) (*store.Community, error) {
	if groupRef == nil || groupRef.ID == "" {
		return nil, errors.NewValidationError("group", "must carry an AP group id")
	}
	apID := groupRef.ID

	// The bridged_actors row is the source of truth for consent, keys, and
	// profile freshness — the communities row adds follow/backfill state.
	actor, err := m.actors.GetByAPActorID(ctx, apID)
	switch {
	case err == nil:
		// Fail closed on type confusion: a known Person reached here means an
		// attacker-influenced `audience`/`to` named a user IRI as the post's
		// community. Writing into a Person's repo would produce a post whose
		// community DID has no community.profile — permanently unindexable.
		if actor.ActorType != store.ActorTypeGroup {
			return nil, skip(apID,
				fmt.Sprintf("actor is bridged as %s, not a community Group", actor.ActorType))
		}
		if _, err := m.ensureKnownActor(ctx, actor, groupRef, force); err != nil {
			return nil, err
		}
	case errors.IsNotFound(err):
		doc, err := m.actorDoc(ctx, groupRef, force)
		if err != nil {
			return nil, err
		}
		if doc.Type != ap.TypeGroup {
			return nil, errors.NewValidationError("group",
				fmt.Sprintf("object %s has type %q, want Group", apID, doc.Type))
		}
		// bridgeNewActor gets doc as its reference, so its alias check cannot
		// see apID. A Group naming another id is refused here instead: that id
		// may be a bridged community's, and the communities row would be keyed
		// on apID while the bridged_actors row is keyed on doc.ID.
		if doc.ID != apID {
			return nil, skip(apID, fmt.Sprintf(
				"group document names a different id %s; refusing to bridge an alias", doc.ID))
		}
		if actor, err = m.bridgeNewActor(ctx, doc, store.ActorTypeGroup, true); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("materialize: look up community actor %s: %w", apID, err)
	}

	community, err := m.communities.UpsertCommunity(ctx, store.Community{
		APGroupID:         apID,
		DID:               actor.DID,
		PreferredUsername: preferredUsernameFor(groupRef, actor),
		Instance:          groupRef.Host(),
	})
	if err != nil {
		return nil, fmt.Errorf("materialize: upsert community %s: %w", apID, err)
	}
	return community, nil
}

// preferredUsernameFor picks the community's preferredUsername from the
// freshest source available: the AP document when it carries one, else the
// local part of the stored bridged handle.
func preferredUsernameFor(groupRef *ap.Object, actor *store.BridgedActor) string {
	if groupRef.PreferredUsername != "" {
		return groupRef.PreferredUsername
	}
	if idx := strings.IndexByte(actor.Handle, '.'); idx > 0 {
		return actor.Handle[:idx]
	}
	return actor.Handle
}

// actorDoc returns a full actor document for a reference. An embedded actor
// object is trusted ONLY when allowEmbedded is set — the signature-verified
// Update path (task 06 verifies signer == actor before calling Refresh*).
// On every content path (attributedTo/audience) it MUST be false: those
// objects are attacker-influenced, and an inline `attributedTo` claiming a
// victim's id would otherwise mint/materialize a forged profile keyed to
// that id. Bare/untrusted refs are always fetched fresh by IRI (and
// FetchObject binds the returned id to the fetch authority).
func (m *Materializer) actorDoc(ctx context.Context, actorRef *ap.Object, allowEmbedded bool) (*ap.Object, error) {
	if allowEmbedded && actorRef.IsActor() && actorRef.PreferredUsername != "" {
		return actorRef, nil
	}
	doc, err := m.fetcher.FetchActor(ctx, actorRef.ID)
	if err != nil {
		if errors.IsTombstoned(err) {
			return nil, skip(actorRef.ID, "actor is tombstoned upstream")
		}
		if errors.IsNotFound(err) {
			return nil, skip(actorRef.ID, "actor document is unavailable upstream")
		}
		return nil, fmt.Errorf("materialize: fetch actor %s: %w", actorRef.ID, err)
	}
	// Bind the fetched actor's self-asserted id to the fetch authority (the
	// id becomes the bridged_actors / ap_objects key); reject a cross-host
	// claim. Empty id inherits the requested IRI.
	if doc.ID == "" {
		doc.ID = actorRef.ID
	} else if !ap.SameAuthority(doc.ID, actorRef.ID) {
		return nil, skip(actorRef.ID,
			fmt.Sprintf("actor document served a cross-authority id %s", doc.ID))
	}
	return doc, nil
}

// knownActorDoc is actorDoc for an actor already bridged: the refreshed
// document must still name stored's id. One naming another id (another bridged
// actor's, or a post's) is refused before anything is written, or its profile
// commit would repoint that id's mapping at stored's repo. Its only caller,
// ensureKnownActor, has already healed a row stranded on a content id.
func (m *Materializer) knownActorDoc(ctx context.Context, stored *store.BridgedActor, actorRef *ap.Object, allowEmbedded bool) (*ap.Object, error) {
	doc, err := m.actorDoc(ctx, actorRef, allowEmbedded)
	if err != nil {
		return nil, err
	}
	if doc.ID != stored.APActorID {
		return nil, skip(stored.APActorID,
			fmt.Sprintf("refreshed document names a different actor %s", doc.ID))
	}
	return doc, nil
}

// profileFresh reports whether the actor's materialized profile is within
// the refresh TTL.
func (m *Materializer) profileFresh(actor *store.BridgedActor) bool {
	return actor.ProfileSyncedAt != nil && m.now().Sub(*actor.ProfileSyncedAt) < m.profileTTL
}

// hasNobridgeMarker reports whether the actor opted out of bridging via
// #nobridge/#nobot in its summary (Lemmy bios are HTML; match the raw text)
// or its hashtag tags.
func hasNobridgeMarker(doc *ap.Object) bool {
	summary := strings.ToLower(doc.Summary)
	if doc.Source != nil {
		summary += " " + strings.ToLower(doc.Source.Content)
	}
	for _, marker := range nobridgeMarkers {
		if containsHashtag(summary, marker) {
			return true
		}
	}
	for _, tag := range doc.Tag {
		name := strings.ToLower(tag.Name)
		for _, marker := range nobridgeMarkers {
			if name == marker || name == strings.TrimPrefix(marker, "#") {
				return true
			}
		}
	}
	return false
}

// containsHashtag reports whether haystack contains marker as a whole
// hashtag token — not a prefix of a longer tag (so "#nobot" does not match
// "#nobotany"). marker is expected lowercase; haystack must already be
// lowercased.
func containsHashtag(haystack, marker string) bool {
	from := 0
	for {
		i := strings.Index(haystack[from:], marker)
		if i < 0 {
			return false
		}
		end := from + i + len(marker)
		if end >= len(haystack) || !isTagChar(haystack[end]) {
			return true
		}
		from += i + len(marker)
	}
}

// isTagChar reports whether a byte can continue a hashtag word.
func isTagChar(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
}

// buildActorProfile translates an AP Person document into a
// social.coves.actor.profile record. The bio always ends with the
// provenance line (PLAN.md locked decision 6): the original bio is
// truncated so line and bio together fit the 256-grapheme lexicon cap.
func (m *Materializer) buildActorProfile(doc *ap.Object, fallbackCreatedAt time.Time, avatar, banner *atdata.Blob) map[string]any {
	record := map[string]any{
		"$type":     CollectionActorProfile,
		"createdAt": recordDatetime(actorCreatedAt(doc, fallbackCreatedAt)),
	}
	displayName := doc.Name
	if displayName == "" {
		displayName = doc.PreferredUsername
	}
	if displayName != "" {
		record["displayName"] = truncateText(displayName, 64, 640)
	}
	record["bio"] = bioWithProvenance(markdownFromSummary(doc),
		provenanceLine("@", doc.PreferredUsername, doc.Host()), 256, 2560)
	if avatar != nil {
		record["avatar"] = *avatar
	}
	if banner != nil {
		record["banner"] = *banner
	}
	return record
}

// buildCommunityProfile translates an AP Group document into a
// social.coves.community.profile record. createdBy and hostedBy are the
// bridge's service DID (the bridge hosts and administers the mirror).
func (m *Materializer) buildCommunityProfile(doc *ap.Object, fallbackCreatedAt time.Time, avatar, banner *atdata.Blob) map[string]any {
	record := map[string]any{
		"$type": CollectionCommunityProfile,
		// name is a DNS label (the local part of the community's handle), and
		// the lexicon caps it at 63 — the DNS label limit — not 64. Nothing
		// here validates against the lexicon, so the cap must hold at build
		// time or a 64-byte name ships and the appview refuses it.
		"name":      truncateText(doc.PreferredUsername, 63, 63),
		"createdBy": m.serviceDID,
		"hostedBy":  m.serviceDID,
		"createdAt": recordDatetime(actorCreatedAt(doc, fallbackCreatedAt)),
		// Explicit even though the lexicon defaults it: at least one
		// consumer (the Coves AppView before 2026-07-13) mapped a missing
		// visibility to the empty string and refused the row. Emitting the
		// default costs nothing and survives consumers that don't apply
		// lexicon defaults.
		"visibility": "public",
	}
	if doc.Name != "" {
		record["displayName"] = truncateText(doc.Name, 128, 1280)
	}
	// origin is the instance the community REALLY lives on ("lemmy.world"),
	// so clients can render !technology@lemmy.world instead of the flattened
	// DNS handle technology.lemmy-world.tdpl.io — the handle can carry the
	// Lemmy host only lossily (dots become hyphens), and the mapping is not
	// reversible on the client side.
	//
	// The value is SELF-ASSERTED: nothing in the record proves it. The Coves
	// AppView honours it only when the writing repo's PDS is in its
	// TRUSTED_BRIDGE_PDS_HOSTS or the origin is provable from the repo's
	// DNS-verified handle (see admitCommunityOrigin in the Coves repo; that
	// side lives on the coves feat/community-origin-resolve stack). A
	// bridged community's handle proves tdpl.io, never lemmy.world, so this
	// field renders on Coves ONLY because the bridge PDS is in that trust
	// list — an appview that does not trust us drops it with a warning and
	// indexes the community without an origin (the record is never refused
	// over it). It is the bare lowercase hostname: the only form the appview
	// accepts — a scheme, port, IP literal or trailing dot is dropped at
	// index time, so originHost omits the field rather than assert one that
	// would be thrown away or, worse, name the wrong instance.
	//
	// Not to be confused with store.Origin (models.go), which records which
	// SIDE of the bridge authored an object (fediverse vs bridge) for echo
	// suppression; store.OriginInstance is the field with this meaning.
	if originInstance := originHost(doc); originInstance != "" {
		record["origin"] = originInstance
	}
	record["description"] = bioWithProvenance(markdownFromSummary(doc),
		provenanceLine("!", doc.PreferredUsername, doc.Host()), 1000, 10000)
	if doc.Sensitive != nil && *doc.Sensitive {
		record["contentWarnings"] = []any{"nsfw"}
	}
	if avatar != nil {
		record["avatar"] = *avatar
	}
	if banner != nil {
		record["banner"] = *banner
	}
	return record
}

// actorCreatedAt prefers the AP published time; absent that, the stable
// bridged_actors creation time (never time.Now(), which would produce a new
// record CID on every refresh and defeat idempotent re-puts).
func actorCreatedAt(doc *ap.Object, fallback time.Time) time.Time {
	if doc.Published.OK() {
		return doc.Published.Time
	}
	if !fallback.IsZero() {
		return fallback
	}
	return time.Unix(0, 0)
}

// markdownFromSummary renders the actor's summary/bio as markdown,
// preferring the AP source markdown like body content does.
func markdownFromSummary(doc *ap.Object) string {
	if doc.Source != nil && strings.TrimSpace(doc.Source.Content) != "" {
		mt := doc.Source.MediaType
		if mt == "" || strings.HasPrefix(mt, "text/markdown") || strings.HasPrefix(mt, "text/plain") {
			return stripActiveHTML(strings.TrimSpace(doc.Source.Content))
		}
	}
	return htmlToMarkdown(doc.Summary)
}

// provenanceLine renders the visible bridging label: sigil is "@" for
// persons, "!" for communities (the fediverse conventions).
func provenanceLine(sigil, username, instance string) string {
	return fmt.Sprintf("🌉 bridged from %s%s@%s by Tidepool", sigil, username, instance)
}

// bioWithProvenance appends the provenance line to a bio, truncating the
// original bio so the whole value fits both the grapheme and byte lexicon
// caps. The provenance line is always preserved intact.
func bioWithProvenance(bio, provenance string, maxGraphemes, maxBytes int) string {
	if bio == "" {
		return truncateText(provenance, maxGraphemes, maxBytes)
	}
	// Two graphemes for the blank line between bio and provenance.
	budget := maxGraphemes - graphemeCount(provenance) - 2
	byteBudget := maxBytes - len(provenance) - 2
	if budget <= 0 || byteBudget <= 0 {
		return truncateText(provenance, maxGraphemes, maxBytes)
	}
	return truncateText(bio, budget, byteBudget) + "\n\n" + provenance
}

// maxOriginHostLen is the lexicon's maxLength for community.profile.origin
// (the DNS name limit).
const maxOriginHostLen = 253

// originHost derives the community.profile origin from the Group's canonical
// id: the lowercase hostname, or "" when the id carries nothing the appview
// would accept as one. A non-default port is a different instance than the
// bare host (lemmy.example:8536 is not lemmy.example), so rather than assert
// a wrong origin the field is omitted and the community keeps rendering by
// its flattened handle. IP literals and over-long names are omitted for the
// same reason: the appview refuses them, and an omitted field degrades to
// today's behaviour while a refused record would not.
func originHost(doc *ap.Object) string {
	if doc.ID == "" {
		return ""
	}
	u, err := url.Parse(doc.ID)
	if err != nil || u.Port() != "" {
		return ""
	}
	host := strings.ToLower(strings.TrimSuffix(u.Hostname(), "."))
	if host == "" || len(host) > maxOriginHostLen || net.ParseIP(host) != nil {
		return ""
	}
	return host
}
