// Package outbound is the reverse materializer and the pipe that carries it
// (task 15, decisions 12/15/16): social.coves.* records already summarized into
// consume.Intents are translated into ActivityPub vocabulary, written to the
// split activities/deliveries queue, and delivered to community inboxes with
// per-actor HTTP signatures, per-community ordering, explicit causal
// dependencies, and at-least-once semantics.
//
// The three moving parts:
//
//   - Translator renders a consume.Intent into a canonical AP activity
//     (Create/Update/Delete{Note}, Like/Dislike/Undo). It owns the wire format
//     — the Lemmy quirks (to⊇Public required on Notes, attributedTo a single
//     string, source+content duality, no summary on Delete) live here.
//   - Enqueuer translates at enqueue time and writes ONE outbound_activities
//     row plus ONE outbound_deliveries row per target, INSIDE the caller's rev
//     gate transaction so the enqueue commits with the gate advance or not at
//     all.
//   - Worker claims a delivery, rechecks consent, enforces causal gating,
//     signs with the actor's key and POSTs to the community inbox, then records
//     the terminal outcome under the delivery's fencing token.
package outbound

import (
	"context"

	"tidepool/internal/ap"
)

// SignerProvider yields the per-actor AP Signer a delivery is signed with. The
// worker signs each delivery as the PERSONA that authored the record, not as
// the service actor — Lemmy attributes the activity to the signing key's owner.
// personas.Service satisfies this (its actorSigner, exported for task 15).
type SignerProvider interface {
	// SignerFor returns the Signer whose keyId is "{actorID}#main-key" for the
	// persona minted under did. A DID with no minted actor is an error
	// satisfying errors.IsNotFound.
	SignerFor(ctx context.Context, did string) (*ap.Signer, error)
}

// InboxResolver resolves a community's target inbox from its Group actor
// document, preferring endpoints.sharedInbox, cached with a TTL. On a
// 401/404/410 the worker asks it to re-resolve ONCE before poisoning, so an
// endpoint rotation is not mistaken for a dead inbox.
type InboxResolver interface {
	// ResolveInbox returns the AP inbox URL for a community AP Group id.
	ResolveInbox(ctx context.Context, communityAPID string) (inbox string, err error)
}

// ActivitySender POSTs a signed activity to a remote inbox as a specific
// persona. *ap.Client satisfies it via SendActivityAs (the per-actor signing
// path, distinct from the service actor's configured-Signer SendActivity).
type ActivitySender interface {
	SendActivityAs(ctx context.Context, signer *ap.Signer, inbox string, activity any) error
}
