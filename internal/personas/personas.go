// Package personas serves the Coves user origin's ActivityPub identity
// surface (task 13): Person actors keyed by DID under AP_USER_ORIGIN
// (https://coves.social), WebFinger for their local parts, and the shared
// inbox. Key material stays behind internal/identity — the service holds a
// Custodian, never a plaintext PEM.
package personas

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/identity"
	"tidepool/internal/store"
)

// currentRSAKeyVersion stamps newly minted actor keys. Rotation would mint a
// version 2 alongside the published key it replaces; nothing does yet.
const currentRSAKeyVersion = 1

// maxLocalPartAttempts bounds the collision search: attempt 1 claims the bare
// local part and the rest append "-2" ... "-99", the suffix range
// MaxLocalPartLen reserves room for. A namespace that has genuinely exhausted
// 99 claimants on one name is a condition to report, not to keep grinding on.
const maxLocalPartAttempts = 99

// Options configures a Service.
type Options struct {
	// DB is the bridge database (ap_actors lives here).
	DB *sql.DB
	// Custodian seals and opens per-actor AP RSA keys.
	Custodian *identity.Custodian
	// UserOrigin is AP_USER_ORIGIN: the scheme+host new actors are minted
	// under, e.g. "https://coves.social". It seeds NEW rows only; serving
	// derives every URL from the stored actor_id.
	UserOrigin string
	// ServiceActor is the bridge's own AP identity. The user origin's
	// instance ("Site") actor is the SAME bridge speaking under a second
	// origin, so it republishes this actor's RSA key under an id derived
	// from UserOrigin; only Key and CreatedAt are read. Optional: without
	// it the origin apex serves no instance actor (Lemmy tolerates that).
	ServiceActor *ap.ServiceActor
	// InboxHandler receives POST /ap/inbox. The user origin does NOT run a
	// second verify pipeline: deliveries go verbatim to the ingest inbox
	// that already does signature verification, authority binding, dedupe,
	// and queueing (ingest.Inbox.InboxHandler). Nil means the origin
	// advertises an inbox it cannot serve, so the route 404s.
	InboxHandler http.Handler
}

// Service mints and serves Coves user actors.
type Service struct {
	actors     store.APActors
	custodian  *identity.Custodian
	userOrigin string
	// userHost is UserOrigin's scheme-less lowercase host. It is both the
	// native handle suffix new local parts derive against and the
	// normalized_origin they are stored under — the same string Host
	// routing hands the webfinger endpoint, so a lookup needs no reshaping.
	userHost string
	// serviceActor is the bridge identity the origin apex republishes. Nil
	// means the apex publishes nothing.
	serviceActor *ap.ServiceActor
	// inboxHandler is the ingest inbox this origin's shared inbox dispatches
	// to. Nil means the route 404s.
	inboxHandler http.Handler
}

// New builds a Service. UserOrigin is parsed once here: the host it yields
// keys every actor this service mints, so an origin that cannot produce one
// is a startup error rather than a surprise at mint time.
func New(opts Options) (*Service, error) {
	host, err := originHost(opts.UserOrigin)
	if err != nil {
		return nil, err
	}
	return &Service{
		actors:     store.NewAPActors(opts.DB),
		custodian:  opts.Custodian,
		userOrigin: opts.UserOrigin,
		userHost:   host,

		serviceActor: opts.ServiceActor,
		inboxHandler: opts.InboxHandler,
	}, nil
}

// originHost reduces an origin URL to the scheme-less lowercase host.
func originHost(origin string) (string, error) {
	parsed, err := url.Parse(origin)
	if err != nil {
		return "", errors.NewValidationError("user_origin", err.Error())
	}
	if parsed.Host == "" {
		return "", errors.NewValidationError("user_origin",
			fmt.Sprintf("must be an absolute origin URL, got %q", origin))
	}
	return strings.ToLower(parsed.Host), nil
}

// CreateActorForDID get-or-creates the AP Person actor for a Coves DID:
// mints an RSA key, seals it via the custodian, derives and freezes the
// local part from handle, and writes the ap_actors row.
//
// It is called on every federating interaction, so the existing-row path
// returns FIRST and handle is then ignored entirely: the local part is frozen
// at creation, and re-deriving it after a rename would strand every federated
// mention of the old name. Re-minting the key would be worse — it would
// orphan every signature the published key has already made.
//
// The actor is Person, not Service: Lemmy classifies Service actors as bots
// and drops their votes.
func (s *Service) CreateActorForDID(ctx context.Context, did, handle string) (*store.APActor, error) {
	existing, err := s.actors.GetByDID(ctx, did)
	if err == nil {
		return existing, nil
	}
	if !errors.IsNotFound(err) {
		return nil, fmt.Errorf("personas: look up actor for %s: %w", did, err)
	}

	base, err := DeriveLocalPart(handle, s.userHost)
	if err != nil {
		return nil, err
	}

	// Minting is entirely local — key generation plus one INSERT. Nothing
	// here reads the appview or the PDS: the profile columns start empty
	// and task 14's sync fills them, so a federating interaction never
	// waits on a third party to get an identity.
	key, err := ap.GenerateRSAKey()
	if err != nil {
		return nil, fmt.Errorf("personas: generate AP key for %s: %w", did, err)
	}
	sealed, err := s.custodian.EncryptActorRSAKey(did, key)
	if err != nil {
		return nil, fmt.Errorf("personas: seal AP key for %s: %w", did, err)
	}
	publicPEM, err := ap.EncodePublicKeyPEM(&key.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("personas: encode public key for %s: %w", did, err)
	}

	actor := store.APActor{
		DID:              did,
		Kind:             store.ActorTypePerson,
		ActorID:          s.userOrigin + "/ap/actor/" + did,
		NormalizedOrigin: s.userHost,
		RSAKeySealed:     sealed,
		RSAKeyVersion:    currentRSAKeyVersion,
		PublicKeyPEM:     string(publicPEM),
	}

	// The collision search is driven by the INSERT's unique violation, not
	// by a SELECT-then-INSERT pre-check: two simultaneous mints of the same
	// derived name would both see a free namespace and one would fail.
	// Letting the constraint arbitrate means the loser simply takes the
	// next name.
	for attempt := 1; attempt <= maxLocalPartAttempts; attempt++ {
		actor.LocalPart = suffixedLocalPart(base, attempt)
		created, createErr := s.actors.Create(ctx, actor)
		if createErr == nil {
			return created, nil
		}
		var conflict errors.ConflictError
		if stderrors.As(createErr, &conflict) {
			switch conflict.Field {
			case "local_part":
				continue
			case "did", "actor_id":
				// Lost a get-or-create race for this DID. The winner's row
				// is the answer: returning it (rather than an error) is
				// what makes concurrent callers converge on ONE key.
				winner, getErr := s.actors.GetByDID(ctx, did)
				if getErr != nil {
					return nil, fmt.Errorf("personas: reload actor for %s after mint race: %w", did, getErr)
				}
				return winner, nil
			}
		}
		return nil, fmt.Errorf("personas: create actor for %s: %w", did, createErr)
	}
	return nil, errors.NewConflictError("ap_actor", "local_part", base)
}

// suffixedLocalPart names the attempt'th claimant of base: the first keeps
// the bare local part, later ones get "-2", "-3", ... There is no "-1" —
// the bare name IS the first claim.
func suffixedLocalPart(base string, attempt int) string {
	if attempt <= 1 {
		return base
	}
	return base + "-" + strconv.Itoa(attempt)
}

// actorSigner unseals a minted actor's RSA key and returns a Signer whose
// keyID is "{actor_id}#main-key" — the same id the actor document publishes,
// so a verifier that fetches the document finds the key it needs there.
// A DID with no minted actor surfaces as errors.IsNotFound.
func (s *Service) actorSigner(ctx context.Context, did string) (*ap.Signer, error) {
	actor, err := s.actors.GetByDID(ctx, did)
	if err != nil {
		return nil, err
	}
	key, err := s.custodian.DecryptActorRSAKey(did, actor.RSAKeySealed)
	if err != nil {
		return nil, fmt.Errorf("personas: unseal AP key for %s: %w", did, err)
	}
	return ap.NewSigner(actor.ActorID+"#main-key", key), nil
}
