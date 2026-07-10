package ap

import (
	"context"
	"crypto/rsa"
	"encoding/json"
	"fmt"

	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// ServiceKeyName is the service_keys row holding the bridge's AP-side RSA
// private key.
const ServiceKeyName = "service-actor"

// ServiceActorPath is the path the bridge's actor document will be served
// from (the HTTP route itself lands with the inbox in task 06).
const ServiceActorPath = "/actor"

// serviceActorContext is the JSON-LD context for the service actor document:
// core AS2 plus the security vocabulary that defines publicKey.
const serviceActorContext = `["https://www.w3.org/ns/activitystreams","https://w3id.org/security/v1"]`

// ServiceActor is the bridge's own AP identity: a Service actor whose
// RSA key signs every outbound request (fetches and Follows). This is the
// AP-side interop key — entirely distinct from the atproto secp256k1 repo
// keys task 03 mints.
type ServiceActor struct {
	// ID is the actor's canonical id, {scheme}://{hostname}/actor.
	ID string
	// Hostname is the bridge's public hostname (config.BridgeHostname).
	Hostname string
	// Scheme is the URL scheme the bridge's own AP URLs are built with
	// (config.BridgeScheme). "https" everywhere real; "http" exists for the
	// local e2e harness, where a debug-mode Lemmy federates with the bridge
	// over plain HTTP inside one compose network. Empty means "https".
	Scheme string
	// Key is the actor's RSA private key.
	Key *rsa.PrivateKey
}

// BaseURL is the origin the bridge's own AP URLs live under,
// e.g. "https://bridge.example". Defensive default: an unset Scheme (a
// hand-built test literal) renders https, never a schemeless URL.
func (a *ServiceActor) BaseURL() string {
	scheme := a.Scheme
	if scheme == "" {
		scheme = "https"
	}
	return scheme + "://" + a.Hostname
}

// LoadOrCreateServiceActor returns the bridge's service actor, loading its
// RSA key from the service_keys store or generating and persisting one on
// first run. Losing a concurrent-bootstrap insert race falls back to the
// winner's key, so every process converges on the same keypair.
// scheme is the URL scheme for the actor's own URLs ("https", or "http" for
// the local e2e harness); empty defaults to https.
func LoadOrCreateServiceActor(ctx context.Context, keys store.ServiceKeys, hostname, scheme string) (*ServiceActor, error) {
	if hostname == "" {
		return nil, errors.NewValidationError("hostname", "must not be empty")
	}
	switch scheme {
	case "":
		scheme = "https"
	case "http", "https":
	default:
		return nil, errors.NewValidationError("scheme", fmt.Sprintf("must be http or https, got %q", scheme))
	}

	stored, err := keys.Get(ctx, ServiceKeyName)
	switch {
	case err == nil:
		// Existing key.
	case errors.IsNotFound(err):
		key, err := GenerateRSAKey()
		if err != nil {
			return nil, err
		}
		pemBytes, err := EncodePrivateKeyPEM(key)
		if err != nil {
			return nil, err
		}
		stored, err = keys.Create(ctx, ServiceKeyName, pemBytes)
		if errors.IsAlreadyExists(err) {
			// Another instance won the bootstrap race; use its key.
			stored, err = keys.Get(ctx, ServiceKeyName)
			if err != nil {
				return nil, fmt.Errorf("ap: reload service key after lost create race: %w", err)
			}
		} else if err != nil {
			return nil, fmt.Errorf("ap: persist service key: %w", err)
		}
	default:
		return nil, fmt.Errorf("ap: load service key: %w", err)
	}

	key, err := ParsePrivateKeyPEM(stored.PrivateKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("ap: stored service key is corrupt: %w", err)
	}

	return &ServiceActor{
		ID:       scheme + "://" + hostname + ServiceActorPath,
		Hostname: hostname,
		Scheme:   scheme,
		Key:      key,
	}, nil
}

// KeyID is the id the actor document publishes for its public key and the
// keyId sent on signatures. Lemmy's key resolution splits keyId on '#', so
// the fragment must be present; #main-key matches Lemmy's own convention.
func (a *ServiceActor) KeyID() string { return a.ID + "#main-key" }

// InboxURL is the actor's inbox (served by task 06).
func (a *ServiceActor) InboxURL() string { return a.BaseURL() + "/inbox" }

// OutboxURL is the actor's outbox.
func (a *ServiceActor) OutboxURL() string { return a.BaseURL() + "/outbox" }

// Signer returns a request signer using the actor's key.
func (a *ServiceActor) Signer() *Signer { return NewSigner(a.KeyID(), a.Key) }

// Document builds the Service actor document served at
// {scheme}://{hostname}/actor. Lemmy requires the publicKey block (RSA, SPKI
// PEM) to accept our signed requests.
func (a *ServiceActor) Document() (*Object, error) {
	publicPEM, err := EncodePublicKeyPEM(&a.Key.PublicKey)
	if err != nil {
		return nil, err
	}
	return &Object{
		Context: json.RawMessage(serviceActorContext),
		ID:      a.ID,
		// Service, not Application: Lemmy deserializes a Follow's actor as its
		// Person protocol type, whose kind enum is Person|Service|Organization
		// (crates/apub .../protocol/.../person.rs, 0.19 and main alike) — an
		// Application actor fails deserialization and the Follow is dropped.
		// Service is the standard AS2 type for bots/bridges and every other
		// platform accepts it.
		Type:              TypeService,
		PreferredUsername: a.Hostname,
		Name:              "Tidepool bridge",
		Summary:           "Bridges threadiverse communities into atproto. " + a.BaseURL(),
		Inbox:             a.InboxURL(),
		Outbox:            a.OutboxURL(),
		PublicKey: &PublicKey{
			ID:           a.KeyID(),
			Owner:        a.ID,
			PublicKeyPem: string(publicPEM),
		},
	}, nil
}

// DocumentJSON renders the actor document as AS2 JSON.
func (a *ServiceActor) DocumentJSON() ([]byte, error) {
	doc, err := a.Document()
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("ap: encode service actor document: %w", err)
	}
	return data, nil
}
