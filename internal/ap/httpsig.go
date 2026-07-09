// This file implements draft-cavage HTTP signatures (the flavor the
// fediverse actually speaks), exactly as Lemmy validates them.
//
// What Lemmy requires, from LemmyNet/activitypub-federation-rust
// src/http_signatures.rs (the library Lemmy federates with):
//
//   - Verification uses http_signature_normalization Config::new()
//     .set_expiration(EXPIRES_AFTER).require_digest(), where EXPIRES_AFTER is
//     one hour. require_digest() marks the Digest header REQUIRED among the
//     signed headers for EVERY verified request — including signed GETs (the
//     crate comment says digest "doesn't make sense for GET", but Lemmy
//     applies one shared config to inbox POSTs and authorized fetches alike).
//     We therefore always send a Digest header, computed over the empty body
//     for GETs, and always include it in the signature. bridgy-fed does the
//     same (HTTP_SIG_HEADERS = ('Date', 'Host', 'Digest', '(request-target)')
//     for both GET and POST) and interoperates with Lemmy in production.
//   - Signatures are RSA PKCS#1 v1.5 over SHA-256 (Pkcs1v15Sign::new::<Sha256>).
//     Lemmy requires RSA actor keys; this is the bridge's AP-side key,
//     distinct from the atproto secp256k1 repo keys.
//   - keyId must be "{actorID}#main-key" — Lemmy's signing_actor() extracts
//     the actor id with the regex keyId="([^"]+)#([^"]+)".
//   - Lemmy's own outbound signatures cover
//     "(request-target) content-type date digest host" (see the crate's
//     test_sign fixture); our Verify accepts any signed-header set that
//     includes (request-target) and date, and requires digest on requests
//     with a body.
//
// We sign "(request-target) host date digest" plus, on requests with a body,
// "content-type". The signature algorithm parameter is emitted as
// "rsa-sha256"; on verification "hs2019" is treated as rsa-sha256, matching
// bridgy-fed's compat note (Mastodon emits hs2019 but still means RSA-SHA256).

package ap

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"net/http"
	"strings"
	"time"

	"tidepool/internal/errors"
)

// ServiceKeyBits is the RSA modulus size for generated AP keys, matching
// Lemmy's generate_actor_keypair (2048).
const ServiceKeyBits = 2048

// maxDateSkew is how far a request's Date header may deviate from now before
// verification fails. Lemmy tolerates one hour (EXPIRES_AFTER); we match it.
const maxDateSkew = time.Hour

// GenerateRSAKey generates a new 2048-bit RSA keypair for AP signing.
func GenerateRSAKey() (*rsa.PrivateKey, error) {
	key, err := rsa.GenerateKey(rand.Reader, ServiceKeyBits)
	if err != nil {
		return nil, fmt.Errorf("ap: generate RSA key: %w", err)
	}
	return key, nil
}

// EncodePrivateKeyPEM encodes an RSA private key as PKCS#8 PEM, the format
// Lemmy uses for its own keys.
func EncodePrivateKeyPEM(key *rsa.PrivateKey) ([]byte, error) {
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("ap: marshal private key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), nil
}

// ParsePrivateKeyPEM decodes a PKCS#8 or PKCS#1 PEM RSA private key.
func ParsePrivateKeyPEM(pemBytes []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, fmt.Errorf("ap: private key PEM: no PEM block found")
	}
	if parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		rsaKey, ok := parsed.(*rsa.PrivateKey)
		if !ok {
			return nil, fmt.Errorf("ap: private key PEM: not an RSA key (%T)", parsed)
		}
		return rsaKey, nil
	}
	rsaKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("ap: parse private key PEM: %w", err)
	}
	return rsaKey, nil
}

// EncodePublicKeyPEM encodes an RSA public key as SPKI PEM ("PUBLIC KEY"),
// the format every fediverse implementation publishes in publicKeyPem.
func EncodePublicKeyPEM(key *rsa.PublicKey) ([]byte, error) {
	der, err := x509.MarshalPKIXPublicKey(key)
	if err != nil {
		return nil, fmt.Errorf("ap: marshal public key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}), nil
}

// ParsePublicKeyPEM decodes an SPKI ("PUBLIC KEY") or PKCS#1 ("RSA PUBLIC
// KEY") PEM RSA public key, as found in actors' publicKeyPem fields.
func ParsePublicKeyPEM(pemBytes []byte) (*rsa.PublicKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, fmt.Errorf("ap: public key PEM: no PEM block found")
	}
	if parsed, err := x509.ParsePKIXPublicKey(block.Bytes); err == nil {
		rsaKey, ok := parsed.(*rsa.PublicKey)
		if !ok {
			return nil, fmt.Errorf("ap: public key PEM: not an RSA key (%T)", parsed)
		}
		return rsaKey, nil
	}
	rsaKey, err := x509.ParsePKCS1PublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("ap: parse public key PEM: %w", err)
	}
	return rsaKey, nil
}

// Signer signs outbound HTTP requests with the bridge's AP-side RSA key.
type Signer struct {
	keyID      string
	privateKey *rsa.PrivateKey
}

// NewSigner creates a Signer. keyID is the full key id published in the
// actor document, e.g. "https://bridge.example/actor#main-key".
func NewSigner(keyID string, privateKey *rsa.PrivateKey) *Signer {
	return &Signer{keyID: keyID, privateKey: privateKey}
}

// KeyID returns the signer's key id.
func (s *Signer) KeyID() string { return s.keyID }

// SignRequest adds Date, Host, Digest, and Signature headers to req, signing
// over "(request-target) host date digest" (plus content-type when the
// request carries a body). body must be the exact request payload (nil for
// GET). The caller sets req.Body itself; SignRequest only reads body to
// compute the digest.
func (s *Signer) SignRequest(req *http.Request, body []byte) error {
	now := time.Now().UTC().Format(http.TimeFormat)
	req.Header.Set("Date", now)
	// Go sends req.Host (or req.URL.Host) as the Host header; mirror it in
	// the header map so the signing string and the wire agree.
	host := req.Host
	if host == "" {
		host = req.URL.Host
	}
	req.Host = host

	digest := "SHA-256=" + base64.StdEncoding.EncodeToString(sha256Sum(body))
	req.Header.Set("Digest", digest)

	signedHeaders := []string{"(request-target)", "host", "date", "digest"}
	if len(body) > 0 {
		// Lemmy signs content-type on its own POSTs; include it for parity.
		if req.Header.Get("Content-Type") == "" {
			req.Header.Set("Content-Type", ContentTypeActivityJSON)
		}
		signedHeaders = append(signedHeaders, "content-type")
	}

	signingString := buildSigningString(req, host, signedHeaders)
	hashed := sha256.Sum256([]byte(signingString))
	signature, err := rsa.SignPKCS1v15(rand.Reader, s.privateKey, crypto.SHA256, hashed[:])
	if err != nil {
		return fmt.Errorf("ap: sign request: %w", err)
	}

	req.Header.Set("Signature", fmt.Sprintf(
		`keyId="%s",algorithm="rsa-sha256",headers="%s",signature="%s"`,
		s.keyID,
		strings.Join(signedHeaders, " "),
		base64.StdEncoding.EncodeToString(signature),
	))
	return nil
}

func sha256Sum(data []byte) []byte {
	sum := sha256.Sum256(data)
	return sum[:]
}

// buildSigningString assembles the draft-cavage signing string for the given
// header list, using req's method, path, and headers.
func buildSigningString(req *http.Request, host string, headers []string) string {
	lines := make([]string, 0, len(headers))
	for _, header := range headers {
		switch header {
		case "(request-target)":
			target := req.URL.RequestURI()
			lines = append(lines, fmt.Sprintf("(request-target): %s %s",
				strings.ToLower(req.Method), target))
		case "host":
			lines = append(lines, "host: "+host)
		default:
			lines = append(lines, header+": "+req.Header.Get(header))
		}
	}
	return strings.Join(lines, "\n")
}

// KeyResolver resolves a signature keyId to the RSA public key the signing
// actor publishes. The AP client implements it (fetch actor, read
// publicKey.publicKeyPem) with caching; tests inject stubs.
type KeyResolver interface {
	// ResolveKey returns the public key for keyID and the id of the actor
	// that owns it.
	ResolveKey(ctx context.Context, keyID string) (key *rsa.PublicKey, ownerID string, err error)
}

// freshKeyResolver is an optional extension of KeyResolver: it re-resolves a
// key bypassing any positive cache. The Verifier uses it for a single
// retry-with-fresh-key when a cached key fails to verify a signature — the
// remote may have rotated its key while we still held the old one in cache,
// which is otherwise indistinguishable from a forgery and would blackhole all
// of that actor's deliveries for the full cache TTL. The Client implements it
// via ResolveKeyFresh (task 06 relies on this path).
type freshKeyResolver interface {
	ResolveKeyFresh(ctx context.Context, keyID string) (*rsa.PublicKey, string, error)
}

// cacheAwareKeyResolver is an optional extension of KeyResolver that reports
// whether the resolved key was served from a cache. The Verifier uses it to
// GATE the fresh-key retry: a key that was just fetched from the network
// cannot be stale, so a signature failing against it is a forgery (or
// garbage) and a re-fetch gains nothing — it only lets an attacker turn one
// bogus delivery into two outbound fetches (amplification). The Client
// implements it via ResolveKeyDetailed.
type cacheAwareKeyResolver interface {
	ResolveKeyDetailed(ctx context.Context, keyID string) (key *rsa.PublicKey, ownerID string, fromCache bool, err error)
}

// KeyResolverFunc adapts a function to the KeyResolver interface.
type KeyResolverFunc func(ctx context.Context, keyID string) (*rsa.PublicKey, string, error)

// ResolveKey calls f.
func (f KeyResolverFunc) ResolveKey(ctx context.Context, keyID string) (*rsa.PublicKey, string, error) {
	return f(ctx, keyID)
}

// SignatureError reports a failed HTTP signature verification. It unwraps to
// ErrInvalidInput so task 06 can reject unauthenticated deliveries uniformly.
type SignatureError struct {
	Reason string
}

func (e SignatureError) Error() string {
	return "ap: http signature verification failed: " + e.Reason
}

// Unwrap makes errors.Is(err, errors.ErrInvalidInput) true.
func (e SignatureError) Unwrap() error { return errors.ErrInvalidInput }

// Verifier checks inbound draft-cavage HTTP signatures (task 06 wires it to
// the inbox). Keys are resolved through a KeyResolver.
type Verifier struct {
	resolver KeyResolver
	// now is stubbed in tests.
	now func() time.Time
}

// NewVerifier creates a Verifier that resolves signing keys with resolver.
func NewVerifier(resolver KeyResolver) *Verifier {
	return &Verifier{resolver: resolver, now: time.Now}
}

// Verify checks the request's HTTP signature and returns the AP actor id
// that owns the signing key. body must be the request payload already read
// by the caller (the inbox buffers it anyway to parse the activity).
//
// Validation rules (matching Lemmy's verifier plus the digest rules
// bridgy-fed applies):
//   - a Signature header must be present, with keyId, headers, signature;
//   - "(request-target)" and "date" must be among the signed headers;
//   - the Date header must be within maxDateSkew of now (Lemmy: one hour);
//   - requests with a body must have a Digest header, it must be signed,
//     and it must match SHA-256(body);
//   - algorithm, when present, must be rsa-sha256 or hs2019 (treated as
//     rsa-sha256, the bridgy-fed compat rule).
//
// All verification failures unwrap to errors.ErrInvalidInput via
// SignatureError; key-resolution failures propagate as-is.
func (v *Verifier) Verify(ctx context.Context, req *http.Request, body []byte) (actorID string, err error) {
	header := req.Header.Get("Signature")
	if header == "" {
		return "", SignatureError{Reason: "missing Signature header"}
	}
	fields := parseSignatureHeader(header)
	keyID := fields["keyid"]
	signatureB64 := fields["signature"]
	if keyID == "" || signatureB64 == "" {
		return "", SignatureError{Reason: "Signature header missing keyId or signature"}
	}
	switch algorithm := fields["algorithm"]; algorithm {
	case "", "rsa-sha256", "hs2019":
		// hs2019 in the wild means "figure it out"; every fediverse
		// implementation that sends it uses RSA-SHA256 (bridgy-fed applies
		// the same mapping).
	default:
		return "", SignatureError{Reason: fmt.Sprintf("unsupported algorithm %q", algorithm)}
	}

	signedHeaders := strings.Fields(strings.ToLower(fields["headers"]))
	if len(signedHeaders) == 0 {
		// Per draft-cavage the default is "date" alone.
		signedHeaders = []string{"date"}
	}
	if !containsString(signedHeaders, "(request-target)") {
		return "", SignatureError{Reason: "(request-target) not signed"}
	}
	if !containsString(signedHeaders, "date") {
		return "", SignatureError{Reason: "date not signed"}
	}
	// Host must be signed: it binds the signature to the request's authority.
	// Every fediverse signer (Lemmy, Mastodon, bridgy-fed) signs host, and
	// without it a signature captured for one host could be replayed against
	// another. Signer always includes it.
	if !containsString(signedHeaders, "host") {
		return "", SignatureError{Reason: "host not signed"}
	}

	// Date skew.
	date, err := http.ParseTime(req.Header.Get("Date"))
	if err != nil {
		return "", SignatureError{Reason: "missing or malformed Date header"}
	}
	if skew := v.now().Sub(date); skew > maxDateSkew || skew < -maxDateSkew {
		return "", SignatureError{Reason: fmt.Sprintf("date skew %s exceeds %s", skew.Round(time.Second), maxDateSkew)}
	}

	// Digest: required and checked whenever the request carries a body
	// (POST inbox deliveries). GETs without a body may omit it.
	if len(body) > 0 {
		if !containsString(signedHeaders, "digest") {
			return "", SignatureError{Reason: "digest not signed on request with body"}
		}
		digest := req.Header.Get("Digest")
		expected := base64.StdEncoding.EncodeToString(sha256Sum(body))
		if !digestMatches(digest, expected) {
			return "", SignatureError{Reason: "digest mismatch"}
		}
	}

	// Resolve the signing key, learning whether it came from a cache when
	// the resolver can tell us (the Client can): only a cached key may be
	// stale, so only a cached key earns the rotation retry below.
	var (
		key       *rsa.PublicKey
		ownerID   string
		fromCache bool
	)
	if aware, ok := v.resolver.(cacheAwareKeyResolver); ok {
		key, ownerID, fromCache, err = aware.ResolveKeyDetailed(ctx, keyID)
	} else {
		// Resolvers that cannot report cache provenance keep the old
		// behavior (retry allowed) — better a rare extra fetch than
		// blackholing a rotated key for the cache TTL.
		fromCache = true
		key, ownerID, err = v.resolver.ResolveKey(ctx, keyID)
	}
	if err != nil {
		return "", fmt.Errorf("ap: resolve signing key %q: %w", keyID, err)
	}

	host := req.Host
	if host == "" {
		host = req.URL.Host
	}
	signingString := buildSigningString(req, host, signedHeaders)
	signature, err := base64.StdEncoding.DecodeString(signatureB64)
	if err != nil {
		return "", SignatureError{Reason: "signature is not valid base64"}
	}
	hashed := sha256.Sum256([]byte(signingString))
	if rsa.VerifyPKCS1v15(key, crypto.SHA256, hashed[:], signature) == nil {
		return ownerID, nil
	}
	// The (possibly cached) key did not verify. If the key came from a cache
	// and the resolver can bypass it, try exactly once with a freshly-fetched
	// key: a remote key rotation leaves a stale key in cache that fails every
	// signature until it expires. The retry is gated on fromCache — a key
	// fetched fresh from the network moments ago cannot be stale, and
	// re-fetching it would just let forged deliveries amplify outbound
	// traffic. We also only re-verify when the fresh key actually differs, so
	// a forged signature does not gain extra verification attempts.
	if fresh, ok := v.resolver.(freshKeyResolver); ok && fromCache {
		freshKey, freshOwner, ferr := fresh.ResolveKeyFresh(ctx, keyID)
		if ferr == nil && !freshKey.Equal(key) &&
			rsa.VerifyPKCS1v15(freshKey, crypto.SHA256, hashed[:], signature) == nil {
			return freshOwner, nil
		}
	}
	return "", SignatureError{Reason: "signature does not verify"}
}

// digestMatches compares a Digest header (possibly multi-valued, e.g.
// "SHA-256=xxx,sha-512=yyy") against the expected SHA-256 base64 value.
func digestMatches(header, expectedB64 string) bool {
	for _, part := range strings.Split(header, ",") {
		part = strings.TrimSpace(part)
		algorithm, value, found := strings.Cut(part, "=")
		if !found {
			continue
		}
		if strings.EqualFold(algorithm, "SHA-256") && value == expectedB64 {
			return true
		}
	}
	return false
}

// parseSignatureHeader parses the comma-separated key="value" pairs of a
// draft-cavage Signature header. Keys are lower-cased. Values may contain
// commas (base64 never does, but headers lists don't either — split on
// `",` boundaries would be fragile, so scan properly).
func parseSignatureHeader(header string) map[string]string {
	fields := make(map[string]string)
	rest := header
	for rest != "" {
		rest = strings.TrimLeft(rest, " \t,")
		equals := strings.IndexByte(rest, '=')
		if equals < 0 {
			break
		}
		key := strings.ToLower(strings.TrimSpace(rest[:equals]))
		rest = rest[equals+1:]
		var value string
		if strings.HasPrefix(rest, `"`) {
			closing := strings.IndexByte(rest[1:], '"')
			if closing < 0 {
				value = rest[1:]
				rest = ""
			} else {
				value = rest[1 : 1+closing]
				rest = rest[closing+2:]
			}
		} else {
			comma := strings.IndexByte(rest, ',')
			if comma < 0 {
				value = rest
				rest = ""
			} else {
				value = rest[:comma]
				rest = rest[comma+1:]
			}
			value = strings.TrimSpace(value)
		}
		fields[key] = value
	}
	return fields
}

func containsString(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}

// ActorIDFromKeyID strips the fragment from a keyId, yielding the actor id
// ("https://host/u/alice#main-key" → "https://host/u/alice"). Lemmy resolves
// keys the same way.
func ActorIDFromKeyID(keyID string) string {
	if hash := strings.IndexByte(keyID, '#'); hash >= 0 {
		return keyID[:hash]
	}
	return keyID
}
