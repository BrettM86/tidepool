// Package labels consumes the Coves labeler's com.atproto.label.subscribeLabels
// stream: it verifies each label against the labeler's DID-document key and
// commits it, with its appliers, through a Store.
package labels

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/labeling"
	"github.com/bluesky-social/indigo/atproto/syntax"
)

// Cursor is a position in the labeler's stream: Seq within reset Epoch.
type Cursor struct{ Epoch, Seq int64 }

// Labeler is a resolved labeler identity.
type Labeler struct {
	DID, Endpoint string
	Key           atcrypto.PublicKey
}

const (
	labelKeyFragment       = "#atproto_label"
	labelerServiceFragment = "#atproto_labeler"
	// maxDocumentBytes bounds a DID document read; real documents are a few
	// kilobytes.
	maxDocumentBytes = 1 << 20
)

// Resolver resolves a labeler DID (did:plc or did:web) to its label key and
// subscribeLabels endpoint. did:plc documents come from PLCDirectoryURL;
// did:web documents from https://<host>/.well-known/did.json. HTTPClient
// carries every request; production passes the SSRF-guarded client.
// AllowInsecureEndpoint accepts a plain-http #atproto_labeler endpoint; off,
// only https is accepted, because label signatures do not cover the stream
// seq and error frames are unsigned, so plaintext lets an on-path attacker
// replay, reset or skip labels. Production sets it only for local dev.
type Resolver struct {
	PLCDirectoryURL       string
	HTTPClient            *http.Client
	AllowInsecureEndpoint bool
}

// didDocument is the subset of a DID document the resolver reads.
type didDocument struct {
	ID                 string `json:"id"`
	VerificationMethod []struct {
		ID                 string `json:"id"`
		PublicKeyMultibase string `json:"publicKeyMultibase"`
	} `json:"verificationMethod"`
	Service []struct {
		ID              string `json:"id"`
		ServiceEndpoint string `json:"serviceEndpoint"`
	} `json:"service"`
}

// Resolve resolves did to its #atproto_label key and #atproto_labeler endpoint.
func (r *Resolver) Resolve(ctx context.Context, did string) (Labeler, error) {
	parsed, err := syntax.ParseDID(did)
	if err != nil {
		return Labeler{}, fmt.Errorf("labels: resolve %q: %w", did, err)
	}
	documentURL, err := r.documentURL(parsed)
	if err != nil {
		return Labeler{}, err
	}
	document, err := r.fetch(ctx, documentURL)
	if err != nil {
		return Labeler{}, fmt.Errorf("labels: resolve %s: %w", did, err)
	}
	if document.ID != did {
		return Labeler{}, fmt.Errorf("labels: resolve %s: document id is %q", did, document.ID)
	}
	labeler := Labeler{DID: did}
	for _, method := range document.VerificationMethod {
		if method.ID != labelKeyFragment && method.ID != did+labelKeyFragment {
			continue
		}
		labeler.Key, err = atcrypto.ParsePublicMultibase(method.PublicKeyMultibase)
		if err != nil {
			return Labeler{}, fmt.Errorf("labels: resolve %s: %s key: %w", did, labelKeyFragment, err)
		}
		break
	}
	if labeler.Key == nil {
		return Labeler{}, fmt.Errorf("labels: resolve %s: document has no %s key", did, labelKeyFragment)
	}
	for _, service := range document.Service {
		if service.ID == labelerServiceFragment || service.ID == did+labelerServiceFragment {
			labeler.Endpoint = service.ServiceEndpoint
			break
		}
	}
	endpoint, err := url.Parse(labeler.Endpoint)
	if labeler.Endpoint == "" || err != nil || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.Host == "" {
		return Labeler{}, fmt.Errorf("labels: resolve %s: document has no http(s) %s service", did, labelerServiceFragment)
	}
	if endpoint.Scheme != "https" && !r.AllowInsecureEndpoint {
		return Labeler{}, fmt.Errorf("labels: resolve %s: %s service %q is not https (plain http needs ALLOW_PRIVATE_FETCH)", did, labelerServiceFragment, labeler.Endpoint)
	}
	return labeler, nil
}

// documentURL is where did's document is served.
func (r *Resolver) documentURL(did syntax.DID) (string, error) {
	switch did.Method() {
	case "plc":
		if r.PLCDirectoryURL == "" {
			return "", fmt.Errorf("labels: resolve %s: no PLC directory configured", did)
		}
		return strings.TrimSuffix(r.PLCDirectoryURL, "/") + "/" + did.String(), nil
	case "web":
		segments := strings.Split(did.Identifier(), ":")
		host, err := url.PathUnescape(segments[0])
		if err != nil {
			return "", fmt.Errorf("labels: resolve %s: invalid did:web host: %w", did, err)
		}
		if host == "" || strings.ContainsAny(host, "/?#@") {
			return "", fmt.Errorf("labels: resolve %s: invalid did:web host", did)
		}
		if len(segments) == 1 {
			return "https://" + host + "/.well-known/did.json", nil
		}
		path := make([]string, 0, len(segments)-1)
		for _, segment := range segments[1:] {
			unescaped, err := url.PathUnescape(segment)
			if err != nil {
				return "", fmt.Errorf("labels: resolve %s: invalid did:web path: %w", did, err)
			}
			if unescaped == "" || strings.ContainsAny(unescaped, "/?#") {
				return "", fmt.Errorf("labels: resolve %s: invalid did:web path", did)
			}
			path = append(path, url.PathEscape(unescaped))
		}
		return "https://" + host + "/" + strings.Join(path, "/") + "/did.json", nil
	default:
		return "", fmt.Errorf("labels: resolve %s: unsupported DID method %q", did, did.Method())
	}
}

func (r *Resolver) fetch(ctx context.Context, documentURL string) (didDocument, error) {
	client := r.HTTPClient
	if client == nil {
		return didDocument{}, fmt.Errorf("no HTTP client configured")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, documentURL, nil)
	if err != nil {
		return didDocument{}, err
	}
	request.Header.Set("Accept", "application/did+json, application/json")
	response, err := client.Do(request)
	if err != nil {
		return didDocument{}, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return didDocument{}, fmt.Errorf("GET %s: status %d", documentURL, response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxDocumentBytes+1))
	if err != nil {
		return didDocument{}, fmt.Errorf("GET %s: %w", documentURL, err)
	}
	if len(body) > maxDocumentBytes {
		return didDocument{}, fmt.Errorf("GET %s: document exceeds %d bytes", documentURL, maxDocumentBytes)
	}
	var document didDocument
	if err := json.Unmarshal(body, &document); err != nil {
		return didDocument{}, fmt.Errorf("GET %s: decode document: %w", documentURL, err)
	}
	return document, nil
}

// Verify checks the label's syntax, that its src is labelerDID, and its
// signature against key.
func Verify(l *labeling.Label, labelerDID string, key atcrypto.PublicKey) error {
	if err := l.VerifySyntax(); err != nil {
		return fmt.Errorf("labels: verify: %w", err)
	}
	if l.SourceDID != labelerDID {
		return fmt.Errorf("labels: verify: src %q is not the configured labeler %q", l.SourceDID, labelerDID)
	}
	if key == nil {
		return fmt.Errorf("labels: verify: no labeler key")
	}
	if err := l.VerifySignature(key); err != nil {
		return fmt.Errorf("labels: verify: signature: %w", err)
	}
	return nil
}
