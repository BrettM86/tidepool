package labels_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/labels"
)

const (
	servedLabelKey   = "zQ3shRFP8jzC6LKjjJZztWppqXVjBdDBzNRXQ8qB2cBRDxDUC"
	servedAtprotoKey = "zQ3shWHG1rP4b7gzhP7zWjsCxERQQiCKzSTMjFTEZ6dWuYx4E"
	servedEndpoint   = "https://coves.example:8443"
)

// labelerDocument is a DID document shaped like the one the Coves label
// publisher serves: an #atproto key beside the #atproto_label key, and the
// #atproto_labeler service.
func labelerDocument(did string, withLabelKey, withLabelerService bool) string {
	methods := []string{fmt.Sprintf(`{"id":%q,"type":"Multikey","controller":%q,"publicKeyMultibase":%q}`, did+"#atproto", did, servedAtprotoKey)}
	if withLabelKey {
		methods = append(methods, fmt.Sprintf(`{"id":%q,"type":"Multikey","controller":%q,"publicKeyMultibase":%q}`, did+"#atproto_label", did, servedLabelKey))
	}
	services := []string{}
	if withLabelerService {
		services = append(services, fmt.Sprintf(`{"id":"#atproto_labeler","type":"AtprotoLabeler","serviceEndpoint":%q}`, servedEndpoint))
	}
	return fmt.Sprintf(`{"@context":["https://www.w3.org/ns/did/v1","https://w3id.org/security/multikey/v1"],"id":%q,"verificationMethod":[%s],"service":[%s]}`,
		did, strings.Join(methods, ","), strings.Join(services, ","))
}

func TestResolver_ResolvesTheLabelKeyAndLabelerEndpoint(t *testing.T) {
	for _, tc := range []struct {
		name               string
		method             string
		withLabelKey       bool
		withLabelerService bool
		wantErr            bool
	}{
		{name: "did:web with key and service", method: "web", withLabelKey: true, withLabelerService: true},
		{name: "did:web missing the label key", method: "web", withLabelerService: true, wantErr: true},
		{name: "did:web missing the labeler service", method: "web", withLabelKey: true, wantErr: true},
		{name: "did:plc with key and service", method: "plc", withLabelKey: true, withLabelerService: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var did string
			var resolver *labels.Resolver
			switch tc.method {
			case "web":
				var document string
				srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path != "/.well-known/did.json" {
						http.NotFound(w, r)
						return
					}
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(document))
				}))
				t.Cleanup(srv.Close)
				did = "did:web:" + strings.Replace(strings.TrimPrefix(srv.URL, "https://"), ":", "%3A", 1)
				document = labelerDocument(did, tc.withLabelKey, tc.withLabelerService)
				resolver = &labels.Resolver{PLCDirectoryURL: "http://127.0.0.1:1", HTTPClient: srv.Client()}
			case "plc":
				did = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
				document := labelerDocument(did, tc.withLabelKey, tc.withLabelerService)
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path != "/"+did {
						http.NotFound(w, r)
						return
					}
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(document))
				}))
				t.Cleanup(srv.Close)
				resolver = &labels.Resolver{PLCDirectoryURL: srv.URL, HTTPClient: srv.Client()}
			}

			labeler, err := resolver.Resolve(context.Background(), did)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, did, labeler.DID)
			assert.Equal(t, servedEndpoint, labeler.Endpoint)
			require.NotNil(t, labeler.Key, "resolved key")
			assert.Equal(t, servedLabelKey, labeler.Key.Multibase())
		})
	}
}

// TestResolver_RequiresHTTPSLabelerEndpointUnlessInsecureAllowed pins that a
// plain-http #atproto_labeler endpoint is refused unless the resolver allows
// insecure endpoints: label signatures do not cover the stream seq and error
// frames are unsigned, so plaintext lets an on-path attacker replay, reset or
// skip labels.
func TestResolver_RequiresHTTPSLabelerEndpointUnlessInsecureAllowed(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		endpoint              string
		allowInsecureEndpoint bool
		wantErr               bool
	}{
		{name: "http refused when insecure endpoints are not allowed", endpoint: "http://coves.example:8080", wantErr: true},
		{name: "http accepted when insecure endpoints are allowed", endpoint: "http://coves.example:8080", allowInsecureEndpoint: true},
		{name: "https accepted when insecure endpoints are not allowed", endpoint: "https://coves.example:8443"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			did := "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
			document := fmt.Sprintf(`{"id":%q,"verificationMethod":[{"id":"#atproto_label","type":"Multikey","controller":%q,"publicKeyMultibase":%q}],"service":[{"id":"#atproto_labeler","type":"AtprotoLabeler","serviceEndpoint":%q}]}`,
				did, did, servedLabelKey, tc.endpoint)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/"+did {
					http.NotFound(w, r)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(document))
			}))
			t.Cleanup(srv.Close)
			resolver := &labels.Resolver{PLCDirectoryURL: srv.URL, HTTPClient: srv.Client(), AllowInsecureEndpoint: tc.allowInsecureEndpoint}

			labeler, err := resolver.Resolve(context.Background(), did)
			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "https")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.endpoint, labeler.Endpoint)
		})
	}
}
