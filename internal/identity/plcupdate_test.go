package identity

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

func TestUpdateHandle_PLCOperation(t *testing.T) {
	const did = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	rotationKey, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	rotationPublic, err := rotationKey.PublicKey()
	require.NoError(t, err)
	secondKey, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	secondPublic, err := secondKey.PublicKey()
	require.NoError(t, err)
	last := map[string]any{
		"type": "plc_operation", "rotationKeys": []any{rotationPublic.DIDKey(), secondPublic.DIDKey()},
		"verificationMethods": map[string]any{"atproto": "did:key:zUnusualSigningMethod"},
		"services":            map[string]any{"atproto_pds": map[string]any{"type": "AtprotoPersonalDataServer", "endpoint": "https://other-pds.example"}},
		"alsoKnownAs":         []any{"at://alice.example.com.tidepool.test"}, "prev": nil,
	}
	unsigned, err := atdata.MarshalCBOR(last)
	require.NoError(t, err)
	oldSignature, err := rotationKey.HashAndSign(unsigned)
	require.NoError(t, err)
	last["sig"] = base64.RawURLEncoding.EncodeToString(oldSignature)
	complete, err := atdata.MarshalCBOR(last)
	require.NoError(t, err)
	previousCID, err := (cid.Prefix{Version: 1, Codec: cid.DagCBOR, MhType: multihash.SHA2_256, MhLength: 32}).Sum(complete)
	require.NoError(t, err)

	var mutex sync.Mutex
	var requests []struct {
		method string
		path   string
		body   map[string]any
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if r.Method == http.MethodPost {
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		}
		mutex.Lock()
		requests = append(requests, struct {
			method string
			path   string
			body   map[string]any
		}{r.Method, r.URL.Path, body})
		mutex.Unlock()
		if r.Method == http.MethodGet && r.URL.Path == "/"+did+"/log/last" {
			_ = json.NewEncoder(w).Encode(last)
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/"+did {
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	minter, err := NewMinter(MinterOptions{
		PLCDirectoryURL: server.URL, BridgeHostname: "tidepool.test", HandleZoneRoot: "ap.tidepool.test",
		RotationKey: rotationKey, Custodian: testCustodian(t), Actors: struct{ store.BridgedActors }{},
		HTTPClient: ap.NewGuardedHTTPClient(true, 10*time.Second),
	})
	require.NoError(t, err)
	_, err = minter.UpdateHandle(context.Background(), did, "bob.example.com.ap.tidepool.test", nil)
	require.NoError(t, err)
	mutex.Lock()
	defer mutex.Unlock()
	require.NotEmpty(t, requests)
	post := requests[len(requests)-1]
	require.Equal(t, http.MethodPost, post.method)
	require.Equal(t, "/"+did, post.path)
	foundLast := false
	for _, request := range requests[:len(requests)-1] {
		if request.method == http.MethodGet && request.path == "/"+did+"/log/last" {
			foundLast = true
		}
	}
	require.True(t, foundLast, "at least one GET of the last operation must precede the POST")
	op := post.body
	require.NotNil(t, op)
	require.Len(t, op, 7, "update must contain exactly the PLC operation fields")
	require.Equal(t, previousCID.String(), op["prev"])
	require.Equal(t, []any{"at://bob.example.com.ap.tidepool.test"}, op["alsoKnownAs"])
	for _, field := range []string{"type", "rotationKeys", "verificationMethods", "services"} {
		require.Equal(t, last[field], op[field], field)
	}
	signature, ok := op["sig"].(string)
	require.True(t, ok)
	require.NotEqual(t, last["sig"], signature)
	decoded, err := base64.RawURLEncoding.DecodeString(signature)
	require.NoError(t, err)
	delete(op, "sig")
	encoded, err := atdata.MarshalCBOR(op)
	require.NoError(t, err)
	require.NoError(t, rotationPublic.HashAndVerify(encoded, decoded))
}

func TestUpdateHandle_AgainstRealPLC(t *testing.T) {
	database := testutil.DB(t)
	testutil.Truncate(t, database, "bridged_actors")
	minter, _, _, _ := testMinter(t, store.NewBridgedActors(database))
	minted, err := minter.MintActor(t.Context(), MintRequest{
		ActorType: store.ActorTypePerson, PreferredUsername: "alice", Instance: "example.com",
	})
	require.NoError(t, err)
	var audit []struct {
		CID string `json:"cid"`
	}
	fetchJSON(t, minter.plcURL+"/"+minted.DID+"/log/audit", &audit)
	require.NotEmpty(t, audit)
	require.NotEmpty(t, audit[0].CID)
	_, err = minter.UpdateHandle(t.Context(), minted.DID, "bob.example.com.ap.tidepool.test", nil)
	require.NoError(t, err)
	var last map[string]any
	fetchJSON(t, minter.plcURL+"/"+minted.DID+"/log/last", &last)
	require.Equal(t, []any{"at://bob.example.com.ap.tidepool.test"}, last["alsoKnownAs"])
	require.Equal(t, audit[0].CID, last["prev"])
	doc := fetchDIDDoc(t, minter.plcURL, minted.DID)
	require.Equal(t, []string{"at://bob.example.com.ap.tidepool.test"}, doc.AlsoKnownAs)
}

func TestUpdateHandle_RejectsInvalidLastOperation(t *testing.T) {
	const did = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	rotationKey, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	rotationPublic, err := rotationKey.PublicKey()
	require.NoError(t, err)
	validOperation := func() map[string]any {
		return map[string]any{
			"type": "plc_operation", "rotationKeys": []any{rotationPublic.DIDKey()},
			"verificationMethods": map[string]any{"atproto": rotationPublic.DIDKey()},
			"services":            map[string]any{"atproto_pds": map[string]any{"type": "AtprotoPersonalDataServer", "endpoint": "https://pds.example"}},
			"alsoKnownAs":         []any{"at://alice.example.com.tidepool.test"}, "prev": nil, "sig": "signature",
		}
	}
	encode := func(operation map[string]any) string {
		encoded, err := json.Marshal(operation)
		require.NoError(t, err)
		return string(encoded)
	}
	without := func(field string) string {
		operation := validOperation()
		delete(operation, field)
		return encode(operation)
	}
	with := func(field string, value any) string {
		operation := validOperation()
		operation[field] = value
		return encode(operation)
	}
	cases := []struct {
		name string
		body string
	}{
		{"missing type", without("type")},
		{"missing rotationKeys", without("rotationKeys")},
		{"missing verificationMethods", without("verificationMethods")},
		{"missing services", without("services")},
		{"rotationKeys not an array", with("rotationKeys", rotationPublic.DIDKey())},
		{"rotationKeys entry not a string", with("rotationKeys", []any{rotationPublic.DIDKey(), 7})},
		{"verificationMethods not an object", with("verificationMethods", []any{rotationPublic.DIDKey()})},
		{"services not an object", with("services", "https://pds.example")},
		{"null body", "null"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			var mutex sync.Mutex
			posts := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					mutex.Lock()
					posts++
					mutex.Unlock()
					return
				}
				if r.Method == http.MethodGet && r.URL.Path == "/"+did+"/log/last" {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(testCase.body))
					return
				}
				http.NotFound(w, r)
			}))
			defer server.Close()
			minter, err := NewMinter(MinterOptions{
				PLCDirectoryURL: server.URL, BridgeHostname: "tidepool.test", HandleZoneRoot: "ap.tidepool.test",
				RotationKey: rotationKey, Custodian: testCustodian(t), Actors: struct{ store.BridgedActors }{},
				HTTPClient: ap.NewGuardedHTTPClient(true, 10*time.Second),
			})
			require.NoError(t, err)
			_, err = minter.UpdateHandle(context.Background(), did, "bob.example.com.ap.tidepool.test", nil)
			require.Error(t, err)
			require.Contains(t, err.Error(), "PLC last operation")
			mutex.Lock()
			defer mutex.Unlock()
			require.Zero(t, posts, "an invalid last operation must not be signed and submitted")
		})
	}
}

func TestUpdateHandle_RejectsOversizedLastOperation(t *testing.T) {
	const did = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	rotationKey, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	var mutex sync.Mutex
	posts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			mutex.Lock()
			posts++
			mutex.Unlock()
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"plc_operation","padding":"` + strings.Repeat("a", 128<<10) + `"}`))
	}))
	defer server.Close()
	minter, err := NewMinter(MinterOptions{
		PLCDirectoryURL: server.URL, BridgeHostname: "tidepool.test", HandleZoneRoot: "ap.tidepool.test",
		RotationKey: rotationKey, Custodian: testCustodian(t), Actors: struct{ store.BridgedActors }{},
		HTTPClient: ap.NewGuardedHTTPClient(true, 10*time.Second),
	})
	require.NoError(t, err)
	_, err = minter.UpdateHandle(context.Background(), did, "bob.example.com.ap.tidepool.test", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "decode PLC last operation")
	mutex.Lock()
	defer mutex.Unlock()
	require.Zero(t, posts)
}

func TestUpdateHandle_DefaultRetryWaitBacksOff(t *testing.T) {
	const did = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	rotationKey, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	rotationPublic, err := rotationKey.PublicKey()
	require.NoError(t, err)
	last := map[string]any{
		"type": "plc_operation", "rotationKeys": []any{rotationPublic.DIDKey()},
		"verificationMethods": map[string]any{"atproto": rotationPublic.DIDKey()},
		"services":            map[string]any{"atproto_pds": map[string]any{"type": "AtprotoPersonalDataServer", "endpoint": "https://pds.example"}},
		"alsoKnownAs":         []any{"at://alice.example.com.tidepool.test"}, "prev": nil, "sig": "signature",
	}
	var mutex sync.Mutex
	posts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/"+did+"/log/last" {
			_ = json.NewEncoder(w).Encode(last)
			return
		}
		if r.Method == http.MethodPost && r.URL.Path == "/"+did {
			mutex.Lock()
			posts++
			attempt := posts
			mutex.Unlock()
			if attempt == 1 {
				w.WriteHeader(http.StatusTooManyRequests)
			}
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	minter, err := NewMinter(MinterOptions{
		PLCDirectoryURL: server.URL, BridgeHostname: "tidepool.test", HandleZoneRoot: "ap.tidepool.test",
		RotationKey: rotationKey, Custodian: testCustodian(t), Actors: struct{ store.BridgedActors }{},
		HTTPClient: ap.NewGuardedHTTPClient(true, 10*time.Second),
	})
	require.NoError(t, err)
	started := time.Now()
	result, err := minter.UpdateHandle(context.Background(), did, "bob.example.com.ap.tidepool.test", nil)
	elapsed := time.Since(started)
	require.NoError(t, err)
	require.Equal(t, HandleUpdateApplied, result.Outcome)
	mutex.Lock()
	defer mutex.Unlock()
	require.Equal(t, 2, posts)
	require.GreaterOrEqual(t, elapsed, plcUpdateRetryBaseDelay, "the default wait must sleep before retrying")
}

func TestUpdateHandle_ClassifiesPLCRateLimits(t *testing.T) {
	const did = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	rateLimitHeaders := map[string]string{
		"Retry-After": "90", "RateLimit-Limit": "100", "RateLimit-Remaining": "0",
		"X-RateLimit-Reset": "1792360000", "Set-Cookie": "secret=1",
	}
	for _, tc := range []struct {
		name         string
		getStatuses  []int
		getHeaders   map[string]string
		postStatuses []int
		postHeaders  map[string]string
		// postHeaderValues adds every listed value of a header, for headers repeated in one response.
		postHeaderValues map[string][]string
		wantApplied      bool
		wantRateLimit    map[string]string
		// wantRateLimitIncludes lists entries the captured headers must contain, without pinning the rest.
		wantRateLimitIncludes map[string]string
		wantWarnings          int
		wantRequest           string
		wantValues            []string
	}{
		{
			name: "every POST rate limited", postStatuses: []int{http.StatusTooManyRequests}, postHeaders: rateLimitHeaders,
			wantRateLimit: map[string]string{
				"Retry-After": "90", "Ratelimit-Limit": "100", "Ratelimit-Remaining": "0", "X-Ratelimit-Reset": "1792360000",
			},
			wantWarnings: 5, wantRequest: "post", wantValues: []string{"90", "100", "0", "1792360000"},
		},
		{
			name: "every head read rate limited", getStatuses: []int{http.StatusTooManyRequests},
			getHeaders:    map[string]string{"Retry-After": "120", "Set-Cookie": "secret=1"},
			wantRateLimit: map[string]string{"Retry-After": "120"},
			wantWarnings:  5, wantRequest: "get", wantValues: []string{"120"},
		},
		{
			name: "every POST unavailable", postStatuses: []int{http.StatusServiceUnavailable},
			postHeaders: map[string]string{"Retry-After": "90", "Set-Cookie": "secret=1"},
		},
		{
			name: "rate limited POSTs then accepted",
			postStatuses: []int{
				http.StatusTooManyRequests, http.StatusTooManyRequests, http.StatusTooManyRequests,
				http.StatusTooManyRequests, http.StatusOK,
			},
			postHeaders: rateLimitHeaders, wantApplied: true,
			wantWarnings: 4, wantRequest: "post", wantValues: []string{"90", "100", "0", "1792360000"},
		},
		{
			name: "bare combined RateLimit headers captured", postStatuses: []int{http.StatusTooManyRequests},
			postHeaders: map[string]string{
				"RateLimit": `"default";r=0;t=60`, "RateLimit-Policy": `"default";q=100;w=60`, "X-RateLimit": "5",
				"Set-Cookie": "secret=1",
			},
			wantRateLimitIncludes: map[string]string{
				"Ratelimit": `"default";r=0;t=60`, "Ratelimit-Policy": `"default";q=100;w=60`, "X-Ratelimit": "5",
			},
			wantWarnings: 5, wantRequest: "post", wantValues: []string{`"default";r=0;t=60`, `"default";q=100;w=60`, "5"},
		},
		{
			name: "Retry-After kept beside many rate-limit headers", postStatuses: []int{http.StatusTooManyRequests},
			postHeaders: map[string]string{
				"RateLimit-Extra-01": "1", "RateLimit-Extra-02": "2", "RateLimit-Extra-03": "3", "RateLimit-Extra-04": "4",
				"RateLimit-Extra-05": "5", "RateLimit-Extra-06": "6", "RateLimit-Extra-07": "7", "RateLimit-Extra-08": "8",
				"RateLimit-Extra-09": "9", "RateLimit-Extra-10": "10", "RateLimit-Extra-11": "11", "RateLimit-Extra-12": "12",
				"RateLimit-Extra-13": "13", "RateLimit-Extra-14": "14", "RateLimit-Extra-15": "15", "RateLimit-Extra-16": "16",
				"RateLimit-Extra-17": "17", "RateLimit-Extra-18": "18", "RateLimit-Extra-19": "19", "RateLimit-Extra-20": "20",
				"Retry-After": "90",
			},
			wantRateLimitIncludes: map[string]string{"Retry-After": "90"},
			wantWarnings:          5, wantRequest: "post", wantValues: []string{"90"},
		},
		{
			name: "repeated rate-limit header values joined", postStatuses: []int{http.StatusTooManyRequests},
			postHeaderValues:      map[string][]string{"RateLimit-Remaining": {"0", "7"}},
			wantRateLimitIncludes: map[string]string{"Ratelimit-Remaining": "0, 7"},
			wantWarnings:          5, wantRequest: "post", wantValues: []string{"0, 7"},
		},
		{
			// The runner parses Retry-After as delta-seconds; a joined "120, 120" would not parse.
			name: "repeated Retry-After keeps the first value", postStatuses: []int{http.StatusTooManyRequests},
			postHeaderValues:      map[string][]string{"Retry-After": {"120", "120"}},
			wantRateLimitIncludes: map[string]string{"Retry-After": "120"},
			wantWarnings:          5, wantRequest: "post",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rotationKey, err := atcrypto.GeneratePrivateKeyK256()
			require.NoError(t, err)
			rotationPublic, err := rotationKey.PublicKey()
			require.NoError(t, err)
			last := map[string]any{
				"type": "plc_operation", "rotationKeys": []any{rotationPublic.DIDKey()},
				"verificationMethods": map[string]any{"atproto": rotationPublic.DIDKey()},
				"services":            map[string]any{"atproto_pds": map[string]any{"type": "AtprotoPersonalDataServer", "endpoint": "https://pds.example"}},
				"alsoKnownAs":         []any{"at://alice.example.com.tidepool.test"}, "prev": nil, "sig": "signature",
			}
			var mutex sync.Mutex
			gets, posts := 0, 0
			respond := func(w http.ResponseWriter, statuses []int, index int, headers map[string]string, headerValues map[string][]string) bool {
				if len(statuses) == 0 {
					return false
				}
				status := statuses[min(index, len(statuses)-1)]
				if status == http.StatusOK {
					return false
				}
				for name, value := range headers {
					w.Header().Set(name, value)
				}
				for name, values := range headerValues {
					for _, value := range values {
						w.Header().Add(name, value)
					}
				}
				w.WriteHeader(status)
				return true
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mutex.Lock()
				defer mutex.Unlock()
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/"+did+"/log/last":
					gets++
					if respond(w, tc.getStatuses, gets-1, tc.getHeaders, nil) {
						return
					}
					_ = json.NewEncoder(w).Encode(last)
				case r.Method == http.MethodPost && r.URL.Path == "/"+did:
					posts++
					respond(w, tc.postStatuses, posts-1, tc.postHeaders, tc.postHeaderValues)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			var logs bytes.Buffer
			minter, err := NewMinter(MinterOptions{
				PLCDirectoryURL: server.URL, BridgeHostname: "tidepool.test", HandleZoneRoot: "ap.tidepool.test",
				RotationKey: rotationKey, Custodian: testCustodian(t), Actors: struct{ store.BridgedActors }{},
				HTTPClient: ap.NewGuardedHTTPClient(true, 10*time.Second),
				Logger:     slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
				PLCRetryWait: func(context.Context, time.Duration) error {
					return nil
				},
			})
			require.NoError(t, err)
			result, err := minter.UpdateHandle(context.Background(), did, "bob.example.com.ap.tidepool.test", nil)
			if tc.wantApplied {
				require.NoError(t, err)
				require.Equal(t, HandleUpdateApplied, result.Outcome)
			} else {
				require.ErrorIs(t, err, ErrPLCTransient)
				var rateLimited *PLCRateLimitedError
				switch {
				case tc.wantRateLimit != nil:
					require.True(t, errors.As(err, &rateLimited), "a final 429 must be a rate limit: %v", err)
					require.Equal(t, tc.wantRateLimit, rateLimited.Headers)
				case tc.wantRateLimitIncludes != nil:
					require.True(t, errors.As(err, &rateLimited), "a final 429 must be a rate limit: %v", err)
					for name, value := range tc.wantRateLimitIncludes {
						require.Equal(t, value, rateLimited.Headers[name], "captured header %s; all captured: %v", name, rateLimited.Headers)
					}
				default:
					require.False(t, errors.As(err, &rateLimited), "a 5xx is not a rate limit: %v", err)
				}
			}
			require.NotContains(t, logs.String(), "secret=1", "only rate-limit headers may be logged")
			var warnings []map[string]any
			for _, line := range bytes.Split(bytes.TrimSpace(logs.Bytes()), []byte("\n")) {
				if len(line) == 0 {
					continue
				}
				var record map[string]any
				require.NoError(t, json.Unmarshal(line, &record))
				if record["level"] == "WARN" {
					warnings = append(warnings, record)
				}
			}
			require.Len(t, warnings, tc.wantWarnings, "one Warn per 429 response; logs: %s", logs.String())
			for _, warning := range warnings {
				require.Equal(t, did, warning["did"])
				values := logStringValues(warning)
				require.Contains(t, values, tc.wantRequest, "the record names the rate-limited request: %v", warning)
				require.Subset(t, values, tc.wantValues, "the record carries the rate-limit header values: %v", warning)
			}
		})
	}
}

// logStringValues collects every string leaf of a decoded JSON log record.
func logStringValues(value any) []string {
	switch typed := value.(type) {
	case string:
		return []string{typed}
	case map[string]any:
		var values []string
		for _, nested := range typed {
			values = append(values, logStringValues(nested)...)
		}
		return values
	case []any:
		var values []string
		for _, nested := range typed {
			values = append(values, logStringValues(nested)...)
		}
		return values
	default:
		return nil
	}
}
