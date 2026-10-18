package identity

import (
	"context"
	"encoding/base64"
	"encoding/json"
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
