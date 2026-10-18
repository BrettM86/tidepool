package identity

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/idna"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

type schemeActors struct {
	taken map[string]bool
}

func (a schemeActors) HandleTaken(_ context.Context, handle string) (bool, error) {
	return a.taken[handle], nil
}

func newSchemeMinter(t *testing.T, taken map[string]bool, realActors ...store.BridgedActors) (*Minter, *atomic.Int64) {
	t.Helper()
	requests := new(atomic.Int64)
	plc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method != http.MethodPost || !strings.HasPrefix(r.URL.Path, "/did:plc:") {
			http.Error(w, "unexpected PLC request", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(plc.Close)

	rotationKey, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	var actors interface {
		HandleTaken(context.Context, string) (bool, error)
	} = schemeActors{taken: taken}
	if len(realActors) != 0 {
		actors = realActors[0]
	}
	minter, err := NewMinter(MinterOptions{
		PLCDirectoryURL: plc.URL,
		BridgeHostname:  "tdpl.io",
		HandleZoneRoot:  "ap.tdpl.io",
		RotationKey:     rotationKey,
		Custodian:       testCustodian(t),
		Actors:          actors,
		HTTPClient:      ap.NewGuardedHTTPClient(true, 10*time.Second),
	})
	require.NoError(t, err)
	return minter, requests
}

func TestMintActor_SchemeStoredHandleAvailability(t *testing.T) {
	for _, tc := range []struct {
		name, seededHandle, username, instance, want string
	}{
		{"same host normalizes into occupied handle", "alice-b.lemmy.world.ap.tdpl.io", "Alice_B", "lemmy.world", "alice-b-2.lemmy.world.ap.tdpl.io"},
		{"different host keeps the unsuffixed label", "alice.lemmy.world.ap.tdpl.io", "alice", "lemmy.ml", "alice.lemmy.ml.ap.tdpl.io"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := testutil.DB(t)
			testutil.Truncate(t, db, "bridged_actors", "handle_claims")
			actors := store.NewBridgedActors(db)
			_, err := actors.UpsertActor(t.Context(), store.BridgedActor{
				APActorID: "https://lemmy.world/u/owner", ActorType: store.ActorTypePerson,
				DID: "did:plc:ewvi7nxzyoun6zhxrhs64oiz", Handle: tc.seededHandle,
				SigningKeyEncrypted: []byte("owner-key"), ConsentState: store.ConsentStateOK,
			})
			require.NoError(t, err)
			minter, _ := newSchemeMinter(t, nil, actors)
			minted, err := minter.MintActor(t.Context(), MintRequest{
				ActorType: store.ActorTypePerson, PreferredUsername: tc.username, Instance: tc.instance,
			})
			require.NoError(t, err)
			assert.Equal(t, tc.want, minted.Handle)
		})
	}
}

func TestMintActor_SchemeInstanceAndUsername(t *testing.T) {
	seventyAs := strings.Repeat("a", 70)
	for _, tc := range []struct {
		name, username, instance, want string
		actorType                      store.ActorType
		taken                          map[string]bool
	}{
		{"lowercase instance preserves dots", "alice", "Lemmy.World", "alice.lemmy.world.ap.tdpl.io", store.ActorTypePerson, nil},
		{"normalize username", "Alice_B", "lemmy.world", "alice-b.lemmy.world.ap.tdpl.io", store.ActorTypePerson, nil},
		{"group gets c- prefix", "technology", "lemmy.world", "c-technology.lemmy.world.ap.tdpl.io", store.ActorTypeGroup, nil},
		{"group prefix follows normalization", "Rust_Lang", "lemmy.world", "c-rust-lang.lemmy.world.ap.tdpl.io", store.ActorTypeGroup, nil},
		{"person keeps unprefixed label", "technology", "lemmy.world", "technology.lemmy.world.ap.tdpl.io", store.ActorTypePerson, nil},
		{"person normalizing to c- gets u- prefix", "c_technology", "lemmy.world", "u-c-technology.lemmy.world.ap.tdpl.io", store.ActorTypePerson, nil},
		{"person named c stays unprefixed", "c", "lemmy.world", "c.lemmy.world.ap.tdpl.io", store.ActorTypePerson, nil},
		{"person starting with c but not c- stays unprefixed", "ctech", "lemmy.world", "ctech.lemmy.world.ap.tdpl.io", store.ActorTypePerson, nil},
		// The prefix counts toward the 63-byte label limit.
		{"group prefix within label limit", seventyAs, "lemmy.world",
			"c-" + strings.Repeat("a", 61) + ".lemmy.world.ap.tdpl.io", store.ActorTypeGroup, nil},
		{"group collision suffixes prefixed label", "technology", "lemmy.world",
			"c-technology-2.lemmy.world.ap.tdpl.io", store.ActorTypeGroup,
			map[string]bool{"c-technology.lemmy.world.ap.tdpl.io": true}},
		{"u- prefixed person collision suffixes prefixed label", "c_tech", "lemmy.world",
			"u-c-tech-2.lemmy.world.ap.tdpl.io", store.ActorTypePerson,
			map[string]bool{"u-c-tech.lemmy.world.ap.tdpl.io": true}},
		// A suffix must not turn a person's "c" into a community-style "c-" label.
		{"person c collision gets u- prefix", "c", "lemmy.world",
			"u-c-2.lemmy.world.ap.tdpl.io", store.ActorTypePerson,
			map[string]bool{"c.lemmy.world.ap.tdpl.io": true}},
		{"person c second collision gets u- prefix", "c", "lemmy.world",
			"u-c-3.lemmy.world.ap.tdpl.io", store.ActorTypePerson,
			map[string]bool{"c.lemmy.world.ap.tdpl.io": true, "u-c-2.lemmy.world.ap.tdpl.io": true}},
		{"group collision at label limit trims before suffix", seventyAs, "lemmy.world",
			"c-" + strings.Repeat("a", 59) + "-2.lemmy.world.ap.tdpl.io", store.ActorTypeGroup,
			map[string]bool{"c-" + strings.Repeat("a", 61) + ".lemmy.world.ap.tdpl.io": true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			minter, _ := newSchemeMinter(t, tc.taken)
			identity, err := minter.MintActor(t.Context(), MintRequest{
				ActorType:         tc.actorType,
				PreferredUsername: tc.username,
				Instance:          tc.instance,
			})
			require.NoError(t, err)
			assert.Equal(t, tc.want, identity.Handle)
			for _, label := range strings.Split(identity.Handle, ".") {
				assert.LessOrEqual(t, len(label), 63)
			}
		})
	}
}

func TestMintActor_SchemeUnicodeAndFallback(t *testing.T) {
	for _, tc := range []struct {
		name, username, want string
		actorType            store.ActorType // empty means person
	}{
		{"Japanese", "日本語", "xn--wgv71a119e.lemmy.world.ap.tdpl.io", ""},
		{"ideographic zero", "〇", "xn--w6j.lemmy.world.ap.tdpl.io", ""},
		{"uppercase umlauts", "ÄÖ", "xn--4ca0b.lemmy.world.ap.tdpl.io", ""},
		{"combining umlauts", "äö", "xn--4ca0b.lemmy.world.ap.tdpl.io", ""},
		{"mixed ASCII and CJK", "A日本_B", "a-b.lemmy.world.ap.tdpl.io", ""},
		{"underscores", "___", "ubda251550b.lemmy.world.ap.tdpl.io", ""},
		{"punctuation", "-.-", "uc3ba3324f6.lemmy.world.ap.tdpl.io", ""},
		{"CJK with underscore", "日本_語", "u694f007a01.lemmy.world.ap.tdpl.io", ""},
		{"emoji", "🦀🦀", "u3236f8cd56.lemmy.world.ap.tdpl.io", ""},
		{"symbols", "☃☃", "u9105a15f2a.lemmy.world.ap.tdpl.io", ""},
		{"fullwidth ASCII", "Ａ", "u3075754175.lemmy.world.ap.tdpl.io", ""},
		{"CJK with dot", "日本.語", "u51f13b1424.lemmy.world.ap.tdpl.io", ""},
		// RFC 5892 §2.9 OldHangulJamo: Lo, but DISALLOWED under IDNA2008.
		{"old Hangul Jamo", "\u1100\u1100", "u0a77d591f7.lemmy.world.ap.tdpl.io", ""},
		// 32 distinct non-ASCII CJK characters need more than 59 punycode
		// characters, so their single xn-- label cannot fit within 63.
		// The hash prefix was computed independently from the exact UTF-8 bytes.
		{"overlong IDNA label", "天地玄黄宇宙洪荒日月盈昃辰宿列張寒來暑往秋收冬藏閏餘成歲律呂調陽", "u05e092abf1.lemmy.world.ap.tdpl.io", ""},
		{"group punycode gets c- prefix", "日本", "c-xn--wgv71a.lemmy.world.ap.tdpl.io", store.ActorTypeGroup},
		{"group hash fallback gets c- prefix", "😀", "c-uf0443a342c.lemmy.world.ap.tdpl.io", store.ActorTypeGroup},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actorType := tc.actorType
			if actorType == "" {
				actorType = store.ActorTypePerson
			}
			minter, _ := newSchemeMinter(t, nil)
			identity, err := minter.MintActor(t.Context(), MintRequest{
				ActorType:         actorType,
				PreferredUsername: tc.username,
				Instance:          "lemmy.world",
			})
			require.NoError(t, err)
			assert.Equal(t, tc.want, identity.Handle)
			_, err = syntax.ParseHandle(identity.Handle)
			assert.NoError(t, err)
		})
	}
}

func TestMintActor_SchemeLengthAndCollision(t *testing.T) {
	host := strings.Repeat("x", 63) + "." + strings.Repeat("y", 63) + "." +
		strings.Repeat("z", 63) + "." + strings.Repeat("w", 29)
	require.Len(t, host, 221) // 20 bytes remain for the user label.
	username := strings.Repeat("a", 19) + "-" + strings.Repeat("b", 70)
	firstHandle := "aaaaaaaaaaaaaaaaaaa." + host + ".ap.tdpl.io"
	secondHandle := "aaaaaaaaaaaaaaaaaa-2." + host + ".ap.tdpl.io"
	taken := make(map[string]bool)
	minter, _ := newSchemeMinter(t, taken)
	request := MintRequest{
		ActorType:         store.ActorTypePerson,
		PreferredUsername: username,
		Instance:          host,
	}

	for _, tc := range []struct{ name, want string }{
		{"trim at boundary", firstHandle},
		{"suffix after trimming", secondHandle},
	} {
		t.Run(tc.name, func(t *testing.T) {
			identity, err := minter.MintActor(t.Context(), request)
			require.NoError(t, err)
			assert.Equal(t, tc.want, identity.Handle)
			assert.LessOrEqual(t, len(identity.Handle), 253)
			for _, label := range strings.Split(identity.Handle, ".") {
				assert.LessOrEqual(t, len(label), 63)
			}
			_, err = syntax.ParseHandle(identity.Handle)
			assert.NoError(t, err)
		})
		taken[firstHandle] = true
	}
}

func TestMintActor_SchemePunycodeLabelIsNeverSuffixedOrTruncated(t *testing.T) {
	longHost := strings.Repeat("x", 63) + "." + strings.Repeat("y", 63) + "." +
		strings.Repeat("z", 63) + "." + strings.Repeat("w", 37)
	require.Len(t, longHost, 229) // 12 bytes remain for the user label.
	prefixHost := strings.Repeat("x", 63) + "." + strings.Repeat("y", 63) + "." +
		strings.Repeat("z", 63) + "." + strings.Repeat("w", 35)
	require.Len(t, prefixHost, 227) // 14 bytes remain for the user label.
	for _, tc := range []struct {
		name, username, instance, want string
		actorType                      store.ActorType
		taken                          map[string]bool
	}{
		// "xn--wgv71a119e-2" no longer decodes: punycode reads its last '-'
		// as the delimiter. The colliding name falls back to the hash.
		{"collision suffixes the hash", "日本語", "lemmy.world", "u77710aedc7-2.lemmy.world.ap.tdpl.io",
			store.ActorTypePerson, map[string]bool{"xn--wgv71a119e.lemmy.world.ap.tdpl.io": true}},
		// Truncating "xn--wgv71a119e" to 12 bytes decodes to other text.
		{"long host truncates the hash", "日本語", longHost, "u77710aedc7." + longHost + ".ap.tdpl.io",
			store.ActorTypePerson, nil},
		// "ucf2abf0c5b" is the first 10 hex of sha256("日本"), computed independently.
		{"group collision suffixes the prefixed hash", "日本", "lemmy.world", "c-ucf2abf0c5b-2.lemmy.world.ap.tdpl.io",
			store.ActorTypeGroup, map[string]bool{"c-xn--wgv71a.lemmy.world.ap.tdpl.io": true}},
		// "xn--wgv71a119e" (14) fits the budget but "c-xn--wgv71a119e" (16) does not.
		{"group prefix pushes punycode past budget", "日本語", prefixHost, "c-u77710aedc7." + prefixHost + ".ap.tdpl.io",
			store.ActorTypeGroup, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			minter, _ := newSchemeMinter(t, tc.taken)
			identity, err := minter.MintActor(t.Context(), MintRequest{
				ActorType:         tc.actorType,
				PreferredUsername: tc.username,
				Instance:          tc.instance,
			})
			require.NoError(t, err)
			assert.Equal(t, tc.want, identity.Handle)
			assert.LessOrEqual(t, len(identity.Handle), 253)
			userLabel, _, _ := strings.Cut(identity.Handle, ".")
			if tc.actorType == store.ActorTypeGroup {
				userLabel = strings.TrimPrefix(userLabel, "c-")
			}
			if strings.HasPrefix(userLabel, "xn--") {
				_, err := idna.Lookup.ToUnicode(userLabel)
				assert.NoError(t, err, "punycode user label must decode")
			} else {
				assert.Regexp(t, `^u[0-9a-f]{10}(-[0-9]+)?$`, userLabel)
			}
		})
	}
}

func TestMintActor_SchemeRejectsIPAndPortHostsBeforePLC(t *testing.T) {
	for _, instance := range []string{"2001:db8::1", "127.0.0.1", "example.com:8443"} {
		t.Run(instance, func(t *testing.T) {
			minter, requests := newSchemeMinter(t, nil)
			_, err := minter.MintActor(t.Context(), MintRequest{
				ActorType:         store.ActorTypePerson,
				PreferredUsername: "alice",
				Instance:          instance,
			})
			require.Error(t, err)
			assert.True(t, errors.IsValidation(err), "got %v", err)
			assert.Zero(t, requests.Load())
		})
	}
}

func TestMintActor_SchemeHostTooLongBeforePLC(t *testing.T) {
	for _, tc := range []struct {
		name      string
		host      string
		length    int
		actorType store.ActorType
	}{
		// With zone (10), one label and two dots (3), the total is 254.
		{"one byte over", strings.Repeat("x", 63) + "." + strings.Repeat("y", 63) + "." +
			strings.Repeat("z", 63) + "." + strings.Repeat("w", 49), 241, store.ActorTypePerson},
		// Leaves a negative budget for the user label.
		{"far over", strings.Repeat("x", 63) + "." + strings.Repeat("y", 63) + "." +
			strings.Repeat("z", 63) + "." + strings.Repeat("w", 58), 250, store.ActorTypePerson},
		// The group label "c-a" (3), zone (10) and two dots total 254.
		{"group prefix one byte over", strings.Repeat("x", 63) + "." + strings.Repeat("y", 63) + "." +
			strings.Repeat("z", 63) + "." + strings.Repeat("w", 47), 239, store.ActorTypeGroup},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Len(t, tc.host, tc.length)
			minter, requests := newSchemeMinter(t, nil)
			_, err := minter.MintActor(t.Context(), MintRequest{
				ActorType:         tc.actorType,
				PreferredUsername: "a",
				Instance:          tc.host,
			})
			require.Error(t, err)
			assert.True(t, errors.IsValidation(err), "got %v", err)
			assert.Zero(t, requests.Load())
		})
	}

	t.Run("exact fit", func(t *testing.T) {
		host := strings.Repeat("x", 63) + "." + strings.Repeat("y", 63) + "." +
			strings.Repeat("z", 63) + "." + strings.Repeat("w", 48)
		require.Len(t, host, 240)
		minter, _ := newSchemeMinter(t, nil)
		identity, err := minter.MintActor(t.Context(), MintRequest{
			ActorType:         store.ActorTypePerson,
			PreferredUsername: "alice",
			Instance:          host,
		})
		require.NoError(t, err)
		assert.Equal(t, "a."+host+".ap.tdpl.io", identity.Handle)
		assert.Len(t, identity.Handle, 253)
	})

	t.Run("group exact fit", func(t *testing.T) {
		host := strings.Repeat("x", 63) + "." + strings.Repeat("y", 63) + "." +
			strings.Repeat("z", 63) + "." + strings.Repeat("w", 46)
		require.Len(t, host, 238)
		minter, _ := newSchemeMinter(t, nil)
		identity, err := minter.MintActor(t.Context(), MintRequest{
			ActorType:         store.ActorTypeGroup,
			PreferredUsername: "a",
			Instance:          host,
		})
		require.NoError(t, err)
		assert.Equal(t, "c-a."+host+".ap.tdpl.io", identity.Handle)
		assert.Len(t, identity.Handle, 253)
	})
}

func TestHandleFor_RejectsUnknownActorType(t *testing.T) {
	for _, tc := range []struct {
		name      string
		actorType store.ActorType
	}{
		{"empty", ""},
		{"service", "service"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			minter, _ := newSchemeMinter(t, nil)
			handle, err := minter.HandleFor(t.Context(), tc.actorType, "alice", "lemmy.world")
			require.Error(t, err)
			assert.True(t, errors.IsValidation(err), "got %v", err)
			assert.Empty(t, handle)
		})
	}
}
