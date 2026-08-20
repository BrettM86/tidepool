package materialize

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// SubjectRKey must byte-match Coves' posts.SubjectRkey. The two systems write
// acceptance records into the SAME community repos: Coves' engine for native
// posts, this bridge for bridged ones. If the derivations fork, the same post
// gets two acceptance rkeys and neither side can see or update the other's
// record — a silent partition of acceptance identity, not a crash.
//
// THE GOLDEN VALUES ARE HARD-CODED, NOT RECOMPUTED, and are copied verbatim
// from the Coves repo's internal/core/posts/rkey_test.go (which produced them
// OUTSIDE Go, with:
//
//	python3 -c 'import hashlib,base64;print(base64.b32encode(
//	    hashlib.sha256(URI.encode()).digest()).decode().lower().rstrip("="))'
//
// so they encode the spec rather than either implementation). A test that
// hashed the URI itself and compared would pass against base32-Hex, against
// uppercase, against padded output, and against a completely different digest.
const (
	// goldenSubjectURI is the canonical vector: an ordinary post in an ordinary
	// author repo.
	goldenSubjectURI  = "at://did:plc:abc123/social.coves.community.postv2/3kjzl5kcb2s2v"
	goldenSubjectRKey = "xxdmibjaexx43drostplutjbp7g4oaw3uriugf5twafpldfkupca"

	// goldenSiblingURI differs from the canonical vector in its final character
	// only, so "distinct URIs get distinct keys" is proven at the smallest
	// possible difference rather than at a comfortable one.
	goldenSiblingURI  = "at://did:plc:abc123/social.coves.community.postv2/3kjzl5kcb2s2w"
	goldenSiblingRKey = "iyhgczhg7xsbrayzrrs2qa4fks6amctx7ghyjakqyrlhxztbbl5a"
)

// subjectRKeyLength is what the PRD's §3.2 promises: SHA-256 is 256 bits,
// base32 packs 5 bits per character, and 256/5 rounds up to 52 characters
// (with the padding stripped, which is why 52 and not 56).
const subjectRKeyLength = 52

// rkeyCharset is the set base32 draws from once lowercased. It is also a
// subset of the atProto record-key charset, which is the property that makes a
// digest safe to use as a key at all.
const rkeyCharset = "abcdefghijklmnopqrstuvwxyz234567"

// TestSubjectRKey_GoldenVector pins the canonical encoding.
func TestSubjectRKey_GoldenVector(t *testing.T) {
	t.Parallel()

	assert.Equal(t, goldenSubjectRKey, SubjectRKey(goldenSubjectURI),
		"the rkey for %s is fixed by the PRD and by every acceptance record Coves has already "+
			"written under it; a different value here means the encoding diverged (uppercase, "+
			"padded, base32-Hex, or a different digest) and this bridge's acceptances no longer "+
			"address the same records Coves does",
		goldenSubjectURI)

	assert.Equal(t, goldenSiblingRKey, SubjectRKey(goldenSiblingURI))
}

// TestSubjectRKey_ShapeIsFixedRegardlessOfSubjectLength: the key is a fixed 52
// rkey-safe characters for any subject, including attacker-chosen ones.
func TestSubjectRKey_ShapeIsFixedRegardlessOfSubjectLength(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		uri  string
	}{
		{name: "ordinary did:plc subject", uri: goldenSubjectURI},
		{
			// 580 bytes: a did:web whose authority alone is over the 512-byte
			// rkey limit, so a readable transform would produce a key the PDS
			// refuses outright.
			name: "did:web subject longer than the 512-byte rkey limit",
			uri:  "at://" + longDIDWeb() + "/social.coves.community.postv2/3kjzl5kcb2s2v",
		},
		{
			// The maximum legal DID length. Nothing about the answer's shape
			// may vary with it.
			name: "2048-byte DID, the legal maximum",
			uri:  "at://" + didOfLength(2048) + "/social.coves.community.postv2/3kjzl5kcb2s2v",
		},
		{
			name: "percent-escaped authority",
			uri:  "at://did:web:example.com%3A8443/social.coves.community.postv2/3kjzl5kcb2s2v",
		},
		{
			name: "empty subject",
			uri:  "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rkey := SubjectRKey(tc.uri)

			assert.Lenf(t, rkey, subjectRKeyLength,
				"the derivation promises a fixed %d characters for a subject of any length; "+
					"this one was %d bytes", subjectRKeyLength, len(tc.uri))

			for i, r := range rkey {
				assert.Containsf(t, rkeyCharset, string(r),
					"rkey %q holds %q at index %d, which is outside the unpadded lowercase base32 "+
						"alphabet — uppercase and '=' are the two slips that produce a key the PDS rejects",
					rkey, string(r), i)
			}
		})
	}
}

// TestSubjectRKey_LongSubjectGoldenVectors: the long vectors get golden values
// too, not merely a shape check. A truncating implementation — hashing only
// the first N bytes, or a readable transform that clipped to 512 — passes the
// shape assertions above and collides two different long subjects onto one key.
func TestSubjectRKey_LongSubjectGoldenVectors(t *testing.T) {
	t.Parallel()

	assert.Equal(t,
		"fktiwazbhqfsqjg5e7yypm3ukbalk72bfplcr2d2ukm7iuwqeyqa",
		SubjectRKey("at://"+longDIDWeb()+"/social.coves.community.postv2/3kjzl5kcb2s2v"))

	assert.Equal(t,
		"4pri7ptezxzyzpzzysj5for6shd54yt5vvch6v3nj4e7kmfg62qq",
		SubjectRKey("at://"+didOfLength(2048)+"/social.coves.community.postv2/3kjzl5kcb2s2v"))
}

// TestSubjectRKey_IsStableAcrossCalls: stability is the whole contract. If it
// ever stops holding — a map iteration folded into the derivation, a
// timestamp, a random salt — a redelivered post mints a second acceptance
// record instead of idempotently re-putting the first.
func TestSubjectRKey_IsStableAcrossCalls(t *testing.T) {
	t.Parallel()

	first := SubjectRKey(goldenSubjectURI)
	for i := 0; i < 32; i++ {
		require.Equalf(t, first, SubjectRKey(goldenSubjectURI),
			"call %d returned a different key for the same subject", i)
	}
}

// TestSubjectRKey_DistinguishesSubjectsThatDifferInBytesOnly: the subject's
// RAW BYTES are the identity. No normalization, no percent-decoding, no case
// folding — Coves' AppView indexes and looks records up under exactly the
// bytes it stored, so a writer that normalized would key its records to a URI
// the reader never asks for.
func TestSubjectRKey_DistinguishesSubjectsThatDifferInBytesOnly(t *testing.T) {
	t.Parallel()

	escaped := "at://did:web:example.com%3A8443/social.coves.community.postv2/3kjzl5kcb2s2v"
	decoded := "at://did:web:example.com:8443/social.coves.community.postv2/3kjzl5kcb2s2v"

	assert.NotEqual(t, SubjectRKey(escaped), SubjectRKey(decoded),
		"the escaped and decoded spellings are different byte strings and must key differently; "+
			"a function that percent-decoded first would let two rows the AppView keeps apart "+
			"collide onto one acceptance record")

	assert.Equal(t, "ip6rb5ppturmcux6r54fglc57je3cgfofvi3eajodrxqkgcwjrna", SubjectRKey(escaped))
	assert.Equal(t, "jusonlequb2pkc34y5uwa65ng22cwz5qef3gorg5huqebglag37a", SubjectRKey(decoded))

	assert.NotEqual(t, SubjectRKey(goldenSubjectURI), SubjectRKey(goldenSiblingURI),
		"two posts differing in one character of their rkey must not share an acceptance record")

	// Case is a byte difference like any other.
	assert.NotEqual(t, SubjectRKey(goldenSubjectURI), SubjectRKey(strings.ToUpper(goldenSubjectURI)))
}

// longDIDWeb is a did:web whose authority alone exceeds the 512-byte rkey
// limit: eight maximum-length DNS labels and a TLD. Copied verbatim from
// Coves' rkey_test.go — the golden vectors above are keyed to these exact
// bytes.
func longDIDWeb() string {
	label := strings.Repeat("a", 63)
	labels := make([]string, 8)
	for i := range labels {
		labels[i] = label
	}
	return "did:web:" + strings.Join(labels, ".") + ".example.com"
}

// didOfLength returns a syntactically DID-shaped identifier of exactly n bytes.
func didOfLength(n int) string {
	const prefix = "did:web:"
	return prefix + strings.Repeat("b", n-len(prefix))
}
