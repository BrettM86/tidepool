package materialize

import (
	"crypto/sha256"
	"encoding/base32"
	"strings"
)

// subjectRKeyEncoding is RFC 4648 base32 — the STANDARD alphabet — with the
// padding dropped at encode time rather than trimmed afterwards, because '='
// is not in the atProto record-key charset. The lowercasing applied after is
// what keeps the rest of the key inside that charset: the standard alphabet is
// uppercase, and to a PDS that treats record keys as opaque bytes an uppercase
// key is simply a different key.
//
// base32-Hex, the other encoding in this package's stdlib dependency, draws
// from a DIFFERENT alphabet and would mint a different — and equally
// plausible-looking — key for every subject in the network. It is not
// interchangeable here.
var subjectRKeyEncoding = base32.StdEncoding.WithPadding(base32.NoPadding)

// SubjectRKey derives the record key a community's acceptance (and removal)
// for one post share.
//
// It is the unpadded lowercase base32 encoding of the SHA-256 digest of the
// post's AT-URI: a fixed 52 characters, drawn entirely from the rkey-safe
// charset and far inside the 512-byte rkey limit, for a subject of any length.
//
// THIS MUST BYTE-MATCH COVES' posts.SubjectRkey. Coves' author-owned-posts
// flip (PLAN.md decisions 13/20) deleted every write of a post into a
// community repo; what a community repo now holds is acceptance and removal
// records, and TWO engines write them into the same repos — Coves' own for
// native postv2s, this bridge for bridged ones. If the two derivations fork,
// one post acquires two acceptance rkeys, and neither side can see, update, or
// retract the record the other wrote. That is a silent partition of acceptance
// identity, not an error anything surfaces. The golden vectors in the test
// beside this file are the contract; they were computed outside Go so they pin
// the spec rather than either implementation.
//
// WHY A DIGEST RATHER THAN A READABLE TRANSFORM. The obvious scheme — strip
// `at://`, swap `/` for `:` — is not total over the legal subject space. DIDs
// run to 2048 bytes and may carry percent-escapes, so such a transform can
// emit keys that exceed the rkey limit or leave the rkey charset. A key
// function that fails on some inputs fails on exactly the identifiers a
// hostile author gets to choose. A fixed-size digest is total, and just as
// deterministic.
//
// WHY ONE FUNCTION FOR BOTH RECORD TYPES. The acceptance and the removal for a
// subject share this key. Record keys are scoped to their collection, so there
// is nothing to collide; a per-collection salt would buy nothing and cost the
// property that makes the removal commit shapeable — both of a subject's
// records are reached by ONE derivation, so pre-reading them is two lookups of
// one computed value rather than a search.
//
// PASS THE MAPPING'S AT-URI VERBATIM, NEVER AN AP ID. The subject is the
// atproto at://did/collection/rkey the record lives at — the identity Coves
// indexes under. Hashing an origin-platform AP id instead produces a
// well-formed key for a subject nobody addresses.
//
// THE ARGUMENT IS BYTES, NOT A PARSED URI. Whatever the subject string holds
// is what gets hashed: no normalization, no percent-decoding, no case folding.
// Those bytes are the identity Coves' AppView indexes under, so a writer that
// normalized would key its records to a URI the reader never asks for.
func SubjectRKey(subjectATURI string) string {
	digest := sha256.Sum256([]byte(subjectATURI))
	return strings.ToLower(subjectRKeyEncoding.EncodeToString(digest[:]))
}
