# Vendored Coves lexicons

These JSON files are re-synced from the Coves repository via
`scripts/sync-lexicons.sh` (see that script's header for the ownership
rules — `social/coves/bridge/` is Tidepool-owned and survives re-syncs).
Records the bridge writes are validated against this exact set.

## Bridge conventions for lexicon-unconstrained fields

`social.coves.community.postv2` deliberately leaves `originalAuthor`,
`federatedFrom`, and `location` as `type: unknown` — consumers MUST
tolerate any object there and MUST NOT treat them as authorship claims
(authorship is the repository the record lives in). Tidepool is the
first writer of these fields; the shapes below are the bridge's
published convention (additive forever — fields may be added, never
renamed or repurposed):

### `originalAuthor` — who wrote this on the origin platform

```json
{
  "apId": "https://lemmy.world/u/ExampleUser",
  "handle": "ExampleUser",
  "instance": "lemmy.world",
  "displayName": "Example Display Name"
}
```

- `apId` — the actor's canonical ActivityPub IRI (authority-bound at
  materialization; never taken from inline objects).
- `handle` — the BARE origin-platform username (the local part, original
  casing, derived from the canonical actor IRI). NOT the bridged
  atproto handle: `instance` sits beside it, and the bridged handle is
  derivable from the repo DID.
- `instance` — the origin host.
- `displayName` — optional; present only when the FETCHED,
  authority-bound actor document asserts a `name`. Never sourced from
  inline `attributedTo` objects on content (attacker-influenced), and
  omitted rather than backfilled when the actor document has none.

### `federatedFrom` — where this record was bridged from

```json
{
  "platform": "lemmy",
  "instance": "lemmy.world",
  "apId": "https://lemmy.world/post/12345"
}
```

- `platform` — the origin software family (`"lemmy"` for anything
  speaking Lemmy's dialect; PieFed/Mbin get their own values if and
  when quirk handling diverges).
- `instance` — the origin host of the object.
- `apId` — the object's canonical AP id (the same id `ap_objects` maps).

## Acceptance / removal record keys

`social.coves.community.acceptance` and `social.coves.community.removal`
use the digest record key from Coves' PRD_AUTHOR_OWNED_POSTS §3.2: the
unpadded lowercase base32 (RFC 4648 standard alphabet) of the SHA-256
digest of the canonical subject at-uri — fixed 52 chars, raw bytes
hashed with no normalization. The Go implementation is
`internal/materialize.SubjectRKey`, golden-pinned byte-for-byte against
Coves' test vectors: a divergence forks acceptance identity between the
two engines writing into community repos.
