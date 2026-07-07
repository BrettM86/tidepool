# Task 02 — ActivityPub protocol layer, client side (~1000 LOC)

## Goal
Everything needed to *read* the Lemmy-flavored fediverse: typed AP vocab,
WebFinger resolution, HTTP-signature-signed fetch, and collection paging.
No inbox/server here (task 06); no translation (task 05).

## Deliverables
- `internal/ap/vocab.go` — structs + JSON (un)marshalling for the objects
  Lemmy/FEP-1b12 actually emit. Tolerant parsing (json-ld-lite): fields that
  may be string-or-object (`to`, `cc`, `attributedTo`, `object`, `icon`)
  handled with custom unmarshallers. Types: `Group`, `Person`, `Page`
  (posts), `Note` (comments), `Article`, `Create`, `Update`, `Delete`,
  `Announce`, `Follow`, `Accept`, `Undo`, `Like`, `Dislike`, `Tombstone`,
  `Image`, `Link/Hashtag`, `Collection`/`OrderedCollection`(+Page),
  `source` (markdown), `language`, Lemmy extensions (`sensitive`,
  `commentsEnabled`/`postingRestrictedToMods`, `stickied`).
- `internal/ap/webfinger.go` — resolve `!community@instance` /
  `user@instance` → actor URL via `/.well-known/webfinger`, and reverse
  (fetch actor, read `preferredUsername` + host).
- `internal/ap/httpsig.go` — draft-cavage HTTP signatures exactly as Lemmy
  validates them: sign `(request-target) host date digest` with RSA-SHA256
  (Lemmy requires RSA actor keys for AP interop; the bridge's *AP-side*
  service key is RSA — distinct from atproto secp256k1 repo keys). Signed
  GET (many instances require authorized fetch) and signed POST (used by
  task 06 for Follow). Verify() lives here too (task 06 consumes it):
  fetch remote actor's publicKeyPem with caching, check date skew, digest.
- `internal/ap/client.go` — fetch AP objects with
  `Accept: application/activity+json`, retries with backoff, per-host rate
  limiting, 5xx/410 handling (410 → treat as deleted), response size cap,
  in-flight dedupe. `FetchActor`, `FetchObject`, `FetchCollection` (page
  through `OrderedCollectionPage.next`, cap pages, yield items).
- `internal/ap/service_actor.go` — the bridge's own `Application` actor
  document served later at `https://BRIDGE_HOSTNAME/actor` (generation +
  RSA keypair persistence in DB; the HTTP route itself can land in task 06
  with the inbox, but the document/key logic lives here).
- Golden-file tests: real captured Lemmy JSON (fetch a handful of live
  fixtures from lemmy.world/lemmy.ml at implementation time — a Group, a
  Page with image embed, a Note with parent, an Announce{Create{Note}},
  a Like — and commit them under `internal/ap/testdata/`). Parse → assert
  fields. Round-trip signing test with a local verifier.

## Definition of done
- All fixtures parse without error; unknown fields ignored, never fatal.
- Signed GET against a live Lemmy instance works (manual smoke, documented
  in the task notes; CI tests use recorded fixtures only).
- `go test ./...` green.

## References
- `~/Code/granary/granary/as2.py` (AP quirk handling),
  `~/Code/bridgy-fed/activitypub.py` (signed fetch, key caching, Lemmy
  compat notes), FEP-1b12 spec, Lemmy federation docs
  (join-lemmy.org/docs/contributors/05-federation.html).
