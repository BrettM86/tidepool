// Package ap implements the client side of the Lemmy-flavored ActivityPub
// protocol: tolerant AS2 vocabulary types, WebFinger resolution, draft-cavage
// HTTP signatures, and a signed-fetch client with collection paging.
//
// The vocabulary deliberately avoids a full JSON-LD processor. Like granary's
// as2.py it treats AP as "json-ld-lite": one open Object struct covering every
// type Lemmy/FEP-1b12 emits, with custom unmarshallers for the fields that may
// legally appear as a bare IRI string, a single object, or an array of either.
// Unknown fields are ignored, never fatal.
package ap

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"time"
)

// AS2 media types. Lemmy serves and accepts application/activity+json;
// Mastodon prefers the ld+json profile form. We send both in Accept.
const (
	ContentTypeActivityJSON = "application/activity+json"
	ContentTypeLDJSON       = `application/ld+json; profile="https://www.w3.org/ns/activitystreams"`
	acceptActivityJSON      = ContentTypeActivityJSON + `, application/ld+json; profile="https://www.w3.org/ns/activitystreams"; q=0.9`
)

// PublicAudience is the special AS2 collection meaning "public".
const PublicAudience = "https://www.w3.org/ns/activitystreams#Public"

// AP object/activity types Lemmy and FEP-1b12 emit.
const (
	TypeGroup       = "Group"
	TypePerson      = "Person"
	TypeApplication = "Application"
	TypeService     = "Service"

	TypePage    = "Page"
	TypeNote    = "Note"
	TypeArticle = "Article"

	TypeCreate   = "Create"
	TypeUpdate   = "Update"
	TypeDelete   = "Delete"
	TypeAnnounce = "Announce"
	TypeFollow   = "Follow"
	TypeAccept   = "Accept"
	TypeReject   = "Reject"
	TypeUndo     = "Undo"
	TypeLike     = "Like"
	TypeDislike  = "Dislike"

	TypeTombstone = "Tombstone"
	TypeImage     = "Image"
	TypeLink      = "Link"
	TypeHashtag   = "Hashtag"
	TypeMention   = "Mention"

	TypeCollection            = "Collection"
	TypeOrderedCollection     = "OrderedCollection"
	TypeCollectionPage        = "CollectionPage"
	TypeOrderedCollectionPage = "OrderedCollectionPage"
)

// Object is the universal AS2 object: every field any Lemmy/FEP-1b12 type
// uses, in one open struct. The Type field discriminates. Fields that may be
// a bare IRI, an inline object, or an array use the tolerant wrapper types
// below (Ref, Refs, Audience, Links, Tags, Languages).
type Object struct {
	Context json.RawMessage `json:"@context,omitempty"`
	ID      string          `json:"id,omitempty"`
	Type    string          `json:"type,omitempty"`

	// Addressing. Lemmy emits arrays of IRI strings; other implementations
	// emit single strings or inline objects. Audience flattens all of them
	// to IRIs.
	To  Audience `json:"to,omitempty"`
	Cc  Audience `json:"cc,omitempty"`
	Bto Audience `json:"bto,omitempty"`
	Bcc Audience `json:"bcc,omitempty"`
	// Audience on Lemmy objects/activities is the community IRI (FEP-1b12).
	Audience Audience `json:"audience,omitempty"`

	// Actor/attribution: string or object (or array of either).
	Actor        *Object `json:"actor,omitempty"`
	AttributedTo Refs    `json:"attributedTo,omitempty"`

	// The activity payload: string or inline object, possibly nested
	// (Announce{Create{Note}}).
	Object *Object `json:"object,omitempty"`
	Target *Object `json:"target,omitempty"`

	// Content.
	Name      string  `json:"name,omitempty"`
	Content   string  `json:"content,omitempty"`
	Summary   string  `json:"summary,omitempty"`
	MediaType string  `json:"mediaType,omitempty"`
	Source    *Source `json:"source,omitempty"`
	URL       Links   `json:"url,omitempty"`
	InReplyTo *Object `json:"inReplyTo,omitempty"`
	Tag       Tags    `json:"tag,omitempty"`
	Attach    Tags    `json:"attachment,omitempty"`
	Icon      *Object `json:"icon,omitempty"`
	Image     *Object `json:"image,omitempty"`
	Published *Time   `json:"published,omitempty"`
	Updated   *Time   `json:"updated,omitempty"`
	// Replies, when advertised, is the object's replies collection (a bare
	// IRI or an inline collection). Task 06's backfill pages through it.
	Replies *Object `json:"replies,omitempty"`

	// Lemmy emits a single language object on posts/comments but an ARRAY
	// of language objects on Group actors; Languages accepts both.
	Language Languages `json:"language,omitempty"`

	// Lemmy extensions.
	Sensitive               *bool `json:"sensitive,omitempty"`
	CommentsEnabled         *bool `json:"commentsEnabled,omitempty"`
	PostingRestrictedToMods *bool `json:"postingRestrictedToMods,omitempty"`
	Stickied                *bool `json:"stickied,omitempty"`
	Distinguished           *bool `json:"distinguished,omitempty"`

	// Actor plumbing.
	PreferredUsername string     `json:"preferredUsername,omitempty"`
	Inbox             string     `json:"inbox,omitempty"`
	Outbox            string     `json:"outbox,omitempty"`
	Followers         string     `json:"followers,omitempty"`
	Following         string     `json:"following,omitempty"`
	Featured          string     `json:"featured,omitempty"`
	Moderators        string     `json:"moderators,omitempty"`
	Endpoints         *Endpoints `json:"endpoints,omitempty"`
	PublicKey         *PublicKey `json:"publicKey,omitempty"`

	// Tombstone.
	FormerType string `json:"formerType,omitempty"`
	Deleted    *Time  `json:"deleted,omitempty"`

	// Collections. First/Next may be a bare IRI or an inline page object.
	TotalItems   int     `json:"totalItems,omitempty"`
	First        *Object `json:"first,omitempty"`
	Last         *Object `json:"last,omitempty"`
	Next         *Object `json:"next,omitempty"`
	Prev         *Object `json:"prev,omitempty"`
	PartOf       string  `json:"partOf,omitempty"`
	Items        Refs    `json:"items,omitempty"`
	OrderedItems Refs    `json:"orderedItems,omitempty"`
}

// objectAlias strips Object's methods so (un)marshalling the plain struct
// does not recurse through the tolerant wrappers.
type objectAlias Object

// UnmarshalJSON accepts either a bare IRI string (decoding to an Object with
// only ID set — how references appear on the wire) or a JSON object. This is
// the core of tolerant string-or-object parsing: every *Object field gets it
// for free.
func (o *Object) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || string(data) == "null" {
		return nil
	}
	if data[0] == '"' {
		var iri string
		if err := json.Unmarshal(data, &iri); err != nil {
			return err
		}
		*o = Object{ID: iri}
		return nil
	}
	if data[0] == '[' {
		// Some servers emit a single-valued field (actor/object/target) as a
		// one-element array. The tolerant-parse contract says degrade, don't
		// fail: take the first non-null element rather than sinking the whole
		// object. Extra elements are dropped (these fields are logically
		// single-valued in the shapes we consume).
		var raw []json.RawMessage
		if err := json.Unmarshal(data, &raw); err != nil {
			return err
		}
		for _, item := range raw {
			trimmed := bytes.TrimSpace(item)
			if len(trimmed) == 0 || string(trimmed) == "null" {
				continue
			}
			return o.UnmarshalJSON(trimmed)
		}
		*o = Object{}
		return nil
	}
	var alias objectAlias
	if err := json.Unmarshal(data, &alias); err != nil {
		return err
	}
	*o = Object(alias)
	return nil
}

// MarshalJSON emits a bare IRI string when only the ID is set (the compact
// wire form of a reference), otherwise the full object.
func (o Object) MarshalJSON() ([]byte, error) {
	if o.ID != "" && o.isIDOnly() {
		return json.Marshal(o.ID)
	}
	return json.Marshal(objectAlias(o))
}

// isIDOnly reports whether every field except ID is zero. A DeepEqual against
// the zero Object short-circuits on the first non-zero field, unlike the old
// marshal-and-byte-compare which paid an O(n) JSON encode on every Object
// marshal (including large collection pages).
func (o Object) isIDOnly() bool {
	clone := o
	clone.ID = ""
	return reflect.DeepEqual(clone, Object{})
}

// IsActor reports whether the object's type is an AP actor type.
func (o *Object) IsActor() bool {
	switch o.Type {
	case TypeGroup, TypePerson, TypeApplication, TypeService:
		return true
	}
	return false
}

// IsCollection reports whether the object is a collection or collection page.
func (o *Object) IsCollection() bool {
	switch o.Type {
	case TypeCollection, TypeOrderedCollection, TypeCollectionPage, TypeOrderedCollectionPage:
		return true
	}
	return false
}

// IsTombstone reports whether the object is an AS2 Tombstone.
func (o *Object) IsTombstone() bool { return o.Type == TypeTombstone }

// IsPublic reports whether the object is addressed to the AS2 public
// collection in to, cc, or audience.
func (o *Object) IsPublic() bool {
	for _, list := range []Audience{o.To, o.Cc, o.Audience} {
		for _, iri := range list {
			// Both spellings appear in the wild ("Public", "as:Public",
			// full IRI); Lemmy always uses the full IRI.
			if iri == PublicAudience || iri == "as:Public" || iri == "Public" {
				return true
			}
		}
	}
	return false
}

// Host returns the hostname of the object's canonical id, or "" if the id
// is absent or unparseable.
func (o *Object) Host() string {
	if o.ID == "" {
		return ""
	}
	u, err := url.Parse(o.ID)
	if err != nil {
		return ""
	}
	return u.Hostname()
}

// URLString returns the object's primary url as a string (first Link href),
// or "" if none.
func (o *Object) URLString() string {
	for _, link := range o.URL {
		if link.Href != "" {
			return link.Href
		}
	}
	return ""
}

// Time is a tolerant AP timestamp. Lemmy emits RFC3339 with sub-second
// precision; other fediverse software occasionally emits variants. A value
// that fails every known layout parses to the zero Time instead of failing
// the whole object (tolerant parsing: a bad date must never be fatal).
//
// Valid distinguishes "present and parsed" from "present but unparseable /
// wrong JSON type": a bad `published` yields a non-nil *Time whose Valid is
// false, so downstream code (task 05 rkey/TID derivation) can tell it apart
// from a real timestamp with OK() rather than being fooled by a nil check or
// a fabricated year-0001 value.
type Time struct {
	time.Time
	// Valid is true only when the wire value parsed to a real timestamp.
	Valid bool
}

// OK reports whether the timestamp is present, parsed, and non-zero. Task 05
// calls it before deriving a TID from a published date.
func (t *Time) OK() bool {
	return t != nil && t.Valid && !t.IsZero()
}

// apTimeLayouts are tried in order when parsing AP timestamps.
var apTimeLayouts = []string{
	time.RFC3339Nano,             // Lemmy, Mastodon: 2026-07-07T03:27:37.028201Z
	time.RFC3339,                 //
	"2006-01-02T15:04:05.999999", // missing zone, seen from misbehaving servers
	"2006-01-02T15:04:05",        //
	time.RFC1123,                 // legacy OStatus-era software
	time.RFC1123Z,                //
}

// UnmarshalJSON parses the timestamp tolerantly; unparseable values yield
// the zero Time, never an error.
func (t *Time) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		// Not a JSON string (e.g. a number or object) — tolerate as invalid.
		t.Time = time.Time{}
		t.Valid = false
		return nil //nolint:nilerr // tolerance is deliberate here
	}
	for _, layout := range apTimeLayouts {
		if parsed, err := time.Parse(layout, s); err == nil {
			t.Time = parsed.UTC()
			t.Valid = true
			return nil
		}
	}
	// Present but unparseable: mark invalid so callers don't mistake the zero
	// time for a real (year-0001) timestamp.
	t.Time = time.Time{}
	t.Valid = false
	return nil
}

// MarshalJSON emits RFC3339 UTC, Lemmy style. An invalid or zero timestamp
// marshals to null rather than a fabricated year-0001 date, so a malformed
// inbound `published` never round-trips as a plausible-looking timestamp.
func (t Time) MarshalJSON() ([]byte, error) {
	if !t.Valid || t.IsZero() {
		return []byte("null"), nil
	}
	return json.Marshal(t.UTC().Format("2006-01-02T15:04:05.000000Z07:00"))
}

// NewTime wraps a time.Time as an AP timestamp.
func NewTime(t time.Time) *Time { return &Time{Time: t, Valid: true} }

// Refs is a list of objects/references that may appear on the wire as a
// single string, a single object, or an array of either.
type Refs []Object

// UnmarshalJSON accepts string | object | array of (string | object).
func (rs *Refs) UnmarshalJSON(data []byte) error {
	return unmarshalOneOrMany(data, (*[]Object)(rs))
}

// First returns the first ref, or nil if the list is empty.
func (rs Refs) First() *Object {
	if len(rs) == 0 {
		return nil
	}
	return &rs[0]
}

// FirstID returns the id of the first ref, or "".
func (rs Refs) FirstID() string {
	if first := rs.First(); first != nil {
		return first.ID
	}
	return ""
}

// Audience is a list of IRIs that may appear on the wire as a single string,
// a single object (id extracted), or an array of either.
type Audience []string

// UnmarshalJSON accepts string | object | array of (string | object) and
// flattens everything to IRI strings.
func (a *Audience) UnmarshalJSON(data []byte) error {
	var refs Refs
	if err := refs.UnmarshalJSON(data); err != nil {
		return err
	}
	out := make(Audience, 0, len(refs))
	for i := range refs {
		if refs[i].ID != "" {
			out = append(out, refs[i].ID)
		}
	}
	*a = out
	return nil
}

// Contains reports whether the audience includes the given IRI.
func (a Audience) Contains(iri string) bool {
	for _, v := range a {
		if v == iri {
			return true
		}
	}
	return false
}

// Link is an AS2 Link (or Hashtag/Mention tag, or attachment). Lemmy post
// attachments are Links with href+mediaType; tags are Hashtags/Mentions with
// href+name.
type Link struct {
	Type      string `json:"type,omitempty"`
	Href      string `json:"href,omitempty"`
	Name      string `json:"name,omitempty"`
	MediaType string `json:"mediaType,omitempty"`
	// Image attachments (PieFed) carry url instead of href.
	URL string `json:"url,omitempty"`
}

// UnmarshalJSON accepts a bare IRI string (→ Href) or a Link object.
func (l *Link) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || string(data) == "null" {
		return nil
	}
	if data[0] == '"' {
		var href string
		if err := json.Unmarshal(data, &href); err != nil {
			return err
		}
		*l = Link{Href: href}
		return nil
	}
	type plain Link
	var p plain
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	*l = Link(p)
	return nil
}

// MarshalJSON emits a bare string when only Href is set.
func (l Link) MarshalJSON() ([]byte, error) {
	if l.Type == "" && l.Name == "" && l.MediaType == "" && l.URL == "" && l.Href != "" {
		return json.Marshal(l.Href)
	}
	type plain Link
	return json.Marshal(plain(l))
}

// Links is a list of Link that may appear as string | object | array.
type Links []Link

// UnmarshalJSON accepts string | object | array of (string | object).
func (ls *Links) UnmarshalJSON(data []byte) error {
	return unmarshalOneOrMany(data, ls)
}

// Tags is a list of Link used for `tag` and `attachment`, tolerating
// single-value and array forms.
type Tags []Link

// UnmarshalJSON accepts string | object | array of (string | object).
func (ts *Tags) UnmarshalJSON(data []byte) error {
	return unmarshalOneOrMany(data, (*[]Link)(ts))
}

// unmarshalOneOrMany decodes JSON that may be a single value or an array of
// values into a slice whose element type has a tolerant UnmarshalJSON.
func unmarshalOneOrMany[S ~[]E, E any](data []byte, out *S) error {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || string(data) == "null" {
		return nil
	}
	if data[0] == '[' {
		var raw []json.RawMessage
		if err := json.Unmarshal(data, &raw); err != nil {
			return err
		}
		result := make(S, len(raw))
		for i, item := range raw {
			if err := json.Unmarshal(item, &result[i]); err != nil {
				return err
			}
		}
		*out = result
		return nil
	}
	var single E
	if err := json.Unmarshal(data, &single); err != nil {
		return err
	}
	*out = S{single}
	return nil
}

// Source carries the original markdown of an object (Lemmy always includes
// it alongside the rendered HTML content).
type Source struct {
	Content   string `json:"content,omitempty"`
	MediaType string `json:"mediaType,omitempty"`
}

// Language is Lemmy's language tag ({identifier, name}).
type Language struct {
	Identifier string `json:"identifier,omitempty"`
	Name       string `json:"name,omitempty"`
}

// Languages tolerates Lemmy's two wire forms: a single language object on
// posts/comments, an array of them on Group actors.
type Languages []Language

// UnmarshalJSON accepts object | array of objects.
func (ls *Languages) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || string(data) == "null" {
		return nil
	}
	if data[0] == '[' {
		type plain []Language
		var p plain
		if err := json.Unmarshal(data, &p); err != nil {
			return err
		}
		*ls = Languages(p)
		return nil
	}
	var single Language
	if err := json.Unmarshal(data, &single); err != nil {
		return err
	}
	*ls = Languages{single}
	return nil
}

// Endpoints is the actor endpoints object; Lemmy only uses sharedInbox.
type Endpoints struct {
	SharedInbox string `json:"sharedInbox,omitempty"`
}

// PublicKey is the actor's RSA public key as published for HTTP signatures.
// Lemmy requires RSA keys and keyId "{actorID}#main-key".
type PublicKey struct {
	ID           string `json:"id,omitempty"`
	Owner        string `json:"owner,omitempty"`
	PublicKeyPem string `json:"publicKeyPem,omitempty"`
}

// SharedInboxOrInbox returns the actor's shared inbox when present,
// otherwise its own inbox (Lemmy prefers sharedInbox for delivery).
func (o *Object) SharedInboxOrInbox() string {
	if o.Endpoints != nil && o.Endpoints.SharedInbox != "" {
		return o.Endpoints.SharedInbox
	}
	return o.Inbox
}

// ParseObject decodes an AP object from JSON, requiring that the payload is
// a JSON object (not a bare string/array).
func ParseObject(data []byte) (*Object, error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, fmt.Errorf("ap: payload is not a JSON object")
	}
	var obj Object
	if err := json.Unmarshal(trimmed, &obj); err != nil {
		return nil, fmt.Errorf("ap: parse object: %w", err)
	}
	return &obj, nil
}
