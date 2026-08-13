package outbound

import (
	"encoding/json"
	"fmt"
	"strings"

	"tidepool/internal/ap"
	"tidepool/internal/apobject"
	"tidepool/internal/consume"
	"tidepool/internal/errors"
)

// contextActivityStreams is the AS2 context every activity we emit carries at
// its top level. The Note/Page/vote shapes use only core AS2 terms
// (attributedTo, source, mediaType, audience, Like/Dislike/Undo), so the plain
// context is sufficient for Lemmy 0.19.20 to parse them.
const contextActivityStreams = apobject.ContextActivityStreams

// TranslatedActivity is a rendered AP activity ready to persist and deliver:
// its canonical wire form (Payload, byte-stable once stored) plus the coarse
// facts the queue indexes on.
type TranslatedActivity struct {
	// ActivityID is the deterministic id the intent carries (consume.ActivityID).
	ActivityID string
	// Kind is the AP activity type: Create, Update, Delete, Like, Dislike, Undo.
	Kind string
	// Payload is the canonical wire activity JSON.
	Payload []byte
	// ParentATURI is the causal dependency copied onto the activity row.
	ParentATURI string
}

// Translator renders a consume.Intent into an AP activity. It owns the wire
// format and every Lemmy quirk (verified against Lemmy 0.19.20): a Note's `to`
// MUST include as:Public with the community in cc, a Page carries the community
// in `to`, `attributedTo` is a SINGLE STRING, HTML `content` AND
// `source:{content,mediaType:text/markdown}` are both carried, `audience` is
// the community AP id, a Delete carries NO `summary`, and an Undo embeds the
// full inner vote object.
type Translator struct {
	userOrigin string
}

// NewTranslator builds a Translator that mints ids and self-references under
// userOrigin (AP_USER_ORIGIN).
func NewTranslator(userOrigin string) *Translator {
	return &Translator{userOrigin: userOrigin}
}

// Translate renders one intent into a canonical activity, addressed and signed
// as actorID (the persona's AP actor id, a single string). It hand-builds the
// wire JSON from the intent's already-resolved ids and its snapshot's raw
// record — no DB lookups and no round-trip through ap.Object, so the byte shape
// is exactly what Lemmy demanded.
func (t *Translator) Translate(actorID string, intent consume.Intent) (*TranslatedActivity, error) {
	if actorID == "" {
		return nil, errors.NewValidationError("actorID", "must not be empty")
	}
	switch typed := intent.(type) {
	case consume.CommentIntent:
		return t.comment(actorID, typed)
	case consume.PostIntent:
		return t.post(actorID, typed)
	case consume.VoteIntent:
		return t.vote(actorID, typed)
	default:
		return nil, errors.NewValidationError("intent", fmt.Sprintf("no translation for %T", intent))
	}
}

// comment renders a native comment as Create/Update{Note} (create/update) or a
// bare Delete (delete).
func (t *Translator) comment(actorID string, intent consume.CommentIntent) (*TranslatedActivity, error) {
	if intent.CommunityAPID == "" {
		return nil, errors.NewValidationError("communityApId", "must not be empty")
	}
	snap, err := apobject.ParseSnapshot(intent.Snapshot)
	if err != nil {
		return nil, err
	}
	atURI, _ := snap["atUri"].(string)
	if atURI == "" {
		return nil, errors.NewValidationError("snapshot.atUri", "must not be empty")
	}
	objectURL := t.objectURL(atURI)
	parentATURI, _ := snap["parentAtUri"].(string)

	if intent.Op == "delete" {
		return t.finish(intent.ID, "Delete", parentATURI,
			t.deleteActivity(intent.ID, actorID, intent.CommunityAPID, objectURL))
	}

	record, _ := snap["record"].(map[string]any)
	note := apobject.BuildNote(actorID, intent.CommunityAPID, intent.ParentAPID, objectURL, record)
	kind := "Create"
	if intent.Op == "update" {
		kind = "Update"
	}
	return t.finish(intent.ID, kind, parentATURI,
		t.wrapActivity(kind, intent.ID, actorID, intent.CommunityAPID, note))
}

// post renders a native post as Create/Update{Page} or a bare Delete.
func (t *Translator) post(actorID string, intent consume.PostIntent) (*TranslatedActivity, error) {
	if intent.CommunityAPID == "" {
		return nil, errors.NewValidationError("communityApId", "must not be empty")
	}
	snap, err := apobject.ParseSnapshot(intent.Snapshot)
	if err != nil {
		return nil, err
	}
	atURI, _ := snap["atUri"].(string)
	if atURI == "" {
		return nil, errors.NewValidationError("snapshot.atUri", "must not be empty")
	}
	objectURL := t.objectURL(atURI)

	if intent.Op == "delete" {
		// A post has no causal parent, so the activity row carries none.
		return t.finish(intent.ID, "Delete", "",
			t.deleteActivity(intent.ID, actorID, intent.CommunityAPID, objectURL))
	}

	record, _ := snap["record"].(map[string]any)
	page, err := apobject.BuildPage(actorID, intent.CommunityAPID, objectURL, record)
	if err != nil {
		return nil, err
	}
	kind := "Create"
	if intent.Op == "update" {
		kind = "Update"
	}
	return t.finish(intent.ID, kind, "",
		t.wrapActivity(kind, intent.ID, actorID, intent.CommunityAPID, page))
}

// vote renders a Like/Dislike (create) or the Undo of one (undo). An Undo
// embeds the FULL inner vote object reconstructed from state — a bare-URL inner
// object fails Lemmy's parse, so the inner {type, id, actor, object} is spelled
// out.
func (t *Translator) vote(actorID string, intent consume.VoteIntent) (*TranslatedActivity, error) {
	if intent.CommunityAPID == "" {
		return nil, errors.NewValidationError("communityApId", "must not be empty")
	}
	if intent.SubjectAPID == "" {
		return nil, errors.NewValidationError("subjectApId", "must not be empty")
	}
	voteType, err := voteActivityType(intent.Direction)
	if err != nil {
		return nil, err
	}
	community := intent.CommunityAPID

	if intent.Op == "undo" {
		if intent.InnerActivityID == "" {
			return nil, errors.NewValidationError("innerActivityId", "an Undo must name the activity it withdraws")
		}
		activity := map[string]any{
			"@context": contextActivityStreams,
			"id":       intent.ID,
			"type":     "Undo",
			"actor":    actorID,
			"to":       []string{ap.PublicAudience},
			"cc":       []string{community},
			"audience": community,
			"object": map[string]any{
				"type":     voteType,
				"id":       intent.InnerActivityID,
				"actor":    actorID,
				"object":   intent.SubjectAPID,
				"audience": community,
			},
		}
		return t.finish(intent.ID, "Undo", "", activity)
	}

	activity := map[string]any{
		"@context": contextActivityStreams,
		"id":       intent.ID,
		"type":     voteType,
		"actor":    actorID,
		"object":   intent.SubjectAPID,
		"to":       []string{ap.PublicAudience},
		"cc":       []string{community},
		"audience": community,
	}
	return t.finish(intent.ID, voteType, "", activity)
}

// wrapActivity wraps a rendered object (Note/Page) in the outer Create/Update
// activity, addressed to Public with the community cc'd and set as audience.
func (t *Translator) wrapActivity(kind, id, actorID, communityAPID string, object map[string]any) map[string]any {
	return map[string]any{
		"@context": contextActivityStreams,
		"id":       id,
		"type":     kind,
		"actor":    actorID,
		"to":       []string{ap.PublicAudience},
		"cc":       []string{communityAPID},
		"audience": communityAPID,
		"object":   object,
	}
}

// deleteActivity is a self-delete: a Delete of the bare object URL and NOTHING
// else. Lemmy reads a `summary` on a Delete as a MOD-REMOVAL reason, so a
// self-delete must omit it or it looks like moderation.
func (t *Translator) deleteActivity(id, actorID, communityAPID, objectURL string) map[string]any {
	return map[string]any{
		"@context": contextActivityStreams,
		"id":       id,
		"type":     "Delete",
		"actor":    actorID,
		"to":       []string{ap.PublicAudience},
		"cc":       []string{communityAPID},
		"audience": communityAPID,
		"object":   objectURL,
	}
}

// finish marshals the built activity into the canonical payload.
func (t *Translator) finish(activityID, kind, parentATURI string, activity map[string]any) (*TranslatedActivity, error) {
	payload, err := json.Marshal(activity)
	if err != nil {
		return nil, fmt.Errorf("translate %s: encode activity: %w", activityID, err)
	}
	return &TranslatedActivity{
		ActivityID:  activityID,
		Kind:        kind,
		Payload:     payload,
		ParentATURI: parentATURI,
	}, nil
}

// objectURL is the AP id a native record federates as: the served
// /ap/object/{did}/{collection}/{rkey} URL, which is exactly the at-uri's three
// parts hung under the origin.
func (t *Translator) objectURL(atURI string) string {
	return t.userOrigin + "/ap/object/" + strings.TrimPrefix(atURI, "at://")
}

// voteActivityType maps a vote direction to its AP activity type.
func voteActivityType(direction string) (string, error) {
	switch direction {
	case "up":
		return "Like", nil
	case "down":
		return "Dislike", nil
	default:
		return "", errors.NewValidationError("direction", "must be up or down, got "+direction)
	}
}
