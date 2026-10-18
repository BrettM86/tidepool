package handlemigration

import (
	"net/url"
	"strings"
	"unicode/utf8"

	"tidepool/internal/identity"
)

// usernameFromActorID reads the username from an actor URL of the form
// https://host/{u,c,m,users}/<name>. It accepts the name only when the
// minter's label rule maps it to the old handle's first label, or to that
// label without one "-<digits>" collision suffix, so the URL can only confirm
// the username the old handle was minted from. A name containing NUL or not
// valid UTF-8 is refused. The check also refuses, as safe false negatives, a
// name whose old label was truncated before a collision suffix, and one that
// differs from the minted name only by Unicode normalization.
func usernameFromActorID(apActorID, oldHandle string) (string, bool) {
	actorURL, err := url.Parse(apActorID)
	if err != nil || actorURL.Scheme != "https" || actorURL.Host == "" || actorURL.Opaque != "" ||
		actorURL.RawQuery != "" || actorURL.ForceQuery || actorURL.Fragment != "" {
		return "", false
	}
	// Segments are counted on the escaped path, so an encoded "/" stays
	// inside the name.
	segments := strings.Split(actorURL.EscapedPath(), "/")
	if len(segments) != 3 || segments[0] != "" || segments[2] == "" {
		return "", false
	}
	switch segments[1] {
	case "u", "c", "m", "users":
	default:
		return "", false
	}
	name := strings.TrimPrefix(actorURL.Path, "/"+segments[1]+"/")
	if strings.ContainsRune(name, 0) || !utf8.ValidString(name) {
		return "", false
	}
	label := identity.NormalizeLabel(name)
	if label == "" {
		return "", false
	}
	oldLabel, _, _ := strings.Cut(strings.ToLower(oldHandle), ".")
	if label == oldLabel {
		return name, true
	}
	if index := strings.LastIndexByte(oldLabel, '-'); index >= 0 {
		suffix := oldLabel[index+1:]
		if suffix != "" && strings.Trim(suffix, "0123456789") == "" && label == oldLabel[:index] {
			return name, true
		}
	}
	return "", false
}
