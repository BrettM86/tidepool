package personas

import (
	"fmt"
	"strings"

	"github.com/bluesky-social/indigo/atproto/syntax"

	"tidepool/internal/errors"
)

// MaxLocalPartLen caps a derived local part. Lemmy stores a remote actor's
// preferredUsername in a varchar(255); the derivation stays under that with
// room left for a collision suffix ("-2" ... "-99"), so a suffixed part can
// never overflow what the far side will accept.
const MaxLocalPartLen = 251

// DeriveLocalPart maps an atproto handle to the WebFinger local part (and
// preferredUsername) of the user-origin actor, FROZEN at actor creation.
//
// nativeSuffix is the user origin's host (e.g. "coves.social"), injected
// rather than read from config, so the derivation is a pure function and
// vanity origins can each derive their own space.
//
// The rules (task 13):
//   - exactly one label in front of nativeSuffix — the native handle space —
//     yields that label: "alice.coves.social" → "alice";
//   - anything else keeps its FULL handle, preserving provenance:
//     "bretton.dev" → "bretton.dev", "alice.blog.coves.social" →
//     "alice.blog.coves.social" (a deeper subdomain is not native);
//   - the result is lowercased and truncated to MaxLocalPartLen.
//
// Dots in the local part are deliberate: Lemmy 0.19.20 accepts them both in
// remote WebFinger resolution and in its mention regex.
//
// The native match is on a LABEL BOUNDARY ("."+nativeSuffix), never a bare
// suffix test. A bare strings.HasSuffix would read "evilcoves.social" as
// native and derive the local part "evil" — letting anyone who registers a
// domain ending in our host's name choose their name ON OUR ORIGIN, which is
// an impersonation primitive rather than a formatting bug. The apex itself is
// refused for the same reason: it is the instance actor's own name, and an
// actor holding it would collide with the origin's identity at WebFinger.
//
// A caller that derives a local part is deciding an actor's permanent name:
// the result is written once at creation and FROZEN there, so a later handle
// change refreshes the profile cache and nothing else. That is why this is a
// pure function of (handle, nativeSuffix) with no clock, config, or store
// reads — the same inputs must agree forever, including after the user
// renames and after the deployment's origin changes.
func DeriveLocalPart(handle, nativeSuffix string) (string, error) {
	// Without a suffix every handle would look native and surrender its
	// first label, so an unset origin is refused rather than defaulted.
	if nativeSuffix == "" {
		return "", errors.NewValidationError("native_suffix", "must not be empty")
	}
	// The suffix arrives from config and from a routed Host, which spell the
	// same authority several ways. It gets the SAME normalization as the
	// handle: an unnormalized suffix silently demotes native handles to
	// foreign ones, minting "alice.coves.social" instead of "alice" —
	// permanently, since the local part is frozen.
	suffix := strings.TrimSuffix(strings.ToLower(strings.TrimSpace(nativeSuffix)), ".")

	// Case is not identity: handles are compared and stored lowercased, and
	// normalizing before parsing keeps "Alice.Coves.Social" and its
	// lowercase twin from deriving two different actors.
	normalized := strings.ToLower(handle)

	// atproto handle syntax is the whole input gate: it rejects the empty
	// string, single labels, leading/trailing dots, underscores, non-ASCII,
	// and anything over 253 chars, so nothing downstream needs IDNA or
	// punycode machinery.
	if _, err := syntax.ParseHandle(normalized); err != nil {
		return "", errors.NewValidationError("handle", err.Error())
	}
	if normalized == suffix {
		return "", errors.NewValidationError("handle",
			fmt.Sprintf("%q is the origin apex, not a user handle", suffix))
	}

	local := normalized
	if prefix, ok := strings.CutSuffix(normalized, "."+suffix); ok && !strings.Contains(prefix, ".") {
		// Exactly one label in front of the suffix: the native space.
		// A deeper subdomain keeps its full handle — it is a different
		// namespace that merely lives under the same domain.
		local = prefix
	}
	if len(local) > MaxLocalPartLen {
		// The cut is blind, so it can land on a separator. A local part
		// ending in "." or "-" is not a name anything renders or matches
		// sanely: Lemmy's mention regex wants a trailing alphanumeric, and
		// a trailing dot reads as a hostname root.
		local = strings.TrimRight(local[:MaxLocalPartLen], ".-")
	}
	return local, nil
}
