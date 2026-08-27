package materialize

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"tidepool/internal/ap"
)

// TestOriginHost: community.profile.origin is the bare lowercase hostname
// of the Group id, or omitted when the id carries something the appview
// would refuse — a port names a different instance than the bare host, and
// IP literals, trailing dots, over-long names and unparseable ids have no
// hostname to assert.
func TestOriginHost(t *testing.T) {
	cases := map[string]string{
		"https://lemmy.world/c/technology":                 "lemmy.world",
		"https://LEMMY.World/c/technology":                 "lemmy.world",
		"https://lemmy.world./c/technology":                "lemmy.world",
		"http://lemmy/c/technology":                        "lemmy",
		"https://lemmy.example:8536/c/tech":                "",
		"https://[::1]:8536/c/tech":                        "",
		"https://[::1]/c/tech":                             "",
		"http://127.0.0.1/c/tech":                          "",
		"":                                                 "",
		"lemmy.world/c/technology":                         "",
		"https://" + strings.Repeat("a.", 130) + "com/c/x": "",
	}
	for id, want := range cases {
		assert.Equal(t, want, originHost(&ap.Object{ID: id}), "id %q", id)
	}
}
