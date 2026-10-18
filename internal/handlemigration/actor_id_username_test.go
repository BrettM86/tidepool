package handlemigration

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUsernameFromActorID(t *testing.T) {
	for _, tc := range []struct {
		name      string
		actorID   string
		oldHandle string
		want      string
		ok        bool
	}{
		{name: "lemmy user", actorID: "https://lemmy.world/u/Lost_My_Mind",
			oldHandle: "lost-my-mind.lemmy-world.tdpl.io", want: "Lost_My_Mind", ok: true},
		{name: "lemmy community", actorID: "https://lemmy.world/c/books",
			oldHandle: "books.lemmy-world.tdpl.io", want: "books", ok: true},
		{name: "kbin magazine", actorID: "https://kbin.example/m/books",
			oldHandle: "books.kbin-example.tdpl.io", want: "books", ok: true},
		{name: "users path with collision suffix", actorID: "https://mastodon.example/users/alice",
			oldHandle: "alice-2.mastodon-example.tdpl.io", want: "alice", ok: true},
		{name: "exact match keeps digit suffix in the name", actorID: "https://x.example/u/alice-2",
			oldHandle: "alice-2.x-example.tdpl.io", want: "alice-2", ok: true},
		{name: "non-digit suffix is not a collision suffix", actorID: "https://x.example/u/alice",
			oldHandle: "alice-two.x-example.tdpl.io"},
		{name: "percent-decoded name", actorID: "https://x.example/u/caf%C3%A9_fan",
			oldHandle: "caf-fan.x-example.tdpl.io", want: "café_fan", ok: true},
		{name: "plus is not a space", actorID: "https://x.example/u/a+b",
			oldHandle: "a-b.x-example.tdpl.io", want: "a+b", ok: true},
		{name: "long name truncated to 63",
			actorID:   "https://x.example/u/abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij",
			oldHandle: "abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabc.x-example.tdpl.io",
			want:      "abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij", ok: true},
		{name: "name normalizes to empty", actorID: "https://x.example/u/%E6%97%A5%E6%9C%AC%E8%AA%9E",
			oldHandle: "u0123456789.x-example.tdpl.io"},
		{name: "different name", actorID: "https://x.example/u/alice", oldHandle: "bob.x-example.tdpl.io"},
		{name: "unknown path prefix", actorID: "https://x.example/profile/alice", oldHandle: "alice.x-example.tdpl.io"},
		{name: "nested path", actorID: "https://x.example/ap/users/123", oldHandle: "123.x-example.tdpl.io"},
		{name: "two segments", actorID: "https://x.example/u/a/b", oldHandle: "a.x-example.tdpl.io"},
		{name: "query", actorID: "https://x.example/u/alice?x=1", oldHandle: "alice.x-example.tdpl.io"},
		{name: "fragment", actorID: "https://x.example/u/alice#f", oldHandle: "alice.x-example.tdpl.io"},
		{name: "empty segment", actorID: "https://x.example/u/", oldHandle: "alice.x-example.tdpl.io"},
		{name: "space segment", actorID: "https://x.example/u/%20", oldHandle: "alice.x-example.tdpl.io"},
		{name: "trailing slash", actorID: "https://x.example/u/alice/", oldHandle: "alice.x-example.tdpl.io"},
		{name: "not https", actorID: "http://x.example/u/alice", oldHandle: "alice.x-example.tdpl.io"},
		{name: "empty old handle", actorID: "https://x.example/u/alice", oldHandle: ""},
		{name: "decoded NUL", actorID: "https://x.example/u/alice%00", oldHandle: "alice.x-example.tdpl.io"},
		{name: "decoded name is not valid UTF-8", actorID: "https://x.example/u/alice%FF",
			oldHandle: "alice.x-example.tdpl.io"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := usernameFromActorID(tc.actorID, tc.oldHandle)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.want, got)
		})
	}
}
