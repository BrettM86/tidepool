package ingest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseFollowList_Instances(t *testing.T) {
	for _, tc := range []struct {
		name            string
		content         string
		wantCommunities []string
		want            []InstanceEntry
	}{
		{
			name:    "host alone",
			content: "communities: []\ninstances:\n  - host: lemmy.world\n",
			want:    []InstanceEntry{{Host: "lemmy.world"}},
		},
		{
			name:    "dotless host",
			content: "communities: []\ninstances:\n  - host: lemmy\n",
			want:    []InstanceEntry{{Host: "lemmy"}},
		},
		{
			name: "exclude in handle and URL form",
			content: `
communities: []
instances:
  - host: lemmy.world
    exclude:
      - "!technology@lemmy.world"
      - "https://lemmy.world/c/gaming"
`,
			want: []InstanceEntry{{
				Host:    "lemmy.world",
				Exclude: []string{"!technology@lemmy.world", "https://lemmy.world/c/gaming"},
			}},
		},
		{
			name: "exclude belongs to the host whatever its case and port",
			content: `
communities: []
instances:
  - host: lemmy.world
    exclude:
      - "!Technology@LEMMY.World"
      - "https://Lemmy.World/c/Gaming"
      - "http://lemmy.world:8536/c/local"
`,
			want: []InstanceEntry{{
				Host: "lemmy.world",
				Exclude: []string{
					"!Technology@LEMMY.World", "https://Lemmy.World/c/Gaming", "http://lemmy.world:8536/c/local",
				},
			}},
		},
		{
			name: "several hosts next to communities",
			content: `
communities:
  - "!linux@lemmy.ml"
instances:
  - host: lemmy.world
  - host: lemmy
    exclude:
      - "!some_name@lemmy"
`,
			wantCommunities: []string{"!linux@lemmy.ml"},
			want: []InstanceEntry{
				{Host: "lemmy.world"},
				{Host: "lemmy", Exclude: []string{"!some_name@lemmy"}},
			},
		},
		{
			name:    "empty exclude",
			content: "communities: []\ninstances:\n  - host: lemmy.world\n    exclude: []\n",
			want:    []InstanceEntry{{Host: "lemmy.world"}},
		},
		{
			name:    "empty list",
			content: "communities: []\ninstances: []\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			list, err := ParseFollowList(writeFollowList(t, tc.content))
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.wantCommunities, list.Communities)
			require.Len(t, list.Instances, len(tc.want))
			for index, want := range tc.want {
				assert.Equal(t, want.Host, list.Instances[index].Host)
				assert.ElementsMatch(t, want.Exclude, list.Instances[index].Exclude)
			}
		})
	}
}

// TestParseFollowList_RejectedInstances: each form stops the parse with an
// error naming the entry, so a bad entry never reaches a listing or a mint.
func TestParseFollowList_RejectedInstances(t *testing.T) {
	host := func(value string) string {
		return "communities: []\ninstances:\n  - host: \"" + value + "\"\n"
	}
	exclude := func(value string) string {
		return "communities: []\ninstances:\n  - host: lemmy.world\n    exclude:\n      - \"" + value + "\"\n"
	}
	for _, tc := range []struct {
		name    string
		content string
		want    string
	}{
		{"uppercase host", host("Lemmy.World"), "Lemmy.World"},
		{"host with a scheme", host("https://lemmy.world"), "https://lemmy.world"},
		{"host with a port", host("lemmy.world:8536"), "lemmy.world:8536"},
		{"host with a path", host("lemmy.world/c/tech"), "lemmy.world/c/tech"},
		{"host with a trailing dot", host("lemmy.world."), "lemmy.world."},
		{"empty host", host(""), "instances entry 1"},
		{
			"empty host after a valid one",
			"communities: []\ninstances:\n  - host: lemmy.world\n  - host: \"\"\n",
			"instances entry 2",
		},
		{"entry with no host", "communities: []\ninstances:\n  - exclude: []\n", "instances entry 1"},
		{
			"duplicate host",
			"communities: []\ninstances:\n  - host: lemmy.world\n  - host: lemmy.ml\n  - host: lemmy.world\n",
			"lemmy.world",
		},
		{"exclude handle without its bang", exclude("technology@lemmy.world"), "technology@lemmy.world"},
		{"exclude handle with two ats", exclude("!a@b@lemmy.world"), "!a@b@lemmy.world"},
		{"exclude with whitespace", exclude("!tech @lemmy.world"), "!tech @lemmy.world"},
		{"exclude that is empty", exclude(""), "instances entry 1"},
		{"exclude handle on another host", exclude("!technology@lemmy.ml"), "!technology@lemmy.ml"},
		{"exclude URL on another host", exclude("https://lemmy.ml/c/technology"), "https://lemmy.ml/c/technology"},
		{"exclude handle on a host with the entry as a suffix", exclude("!tech@not-lemmy.world"), "!tech@not-lemmy.world"},
		// A truncated file must not read as "unfollow every instance community".
		{"null key", "communities: []\ninstances:\n", "null"},
		{"null item", "communities: []\ninstances:\n  -\n", "instances entry 1"},
		{"tilde item", "communities: []\ninstances:\n  - ~\n", "instances entry 1"},
		{"null after a valid item", "communities: []\ninstances:\n  - host: lemmy.world\n  -\n", "instances entry 2"},
		{
			"null exclude item",
			"communities: []\ninstances:\n  - host: lemmy.world\n    exclude:\n      -\n",
			"instances entry 1",
		},
		// A null exclude must not read as "nothing is excluded": each mint is permanent.
		{
			"null exclude",
			"communities: []\ninstances:\n  - host: lemmy.world\n    exclude:\n",
			"instances entry 1: exclude is null",
		},
		{
			"tilde exclude",
			"communities: []\ninstances:\n  - host: lemmy.world\n    exclude: ~\n",
			"instances entry 1: exclude is null",
		},
		{
			"null exclude after a valid entry",
			"communities: []\ninstances:\n  - host: lemmy.ml\n  - host: lemmy.world\n    exclude:\n",
			"instances entry 2: exclude is null",
		},
		{
			"exclude that is a string",
			"communities: []\ninstances:\n  - host: lemmy.world\n    exclude: \"!tech@lemmy.world\"\n",
			"instances entry 1",
		},
		{
			"exclude that is a mapping",
			"communities: []\ninstances:\n  - host: lemmy.world\n    exclude:\n      name: tech\n",
			"instances entry 1",
		},
		{
			"unknown key in an entry",
			"communities: []\ninstances:\n  - host: lemmy.world\n    excludes: []\n",
			"excludes",
		},
		// A string is not an entry. yaml cuts the value in its message, so the line
		// is what locates it.
		{"string item", "communities: []\ninstances:\n  - \"lemmy.world\"\n", "line 3"},
		{
			"instances does not stand in for the communities key",
			"instances:\n  - host: lemmy.world\n",
			"communities",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := writeFollowList(t, tc.content)
			list, err := ParseFollowList(path)
			assertFollowListRejected(t, path, list, err, tc.want)
		})
	}
}

// TestFollowListDesires: the offline check. A row's stored name is what the
// communities table can really hold: a handle label such as c-technology, or the
// raw name from an embedded Group document. An empty name is a listing result,
// which has no row yet.
func TestFollowListDesires(t *testing.T) {
	const (
		technologyID = "https://lemmy.world/c/technology"
		world        = "lemmy.world"
	)
	listed := func(exclude ...string) []InstanceEntry {
		return []InstanceEntry{{Host: world, Exclude: exclude}}
	}
	for _, tc := range []struct {
		name              string
		communities       []string
		instances         []InstanceEntry
		groupID           string
		instance          string
		preferredUsername string
		want              bool
	}{
		// Explicit handle entries compare the id's last path segment.
		{
			name:        "handle, stored as the handle label",
			communities: []string{"!technology@lemmy.world"},
			groupID:     technologyID, instance: world, preferredUsername: "c-technology", want: true,
		},
		{
			name:        "handle, stored as the raw name",
			communities: []string{"!technology@lemmy.world"},
			groupID:     technologyID, instance: world, preferredUsername: "technology", want: true,
		},
		{
			name:        "handle with an underscore, stored with a dash",
			communities: []string{"!some_community@lemmy.world"},
			groupID:     "https://lemmy.world/c/some_community", instance: world, preferredUsername: "c-some-community", want: true,
		},
		{
			name:        "handle in another case",
			communities: []string{"!TECHNOLOGY@Lemmy.World"},
			groupID:     "https://lemmy.world/c/Technology", instance: world, preferredUsername: "c-technology", want: true,
		},
		{
			name:        "handle is not matched by a stored name when the id has a segment",
			communities: []string{"!technology@lemmy.world"},
			groupID:     "https://lemmy.world/c/gaming", instance: world, preferredUsername: "technology", want: false,
		},
		{
			name:        "handle does not reach another instance's community of the same name",
			communities: []string{"!technology@lemmy.world"},
			groupID:     "https://lemmy.ml/c/technology", instance: "lemmy.ml", preferredUsername: "technology", want: false,
		},
		{
			name:        "id with no path falls back to the stored name",
			communities: []string{"!solo@solo.example"},
			groupID:     "https://solo.example", instance: "solo.example", preferredUsername: "solo", want: true,
		},
		{
			name:        "id with a bare slash falls back to the stored name",
			communities: []string{"!solo@solo.example"},
			groupID:     "https://solo.example/", instance: "solo.example", preferredUsername: "Solo", want: true,
		},
		{
			name:        "id with no path and another stored name",
			communities: []string{"!solo@solo.example"},
			groupID:     "https://solo.example", instance: "solo.example", preferredUsername: "c-solo", want: false,
		},

		// Explicit URL entries compare the id.
		{
			name:        "URL",
			communities: []string{technologyID},
			groupID:     technologyID, instance: world, preferredUsername: "c-technology", want: true,
		},
		{
			name:        "URL in another case",
			communities: []string{"https://Lemmy.World/c/TECHNOLOGY"},
			groupID:     technologyID, instance: world, preferredUsername: "c-technology", want: true,
		},
		{
			name:        "URL of another community",
			communities: []string{"https://lemmy.world/c/gaming"},
			groupID:     technologyID, instance: world, preferredUsername: "c-technology", want: false,
		},

		// Listed hosts.
		{
			name:      "community on a listed host",
			instances: listed(),
			groupID:   technologyID, instance: world, preferredUsername: "c-technology", want: true,
		},
		{
			name:      "instance stored in another case",
			instances: listed(),
			groupID:   technologyID, instance: "Lemmy.World", preferredUsername: "c-technology", want: true,
		},
		{
			name:      "community on a host that is not listed",
			instances: listed(),
			groupID:   "https://lemmy.ml/c/technology", instance: "lemmy.ml", preferredUsername: "c-technology", want: false,
		},
		{
			name:      "listing result on a listed host",
			instances: listed(),
			groupID:   technologyID, instance: world, want: true,
		},

		// Exclude entries, in both forms.
		{
			name:      "exclude by URL",
			instances: listed(technologyID),
			groupID:   technologyID, instance: world, preferredUsername: "c-technology", want: false,
		},
		{
			name:      "exclude by URL in another case",
			instances: listed("https://LEMMY.world/c/Technology"),
			groupID:   technologyID, instance: world, preferredUsername: "c-technology", want: false,
		},
		{
			name:      "exclude by handle, stored as the handle label",
			instances: listed("!technology@lemmy.world"),
			groupID:   technologyID, instance: world, preferredUsername: "c-technology", want: false,
		},
		{
			name:      "exclude by handle in another case",
			instances: listed("!Technology@LEMMY.world"),
			groupID:   technologyID, instance: world, preferredUsername: "technology", want: false,
		},
		{
			name:      "exclude by handle with an underscore",
			instances: listed("!some_community@lemmy.world"),
			groupID:   "https://lemmy.world/c/some_community", instance: world, preferredUsername: "c-some-community", want: false,
		},
		{
			name:      "exclude by handle stops a listing result",
			instances: listed("!technology@lemmy.world"),
			groupID:   technologyID, instance: world, want: false,
		},
		{
			name:      "exclude by URL stops a listing result",
			instances: listed(technologyID),
			groupID:   technologyID, instance: world, want: false,
		},
		{
			name:      "exclude of another community leaves this one",
			instances: listed("!gaming@lemmy.world", "https://lemmy.world/c/linux"),
			groupID:   technologyID, instance: world, preferredUsername: "c-technology", want: true,
		},
		{
			name:      "exclude by handle with an id that has no path",
			instances: []InstanceEntry{{Host: "solo.example", Exclude: []string{"!solo@solo.example"}}},
			groupID:   "https://solo.example", instance: "solo.example", preferredUsername: "solo", want: false,
		},

		// An explicit entry wins over an exclude.
		{
			name:        "in communities by URL and in exclude by URL",
			communities: []string{technologyID},
			instances:   listed(technologyID),
			groupID:     technologyID, instance: world, preferredUsername: "c-technology", want: true,
		},
		{
			name:        "in communities by handle and in exclude by handle",
			communities: []string{"!technology@lemmy.world"},
			instances:   listed("!technology@lemmy.world"),
			groupID:     technologyID, instance: world, preferredUsername: "c-technology", want: true,
		},
		{
			name:        "in communities by handle and in exclude by URL",
			communities: []string{"!technology@lemmy.world"},
			instances:   listed(technologyID),
			groupID:     technologyID, instance: world, preferredUsername: "c-technology", want: true,
		},

		// A second listed host does not reach across hosts.
		{
			name: "exclude applies only to its own host",
			instances: []InstanceEntry{
				{Host: world, Exclude: []string{"!technology@lemmy.world"}},
				{Host: "lemmy.ml"},
			},
			groupID: "https://lemmy.ml/c/technology", instance: "lemmy.ml", preferredUsername: "c-technology", want: true,
		},

		{
			name:    "nothing listed",
			groupID: technologyID, instance: world, preferredUsername: "c-technology", want: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			list := &FollowList{Communities: tc.communities, Instances: tc.instances}
			assert.Equal(t, tc.want, list.Desires(tc.groupID, tc.instance, tc.preferredUsername))
		})
	}
}
