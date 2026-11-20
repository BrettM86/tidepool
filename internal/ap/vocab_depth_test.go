package ap

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// nestedObjects wraps leaf (a JSON value) in depth levels of
// {"object":...}; the outermost object is depth 1, and a scalar leaf adds
// no depth.
func nestedObjects(depth int, leaf string) string {
	return strings.Repeat(`{"object":`, depth) + leaf + strings.Repeat(`}`, depth)
}

// TestParseObject_NestingDepthCap: the depth cap is 32, counted over both
// objects and arrays, and only outside JSON strings.
func TestParseObject_NestingDepthCap(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		wantErr bool
	}{
		{name: "objects exactly at depth 32 parse", payload: nestedObjects(32, `"https://x.example/a"`)},
		{name: "objects at depth 33 are rejected", payload: nestedObjects(33, `"https://x.example/a"`), wantErr: true},
		{
			name:    "arrays count toward depth",
			payload: `{"type":"Note","extension":` + strings.Repeat(`[`, 32) + strings.Repeat(`]`, 32) + `}`,
			wantErr: true,
		},
		{
			name: "closing braces and brackets lower the depth",
			payload: `{"type":"Note","tag":[` +
				strings.Repeat(`{"type":"Hashtag","name":"x"},`, 39) + `{"type":"Hashtag","name":"x"}` +
				`],"to":[` + strings.Repeat(`[],`, 39) + `[]]}`,
		},
		{
			name:    "braces and brackets inside a string do not count",
			payload: `{"type":"Note","name":"` + strings.Repeat(`{[`, 50) + `"}`,
		},
		{
			name:    "an escaped backslash ends the string before the nest",
			payload: `{"name":"\\","object":` + nestedObjects(32, `"https://x.example/a"`) + `}`,
			wantErr: true,
		},
		{
			name:    "an escaped quote keeps the braces inside the string",
			payload: `{"type":"Note","name":"a\"` + strings.Repeat(`{`, 40) + `"}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseObject([]byte(tc.payload))
			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "nesting depth")
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestParseObject_DeepNestingRejectedFast: a ~1 MiB body nested 9,990
// levels deep (just under encoding/json's own 10,000 limit) is rejected
// by a linear pre-scan, not parsed at quadratic cost.
func TestParseObject_DeepNestingRejectedFast(t *testing.T) {
	const depth = 9990
	wrapperBytes := depth * len(`{"object":}`)
	payload := []byte(nestedObjects(depth, `"`+strings.Repeat("a", (1<<20)-wrapperBytes-2)+`"`))
	require.Equal(t, 1<<20, len(payload))

	start := time.Now()
	_, err := ParseObject(payload)
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "nesting depth")
	assert.Less(t, elapsed, 100*time.Millisecond, "the depth check must be linear in body size")
}

// TestFetchObject_RejectsDeeplyNestedDocument: a fetched remote document
// goes through the same depth cap.
func TestFetchObject_RejectsDeeplyNestedDocument(t *testing.T) {
	body := []byte(nestedObjects(9990, `"https://x.example/a"`))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", ContentTypeActivityJSON)
		_, _ = w.Write(body)
	}))
	defer server.Close()

	client := newTestClient(t, ClientOptions{})
	_, err := client.FetchObject(context.Background(), server.URL+"/post/1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nesting depth")
}
