package labels_test

import (
	"testing"

	"github.com/bluesky-social/indigo/atproto/labeling"
	"github.com/stretchr/testify/assert"

	"tidepool/internal/labels"
	"tidepool/internal/labels/labelstest"
)

func TestVerify_AcceptsOnlyTheConfiguredLabelerKeyAndSource(t *testing.T) {
	const labelerDID = "did:web:coves.social"
	const subject = "at://did:plc:ewvi7nxzyoun6zhxrhs64oiz/social.coves.community.post/3m2abcdefgh2a"
	configuredKey := labelstest.NewKey(t)
	otherKey := labelstest.NewKey(t)
	configuredPublic := labelstest.PublicKey(t, configuredKey)

	for _, tc := range []struct {
		name     string
		label    labeling.Label
		accepted bool
	}{
		{
			name:     "configured key and source",
			label:    labelstest.Sign(t, configuredKey, labeling.Label{SourceDID: labelerDID, URI: subject, Val: "!takedown"}),
			accepted: true,
		},
		{
			name:  "signed by another key",
			label: labelstest.Sign(t, otherKey, labeling.Label{SourceDID: labelerDID, URI: subject, Val: "!takedown"}),
		},
		{
			name:  "configured key but another source DID",
			label: labelstest.Sign(t, configuredKey, labeling.Label{SourceDID: "did:web:impostor.example", URI: subject, Val: "!takedown"}),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := labels.Verify(&tc.label, labelerDID, configuredPublic)
			if tc.accepted {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
