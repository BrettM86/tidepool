package materialize

import (
	"fmt"
	"os"
	"testing"

	"github.com/gen2brain/avif"
)

// TestMain refuses to run without -tags nodynamic: on a host with a system
// libavif the decoder would run native code, not the WASM build these tests
// cover.
func TestMain(m *testing.M) {
	if avif.Dynamic() == nil {
		fmt.Fprintln(os.Stderr, "these tests must run with -tags nodynamic: a system libavif was loaded")
		os.Exit(1)
	}
	os.Exit(m.Run())
}
