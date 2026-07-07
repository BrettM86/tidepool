// Package lexicons embeds the vendored Coves lexicon JSON files
// (social.coves.* plus the com.atproto refs they use) and exposes them as
// an indigo lexicon catalog.
//
// The files are synced from the Coves repository by scripts/sync-lexicons.sh.
// Validation uses indigo's atproto/lexicon package — the exact validator the
// Coves validate-lexicon tool runs — because it understands the lexicon
// format natively (refs, unions, blobs, string formats, maxGraphemes).
// The task sheet suggested xeipuuv/gojsonschema "like Coves does", but Coves
// only uses gojsonschema for aggregator config blobs; its record validation
// is indigo's lexicon package, so that is what Tidepool replicates.
package lexicons

import (
	"embed"
	"fmt"
	"sync"

	"github.com/bluesky-social/indigo/atproto/lexicon"
)

//go:embed social com
var files embed.FS

var (
	once    sync.Once
	catalog lexicon.BaseCatalog
	loadErr error
)

// Catalog returns the shared lexicon catalog built from the embedded files.
// The catalog is immutable after load and safe for concurrent use.
func Catalog() (*lexicon.BaseCatalog, error) {
	once.Do(func() {
		catalog = lexicon.NewBaseCatalog()
		loadErr = catalog.LoadEmbedFS(files)
	})
	if loadErr != nil {
		return nil, fmt.Errorf("lexicons: load embedded catalog: %w", loadErr)
	}
	return &catalog, nil
}
