package apobject

import (
	"encoding/json"
	"html"
	"math"
	"sort"
	"strings"
	"unicode/utf8"
)

const imageFacetType = "social.coves.richtext.facet#image"

// imagePlacement is one #image facet Coves' FilterImageFacets keeps: its byte
// range, widened to whole runes, and the gallery entry it names.
type imagePlacement struct {
	start    int
	end      int
	position int // the facet's position in the facets array
	image    GalleryImage
}

// decodeImageRanges returns the inline image ranges the record's #image facets
// render and the gallery indexes those kept facets place. It keeps exactly the placements Coves' FilterImageFacets keeps,
// evaluating facets in facets-array order (Coves richtext imageFacetRules.check):
//   - #image is the facet's only feature;
//   - the range is well formed, non-empty and ends within the content;
//   - the range does not lie wholly inside a #code or #codeBlock range;
//   - index is an integer naming an entry of the gallery;
//   - no earlier facet placed the same index.
//
// A failed facet does not mark its index placed. A kept placement whose gallery
// entry has no URL renders its covered text. Kept placements whose spans
// intersect merge into one range that renders their images in facets-array
// order.
func decodeImageRanges(source string, rawFacets any, gallery []GalleryImage) ([]inlineRange, map[int]bool) {
	facets, ok := rawFacets.([]any)
	if !ok || len(facets) == 0 {
		return nil, nil
	}

	var codeRanges [][2]int
	for _, raw := range facets {
		facet, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		features, _ := facet["features"].([]any)
		if !hasFeatureType(features, inlineCodeFacetType) && !hasFeatureType(features, codeBlockFacetType) {
			continue
		}
		if start, end, ok := coversFacetRange(facet); ok {
			codeRanges = append(codeRanges, [2]int{start, end})
		}
	}

	placed := make(map[int]bool)
	var placements []imagePlacement
	for position, raw := range facets {
		facet, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		features, _ := facet["features"].([]any)
		if !hasFeatureType(features, imageFacetType) || len(features) != 1 {
			continue
		}
		start, end, ok := coversFacetRange(facet)
		if !ok || end > len(source) || insideAnyRange(start, end, codeRanges) {
			continue
		}
		feature, _ := features[0].(map[string]any)
		index, ok := coversJSONInt(feature["index"])
		if !ok || index < 0 || index >= len(gallery) || placed[index] {
			continue
		}
		placed[index] = true
		if gallery[index].URL == "" {
			continue
		}
		// Coves has no rune-boundary rule, so a range splitting a rune widens
		// to the whole rune: the image replaces it rather than leaving half.
		for start > 0 && !utf8.RuneStart(source[start]) {
			start--
		}
		for end < len(source) && !utf8.RuneStart(source[end]) {
			end++
		}
		placements = append(placements, imagePlacement{start: start, end: end, position: position, image: gallery[index]})
	}
	return mergeImagePlacements(placements), placed
}

// unplacedImagePositions returns, in gallery order, the positions of the
// gallery entries with a URL that no kept #image facet placed.
func unplacedImagePositions(gallery []GalleryImage, placed map[int]bool) []int {
	var positions []int
	for position, image := range gallery {
		if image.URL != "" && !placed[position] {
			positions = append(positions, position)
		}
	}
	return positions
}

// mergeImagePlacements joins placements whose spans intersect into one range
// per group, its images in facets-array order. Touching spans stay separate.
func mergeImagePlacements(placements []imagePlacement) []inlineRange {
	sort.SliceStable(placements, func(i, j int) bool { return placements[i].start < placements[j].start })
	var ranges []inlineRange
	for groupStart := 0; groupStart < len(placements); {
		group := []imagePlacement{placements[groupStart]}
		end := placements[groupStart].end
		groupEnd := groupStart + 1
		for groupEnd < len(placements) && placements[groupEnd].start < end {
			group = append(group, placements[groupEnd])
			end = max(end, placements[groupEnd].end)
			groupEnd++
		}
		sort.Slice(group, func(i, j int) bool { return group[i].position < group[j].position })
		images := make([]GalleryImage, len(group))
		for i, placement := range group {
			images[i] = placement.image
		}
		ranges = append(ranges, inlineRange{start: placements[groupStart].start, end: end, images: images})
		groupStart = groupEnd
	}
	return ranges
}

func hasFeatureType(features []any, featureType string) bool {
	for _, raw := range features {
		feature, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if current, _ := feature["$type"].(string); current == featureType {
			return true
		}
	}
	return false
}

// coversFacetRange reads a facet's byte range the way Coves' facetRange does:
// integer bounds with 0 <= start < end. The content bound is checked by the
// caller.
func coversFacetRange(facet map[string]any) (int, int, bool) {
	index, ok := facet["index"].(map[string]any)
	if !ok {
		return 0, 0, false
	}
	start, startOK := coversJSONInt(index["byteStart"])
	end, endOK := coversJSONInt(index["byteEnd"])
	if !startOK || !endOK || start < 0 || end <= start {
		return 0, 0, false
	}
	return start, end, true
}

// coversJSONInt decodes an integer as Coves' jsonInt does: an integral number
// within the int32 range.
func coversJSONInt(raw any) (int, bool) {
	switch number := raw.(type) {
	case float64:
		if number != math.Trunc(number) || number < math.MinInt32 || number > math.MaxInt32 {
			return 0, false
		}
		return int(number), true
	case int:
		if number < math.MinInt32 || number > math.MaxInt32 {
			return 0, false
		}
		return number, true
	case int64:
		if number < math.MinInt32 || number > math.MaxInt32 {
			return 0, false
		}
		return int(number), true
	case json.Number:
		value, err := number.Int64()
		if err != nil || value < math.MinInt32 || value > math.MaxInt32 {
			return 0, false
		}
		return int(value), true
	default:
		return 0, false
	}
}

func insideAnyRange(start, end int, ranges [][2]int) bool {
	for _, outer := range ranges {
		if outer[0] <= start && end <= outer[1] {
			return true
		}
	}
	return false
}

// writeMarkdownImages writes the image nodes Tidepool builds. These are the
// only `![…](…)` nodes in source, because plaintext escaping escapes `!` and
// `[`. The URL is built from a validated DID and a re-encoded CID; the alt is
// author text and is escaped here.
func writeMarkdownImages(b *strings.Builder, images []GalleryImage) {
	for _, image := range images {
		b.WriteString("![")
		b.WriteString(escapeMarkdownImageAlt(image.Alt))
		b.WriteString("](")
		b.WriteString(escapeMarkdownDestination(image.URL))
		b.WriteByte(')')
	}
}

func writeHTMLImages(b *strings.Builder, images []GalleryImage) {
	for _, image := range images {
		b.WriteString(`<img src="`)
		b.WriteString(html.EscapeString(image.URL))
		b.WriteString(`" alt="`)
		b.WriteString(html.EscapeString(flattenImageAlt(image.Alt)))
		b.WriteString(`">`)
	}
}

// flattenImageAlt turns every line break (CRLF, CR or LF) into one space, so
// an alt can neither end the image's line nor open a block.
func flattenImageAlt(alt string) string {
	alt = strings.ReplaceAll(alt, "\r\n", " ")
	return strings.NewReplacer("\r", " ", "\n", " ").Replace(alt)
}

// escapeMarkdownImageAlt escapes an alt for a Markdown image's brackets: the
// backslash and brackets that would end or nest the label, the emphasis and
// code delimiters that would be stripped from the alt, and an & that would
// decode as a character reference.
func escapeMarkdownImageAlt(alt string) string {
	alt = flattenImageAlt(alt)
	var b strings.Builder
	for i := 0; i < len(alt); i++ {
		char := alt[i]
		switch char {
		case '\\', '[', ']', '*', '_', '`', '~':
			b.WriteByte('\\')
		case '&':
			if startsMarkdownEntity(alt[i:]) {
				b.WriteByte('\\')
			}
		}
		b.WriteByte(char)
	}
	return b.String()
}
