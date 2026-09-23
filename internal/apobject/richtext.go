package apobject

import (
	"fmt"
	"html"
	"math"
	"net/url"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/rivo/uniseg"
)

const (
	boldFacetType          = "social.coves.richtext.facet#bold"
	codeBlockFacetType     = "social.coves.richtext.facet#codeBlock"
	blockquoteFacetType    = "social.coves.richtext.facet#blockquote"
	headingFacetType       = "social.coves.richtext.facet#heading"
	inlineCodeFacetType    = "social.coves.richtext.facet#code"
	italicFacetType        = "social.coves.richtext.facet#italic"
	covesLinkFacetType     = "social.coves.richtext.facet#link"
	spoilerFacetType       = "social.coves.richtext.facet#spoiler"
	strikethroughFacetType = "social.coves.richtext.facet#strikethrough"
)

const (
	// maximumCodeLanguageLength caps a code block's info-string language.
	maximumCodeLanguageLength = 40
	// maximumSpoilerReasonBytes and maximumSpoilerReasonGraphemes cap a
	// spoiler reason; a longer reason falls back to the default title.
	maximumSpoilerReasonBytes     = 128
	maximumSpoilerReasonGraphemes = 32
	// maximumBlockLevel is the deepest heading or blockquote level rendered.
	maximumBlockLevel = 6
	// maximumOrderedListMarkerDigits is the longest digit run CommonMark
	// accepts as an ordered list marker.
	maximumOrderedListMarkerDigits = 9
	// maximumEntityNameLength, maximumHexadecimalEntityDigits and
	// maximumDecimalEntityDigits bound the character references markdown-it
	// decodes, so only text it would decode is escaped.
	maximumEntityNameLength        = 32
	maximumHexadecimalEntityDigits = 6
	maximumDecimalEntityDigits     = 7
	// maximumBlockMarkerIndent is the most leading spaces a block marker may
	// have before CommonMark treats the line as indented code.
	maximumBlockMarkerIndent = 3
	// tabStopWidth and indentedCodeColumn follow CommonMark: a tab advances to
	// the next multiple of four columns, and content indented to column four
	// or more is indented code.
	tabStopWidth       = 4
	indentedCodeColumn = 4
	// minimumFenceLength is the shortest code fence or container marker.
	minimumFenceLength = 3
)

type emphasisFeatures uint8

const (
	emphasisBold emphasisFeatures = 1 << iota
	emphasisItalic
	emphasisStrikethrough
)

// inlineRange is one resolved inline span: inline code, emphasis, a link, or
// emphasis that shares its bounds with a link.
type inlineRange struct {
	start    int
	end      int
	code     bool
	features emphasisFeatures
	link     string
}

type validatedFacet struct {
	start      int
	end        int
	blockStart int
	blockEnd   int
	features   []map[string]any
}

type blockKind uint8

const (
	blockHeading blockKind = iota + 1
	blockQuote
	blockCode
	blockSpoiler
)

// blockRange is one block-level facet. Resolved block ranges nest or are
// disjoint; a container's nested blocks follow it in sorted order.
type blockRange struct {
	start    int
	end      int
	kind     blockKind
	level    int
	language string
	reason   string
}

type decodedBlockRange struct {
	headings map[int]struct{}
	quotes   map[int]struct{}
}

type decodedRange struct {
	code        bool
	features    emphasisFeatures
	linkTargets map[string]struct{}
}

// inlineFeature identifies one inline feature so touching ranges of the same
// feature (and the same link target) can be merged before rendering.
type inlineFeature struct {
	code     bool
	features emphasisFeatures
	link     string
}

// renderBody renders a Coves record's text and facets as Lemmy's Markdown
// source and HTML content. It normalizes lone carriage returns, decodes and
// validates the facets, resolves block precedence (code blocks win, spoilers
// fail closed by widening), then renders Markdown and HTML from the same
// resolved ranges.
func renderBody(source string, rawFacets any) (markdown, content string) {
	source = normalizeLoneCarriageReturns(source)
	facets := decodeValidatedFacets(source, rawFacets)
	inlineRanges := decodeInlineRanges(source, facets)
	blockRanges := decodeBlockRanges(source, facets)
	specialRanges := decodeSpecialBlockRanges(source, facets)
	inlineRanges, blockRanges = resolveSpecialBlockRanges(inlineRanges, blockRanges, specialRanges)
	return renderBlockMarkdown(source, inlineRanges, blockRanges), renderBlockHTML(source, inlineRanges, blockRanges)
}

// normalizeLoneCarriageReturns rewrites every \r not followed by \n into \n.
// Lemmy's markdown-it treats a lone \r as a line break, so line-start escaping
// and quote prefixing must see it as one. The rewrite keeps the byte length,
// so facet byte offsets still address the same text.
func normalizeLoneCarriageReturns(source string) string {
	if !strings.Contains(source, "\r") {
		return source
	}
	normalized := []byte(source)
	for i, char := range normalized {
		if char == '\r' && (i+1 == len(normalized) || normalized[i+1] != '\n') {
			normalized[i] = '\n'
		}
	}
	return string(normalized)
}

func decodeValidatedFacets(source string, rawFacets any) []validatedFacet {
	facets, ok := rawFacets.([]any)
	if !ok {
		return nil
	}

	validated := make([]validatedFacet, 0, len(facets))
	for _, rawFacet := range facets {
		facet, ok := rawFacet.(map[string]any)
		if !ok {
			continue
		}
		index, ok := facet["index"].(map[string]any)
		if !ok {
			continue
		}
		start, startOK := decodeBoundedNonNegativeInteger(index["byteStart"], len(source))
		end, endOK := decodeBoundedNonNegativeInteger(index["byteEnd"], len(source))
		if !startOK || !endOK || start >= end || !isRuneBoundary(source, start) || !isRuneBoundary(source, end) {
			continue
		}
		rawFeatures, ok := facet["features"].([]any)
		if !ok {
			continue
		}
		features := make([]map[string]any, 0, len(rawFeatures))
		for _, rawFeature := range rawFeatures {
			if feature, ok := rawFeature.(map[string]any); ok {
				features = append(features, feature)
			}
		}
		blockStart, blockEnd := expandBlockRange(source, start, end)
		validated = append(validated, validatedFacet{
			start: start, end: end,
			blockStart: blockStart, blockEnd: blockEnd,
			features: features,
		})
	}
	return validated
}

// decodeSpecialBlockRanges decodes code blocks and spoilers sorted by
// position. Code blocks with conflicting languages or overlapping ranges are
// dropped. Every spoiler is kept, because a spoiler never loses a conflict:
// resolveSpecialBlockRanges merges and widens them instead.
func decodeSpecialBlockRanges(source string, facets []validatedFacet) []blockRange {
	languagesByRange := make(map[[2]int]map[string]struct{}, len(facets))
	var codeBounds [][2]int
	var spoilers []blockRange
	seenSpoilers := make(map[blockRange]bool, len(facets))
	for _, facet := range facets {
		for _, feature := range facet.features {
			featureType, _ := feature["$type"].(string)
			switch featureType {
			case codeBlockFacetType:
				bounds := [2]int{facet.blockStart, facet.blockEnd}
				if strings.TrimSpace(source[bounds[0]:bounds[1]]) == "" {
					continue
				}
				if languagesByRange[bounds] == nil {
					languagesByRange[bounds] = make(map[string]struct{})
					codeBounds = append(codeBounds, bounds)
				}
				languagesByRange[bounds][safeCodeLanguage(feature["language"])] = struct{}{}
			case spoilerFacetType:
				spoiler := blockRange{start: facet.start, end: facet.end, kind: blockSpoiler,
					reason: safeSpoilerReason(feature["reason"])}
				if !seenSpoilers[spoiler] {
					seenSpoilers[spoiler] = true
					spoilers = append(spoilers, spoiler)
				}
			}
		}
	}

	codeRanges := make([]blockRange, 0, len(codeBounds))
	for _, bounds := range codeBounds {
		languages := languagesByRange[bounds]
		if len(languages) != 1 {
			continue
		}
		for language := range languages {
			codeRanges = append(codeRanges, blockRange{start: bounds[0], end: bounds[1], kind: blockCode, language: language})
		}
	}
	ranges := append(dropConflicting(codeRanges, blockRangesOverlap), spoilers...)
	sortBlockRanges(ranges)
	return ranges
}

func safeCodeLanguage(raw any) string {
	language, ok := raw.(string)
	if !ok || len(language) == 0 || len(language) > maximumCodeLanguageLength {
		return ""
	}
	for i := 0; i < len(language); i++ {
		char := language[i]
		if char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' ||
			char == '_' || char == '+' || char == '-' {
			continue
		}
		return ""
	}
	return language
}

// safeSpoilerReason returns the reason when lemmy-ui's container validator,
// params.trim().match(/^spoiler\s+(.*)$/), is certain to accept it as a
// title, and "" otherwise so the caller falls back to the default title. A
// rejected title would render the spoiler's content visibly. JavaScript's "."
// stops at U+2028 and U+2029, and its trim strips U+FEFF as well as every
// Unicode space, so a reason that begins or ends with such a character could
// trim away to nothing.
func safeSpoilerReason(raw any) string {
	reason, ok := raw.(string)
	if !ok || len(reason) == 0 || len(reason) > maximumSpoilerReasonBytes || uniseg.GraphemeClusterCount(reason) > maximumSpoilerReasonGraphemes ||
		!utf8.ValidString(reason) {
		return ""
	}
	first, _ := utf8.DecodeRuneInString(reason)
	last, _ := utf8.DecodeLastRuneInString(reason)
	if isSpoilerTitleWhitespace(first) || isSpoilerTitleWhitespace(last) {
		return ""
	}
	for _, char := range reason {
		if unicode.IsControl(char) || char == '\u2028' || char == '\u2029' || strings.ContainsRune("\\`*_{}[]()<>#|&:", char) {
			return ""
		}
	}
	return reason
}

// isSpoilerTitleWhitespace reports whether char is whitespace to either Go or
// JavaScript's \s and trim. Go's unicode.IsSpace covers every JavaScript
// whitespace character except U+FEFF, and adds only U+0085.
func isSpoilerTitleWhitespace(char rune) bool {
	return unicode.IsSpace(char) || char == '\ufeff'
}

// resolveSpecialBlockRanges combines the ordinary (heading and quote) and
// special (code and spoiler) blocks into one sorted list of nested or disjoint
// ranges. A code block wins over an overlapping heading or a quote that does
// not contain it; a containing quote keeps the code block nested. Spoilers
// fail closed: see widenSpoilers.
func resolveSpecialBlockRanges(inlineRanges []inlineRange, ordinary, special []blockRange) ([]inlineRange, []blockRange) {
	var codeRanges, spoilers []blockRange
	for _, current := range special {
		if current.kind == blockCode {
			codeRanges = append(codeRanges, current)
		} else {
			spoilers = append(spoilers, current)
		}
	}

	blocks := make([]blockRange, 0, len(ordinary)+len(special))
	for _, current := range ordinary {
		suppressed := false
		for _, code := range codeRanges {
			if blockRangesOverlap(current, code) && (current.kind != blockQuote || !blockContains(current, code)) {
				suppressed = true
				break
			}
		}
		if !suppressed {
			blocks = append(blocks, current)
		}
	}
	blocks = append(blocks, codeRanges...)
	blocks = append(blocks, widenSpoilers(spoilers, blocks)...)

	if len(codeRanges) != 0 {
		filteredInline := inlineRanges[:0]
		for _, current := range inlineRanges {
			if !inlineRangeOverlapsAnyBlock(current, codeRanges) {
				filteredInline = append(filteredInline, current)
			}
		}
		inlineRanges = filteredInline
	}

	sortBlockRanges(blocks)
	return inlineRanges, blocks
}

// widenSpoilers makes spoilers fail closed. Overlapping spoilers merge into
// one, and a spoiler grows to cover every block it touches, except a quote
// that already contains it (the spoiler then renders nested in the quote).
// Growing can create new overlaps, so both steps repeat until nothing changes.
func widenSpoilers(spoilers, blocks []blockRange) []blockRange {
	for {
		spoilers = mergeOverlappingSpoilers(spoilers)
		changed := false
		for i := range spoilers {
			for _, block := range blocks {
				if !blockRangesOverlap(spoilers[i], block) || block.kind == blockQuote && blockContains(block, spoilers[i]) {
					continue
				}
				if block.start < spoilers[i].start {
					spoilers[i].start = block.start
					changed = true
				}
				if block.end > spoilers[i].end {
					spoilers[i].end = block.end
					changed = true
				}
			}
		}
		if !changed {
			return spoilers
		}
	}
}

// mergeOverlappingSpoilers replaces each group of overlapping spoilers with
// one spoiler over their union. Touching spoilers stay separate.
func mergeOverlappingSpoilers(spoilers []blockRange) []blockRange {
	sortBlockRanges(spoilers)
	merged := make([]blockRange, 0, len(spoilers))
	for groupStart := 0; groupStart < len(spoilers); {
		union := spoilers[groupStart]
		groupEnd := groupStart + 1
		for groupEnd < len(spoilers) && spoilers[groupEnd].start < union.end {
			union.end = max(union.end, spoilers[groupEnd].end)
			groupEnd++
		}
		union.reason = mergedSpoilerReason(spoilers[groupStart:groupEnd], union)
		merged = append(merged, union)
		groupStart = groupEnd
	}
	return merged
}

// mergedSpoilerReason keeps the reason every merged spoiler shares, or else
// the single reason of the spoilers that span the whole union (so nested
// spoilers render as their outermost). Any other disagreement falls back to
// the default title.
func mergedSpoilerReason(group []blockRange, union blockRange) string {
	reasons := make(map[string]struct{}, len(group))
	outermostReasons := make(map[string]struct{}, len(group))
	for _, spoiler := range group {
		reasons[spoiler.reason] = struct{}{}
		if spoiler.start == union.start && spoiler.end == union.end {
			outermostReasons[spoiler.reason] = struct{}{}
		}
	}
	for _, candidates := range []map[string]struct{}{reasons, outermostReasons} {
		if len(candidates) == 1 {
			for reason := range candidates {
				return reason
			}
		}
	}
	return ""
}

// sortBlockRanges orders blocks by start, then longest first, so every
// container precedes the blocks nested in it. For equal bounds a quote holds a
// spoiler, and a spoiler holds a heading or code block.
func sortBlockRanges(ranges []blockRange) {
	nestingRank := func(kind blockKind) int {
		switch kind {
		case blockQuote:
			return 0
		case blockSpoiler:
			return 1
		case blockHeading:
			return 2
		default:
			return 3
		}
	}
	sort.Slice(ranges, func(i, j int) bool {
		if ranges[i].start != ranges[j].start {
			return ranges[i].start < ranges[j].start
		}
		if ranges[i].end != ranges[j].end {
			return ranges[i].end > ranges[j].end
		}
		if ranges[i].kind != ranges[j].kind {
			return nestingRank(ranges[i].kind) < nestingRank(ranges[j].kind)
		}
		return ranges[i].reason < ranges[j].reason
	})
}

func blockRangesOverlap(first, second blockRange) bool {
	return spansOverlap(first.start, first.end, second.start, second.end)
}

// spansOverlap reports whether the byte ranges [firstStart, firstEnd) and
// [secondStart, secondEnd) share at least one byte.
func spansOverlap(firstStart, firstEnd, secondStart, secondEnd int) bool {
	return firstStart < secondEnd && secondStart < firstEnd
}

// dropConflicting removes every range that conflicts with at least one other
// range, keeping the survivors in their original order. It filters in place.
func dropConflicting[T any](ranges []T, conflict func(first, second T) bool) []T {
	conflicting := make([]bool, len(ranges))
	for i := range ranges {
		for j := i + 1; j < len(ranges); j++ {
			if conflict(ranges[i], ranges[j]) {
				conflicting[i] = true
				conflicting[j] = true
			}
		}
	}
	kept := ranges[:0]
	for i, current := range ranges {
		if !conflicting[i] {
			kept = append(kept, current)
		}
	}
	return kept
}

func blockContains(outer, inner blockRange) bool {
	return outer.start <= inner.start && inner.end <= outer.end
}

func inlineRangeOverlapsAnyBlock(current inlineRange, ranges []blockRange) bool {
	for _, other := range ranges {
		if spansOverlap(current.start, current.end, other.start, other.end) {
			return true
		}
	}
	return false
}

// decodeBlockRanges decodes headings and quotes. A heading wins over a quote
// with the same bounds; a wider quote keeps the heading nested. Overlapping
// quotes are all dropped.
func decodeBlockRanges(source string, facets []validatedFacet) []blockRange {
	decodedByRange := make(map[[2]int]*decodedBlockRange, len(facets))
	var decodedBounds [][2]int
	for _, facet := range facets {
		start, end := facet.blockStart, facet.blockEnd
		bounds := [2]int{start, end}
		for _, feature := range facet.features {
			featureType, _ := feature["$type"].(string)
			switch featureType {
			case headingFacetType:
				level, ok := decodeBlockLevel(feature["level"])
				if !ok || start == end || strings.ContainsAny(source[start:end], "\r\n") {
					continue
				}
				decoded := decodedBlockRangeFor(decodedByRange, &decodedBounds, bounds)
				if decoded.headings == nil {
					decoded.headings = make(map[int]struct{})
				}
				decoded.headings[level] = struct{}{}
			case blockquoteFacetType:
				level := 1
				if rawLevel, present := feature["level"]; present {
					var valid bool
					level, valid = decodeBlockLevel(rawLevel)
					if !valid {
						continue
					}
				}
				if start == end || strings.TrimSpace(source[start:end]) == "" {
					continue
				}
				decoded := decodedBlockRangeFor(decodedByRange, &decodedBounds, bounds)
				if decoded.quotes == nil {
					decoded.quotes = make(map[int]struct{})
				}
				decoded.quotes[level] = struct{}{}
			}
		}
	}

	headings := make([]blockRange, 0, len(decodedBounds))
	quotes := make([]blockRange, 0, len(decodedBounds))
	for _, bounds := range decodedBounds {
		decoded := decodedByRange[bounds]
		if len(decoded.headings) == 1 {
			for level := range decoded.headings {
				headings = append(headings, blockRange{start: bounds[0], end: bounds[1], kind: blockHeading, level: level})
			}
			continue
		}
		if len(decoded.quotes) == 1 {
			for level := range decoded.quotes {
				quotes = append(quotes, blockRange{start: bounds[0], end: bounds[1], kind: blockQuote, level: level})
			}
		}
	}
	headings = append(headings, dropConflicting(quotes, blockRangesOverlap)...)
	sortBlockRanges(headings)
	return headings
}

func expandBlockRange(source string, start, end int) (int, int) {
	lineStart := strings.LastIndexByte(source[:start], '\n') + 1
	lineEnd := len(source)
	if source[end-1] == '\n' {
		lineEnd = end - 1
	} else if newline := strings.IndexByte(source[end:], '\n'); newline >= 0 {
		lineEnd = end + newline
	}
	if lineEnd > lineStart && source[lineEnd-1] == '\r' {
		lineEnd--
	}
	return lineStart, lineEnd
}

func decodeBlockLevel(raw any) (int, bool) {
	level, ok := decodeBoundedNonNegativeInteger(raw, maximumBlockLevel)
	return level, ok && level >= 1
}

func decodedBlockRangeFor(ranges map[[2]int]*decodedBlockRange, order *[][2]int, bounds [2]int) *decodedBlockRange {
	if ranges[bounds] == nil {
		ranges[bounds] = &decodedBlockRange{}
		*order = append(*order, bounds)
	}
	return ranges[bounds]
}

func decodeInlineRanges(source string, facets []validatedFacet) []inlineRange {
	boundsByFeature := make(map[inlineFeature][][2]int, len(facets))
	for _, facet := range facets {
		start, end := facet.start, facet.end
		bounds := [2]int{start, end}
		for _, feature := range facet.features {
			featureType, ok := feature["$type"].(string)
			if !ok {
				continue
			}
			switch featureType {
			case boldFacetType:
				if hasSafeEmphasisBoundaries(source, start, end) {
					key := inlineFeature{features: emphasisBold}
					boundsByFeature[key] = append(boundsByFeature[key], bounds)
				}
			case inlineCodeFacetType:
				if hasSafeInlineCode(source[start:end]) {
					key := inlineFeature{code: true}
					boundsByFeature[key] = append(boundsByFeature[key], bounds)
				}
			case italicFacetType:
				if hasSafeEmphasisBoundaries(source, start, end) {
					key := inlineFeature{features: emphasisItalic}
					boundsByFeature[key] = append(boundsByFeature[key], bounds)
				}
			case covesLinkFacetType:
				// Surrounding whitespace is trimmed so a link cannot swallow the
				// newline that separates paragraphs or blocks.
				uri, ok := feature["uri"].(string)
				trimmed := strings.TrimLeftFunc(source[start:end], unicode.IsSpace)
				linkStart := end - len(trimmed)
				linkEnd := linkStart + len(strings.TrimRightFunc(trimmed, unicode.IsSpace))
				if !ok || linkStart == linkEnd || containsWhitespaceOnlyBlankLine(source[linkStart:linkEnd]) {
					continue
				}
				target, ok := normalizeHTTPURL(uri)
				if !ok {
					continue
				}
				key := inlineFeature{link: target}
				boundsByFeature[key] = append(boundsByFeature[key], [2]int{linkStart, linkEnd})
			case strikethroughFacetType:
				if hasSafeEmphasisBoundaries(source, start, end) {
					key := inlineFeature{features: emphasisStrikethrough}
					boundsByFeature[key] = append(boundsByFeature[key], bounds)
				}
			}
		}
	}

	featuresByRange := make(map[[2]int]*decodedRange, len(facets))
	for key, allBounds := range boundsByFeature {
		// Emphasis nested in or crossing emphasis of the same kind adds
		// nothing, so it joins into the union; code and links keep their
		// conflict rules below.
		for _, bounds := range mergeTouchingBounds(allBounds, key.features != 0) {
			current := decodedRangeFor(featuresByRange, bounds)
			switch {
			case key.code:
				current.code = true
			case key.link != "":
				if current.linkTargets == nil {
					current.linkTargets = make(map[string]struct{})
				}
				current.linkTargets[key.link] = struct{}{}
			default:
				current.features |= key.features
			}
		}
	}

	ranges := make([]inlineRange, 0, len(featuresByRange))
	codeRanges := make([]inlineRange, 0, len(featuresByRange))
	for bounds, decoded := range featuresByRange {
		current := inlineRange{start: bounds[0], end: bounds[1], code: decoded.code, features: decoded.features}
		if current.code {
			current.features = 0
			codeRanges = append(codeRanges, current)
			ranges = append(ranges, current)
			continue
		}
		if len(decoded.linkTargets) == 1 {
			for target := range decoded.linkTargets {
				current.link = target
			}
		}
		if current.features != 0 || current.link != "" {
			ranges = append(ranges, current)
		}
	}
	// Map iteration order is random; sorting here keeps every later step, and
	// the output, independent of it. Filtering below preserves this order.
	sort.Slice(ranges, func(i, j int) bool {
		if ranges[i].start == ranges[j].start {
			return ranges[i].end > ranges[j].end
		}
		return ranges[i].start < ranges[j].start
	})
	if len(codeRanges) != 0 {
		filtered := ranges[:0]
		for _, current := range ranges {
			if !current.code && inlineRangeOverlapsAnyInline(current, codeRanges) {
				continue
			}
			filtered = append(filtered, current)
		}
		ranges = filtered
	}
	// Crossing ranges conflict, and so do nested code spans or nested links.
	ranges = dropConflicting(ranges, func(first, second inlineRange) bool {
		return first.start < second.start && second.start < first.end && first.end < second.end ||
			second.start < first.start && first.start < second.end && second.end < first.end ||
			inlineRangesNest(first, second) && (first.code && second.code || first.link != "" && second.link != "")
	})
	return dropUnflankedEmphasis(source, ranges)
}

// mergeTouchingBounds drops duplicate ranges of one feature and joins ranges
// that meet end to start, so adjacent facets render as a single span instead
// of colliding delimiters such as "*a**b*" or doubled code backticks. With
// joinOverlapping it also joins contained and crossing ranges into their
// union, so "**ab**cde**fghij**" cannot come from bold nested in bold.
func mergeTouchingBounds(allBounds [][2]int, joinOverlapping bool) [][2]int {
	sort.Slice(allBounds, func(i, j int) bool {
		if allBounds[i][0] == allBounds[j][0] {
			return allBounds[i][1] < allBounds[j][1]
		}
		return allBounds[i][0] < allBounds[j][0]
	})
	unique := make([][2]int, 0, len(allBounds))
	for i, bounds := range allBounds {
		if i == 0 || bounds != allBounds[i-1] {
			unique = append(unique, bounds)
		}
	}
	merged := make([][2]int, 0, len(unique))
	for _, bounds := range unique {
		if last := len(merged) - 1; last >= 0 && (merged[last][1] == bounds[0] || joinOverlapping && bounds[0] < merged[last][1]) {
			merged[last][1] = max(merged[last][1], bounds[1])
			continue
		}
		merged = append(merged, bounds)
	}
	return merged
}

// dropUnflankedEmphasis removes emphasis whose emitted delimiter run would sit
// between a word character and punctuation: a strikethrough delimiter stacked
// with bold or italic on the same range, or a nested range (such as a link's
// bracket) sharing its start or end. CommonMark cannot open or close emphasis
// there, so Lemmy would show literal delimiters; dropping the emphasis keeps
// the Markdown and HTML outputs in agreement. Decisions use the ranges as
// given, so the result does not depend on their order.
func dropUnflankedEmphasis(source string, ranges []inlineRange) []inlineRange {
	kept := make([]inlineRange, 0, len(ranges))
	for i, current := range ranges {
		if current.features != 0 && current.link == "" {
			stacked := current.features&emphasisStrikethrough != 0 && current.features != emphasisStrikethrough
			startTouched, endTouched := stacked, stacked
			for j, other := range ranges {
				if i == j {
					continue
				}
				if other.start == current.start && other.end < current.end {
					startTouched = true
				}
				if other.end == current.end && other.start > current.start {
					endTouched = true
				}
			}
			before, _ := utf8.DecodeLastRuneInString(source[:current.start])
			after, _ := utf8.DecodeRuneInString(source[current.end:])
			if startTouched && current.start != 0 && isWordRune(before) ||
				endTouched && current.end != len(source) && isWordRune(after) {
				current.features = 0
			}
		}
		if current.code || current.features != 0 || current.link != "" {
			kept = append(kept, current)
		}
	}
	return kept
}

func isWordRune(char rune) bool {
	return !unicode.IsSpace(char) && !isMarkdownPunctuation(char)
}

func hasSafeInlineCode(text string) bool {
	return strings.TrimSpace(text) != "" && !strings.ContainsAny(text, "\r\n")
}

func inlineRangeOverlapsAnyInline(current inlineRange, ranges []inlineRange) bool {
	for _, other := range ranges {
		if spansOverlap(current.start, current.end, other.start, other.end) {
			return true
		}
	}
	return false
}

func inlineRangesNest(first, second inlineRange) bool {
	return first.start <= second.start && second.end <= first.end ||
		second.start <= first.start && first.end <= second.end
}

func decodedRangeFor(ranges map[[2]int]*decodedRange, bounds [2]int) *decodedRange {
	if ranges[bounds] == nil {
		ranges[bounds] = &decodedRange{}
	}
	return ranges[bounds]
}

func normalizeHTTPURL(raw string) (string, bool) {
	if raw == "" || raw != strings.TrimSpace(raw) {
		return "", false
	}
	target, err := url.Parse(raw)
	if err != nil || target.Opaque != "" || !isURLRegisteredName(target.Hostname()) ||
		(!strings.EqualFold(target.Scheme, "http") && !strings.EqualFold(target.Scheme, "https")) {
		return "", false
	}

	target.Scheme = strings.ToLower(target.Scheme)
	if target.RawPath != "" {
		target.RawPath = escapeRawURLPart(target.RawPath, isURLPathByte)
	} else {
		target.RawPath = escapeURLPart(target.Path, isURLPathByte)
	}
	target.RawQuery = escapeRawURLPart(target.RawQuery, isURLQueryByte)
	if target.RawFragment != "" {
		target.RawFragment = escapeRawURLPart(target.RawFragment, isURLQueryByte)
	} else {
		target.RawFragment = escapeURLPart(target.Fragment, isURLQueryByte)
	}
	return target.String(), true
}

// isURLRegisteredName reports whether a decoded host is an RFC 3986 reg-name
// (which covers IPv4 addresses) made of unreserved and sub-delimiter
// characters. Non-ASCII characters stay allowed: Go's url package writes them
// back as percent-encoded UTF-8, a valid reg-name that browsers map through
// IDNA. A decoded "%" and bracketed IPv6 literals are rejected; markdown-it
// percent-encodes the brackets, so Lemmy's link would point at an invalid
// host.
func isURLRegisteredName(host string) bool {
	if host == "" {
		return false
	}
	for _, char := range host {
		if char >= utf8.RuneSelf {
			continue
		}
		if !isURLUnreservedByte(byte(char)) && !strings.ContainsRune("!$&'()*+,;=", char) {
			return false
		}
	}
	return true
}

func escapeURLPart(value string, allowed func(byte) bool) string {
	return escapeURLBytes(value, allowed, false)
}

func escapeRawURLPart(value string, allowed func(byte) bool) string {
	return escapeURLBytes(value, allowed, true)
}

func escapeURLBytes(value string, allowed func(byte) bool, preserveEscapes bool) string {
	const hexadecimal = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(value); i++ {
		char := value[i]
		if preserveEscapes && char == '%' && i+2 < len(value) && isHexadecimal(value[i+1]) && isHexadecimal(value[i+2]) {
			b.WriteString(value[i : i+3])
			i += 2
			continue
		}
		if char < utf8.RuneSelf && allowed(char) {
			b.WriteByte(char)
			continue
		}
		b.WriteByte('%')
		b.WriteByte(hexadecimal[char>>4])
		b.WriteByte(hexadecimal[char&15])
	}
	return b.String()
}

func isURLPathByte(char byte) bool {
	return isURLUnreservedByte(char) || strings.ContainsRune("!$&'()*+,;=:@/", rune(char))
}

func isURLQueryByte(char byte) bool {
	return isURLPathByte(char) || char == '?'
}

func isURLUnreservedByte(char byte) bool {
	return char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' ||
		strings.ContainsRune("-._~", rune(char))
}

func isHexadecimal(char byte) bool {
	return char >= '0' && char <= '9' || char >= 'a' && char <= 'f' || char >= 'A' && char <= 'F'
}

func hasSafeEmphasisBoundaries(source string, start, end int) bool {
	text := source[start:end]
	first, _ := utf8.DecodeRuneInString(text)
	last, _ := utf8.DecodeLastRuneInString(text)
	before, after := rune(' '), rune(' ')
	if start != 0 {
		before, _ = utf8.DecodeLastRuneInString(source[:start])
	}
	if end != len(source) {
		after, _ = utf8.DecodeRuneInString(source[end:])
	}
	leftFlanking := !unicode.IsSpace(first) &&
		(!isMarkdownPunctuation(first) || unicode.IsSpace(before) || isMarkdownPunctuation(before))
	rightFlanking := !unicode.IsSpace(last) &&
		(!isMarkdownPunctuation(last) || unicode.IsSpace(after) || isMarkdownPunctuation(after))
	return leftFlanking && rightFlanking && !containsWhitespaceOnlyBlankLine(text)
}

// isMarkdownPunctuation matches CommonMark's Unicode punctuation character,
// which markdown-it also uses for flanking: general category P or S.
func isMarkdownPunctuation(char rune) bool {
	return char >= '!' && char <= '/' || char >= ':' && char <= '@' ||
		char >= '[' && char <= '`' || char >= '{' && char <= '~' ||
		unicode.IsPunct(char) || unicode.IsSymbol(char)
}

func containsWhitespaceOnlyBlankLine(text string) bool {
	previousNewline := strings.IndexByte(text, '\n')
	for previousNewline >= 0 {
		remainder := text[previousNewline+1:]
		nextNewline := strings.IndexByte(remainder, '\n')
		if nextNewline < 0 {
			return false
		}
		nextNewline += previousNewline + 1
		if isWhitespaceOnlyBlankLine(text[previousNewline+1 : nextNewline]) {
			return true
		}
		previousNewline = nextNewline
	}
	return false
}

func isWhitespaceOnlyBlankLine(line string) bool {
	return strings.Trim(line, " \t\r") == ""
}

// decodeBoundedNonNegativeInteger decodes a JSON-decoded integer in
// [0, maximum], such as a facet byte offset or a block level. Negative,
// fractional, oversized and non-numeric values are rejected.
func decodeBoundedNonNegativeInteger(raw any, maximum int) (int, bool) {
	var value uint64
	switch number := raw.(type) {
	case int:
		if number < 0 {
			return 0, false
		}
		value = uint64(number)
	case int8:
		if number < 0 {
			return 0, false
		}
		value = uint64(number)
	case int16:
		if number < 0 {
			return 0, false
		}
		value = uint64(number)
	case int32:
		if number < 0 {
			return 0, false
		}
		value = uint64(number)
	case int64:
		if number < 0 {
			return 0, false
		}
		value = uint64(number)
	case uint:
		value = uint64(number)
	case uint8:
		value = uint64(number)
	case uint16:
		value = uint64(number)
	case uint32:
		value = uint64(number)
	case uint64:
		value = number
	case float64:
		if number < 0 || number != math.Trunc(number) || number > float64(maximum) {
			return 0, false
		}
		return int(number), true
	default:
		return 0, false
	}
	if value > uint64(maximum) {
		return 0, false
	}
	return int(value), true
}

func isRuneBoundary(source string, offset int) bool {
	return offset == 0 || offset == len(source) || utf8.RuneStart(source[offset])
}

func renderBlockMarkdown(source string, inlineRanges []inlineRange, blockRanges []blockRange) string {
	if len(blockRanges) == 0 {
		return renderMarkdown(source, inlineRanges)
	}

	var b strings.Builder
	cursor := 0
	previousWasQuote := false
	previousWasIsolating := false
	for i, block := range blockRanges {
		if block.start < cursor {
			continue
		}
		gap := renderMarkdownSlice(source, inlineRanges, cursor, block.start)
		if previousWasIsolating {
			gap = strings.TrimLeftFunc(gap, unicode.IsSpace)
			ensureMarkdownBlockSeparation(&b)
		}
		if blockIsIsolating(block) {
			gap = strings.TrimRightFunc(gap, unicode.IsSpace)
		}
		if previousWasQuote && !previousWasIsolating && strings.HasPrefix(gap, "\n") && !strings.HasPrefix(gap, "\n\n") {
			b.WriteByte('\n')
		}
		b.WriteString(gap)
		if blockIsIsolating(block) && b.Len() != 0 {
			ensureMarkdownBlockSeparation(&b)
		}

		if block.kind == blockCode {
			writeCodeBlockMarkdown(&b, strings.ReplaceAll(source[block.start:block.end], "\r\n", "\n"), block)
		} else {
			text := renderBlockMarkdown(source[block.start:block.end],
				sliceInlineRanges(inlineRanges, block.start, block.end),
				sliceBlockRanges(blockRanges[i+1:], block.start, block.end))
			switch block.kind {
			case blockHeading:
				b.WriteString(strings.Repeat("#", block.level))
				b.WriteByte(' ')
				b.WriteString(text)
			case blockQuote:
				writeQuoteMarkdown(&b, text, block.level)
			case blockSpoiler:
				writeSpoilerMarkdown(&b, text, block.reason)
			}
		}
		cursor = block.end
		previousWasQuote = block.kind == blockQuote
		previousWasIsolating = blockIsIsolating(block)
	}
	gap := renderMarkdownSlice(source, inlineRanges, cursor, len(source))
	if previousWasIsolating {
		gap = strings.TrimLeftFunc(gap, unicode.IsSpace)
		if gap != "" {
			ensureMarkdownBlockSeparation(&b)
		}
	}
	if previousWasQuote && !previousWasIsolating && strings.HasPrefix(gap, "\n") && !strings.HasPrefix(gap, "\n\n") {
		b.WriteByte('\n')
	}
	b.WriteString(gap)
	return b.String()
}

func writeQuoteMarkdown(b *strings.Builder, text string, level int) {
	prefix := strings.Repeat("> ", level)
	emptyPrefix := strings.TrimSuffix(prefix, " ")
	for i, line := range strings.Split(text, "\n") {
		if i != 0 {
			b.WriteByte('\n')
		}
		if line == "" {
			b.WriteString(emptyPrefix)
			continue
		}
		b.WriteString(prefix)
		b.WriteString(line)
	}
}

// writeSpoilerMarkdown writes a lemmy-ui spoiler container. lemmy-ui opens a
// container only when a title follows "spoiler", so a missing reason uses the
// default title. markdown-it-container closes on any line of at least as many
// colons as the opening marker, even inside a fenced code block, so the marker
// outgrows every colon run that starts a content line.
func writeSpoilerMarkdown(b *strings.Builder, text, reason string) {
	marker := strings.Repeat(":", max(minimumFenceLength, maximumLeadingColonRun(text)+1))
	b.WriteString(marker)
	b.WriteString(" spoiler ")
	b.WriteString(spoilerTitle(reason))
	b.WriteByte('\n')
	b.WriteString(text)
	b.WriteByte('\n')
	b.WriteString(marker)
}

func spoilerTitle(reason string) string {
	if reason == "" {
		return "Spoiler"
	}
	return reason
}

func maximumLeadingColonRun(text string) int {
	maximum := 0
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimLeft(line, " \t")
		maximum = max(maximum, len(line)-len(strings.TrimLeft(line, ":")))
	}
	return maximum
}

func blockIsIsolating(block blockRange) bool {
	return block.kind == blockCode || block.kind == blockSpoiler
}

func ensureMarkdownBlockSeparation(b *strings.Builder) {
	text := b.String()
	newlines := 0
	for i := len(text) - 1; i >= 0 && text[i] == '\n'; i-- {
		newlines++
	}
	for newlines < 2 {
		b.WriteByte('\n')
		newlines++
	}
}

func writeCodeBlockMarkdown(b *strings.Builder, text string, block blockRange) {
	fence := strings.Repeat("`", max(minimumFenceLength, maximumBacktickRun(text)+1))
	b.WriteString(fence)
	b.WriteString(block.language)
	b.WriteByte('\n')
	b.WriteString(text)
	b.WriteByte('\n')
	b.WriteString(fence)
}

func renderMarkdownSlice(source string, ranges []inlineRange, start, end int) string {
	return renderMarkdown(source[start:end], sliceInlineRanges(ranges, start, end))
}

func renderBlockHTML(source string, inlineRanges []inlineRange, blockRanges []blockRange) string {
	if len(blockRanges) == 0 {
		return renderHTML(source, inlineRanges)
	}

	var b strings.Builder
	cursor := 0
	for i, block := range blockRanges {
		if block.start < cursor {
			continue
		}
		writeHTMLSpan(&b, source, inlineRanges, cursor, block.start)
		blockSource := source[block.start:block.end]
		blockInlineRanges := sliceInlineRanges(inlineRanges, block.start, block.end)
		nestedBlocks := sliceBlockRanges(blockRanges[i+1:], block.start, block.end)
		switch block.kind {
		case blockHeading:
			text := renderInlineHTML(blockSource, blockInlineRanges)
			fmt.Fprintf(&b, "<h%d>%s</h%d>\n", block.level, text, block.level)
		case blockQuote:
			b.WriteString(strings.Repeat("<blockquote>\n", block.level))
			b.WriteString(renderBlockHTML(blockSource, blockInlineRanges, nestedBlocks))
			b.WriteString(strings.Repeat("</blockquote>\n", block.level))
		case blockCode:
			b.WriteString("<pre><code")
			if block.language != "" {
				b.WriteString(` class="language-`)
				b.WriteString(block.language)
				b.WriteByte('"')
			}
			b.WriteByte('>')
			b.WriteString(html.EscapeString(strings.ReplaceAll(blockSource, "\r\n", "\n")))
			b.WriteString("</code></pre>\n")
		case blockSpoiler:
			b.WriteString("<details>\n<summary>")
			b.WriteString(html.EscapeString(spoilerTitle(block.reason)))
			b.WriteString("</summary>\n")
			b.WriteString(renderBlockHTML(blockSource, blockInlineRanges, nestedBlocks))
			b.WriteString("</details>\n")
		}
		cursor = block.end
	}
	writeHTMLSpan(&b, source, inlineRanges, cursor, len(source))
	if b.Len() == 0 {
		return "<p></p>\n"
	}
	return b.String()
}

func sliceBlockRanges(ranges []blockRange, start, end int) []blockRange {
	var sliced []blockRange
	for _, current := range ranges {
		if current.start < start || current.end > end {
			continue
		}
		current.start -= start
		current.end -= start
		sliced = append(sliced, current)
	}
	return sliced
}

func writeHTMLSpan(b *strings.Builder, source string, ranges []inlineRange, start, end int) {
	if strings.TrimSpace(source[start:end]) == "" {
		return
	}
	b.WriteString(renderHTML(source[start:end], sliceInlineRanges(ranges, start, end)))
}

func sliceInlineRanges(ranges []inlineRange, start, end int) []inlineRange {
	var sliced []inlineRange
	for _, current := range ranges {
		if current.start < start || current.end > end {
			continue
		}
		current.start -= start
		current.end -= start
		sliced = append(sliced, current)
	}
	return sliced
}

func renderMarkdown(source string, ranges []inlineRange) string {
	var b strings.Builder
	atLineStart := true
	insideCode := false
	offset := 0
	renderFacets(source, ranges,
		func(text string) {
			start := offset
			offset += len(text)
			if insideCode {
				b.WriteString(text)
				return
			}
			atLineStart = appendEscapedPlaintextMarkdown(&b, source, start, offset, atLineStart)
		},
		func(current inlineRange) {
			writeMarkdownFacet(&b, source, current, false)
			insideCode = current.code
		},
		func(current inlineRange) {
			writeMarkdownFacet(&b, source, current, true)
			if current.code {
				insideCode = false
			}
		},
	)
	return b.String()
}

func writeMarkdownFacet(b *strings.Builder, source string, current inlineRange, closing bool) {
	if current.code {
		text := source[current.start:current.end]
		delimiter := strings.Repeat("`", maximumBacktickRun(text)+1)
		padding := text[0] == '`' || text[len(text)-1] == '`' || text[0] == ' ' && text[len(text)-1] == ' '
		if closing && padding {
			b.WriteByte(' ')
		}
		b.WriteString(delimiter)
		if !closing && padding {
			b.WriteByte(' ')
		}
		return
	}
	if closing {
		writeMarkdownEmphasis(b, current.features, true)
		if current.link != "" {
			b.WriteString("](")
			b.WriteString(escapeMarkdownDestination(current.link))
			b.WriteByte(')')
		}
		return
	}
	if current.link != "" {
		b.WriteByte('[')
	}
	writeMarkdownEmphasis(b, current.features, false)
}

func maximumBacktickRun(text string) int {
	maximum, current := 0, 0
	for i := 0; i < len(text); i++ {
		if text[i] == '`' {
			current++
			if current > maximum {
				maximum = current
			}
			continue
		}
		current = 0
	}
	return maximum
}

func escapeMarkdownDestination(destination string) string {
	var b strings.Builder
	for i := 0; i < len(destination); i++ {
		if destination[i] == '\\' || destination[i] == '(' || destination[i] == ')' {
			b.WriteByte('\\')
		}
		if destination[i] == '&' && startsMarkdownEntity(destination[i:]) {
			b.WriteString("&amp;")
			continue
		}
		b.WriteByte(destination[i])
	}
	return b.String()
}

func startsMarkdownEntity(text string) bool {
	if len(text) < 3 || text[0] != '&' {
		return false
	}
	if text[1] != '#' {
		return entityNameEndsWithin(text[1:], maximumEntityNameLength)
	}
	if len(text) >= 4 && (text[2] == 'x' || text[2] == 'X') {
		return hexadecimalEntityEndsWithin(text[3:], maximumHexadecimalEntityDigits)
	}
	return decimalEntityEndsWithin(text[2:], maximumDecimalEntityDigits)
}

func entityNameEndsWithin(text string, maximum int) bool {
	if len(text) == 0 || text[0] < 'A' || text[0] > 'Z' && text[0] < 'a' || text[0] > 'z' {
		return false
	}
	for i := 0; i < len(text) && i <= maximum; i++ {
		char := text[i]
		if char == ';' {
			return i != 0
		}
		if char < '0' || char > '9' && char < 'A' || char > 'Z' && char < 'a' || char > 'z' {
			return false
		}
	}
	return false
}

func decimalEntityEndsWithin(text string, maximum int) bool {
	for i := 0; i < len(text) && i <= maximum; i++ {
		if text[i] == ';' {
			return i != 0
		}
		if text[i] < '0' || text[i] > '9' {
			return false
		}
	}
	return false
}

func hexadecimalEntityEndsWithin(text string, maximum int) bool {
	for i := 0; i < len(text) && i <= maximum; i++ {
		if text[i] == ';' {
			return i != 0
		}
		if !isHexadecimal(text[i]) {
			return false
		}
	}
	return false
}

func writeMarkdownEmphasis(b *strings.Builder, features emphasisFeatures, closing bool) {
	if closing {
		if features&emphasisStrikethrough != 0 {
			b.WriteString("~~")
		}
		if features&emphasisItalic != 0 {
			b.WriteByte('*')
		}
		if features&emphasisBold != 0 {
			b.WriteString("**")
		}
		return
	}
	if features&emphasisBold != 0 {
		b.WriteString("**")
	}
	if features&emphasisItalic != 0 {
		b.WriteByte('*')
	}
	if features&emphasisStrikethrough != 0 {
		b.WriteString("~~")
	}
}

func renderFacets(source string, ranges []inlineRange, writeText func(string), open, close func(inlineRange)) {
	opens := make(map[int][]inlineRange, len(ranges))
	closes := make(map[int][]inlineRange, len(ranges))
	positions := make([]int, 0, len(ranges)*2)
	seenPosition := make(map[int]bool, len(ranges)*2)
	for _, current := range ranges {
		opens[current.start] = append(opens[current.start], current)
		closes[current.end] = append(closes[current.end], current)
		if !seenPosition[current.start] {
			positions = append(positions, current.start)
			seenPosition[current.start] = true
		}
		if !seenPosition[current.end] {
			positions = append(positions, current.end)
			seenPosition[current.end] = true
		}
	}
	sort.Ints(positions)
	for position, closing := range closes {
		sort.Slice(closing, func(i, j int) bool { return closing[i].start > closing[j].start })
		closes[position] = closing
	}
	for position, opening := range opens {
		sort.Slice(opening, func(i, j int) bool { return opening[i].end > opening[j].end })
		opens[position] = opening
	}

	cursor := 0
	for _, position := range positions {
		writeText(source[cursor:position])
		for _, current := range closes[position] {
			close(current)
		}
		for _, current := range opens[position] {
			open(current)
		}
		cursor = position
	}
	writeText(source[cursor:])
}

// appendEscapedPlaintextMarkdown escapes source[start:end] as literal Markdown.
// Line-level decisions (block marker position, setext underline, indented
// code) are made once per source line against the whole line, so they hold
// even when facets split the line into several chunks, and a long line costs
// linear time.
func appendEscapedPlaintextMarkdown(b *strings.Builder, source string, start, end int, atLineStart bool) bool {
	markerStart, setextUnderline, indentedCode := -1, false, false
	if atLineStart {
		markerStart, setextUnderline, indentedCode = describeMarkdownLine(source, start)
	}
	for i := start; i < end; i++ {
		char := source[i]
		if char == '\r' && i+1 < len(source) && source[i+1] == '\n' {
			continue
		}
		if atLineStart && (indentedCode || setextUnderline && markerStart > i) {
			if char == '\t' {
				b.WriteString("&#9;")
			} else {
				b.WriteString("&#32;")
			}
			atLineStart = false
			continue
		}
		if i == markerStart && char == ':' && strings.HasPrefix(source[i:], ":::") {
			b.WriteByte('\\')
		}
		escape := false
		switch char {
		case '\\', '`', '*', '_', '[', ']', '<', '>', '&', '#', '-', '~', '^', '!':
			escape = true
		case '+':
			escape = i == markerStart
		case '.', ')':
			escape = isOrderedListMarker(source, markerStart, i)
		case '=':
			escape = setextUnderline
		}
		if escape {
			b.WriteByte('\\')
		}
		b.WriteByte(char)
		atLineStart = char == '\n'
		if atLineStart {
			markerStart, setextUnderline, indentedCode = describeMarkdownLine(source, i+1)
		}
	}
	return atLineStart
}

func describeMarkdownLine(source string, lineStart int) (markerStart int, setextUnderline, indentedCode bool) {
	markerStart = markdownBlockMarkerStart(source, lineStart)
	return markerStart, isSetextUnderline(source, markerStart), startsIndentedCodeLine(source, lineStart)
}

func markdownBlockMarkerStart(text string, lineStart int) int {
	markerStart := lineStart
	for markerStart < len(text) && markerStart-lineStart < maximumBlockMarkerIndent && text[markerStart] == ' ' {
		markerStart++
	}
	return markerStart
}

// startsIndentedCodeLine reports whether the line starting at lineStart has
// non-blank content indented to column four or more, counting a tab as a jump
// to the next multiple of four as CommonMark does.
func startsIndentedCodeLine(text string, lineStart int) bool {
	column := 0
	for i := lineStart; i < len(text); i++ {
		switch text[i] {
		case ' ':
			column++
		case '\t':
			column += tabStopWidth - column%tabStopWidth
		case '\r', '\n':
			return false
		default:
			return column >= indentedCodeColumn
		}
	}
	return false
}

// isOrderedListMarker reports whether the delimiter ends a run of one to nine
// digits at the line's marker position and is followed by whitespace or the
// end of the line; an empty item such as "2020." alone still opens a list.
func isOrderedListMarker(text string, markerStart, delimiter int) bool {
	if markerStart < 0 || delimiter == markerStart || delimiter-markerStart > maximumOrderedListMarkerDigits ||
		delimiter+1 < len(text) && !strings.ContainsRune(" \t\r\n", rune(text[delimiter+1])) {
		return false
	}
	for i := markerStart; i < delimiter; i++ {
		if text[i] < '0' || text[i] > '9' {
			return false
		}
	}
	return true
}

func isSetextUnderline(text string, markerStart int) bool {
	if markerStart >= len(text) || text[markerStart] != '=' {
		return false
	}
	for i := markerStart; i < len(text) && text[i] != '\n'; i++ {
		if text[i] != '=' && text[i] != ' ' && text[i] != '\t' && text[i] != '\r' {
			return false
		}
	}
	return true
}

// renderHTML renders source that holds no block facets as the HTML Lemmy
// stores as `content`. Paragraphs are split on whitespace-only blank lines,
// each rendered with its inline facets, and text is escaped so the source's
// own angle brackets cannot inject markup.
func renderHTML(source string, ranges []inlineRange) string {
	var b strings.Builder
	paragraphStart := 0
	previousNewline := strings.IndexByte(source, '\n')
	for previousNewline >= 0 {
		remainder := source[previousNewline+1:]
		nextNewline := strings.IndexByte(remainder, '\n')
		if nextNewline < 0 {
			break
		}
		nextNewline += previousNewline + 1
		if isWhitespaceOnlyBlankLine(source[previousNewline+1 : nextNewline]) {
			paragraphEnd := previousNewline
			if paragraphEnd > paragraphStart && source[paragraphEnd-1] == '\r' {
				paragraphEnd--
			}
			writeHTMLParagraph(&b, source, ranges, paragraphStart, paragraphEnd)
			paragraphStart = nextNewline + 1
		}
		previousNewline = nextNewline
	}
	writeHTMLParagraph(&b, source, ranges, paragraphStart, len(source))
	if b.Len() == 0 {
		return "<p></p>\n"
	}
	return b.String()
}

func writeHTMLParagraph(b *strings.Builder, source string, ranges []inlineRange, start, end int) {
	if start >= end {
		return
	}
	paragraph := strings.TrimSpace(renderInlineHTML(source[start:end], sliceInlineRanges(ranges, start, end)))
	if paragraph == "" {
		return
	}
	b.WriteString("<p>")
	b.WriteString(paragraph)
	b.WriteString("</p>\n")
}

func renderInlineHTML(source string, ranges []inlineRange) string {
	var b strings.Builder
	renderFacets(source, ranges,
		func(text string) { b.WriteString(html.EscapeString(strings.ReplaceAll(text, "\r\n", "\n"))) },
		func(current inlineRange) { writeHTMLFacet(&b, current, false) },
		func(current inlineRange) { writeHTMLFacet(&b, current, true) },
	)
	return b.String()
}

func writeHTMLFacet(b *strings.Builder, current inlineRange, closing bool) {
	if current.code {
		if closing {
			b.WriteString("</code>")
		} else {
			b.WriteString("<code>")
		}
		return
	}
	if closing {
		writeHTMLEmphasis(b, current.features, true)
		if current.link != "" {
			b.WriteString("</a>")
		}
		return
	}
	if current.link != "" {
		b.WriteString(`<a href="`)
		b.WriteString(html.EscapeString(current.link))
		b.WriteString(`">`)
	}
	writeHTMLEmphasis(b, current.features, false)
}

func writeHTMLEmphasis(b *strings.Builder, features emphasisFeatures, closing bool) {
	if closing {
		if features&emphasisStrikethrough != 0 {
			b.WriteString("</del>")
		}
		if features&emphasisItalic != 0 {
			b.WriteString("</em>")
		}
		if features&emphasisBold != 0 {
			b.WriteString("</strong>")
		}
		return
	}
	if features&emphasisBold != 0 {
		b.WriteString("<strong>")
	}
	if features&emphasisItalic != 0 {
		b.WriteString("<em>")
	}
	if features&emphasisStrikethrough != 0 {
		b.WriteString("<del>")
	}
}
