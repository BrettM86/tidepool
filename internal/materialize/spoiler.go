package materialize

import (
	"regexp"

	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/parser"
	"github.com/yuin/goldmark/text"
	"github.com/yuin/goldmark/util"
)

// Lemmy (and PieFed) spell spoilers with the markdown-it-container syntax,
// which is not CommonMark and which goldmark therefore leaves as paragraph
// text:
//
//	::: spoiler Bonus Panel
//	![](https://lemmy.world/pictrs/image/….jpeg)
//	:::
//
// Lemmy renders that to <details><summary>Bonus Panel</summary>…</details>,
// and social.coves.richtext.facet has a matching #spoiler feature whose
// optional `reason` is exactly the container title. Without a parser the
// marker lines leak into the canonical plaintext AND the hidden content
// renders revealed — so this file teaches goldmark the container as a real
// block, and richtext.go maps it onto the facet.
//
// The fence is three or more colons; a container closes on the first
// all-colon line at least as long as its opener, or at end of input. Nesting
// falls out of goldmark's innermost-first close order rather than
// markdown-it's longer-fence rule — Lemmy authors do not nest spoilers, and
// the degradation (inner closes first) keeps every range well-formed.
const spoilerParserPriority = 750 // between FencedCodeBlock (700) and Blockquote (800)

// spoilerOpenRe matches an opening fence, capturing the colon run and the
// optional title. `\b` after the container name keeps ":::spoilered" from
// opening a spoiler; a bare "::: spoiler" (no title) is valid and titleless.
var spoilerOpenRe = regexp.MustCompile(`^(:{3,})[ \t]*spoiler\b[ \t]*(.*?)[ \t]*$`)

var kindSpoiler = ast.NewNodeKind("LemmySpoiler")

// spoilerNode is a parsed `::: spoiler` container. fence records the opening
// colon run so the matching close can require at least as many.
type spoilerNode struct {
	ast.BaseBlock
	Reason string
	fence  int
}

func (n *spoilerNode) Kind() ast.NodeKind { return kindSpoiler }

func (n *spoilerNode) Dump(source []byte, level int) {
	ast.DumpHelper(n, source, level, map[string]string{"Reason": n.Reason}, nil)
}

// spoilerBlockParser parses the container as a child-bearing block, so its
// contents keep their normal markdown structure (images, emphasis, lists).
type spoilerBlockParser struct{}

func (spoilerBlockParser) Trigger() []byte { return []byte{':'} }

func (spoilerBlockParser) Open(parent ast.Node, reader text.Reader, pc parser.Context) (ast.Node, parser.State) {
	line, _ := reader.PeekLine()
	pos := pc.BlockOffset()
	if pos < 0 {
		return nil, parser.NoChildren
	}
	m := spoilerOpenRe.FindSubmatch(util.TrimRightSpace(line[pos:]))
	if m == nil {
		return nil, parser.NoChildren
	}
	// The fence line is metadata, not content: consume it whole so the title
	// never reaches the plaintext (it becomes the facet's `reason`).
	reader.AdvanceToEOL()
	return &spoilerNode{Reason: string(m[2]), fence: len(m[1])}, parser.HasChildren
}

func (spoilerBlockParser) Continue(node ast.Node, reader text.Reader, pc parser.Context) parser.State {
	spoiler, ok := node.(*spoilerNode)
	if !ok {
		return parser.Close
	}
	line, _ := reader.PeekLine()
	// Mirror the fenced-code rule: a 4-space indent makes the line content,
	// not a fence.
	if w, pos := util.IndentWidth(line, reader.LineOffset()); w < 4 {
		i := pos
		for ; i < len(line) && line[i] == ':'; i++ {
		}
		if i-pos >= spoiler.fence && util.IsBlank(line[i:]) {
			reader.AdvanceToEOL()
			return parser.Close
		}
	}
	return parser.Continue | parser.HasChildren
}

func (spoilerBlockParser) Close(node ast.Node, reader text.Reader, pc parser.Context) {}

func (spoilerBlockParser) CanInterruptParagraph() bool { return true }

func (spoilerBlockParser) CanAcceptIndentedLine() bool { return false }
