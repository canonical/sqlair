package parse

import (
	"strings"
)

// Parser defines the SDL parser.
type Parser struct {
	input string
	pos   int
}

// NewParser returns a pointer to a Parser.
func NewParser() *Parser {
	return &Parser{}
}

// init resets the state of the parser and sets the input string.
func (p *Parser) init(input string) {
	p.input = input
	p.pos = 0
}

// A checkpoint model for restoring the parser to a saved state. We only use a
// checkpoint within an attempted parsing of an part, not at a higher level
// since we don't keep track of the parts in the checkpoint.
type checkpoint struct {
	parser *Parser
	pos    int
}

// save takes a snapshot of the state of the parser and returns a pointer to a
// checkpoint that represents it.
func (p *Parser) save() *checkpoint {
	return &checkpoint{
		parser: p,
		pos:    p.pos,
	}
}

// restore sets the internal state of the parser to the values stored in the
// checkpoint.
func (cp *checkpoint) restore() {
	cp.parser.pos = cp.pos
}

// advance moves the parser's index forward by one element.
func (p *Parser) advance() bool {
	p.skipSpaces()
	if p.pos == len(p.input) {
		return false
	}
	p.pos++
	return true
}

// ParsedExpr is the AST representation of an SDL statement.
// It has a representation of the original SQL statement in terms of queryParts
// A SQL statement like this:
//
// Select p.* as &Person.* from person where p.name = $Boss.Name
//
// would be represented as:
//
// [stringPart outputPart stringPart inputPart]
type ParsedExpr struct {
	queryParts []queryPart
}

// String returns a textual representation of the AST contained in the
// ParsedExpr for debugging purposes.
func (pe *ParsedExpr) String() string {
	out := "ParsedExpr["
	for i, p := range pe.queryParts {
		if i > 0 {
			out = out + " "
		}
		out = out + p.String()
	}
	out = out + "]"
	return out
}

// parsedExprBuilder keeps track of the parts parsed so far.
type parsedExprBuilder struct {
	// prevPart is the position of Parser.pos when we last finished parsing a
	// part.
	prevPart int
	// partStart is the position of Parser.pos just before we started parsing
	// the current part. We should maintain partStart >= prevPart.
	partStart int
	parts     []queryPart
}

// add pushes the parsed part to the parsedExprBuilder along with the BypassPart
// that stretches from the end of the previous part to the beginning of this
// part.
func (peb *parsedExprBuilder) add(p *Parser, part queryPart) {
	// Add the string between the previous I/O part and the current part.
	if peb.prevPart != peb.partStart {
		peb.parts = append(peb.parts,
			&BypassPart{p.input[peb.prevPart:peb.partStart]})
	}

	if part != nil {
		peb.parts = append(peb.parts, part)
	}

	// Save this position at the end of the part.
	peb.prevPart = p.pos
	// Ensure that partStart >= prevPart.
	peb.partStart = p.pos
}

// Parse takes an input string and parses the input and output parts. It returns
// a pointer to a ParsedExpr.
func (p *Parser) Parse(input string) (*ParsedExpr, error) {
	p.init(input)
	var peb parsedExprBuilder
	for {
		p.skipSpaces()
		peb.partStart = p.pos
		if !p.advance() {
			break
		}
	}
	// Add any remaining unparsed string input to the parser
	peb.add(p, nil)
	return &ParsedExpr{peb.parts}, nil
}

// peekByte returns true if the current byte equals the one passed as parameter.
func (p *Parser) peekByte(b byte) bool {
	return p.pos < len(p.input) && p.input[p.pos] == b
}

// skipByte jumps over the current byte if it matches the byte passed as a
// parameter. Returns true in that case, false otherwise.
func (p *Parser) skipByte(b byte) bool {
	if p.pos < len(p.input) && p.input[p.pos] == b {
		p.pos++
		return true
	}
	return false
}

// skipByteFind advances the parser until it finds a byte that matches the one
// passed as parameter and then jumps over it. In that case returns true. If the
// end of the string is reached and no matching byte was found, it returns
// false.
func (p *Parser) skipByteFind(b byte) bool {
	for i := p.pos; i < len(p.input); i++ {
		if p.input[i] == b {
			p.pos = i + 1
			return true
		}
	}
	return false
}

// skipSpaces advances the parser jumping over consecutive spaces. It stops when
// finding a non-space character. Returns true if the parser position was
// actually changed, false otherwise.
func (p *Parser) skipSpaces() bool {
	mark := p.pos
	for p.pos < len(p.input) {
		if p.input[p.pos] != ' ' {
			break
		}
		p.pos++
	}
	return p.pos != mark
}

// skipString advances the parser and jumps over the string passed as parameter.
// In that case returns true, false otherwise.
// This function is case insensitive.
func (p *Parser) skipString(s string) bool {
	if p.pos+len(s) <= len(p.input) &&
		strings.EqualFold(p.input[p.pos:p.pos+len(s)], s) {
		p.pos += len(s)
		return true
	}
	return false
}

// isNameByte returns true if the byte passed as parameter is considered to be
// one that can be part of a name. It returns false otherwise
func isNameByte(c byte) bool {
	return 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' ||
		'0' <= c && c <= '9' || c == '_'
}
