package parse

import (
	"bytes"
	"fmt"
	"strings"
)

type Parser struct {
	input string
	pos   int
	// prevPart is the value of pos when we last finished parsing a part.
	prevPart int
	// partStart is the value of pos just before we started parsing the part
	// under pos. We maintain partStart >= prevPart.
	partStart int
	parts     []queryPart
}

func NewParser() *Parser {
	return &Parser{}
}

// init resets the state of the parser and sets the input string.
func (p *Parser) init(input string) {
	p.input = input
	p.pos = 0
	p.prevPart = 0
	p.partStart = 0
	p.parts = []queryPart{}
}

// A checkpoint struct for saving parser state to restore later. We only use
// a checkpoint within an attempted parsing of an part, not at a higher level
// since we don't keep track of the parts in the checkpoint.
type checkpoint struct {
	parser    *Parser
	pos       int
	prevPart  int
	partStart int
	parts     []queryPart
}

// save takes a snapshot of the state of the parser and returns a pointer to a
// checkpoint that represents it.
func (p *Parser) save() *checkpoint {
	return &checkpoint{
		parser:    p,
		pos:       p.pos,
		prevPart:  p.prevPart,
		partStart: p.partStart,
		parts:     p.parts,
	}
}

// restore sets the internal state of the parser to the values stored in the
// checkpoint.
func (cp *checkpoint) restore() {
	cp.parser.pos = cp.pos
	cp.parser.prevPart = cp.prevPart
	cp.parser.partStart = cp.partStart
	cp.parser.parts = cp.parts
}

type idClass int

const (
	columnId idClass = iota
	typeId
)

// ParsedExpr is the AST representation of an SQL expression.
// It has a representation of the original SQL statement in terms of queryParts
// A SQL statement like this:
//
// Select p.* as &Person.* from person where p.name = $Boss.Name
//
// would be represented as:
//
// [BypassPart OutputPart BypassPart InputPart]
type ParsedExpr struct {
	queryParts []queryPart
}

// String returns a textual representation of the AST contained in the
// ParsedExpr for debugging purposes.
func (pe *ParsedExpr) String() string {
	var out bytes.Buffer
	out.WriteString("ParsedExpr[")
	for i, p := range pe.queryParts {
		if i > 0 {
			out.WriteString(" ")
		}
		out.WriteString(p.String())
	}
	out.WriteString("]")
	return out.String()
}

// add pushes the parsed part to the parsedExprBuilder along with the BypassPart
// that stretches from the end of the previous part to the beginning of this
// part.
func (p *Parser) add(part queryPart) {
	// Add the string between the previous I/O part and the current part.
	if p.prevPart != p.partStart {
		p.parts = append(p.parts,
			&BypassPart{p.input[p.prevPart:p.partStart]})
	}

	if part != nil {
		p.parts = append(p.parts, part)
	}

	// Save this position at the end of the part.
	p.prevPart = p.pos
	// Ensure that partStart >= prevPart.
	p.partStart = p.pos
}

// Parse takes an input string and parses the input and output parts. It returns
// a pointer to a ParsedExpr.
func (p *Parser) Parse(input string) (expr *ParsedExpr, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("cannot parse expression: %s", err)
		}
	}()
	p.init(input)

	for {
		p.partStart = p.pos

		if ip, ok, err := p.parseInputExpression(); err != nil {
			return nil, err
		} else if ok {
			p.add(ip)
			continue
		}

		if sp, ok, err := p.parseStringLiteral(); err != nil {
			return nil, err
		} else if ok {
			p.add(sp)
			continue
		}

		if p.pos == len(p.input) {
			break
		}

		// If nothing above can be parsed we advance the parser.
		p.pos++
	}
	// Add any remaining unparsed string input to the parser.
	p.add(nil)
	return &ParsedExpr{p.parts}, nil
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
// one that can be part of a name. It returns false otherwise.
func isNameByte(c byte) bool {
	return 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' ||
		'0' <= c && c <= '9' || c == '_'
}

// skipName returns false if the parser is not on a name. Otherwise it advances
// the parser until it is on the first non name byte and returns true.
func (p *Parser) skipName() bool {
	if p.pos >= len(p.input) {
		return false
	}
	start := p.pos
	for p.pos < len(p.input) && isNameByte(p.input[p.pos]) {
		p.pos++
	}
	return p.pos > start
}

// Functions with the prefix parse attempt to parse some construct. They return
// the construct, and an error and/or a bool that indicates if the the construct
// was successfully parsed.
//
// Return cases:
//  - bool == true, err == nil
//		The construct was sucessfully parsed
//  - bool == false, err != nil
//		The construct was recognised but was not correctly formatted
//  - bool == false, err == nil
//		The construct was not the one we are looking for

// parseIdentifier parses either a name made up only of nameBytes or an
// asterisk.
func (p *Parser) parseIdentifier() (string, bool) {
	if p.skipByte('*') {
		return "*", true
	}

	idStart := p.pos
	if p.skipName() {
		return p.input[idStart:p.pos], true
	}
	return "", false
}

// parseFullName parses a column name or Go field name, optionally dot-prefixed by
// its table or type name respectively.
func (p *Parser) parseFullName(idc idClass) (FullName, bool) {
	cp := p.save()
	var fn FullName
	if id, ok := p.parseIdentifier(); ok {
		fn.Prefix = id
		if p.skipByte('.') {
			if id, ok := p.parseIdentifier(); ok {
				fn.Name = id
				return fn, true
			}
		} else {
			// A column name specified without a table prefix is a name not a
			// prefix.
			if idc == columnId {
				fn.Name = fn.Prefix
				fn.Prefix = ""
			}
			return fn, true
		}
	}
	cp.restore()
	return fn, false
}

// parseInputExpression parses an input expression of the form $Type.name.
func (p *Parser) parseInputExpression() (*InputPart, bool, error) {
	cp := p.save()
	var fn FullName
	var ok bool

	if p.skipByte('$') {
		fn, ok = p.parseFullName(typeId)
		if !ok {
			return nil, false, fmt.Errorf("malformed input type")
		}
		return &InputPart{fn}, true, nil
	}
	cp.restore()
	return nil, false, nil
}

// parseStringLiteral parses quoted expressions and ignores their content.
func (p *Parser) parseStringLiteral() (*BypassPart, bool, error) {
	cp := p.save()

	if p.pos < len(p.input) {
		c := p.input[p.pos]
		if c == '"' || c == '\'' {
			p.skipByte(c)
			// TODO Handle escaping
			if !p.skipByteFind(c) {
				// Reached end of string and didn't find the closing quote.
				return nil, false, fmt.Errorf("missing right quote in string literal")
			}
			return &BypassPart{p.input[cp.pos:p.pos]}, true, nil
		}
	}

	cp.restore()
	return nil, false, nil
}
