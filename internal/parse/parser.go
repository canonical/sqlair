package parse

import "strings"

// QueryPart defines a simple interface for all the different parts that
// compose a ParsedExpr.
type QueryPart interface {
	// String returns the original string comprising this query part.
	String() string

	// ToSQL returns the executable SQL resulting from this query part.
	ToSQL() (string, error)
}

// Parser is used to parse the SQLAir DSL.
type Parser struct {
	// input is the DSL statement to be parted.
	input string

	// lastParsedPos is the character position of the last parsed part.
	lastParsedPos int

	// pos is the current character position of the parser.
	pos int
}

// NewParser returns a reference to a new parser.
func NewParser() *Parser {
	return &Parser{}
}

// init initializes the parser.
func (p *Parser) init(input string) {
	p.input = input
	p.lastParsedPos = 0
	p.pos = 0
}

func (p *Parser) Parse(input string) (*ParsedExpr, error) {
	p.init(input)
	return nil, nil
}

// ParsedExpr represents a parsed expression.
// It has a representation of the original SQL statement in terms of QueryParts
// A SQL statement like this:
//
// Select p.* as &Person.* from person where p.name = $Boss.Name
//
// would be represented as:
//
// [stringPart outputPart stringPart inputPart]
type ParsedExpr struct {
	queryParts []QueryPart
}

// peekByte returns true if the current byte
// equals the one passed as parameter.
func (p *Parser) peekByte(b byte) bool {
	return p.pos < len(p.input) && p.input[p.pos] == b
}

// skipByte jumps over the current byte if it matches
// the byte passed as a parameter. Returns true in that case, false otherwise.
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
