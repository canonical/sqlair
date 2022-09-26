package parse

import (
	"fmt"
	"strings"
)

// Parser defines the SDL parser
type Parser struct {
	input string // statement to be parsed
	pos   int    // current position of the parser
}

// A checkpoint model for restoring the parser to the state it was
// We only use a checkpoint within an attempted parsing of an part,
// not at a higher level since we don't keep track of the parts in
// the checkpoint
type checkpoint struct {
	// A pointer to the parser
	parser *Parser
	// Info to restore the parser
	pos int
}

func (p *Parser) save() *checkpoint {
	return &checkpoint{
		parser: p,
		pos:    p.pos,
	}
}

func (cp *checkpoint) restore() {
	cp.parser.pos = cp.pos
}

// NewParser creates a new parser
func NewParser() *Parser {
	return &Parser{}
}

// init initializes the parser
func (p *Parser) init(input string) {
	p.input = input
	p.pos = 0
}

// advance moves the parser's index forward
// by one element.
func (p *Parser) advance() bool {
	p.skipSpaces()
	if p.pos == len(p.input) {
		return false
	}
	p.pos++
	return true
}

// A struct for keeping track of the parts of the input parsed so far.
// The vars could possibly do with better names.

//			  		 lastParsedPos         posBeforePart
//	                       |                     |
//	                       |                     |
//
// [PTPart      ][OutPart][PTPart               ][InputPart ]
// SELECT col1,   &Person   FROM t WHERE t.id =   $Person.ID
type parsedExprBuilder struct {
	// The position of Parser.pos when we last finished parsing a part
	lastParsedPos int
	// The position of Parser.pos just before we started parsing the last part
	posBeforePart int
	qParts        []queryPart
}

// Add the parsed part to the parsedExprBuilder along with the BypassPart
// that streaches from the end of the last previously in qParts to the
// beginning of this part.
func (peb *parsedExprBuilder) add(p *Parser, part queryPart) {
	if peb.lastParsedPos != peb.posBeforePart {
		peb.qParts = append(peb.qParts,
			&BypassPart{p.input[peb.lastParsedPos:peb.posBeforePart]})
	}
	if part != nil {
		peb.qParts = append(peb.qParts, part)
	}
	peb.lastParsedPos = p.pos
	peb.posBeforePart = p.pos
}

func (p *Parser) Parse(input string) (*ParsedExpr, error) {
	p.init(input)
	var peb parsedExprBuilder

	for {
		p.skipSpaces()

		peb.posBeforePart = p.pos
		op, ok, err := p.parseOutputExpression()
		if err != nil {
			return nil, err
		} else if ok {
			peb.add(p, op)
		}
		ip, ok, err := p.parseInputExpression()
		if err != nil {
			return nil, err
		} else if ok {
			peb.add(p, ip)
		}
		sp, ok, err := p.parseStringLiteral()
		if err != nil {
			return nil, err
		} else if ok {
			peb.add(p, sp)
		}

		if !p.advance() {
			break
		}
	}
	// Add the rest of the input to the parser
	peb.add(p, nil)
	return &ParsedExpr{peb.qParts}, nil
}

// ParsedExpr represents a parsed expression.
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

// ====== Parser Helper Functions ======
// These return only boolean values

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

// isNameByte returns true if the byte passed as parameter is considered to be
// one that can be part of a name. It returns false otherwise
func isNameByte(c byte) bool {
	return 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' ||
		'0' <= c && c <= '9' || c == '_'
}

// ====== Parser Functions ======
// These functions attempt to parse some construct, they return a bool and that
// construct, if they n't parse they return false, restore the parser and leave
// the default value in  other return type

func (p *Parser) parseIdentifier() (string, bool) {
	p.skipSpaces()
	if p.pos >= len(p.input) {
		return "", false
	}
	if p.peekByte('*') {
		p.pos++
		return "*", true
	}
	mark := p.pos
	if !isNameByte(p.input[p.pos]) {
		return "", false
	}
	var i int
	for i = p.pos; i < len(p.input); i++ {
		if !isNameByte(p.input[i]) {
			break
		}
	}
	p.pos = i
	return p.input[mark:i], true
}

// Parses a column name or a Go type name. If parsing a Go type name then
// struct name is in FullName.Prefix and the field name (if extant) is in
// FullName.Name.
// When parsing a column the table name (if extant) is in FullName.Prefix and
// the column name is in FullName.Name func (p *Parser)
func (p *Parser) parseFullName(isColumnName bool) (FullName, bool) {
	cp := p.save()
	var fn FullName
	p.skipSpaces()
	if id, ok := p.parseIdentifier(); ok {
		fn.Prefix = id
		if p.skipByte('.') {
			if id, ok := p.parseIdentifier(); ok {
				fn.Name = id
				return fn, true
			}
		} else {
			// A column name specified without a table prefix is a name not a
			// prefix
			if isColumnName {
				fn.Name = fn.Prefix
				fn.Prefix = ""
			}
			return fn, true
		}
	}
	cp.restore()
	return fn, false
}

func (p *Parser) parseOutputExpression() (*OutputPart, bool, error) {
	cp := p.save()
	var err error
	var col FullName
	var goType FullName
	var ok bool

	p.skipSpaces()

	// Case 1: The expression has only one part e.g. "&Person".
	if p.skipByte('&') {
		goType, ok = p.parseFullName(false)
		if !ok {
			err = fmt.Errorf("malformed output expression")
		}
		// col here is empty, this could be replaced by the empty list when we
		// startparsing outputTypes as lists
		return &OutputPart{col, goType}, true, err
	}

	// Case 2: The expression contains an AS e.g. "p.col1 AS &Person".
	if col, ok := p.parseFullName(true); ok {
		p.skipSpaces()

		if p.skipString("AS") {
			p.skipSpaces()
			if p.skipByte('&') {
				goType, ok = p.parseFullName(false)
				if !ok {
					err = fmt.Errorf("malformed output expression")
				}
				return &OutputPart{col, goType}, true, err

			}
		}
	}
	cp.restore()
	return nil, false, nil
}

func (p *Parser) parseInputExpression() (*InputPart, bool, error) {
	cp := p.save()
	var err error
	var fn FullName
	var ok bool

	p.skipSpaces()
	if p.skipByte('$') {
		fn, ok = p.parseFullName(false)
		if !ok {
			err = fmt.Errorf("malformed input type")
		}
	} else {
		cp.restore()
		return nil, false, nil
	}
	return &InputPart{fn}, true, err
}

func (p *Parser) parseStringLiteral() (*BypassPart, bool, error) {
	cp := p.save()
	p.skipSpaces()

	var err error

	if p.pos < len(p.input) {
		c := p.input[p.pos]
		if c == '"' || c == '\'' {
			p.skipByte(c)
			if !p.skipByteFind(c) {
				// Reached end of string and didn't find the closing quote
				err = fmt.Errorf("missing right quote in string literal")
			}
			return &BypassPart{p.input[cp.pos:p.pos]}, true, err
		}
	}

	cp.restore()
	return nil, false, err
}
