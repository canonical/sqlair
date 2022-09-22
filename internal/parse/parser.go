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

func (p *Parser) Parse(input string) (*ParsedExpr, error) {
	p.init(input)
	var qps []QueryPart

	op, ok, err := p.parseOutputExpression()
	if err != nil {
		return nil, err
	} else if ok {
		qps = append(qps, op)
	}

	ip, ok, err := p.parseInputExpression()
	if err != nil {
		return nil, err
	} else if ok {
		qps = append(qps, ip)
	}

	return &ParsedExpr{qps}, nil
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
// construct, if they can't parse they return false, restore the parser and leave
// the default value in the other return type

func (p *Parser) parseIdentifier() (string, bool) {
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

// This parses a qualified expression. It will not parse unqualified names
func (p *Parser) parseQualifiedExpression() (qualifiedExpression, bool) {
	cp := p.save()
	var qe qualifiedExpression
	p.skipSpaces()
	if id, ok := p.parseIdentifier(); ok {
		qe.qualifier = id
		if p.skipByte('.') {
			if id, ok := p.parseIdentifier(); ok {
				qe.name = id
				// We have parsed a full qualified expression
				return qe, true
			}
		}
	}
	cp.restore()
	return qe, false
}

func (p *Parser) parseGoType() (goType, bool) {
	var gt goType

	// It could be a qulaified or unqualified expression
	if qe, ok := p.parseQualifiedExpression(); ok {
		gt = goType{qe}
	} else if id, ok := p.parseIdentifier(); ok {
		gt = goType{qualifiedExpression{id, ""}}
	} else {
		return gt, false
	}
	return gt, true
}

func (p *Parser) parseSQLColumn() (column, bool) {
	var c column

	// It could be a qulaified or unqualified expression
	if qe, ok := p.parseQualifiedExpression(); ok {
		c = column{qe}
	} else if id, ok := p.parseIdentifier(); ok {
		c = column{qualifiedExpression{"", id}}
	} else {
		return c, false
	}
	return c, true
}

func (p *Parser) parseOutputExpression() (*OutputPart, bool, error) {
	cp := p.save()
	var err error
	var op OutputPart

	p.skipSpaces()

	// Case 1: The expression has only one part e.g. "&Person".

	if p.skipByte('&') {
		if gt, ok := p.parseGoType(); ok {
			op.GoType = gt
		} else {
			err = fmt.Errorf("malformed output expression")
		}

		// Case 2: The expression contains an AS e.g. "p.col1 AS &Person".

	} else if c, ok := p.parseSQLColumn(); ok {
		var columns []column
		columns = append(columns, c)
		p.skipSpaces()
		for p.skipByte(',') {
			p.skipSpaces()
			if c, ok := p.parseSQLColumn(); ok {
				columns = append(columns, c)
				p.skipSpaces()
			} else {
				err = fmt.Errorf("unexpected comma")
				break
			}
		}
		op.Columns = columns[0] // Remove [0] when columns becomes multiple
		if err != nil && p.skipString("AS") {
			p.skipSpaces()
			if p.skipByte('&') {
				if gt, ok := p.parseGoType(); ok {
					op.GoType = gt
				} else {
					err = fmt.Errorf("malformed output expression")
				}
			} else {
				cp.restore()
			}
		} else {
			cp.restore()
			return nil, false, nil
		}
	} else {
		cp.restore()
		return nil, false, nil
	}

	return &op, true, err
}

func (p *Parser) parseInputExpression() (*InputPart, bool, error) {
	cp := p.save()
	var ie InputPart
	var err error

	p.skipSpaces()
	if p.skipByte('$') {
		if gt, ok := p.parseGoType(); ok {
			if gt.TagName() == "" {
				err = fmt.Errorf("no qualifier in input expression")
			}
			ie = InputPart{gt}
		} else {
			err = fmt.Errorf("malformed input type")
		}
	} else {
		cp.restore()
		return nil, false, nil
	}
	return &ie, true, err
}
