package expr

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

// ParsedExpr is the AST representation of an SQL expression. The AST is made up
// of queryParts. For example, a SQL statement like this:
//
// Select p.* as &Person.* from person where p.name = $Boss.col_name
//
// would be represented as:
//
// [bypassPart outputPart bypassPart inputPart]
type ParsedExpr struct {
	queryParts []queryPart
}

// String returns a textual representation of the AST contained in the
// ParsedExpr for debugging purposes.
func (pe *ParsedExpr) String() string {
	var out bytes.Buffer
	out.WriteString("[")
	for i, p := range pe.queryParts {
		if i > 0 {
			out.WriteString(" ")
		}
		out.WriteString(p.String())
	}
	out.WriteString("]")
	return out.String()
}

// add pushes the parsed part to the parsedExprBuilder along with the bypassPart
// that stretches from the end of the previous part to the beginning of this
// part.
func (p *Parser) add(part queryPart) {
	// Add the string between the previous I/O part and the current part.
	if p.prevPart != p.partStart {
		p.parts = append(p.parts,
			&bypassPart{p.input[p.prevPart:p.partStart]})
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

		if out, ok, err := p.parseOutputExpression(); err != nil {
			return nil, err
		} else if ok {
			p.add(out)
			continue
		}

		if in, ok, err := p.parseInputExpression(); err != nil {
			return nil, err
		} else if ok {
			p.add(in)
			continue
		}

		if bypass, ok, err := p.parseStringLiteral(); err != nil {
			return nil, err
		} else if ok {
			p.add(bypass)
			continue
		}

		if p.pos == len(p.input) {
			break
		}

		// If nothing above can be parsed we advance the parser.
		p.advance()
	}
	// Add any remaining unparsed string input to the parser.
	p.add(nil)
	return &ParsedExpr{p.parts}, nil
}

// advance increments p.pos until we reach content that may be the start of a
// token we want to parse.
func (p *Parser) advance() {

	quoteBytes := map[byte]bool{
		'"':  true,
		'\'': true,
	}

	// The byte following these bytes might be the start of an expression.
	delimiterBytes := map[byte]bool{
		' ':  true,
		'\t': true,
		'\n': true,
		'\r': true,
	}

	p.pos++
	for p.pos < len(p.input) && !quoteBytes[p.input[p.pos]] &&
		!delimiterBytes[p.input[p.pos]] {
		p.pos++
	}

	if p.pos < len(p.input) && delimiterBytes[p.input[p.pos]] {
		p.pos++
	}

	p.skipSpaces()
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

// isNameByte returns true if the given byte can be part of a name. It returns
// false otherwise.
func isNameByte(c byte) bool {
	return 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' ||
		'0' <= c && c <= '9' || c == '_'
}

// isNameInitialByte returns true if the given byte can appear at the start of a
// name. It returns false otherwise.
func isInitialNameByte(c byte) bool {
	return 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' || c == '_'
}

// skipName advances the parser until it is on the first non name byte and
// returns true. If the p.pos does not start on a name byte it returns false.
func (p *Parser) skipName() bool {
	if p.pos >= len(p.input) {
		return false
	}
	mark := p.pos
	if isInitialNameByte(p.input[p.pos]) {
		p.pos++
		for p.pos < len(p.input) && isNameByte(p.input[p.pos]) {
			p.pos++
		}
	}
	return p.pos > mark
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

// parseIdentifierAsterisk parses a name made up of only nameBytes or of a
// single asterisk. On success it returns the parsed string and true. Otherwise,
// it returns the empty string and false.
func (p *Parser) parseIdentifierAsterisk() (string, bool) {
	if p.skipByte('*') {
		return "*", true
	}
	return p.parseIdentifier()
}

// parseIdentifier parses a name made up of only nameBytes. On success it
// returns the parsed string and true. Otherwise, it returns the empty string
// and false.
func (p *Parser) parseIdentifier() (string, bool) {
	mark := p.pos
	if p.skipName() {
		return p.input[mark:p.pos], true
	}
	return "", false
}

// parseColumn parses a column made up of name bytes, optionally dot-prefixed by
// its table name.
// parseColumn returns an error so that it can be used with parseList.
func (p *Parser) parseColumn() (fullName, bool, error) {
	cp := p.save()

	if id, ok := p.parseIdentifierAsterisk(); ok {
		if id != "*" && p.skipByte('.') {
			if idCol, ok := p.parseIdentifierAsterisk(); ok {
				return fullName{prefix: id, name: idCol}, true, nil
			}
		} else {
			// A column name specified without a table prefix should be in name.
			return fullName{name: id}, true, nil
		}
	}

	cp.restore()
	return fullName{}, false, nil
}

func (p *Parser) parseTarget() (fullName, bool, error) {
	if p.skipByte('&') {
		return p.parseGoFullName()
	}

	return fullName{}, false, nil
}

// parseGoFullName parses a Go type name qualified by a tag name (or asterisk)
// of the form "&TypeName.col_name".
func (p *Parser) parseGoFullName() (fullName, bool, error) {
	cp := p.save()

	if id, ok := p.parseIdentifier(); ok {
		if !p.skipByte('.') {
			return fullName{}, false, fmt.Errorf("column %d: type not qualified", p.pos)
		}

		idField, ok := p.parseIdentifierAsterisk()
		if !ok {
			return fullName{}, false, fmt.Errorf("column %d: invalid identifier", p.pos)
		}
		return fullName{id, idField}, true, nil
	}

	cp.restore()
	return fullName{}, false, nil
}

// parseList takes a parsing function that returns a fullName and parses a
// bracketed, comma seperated, list.
func (p *Parser) parseList(parseFn func(p *Parser) (fullName, bool, error)) ([]fullName, bool, error) {
	cp := p.save()
	if !p.skipByte('(') {
		return nil, false, nil
	}

	parenPos := p.pos

	nextItem := true
	var objs []fullName
	for i := 0; nextItem; i++ {
		p.skipSpaces()
		if obj, ok, err := parseFn(p); ok {
			objs = append(objs, obj)
		} else if err != nil {
			return nil, false, err
		} else if i == 0 {
			// If the first item is not what we are looking for, we exit.
			cp.restore()
			return nil, false, nil
		} else {
			// On subsequent items we return an error.
			return nil, false, fmt.Errorf("column %d: invalid expression", p.pos)
		}

		p.skipSpaces()
		if p.skipByte(')') {
			return objs, true, nil
		}

		nextItem = p.skipByte(',')
	}
	return nil, false, fmt.Errorf("column %d: missing closing parentheses", parenPos)
}

// parseColumns parses a list of columns. For lists of more than one column the
// columns must be enclosed in brackets e.g. "(col1, col2) AS &Person.*".
func (p *Parser) parseColumns() ([]fullName, bool) {

	// Case 1: A single column e.g. "p.name".
	if col, ok, _ := p.parseColumn(); ok {
		return []fullName{col}, true
	}

	// Case 2: Multiple columns e.g. "(p.name, p.id)".
	if cols, ok, _ := p.parseList((*Parser).parseColumn); ok {
		return cols, true
	}

	return nil, false
}

// parseTargets parses the part of the output expression following the
// ampersand. This can be one or more references to Go types.
func (p *Parser) parseTargets() ([]fullName, bool, error) {
	// Case 1: A single target e.g. "&Person.name".
	if target, ok, err := p.parseTarget(); err != nil {
		return nil, false, err
	} else if ok {
		return []fullName{target}, true, nil
	}

	// Case 2: Multiple targets e.g. "(&Person.name, &Person.id)".
	if targets, ok, err := p.parseList((*Parser).parseTarget); err != nil {
		return nil, false, err
	} else if ok {
		return targets, true, nil
	}

	return nil, false, nil
}

// parseOutputExpression requires that the ampersand before the identifiers must
// be preceded by a space and followed by a name byte.
func (p *Parser) parseOutputExpression() (*outputPart, bool, error) {

	// Case 1: There are no columns e.g. "&Person.*".
	if targets, ok, err := p.parseTargets(); err != nil {
		return nil, false, err
	} else if ok {
		return &outputPart{[]fullName{}, targets}, true, nil
	}

	cp := p.save()

	// Case 2: There are columns e.g. "p.col1 AS &Person.*".
	if cols, ok := p.parseColumns(); ok {
		p.skipSpaces()
		if p.skipString("AS") {
			p.skipSpaces()
			if targets, ok, err := p.parseTargets(); err != nil {
				return nil, false, err
			} else if ok {
				return &outputPart{cols, targets}, true, nil
			}
		}
	}

	cp.restore()
	return nil, false, nil
}

// parseInputExpression parses an input expression of the form "$Type.name".
func (p *Parser) parseInputExpression() (*inputPart, bool, error) {
	cp := p.save()

	if p.skipByte('$') {
		if fn, ok, err := p.parseGoFullName(); ok {
			if fn.name == "*" {
				return nil, false, fmt.Errorf("asterisk not allowed "+
					"in expression near %d", p.pos)
			}
			return &inputPart{fn}, true, nil
		} else if err != nil {
			return nil, false, err
		}
	}

	cp.restore()
	return nil, false, nil
}

// parseStringLiteral parses quoted expressions and ignores their content
// including escaped quotes.
func (p *Parser) parseStringLiteral() (*bypassPart, bool, error) {
	cp := p.save()

	if p.skipByte('"') || p.skipByte('\'') {
		c := p.input[p.pos-1]
		// We keep track of whether the next quote has been previously
		// escaped. If not, it might be a closer.
		maybeCloser := true
		for p.skipByteFind(c) {
			// If this looks like a closing quote, check if it might be an
			// escape for a following quote. If not, we're done.
			if maybeCloser && !p.peekByte(c) {
				return &bypassPart{p.input[cp.pos:p.pos]}, true, nil
			}
			maybeCloser = !maybeCloser
		}

		// Reached end of string and didn't find the closing quote
		return nil, false, fmt.Errorf("column %d: missing right quote in string literal", cp.pos)
	}
	cp.restore()
	return nil, false, nil
}
