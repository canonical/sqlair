package expr

import (
	"bytes"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

type Parser struct {
	input string
	pos   int
	// nextPos is start of the next char.
	nextPos int
	// char is the rune starting at pos. char is set to 0 at the end of input.
	char rune
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
	p.nextPos = 0
	p.char = 0
	p.prevPart = 0
	p.partStart = 0
	p.parts = []queryPart{}
	p.nextChar()
}

// nextChar reads the next character from the
// input and increments the position.
func (p *Parser) nextChar() {
	if p.nextPos >= len(p.input) {
		p.char = 0
		p.pos = p.nextPos
		return
	}

	var size int
	p.char, size = utf8.DecodeRuneInString(p.input[p.nextPos:])
	p.pos = p.nextPos
	p.nextPos += size
}

// A checkpoint struct for saving parser state to restore later. We only use
// a checkpoint within an attempted parsing of an part, not at a higher level
// since we don't keep track of the parts in the checkpoint.
type checkpoint struct {
	parser    *Parser
	pos       int
	nextPos   int
	char      rune
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
		nextPos:   p.nextPos,
		char:      p.char,
		prevPart:  p.prevPart,
		partStart: p.partStart,
		parts:     p.parts,
	}
}

// restore sets the internal state of the parser to the values stored in the
// checkpoint.
func (cp *checkpoint) restore() {
	cp.parser.pos = cp.pos
	cp.parser.nextPos = cp.nextPos
	cp.parser.char = cp.char
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

// skipComment jumps over comments as defined by the SQLite spec.
// If no comment is found the parser state is left unchanged.
func (p *Parser) skipComment() bool {
	cp := p.save()
	c := p.char
	if p.skipChar('-') || p.skipChar('/') {
		if (c == '-' && p.skipChar('-')) || (c == '/' && p.skipChar('*')) {
			var end rune
			if c == '-' {
				end = '\n'
			} else {
				end = '*'
			}
			for p.pos < len(p.input) {
				if p.char == end {
					// if end == '\n' (i.e. its a -- comment) dont p.pos++ to keep the newline.
					if end == '*' {
						p.nextChar()
						if !p.skipChar('/') {
							continue
						}
					}
					return true
				}
				p.nextChar()
			}
			// Reached end of input (valid comment end).
			return true
		}
		cp.restore()
		return false
	}
	return false
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
		// Advance the parser to the start of the next expression.
		if err := p.advance(); err != nil {
			return nil, err
		}

		p.partStart = p.pos

		if p.pos >= len(p.input) {
			break
		}

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
	}

	// Add any remaining unparsed string input to the parser.
	p.add(nil)
	return &ParsedExpr{p.parts}, nil
}

// advance increments p.pos until it reaches content that might preceed a token
// we want to parse.
func (p *Parser) advance() error {

loop:
	for p.pos < len(p.input) {
		if ok, err := p.skipStringLiteral(); err != nil {
			return err
		} else if ok {
			continue
		}
		if ok := p.skipComment(); ok {
			continue
		}

		c := p.char
		p.nextChar()
		switch c {
		// If the preceding byte is one of these then we might be at the start
		// of an expression.
		case ' ', '\t', '\n', '\r', '=', ',', '(', '[', '>', '<', '+', '-', '*', '/', '|', '%':
			break loop
		}
	}

	p.skipBlanks()

	return nil

}

// skipStringLiteral jumps over single and double quoted sections of input.
// Doubled up quotes are escaped.
func (p *Parser) skipStringLiteral() (bool, error) {
	cp := p.save()

	c := p.char
	if p.skipChar('"') || p.skipChar('\'') {

		// We keep track of whether the next quote has been previously
		// escaped. If not, it might be a closing quote.
		maybeCloser := true
		for p.skipCharFind(c) {
			// If this looks like a closing quote, check if it might be an
			// escape for a following quote. If not, we're done.
			if maybeCloser && !p.peekChar(c) {
				return true, nil
			}
			maybeCloser = !maybeCloser
		}

		// Reached end of string and didn't find the closing quote
		cp.restore()
		return false, fmt.Errorf("column %d: missing closing quote in string literal", cp.pos)
	}
	return false, nil
}

// peekChar returns true if the current char equals the one passed as parameter.
func (p *Parser) peekChar(c rune) bool {
	return p.pos < len(p.input) && p.char == c
}

// skipChar jumps over the current char if it matches the char passed as a
// parameter. Returns true in that case, false otherwise.
func (p *Parser) skipChar(c rune) bool {
	if p.pos < len(p.input) && p.char == c {
		p.nextChar()
		return true
	}
	return false
}

// skipCharFind advances the parser until it finds a char that matches the one
// passed as parameter and then jumps over it. In that case returns true. If the
// end of the string is reached and no matching char was found, it returns
// false.
func (p *Parser) skipCharFind(c rune) bool {
	cp := p.save()
	for p.pos < len(p.input) {
		if p.char == c {
			p.nextChar()
			return true
		}
		p.nextChar()
	}
	cp.restore()
	return false
}

// skipBlanks advances the parser past spaces, tabs and newlines. Returns
// whether the parser position was changed.
func (p *Parser) skipBlanks() bool {
	mark := p.pos
	for p.pos < len(p.input) {
		if ok := p.skipComment(); ok {
			continue
		}
		switch p.char {
		case ' ', '\t', '\r', '\n':
			p.nextChar()
		default:
			return p.pos != mark
		}
	}
	return p.pos != mark
}

// skipString advances the parser and jumps over the string passed as parameter.
// In that case returns true, false otherwise.
// This function is case insensitive.
func (p *Parser) skipString(s string) bool {
	if p.pos+len(s) <= len(p.input) &&
		strings.EqualFold(p.input[p.pos:p.pos+len(s)], s) {
		// Manually advance the parser to the end of the string.
		p.pos += len(s)
		var size int
		p.char, size = utf8.DecodeRuneInString(p.input[p.pos:])
		p.nextPos = p.pos + size
		return true
	}
	return false
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

// parseIdentifierAsterisk parses an identifier or an asterisk.
func (p *Parser) parseIdentifierAsterisk() (string, bool) {
	if p.skipChar('*') {
		return "*", true
	}
	return p.parseIdentifier()
}

// parseIdentifier parses a name starting with a letter or underscore and
// followed by letters, digits and underscores. This matches the allowed
// characters in Go type names.
func (p *Parser) parseIdentifier() (string, bool) {
	mark := p.pos

	if unicode.IsLetter(p.char) || p.char == '_' {
		p.nextChar()
		for p.pos < len(p.input) && (unicode.IsLetter(p.char) ||
			unicode.IsDigit(p.char) || p.char == '_') {
			p.nextChar()
		}
	}

	if p.pos > mark {
		return p.input[mark:p.pos], true
	}
	return "", false
}

// skipColumnName parses column names inside quotes as well as regular columns.
func (p *Parser) parseColumnName() (string, bool, error) {
	if p.skipChar('*') {
		return "*", true, nil
	}

	mark := p.pos

	if ok, err := p.skipStringLiteral(); err != nil {
		return "", false, err
	} else if ok {
		return p.input[mark:p.pos], true, nil
	}

	for p.pos < len(p.input) && (unicode.IsLetter(p.char) ||
		unicode.IsDigit(p.char) || p.char == '_') {
		p.nextChar()
	}
	if p.pos > mark {
		return p.input[mark:p.pos], true, nil
	}
	return "", false, nil
}

// parseColumn parses a column made up of name bytes, optionally dot-prefixed by
// its table name.
func (p *Parser) parseColumn() (fullName, bool, error) {
	cp := p.save()

	if id, ok, err := p.parseColumnName(); err != nil {
		cp.restore()
		return fullName{}, false, err
	} else if ok {
		if id != "*" && p.skipChar('.') {
			if idCol, ok, err := p.parseColumnName(); err != nil {
				cp.restore()
				return fullName{}, false, err
			} else if ok {
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

func (p *Parser) parseTargetType() (fullName, bool, error) {
	if p.skipChar('&') {
		return p.parseGoFullName()
	}

	return fullName{}, false, nil
}

// parseGoFullName parses a Go type name qualified by a tag name (or asterisk)
// of the form "&TypeName.col_name".
func (p *Parser) parseGoFullName() (fullName, bool, error) {
	cp := p.save()

	if id, ok := p.parseIdentifier(); ok {
		if !p.skipChar('.') {
			return fullName{}, false, fmt.Errorf("column %d: unqualified type, expected %s.* or %s.<db tag>", p.pos, id, id)
		}

		idField, ok := p.parseIdentifierAsterisk()
		if !ok {
			return fullName{}, false, fmt.Errorf("column %d: invalid identifier suffix following %q", p.pos, id)
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
	if !p.skipChar('(') {
		return nil, false, nil
	}

	parenPos := p.pos
	nextItem := true
	var objs []fullName
	for i := 0; nextItem; i++ {
		p.skipBlanks()
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
			return nil, false, fmt.Errorf("column %d: invalid expression in list", p.pos)
		}

		p.skipBlanks()
		if p.skipChar(')') {
			return objs, true, nil
		}

		nextItem = p.skipChar(',')
	}
	return nil, false, fmt.Errorf("column %d: missing closing parentheses", parenPos)
}

// parseColumns parses a single column or a list of columns. Lists must be
// enclosed in parentheses.
func (p *Parser) parseColumns() (cols []fullName, parentheses bool, ok bool) {
	// Case 1: A single column e.g. "p.name".
	if col, ok, _ := p.parseColumn(); ok {
		return []fullName{col}, false, true
	}

	// Case 2: Multiple columns e.g. "(p.name, p.id)".
	if cols, ok, _ := p.parseList((*Parser).parseColumn); ok {
		return cols, true, true
	}

	return nil, false, false
}

// parseTargetTypes parses a single output type or a list of output types.
// Lists of types must be enclosed in parentheses.
func (p *Parser) parseTargetTypes() (types []fullName, parentheses bool, ok bool, err error) {
	// Case 1: A single target e.g. "&Person.name".
	if targetTypes, ok, err := p.parseTargetType(); err != nil {
		return nil, false, false, err
	} else if ok {
		return []fullName{targetTypes}, false, true, nil
	}

	// Case 2: Multiple types e.g. "(&Person.name, &Person.id)".
	if targetTypes, ok, err := p.parseList((*Parser).parseTargetType); err != nil {
		return nil, true, false, err
	} else if ok {
		return targetTypes, true, true, nil
	}

	return nil, false, false, nil
}

// parseOutputExpression requires that the ampersand before the identifiers must
// be followed by a name byte.
func (p *Parser) parseOutputExpression() (*outputPart, bool, error) {
	start := p.pos

	// Case 1: There are no columns e.g. "&Person.*".
	if targetType, ok, err := p.parseTargetType(); err != nil {
		return nil, false, err
	} else if ok {
		return &outputPart{
			sourceColumns: []fullName{},
			targetTypes:   []fullName{targetType},
			raw:           p.input[start:p.pos],
		}, true, nil
	}

	cp := p.save()

	// Case 2: There are columns e.g. "p.col1 AS &Person.*".
	if cols, parenCols, ok := p.parseColumns(); ok {
		p.skipBlanks()
		if p.skipString("AS") {
			p.skipBlanks()
			if targetTypes, parenTypes, ok, err := p.parseTargetTypes(); err != nil {
				return nil, false, err
			} else if ok {
				if parenCols && !parenTypes {
					return nil, false, fmt.Errorf(`column %d: missing parentheses around types after "AS"`, p.pos)
				}
				if !parenCols && parenTypes {
					return nil, false, fmt.Errorf(`column %d: unexpected parentheses around types after "AS"`, p.pos)
				}
				return &outputPart{
					sourceColumns: cols,
					targetTypes:   targetTypes,
					raw:           p.input[start:p.pos],
				}, true, nil
			}
		}
	}

	cp.restore()
	return nil, false, nil
}

// parseInputExpression parses an input expression of the form "$Type.name".
func (p *Parser) parseInputExpression() (*inputPart, bool, error) {
	cp := p.save()

	if p.skipChar('$') {
		if fn, ok, err := p.parseGoFullName(); ok {
			if fn.name == "*" {
				return nil, false, fmt.Errorf(`asterisk not allowed in input expression "$%s"`, fn)
			}
			return &inputPart{sourceType: fn, raw: p.input[cp.pos:p.pos]}, true, nil
		} else if err != nil {
			return nil, false, err
		}
	}

	cp.restore()
	return nil, false, nil
}
