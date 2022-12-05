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
	parts     []QueryPart
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
	p.parts = []QueryPart{}
}

// A checkpoint struct for saving parser state to restore later. We only use
// a checkpoint within an attempted parsing of an part, not at a higher level
// since we don't keep track of the parts in the checkpoint.
type checkpoint struct {
	parser    *Parser
	pos       int
	prevPart  int
	partStart int
	parts     []QueryPart
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
// of QueryParts. For example, a SQL statement like this:
//
// Select p.* as &Person.* from person where p.name = $Boss.col_name
//
// would be represented as:
//
// [BypassPart OutputPart BypassPart InputPart]
type ParsedExpr struct {
	QueryParts []QueryPart
}

// String returns a textual representation of the AST contained in the
// ParsedExpr for debugging purposes.
func (pe *ParsedExpr) String() string {
	var out bytes.Buffer
	out.WriteString("ParsedExpr[")
	for i, p := range pe.QueryParts {
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
func (p *Parser) add(part QueryPart) {
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

		if op, ok, err := p.parseOutputExpression(); err != nil {
			return nil, err
		} else if ok {
			p.add(op)
			continue
		}

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
// its table name. parseColumn returns an error so that it can be used with
// parseList.
func (p *Parser) parseColumn() (FullName, bool, error) {
	cp := p.save()
	if id, ok := p.parseIdentifierAsterisk(); ok {
		if p.skipByte('.') {
			if idCol, ok := p.parseIdentifierAsterisk(); ok {
				return FullName{Prefix: id, Name: idCol}, true, nil
			}
		} else {
			// A column name specified without a table prefix should be in Name.
			return FullName{Name: id}, true, nil
		}
	}
	cp.restore()
	return FullName{}, false, nil
}

// parseGoFullName parses a Go type name qualified by a tag name (or asterisk)
// of the form "TypeName.col_name". On success it returns the parsed FullName,
// true and nil. If a Go full name is found, but not formatted correctly, false
// and an error are returned. Otherwise the error is nil.
func (p *Parser) parseGoFullName() (FullName, bool, error) {
	cp := p.save()
	if id, ok := p.parseIdentifier(); ok {
		if p.skipByte('.') {
			if idField, ok := p.parseIdentifierAsterisk(); ok {
				return FullName{id, idField}, true, nil
			}
			return FullName{}, false,
				fmt.Errorf("invalid identifier near char %d", p.pos)
		}
		return FullName{}, false,
			fmt.Errorf("go object near char %d not qualified", p.pos)
	}
	cp.restore()
	return FullName{}, false, nil
}

// parseList takes a parsing function that returns a FullName and parses a
// bracketed, comma seperated, list. On success it returns an array of FullName
// objects. Otherwise it returns an empty array, false and an error.
func (p *Parser) parseList(parseFn func(p *Parser) (FullName, bool, error)) ([]FullName, bool, error) {
	cp := p.save()
	if p.skipByte('(') {
		parenPos := p.pos
		p.skipSpaces()

		nextItem := true
		var objs []FullName
		for nextItem {
			if obj, ok, err := parseFn(p); ok {
				objs = append(objs, obj)
				p.skipSpaces()
			} else if err != nil {
				return nil, false, err
			} else {
				return nil, false, fmt.Errorf("invalid identifier near char %d", p.pos)
			}
			p.skipSpaces()
			if p.skipByte(')') {
				return objs, true, nil
			}
			nextItem = p.skipByte(',')
			p.skipSpaces()
		}
		return nil, false, fmt.Errorf("missing closing parentheses for char %d", parenPos)
	}
	cp.restore()
	return nil, false, nil
}

// parseColumns parses text in the SQL query of the form "table.colname". If
// there is more than one column then the columns must be enclosed in brackets
// e.g.  "(col1, col2) AS &Person.*".
func (p *Parser) parseColumns() ([]FullName, bool) {
	cp := p.save()
	// We skip a space here to keep consistent with parseTargets which also
	// consumes one space before the start of the expression.
	p.skipByte(' ')
	// Case 1: A single column e.g. p.name
	if col, ok, _ := p.parseColumn(); ok {
		return []FullName{col}, true
	} else if cols, ok, _ := p.parseList((*Parser).parseColumn); ok {
		return cols, true
	}
	cp.restore()
	return nil, false
}

// parseTargets parses the part of the output expression following the
// ampersand. This can be one or more Go objects. If the ampersand is not found
// or is not preceded by a space and succeeded by a name or opening bracket the
// targets are not parsed.
func (p *Parser) parseTargets() ([]FullName, bool, error) {
	cp := p.save()

	// An '&' must be preceded by a space and succeeded by a name or opening
	// bracket.
	if p.skipString(" &") {
		// Case 1: A single target e.g. &Person.name
		if target, ok, err := p.parseGoFullName(); ok {
			return []FullName{target}, true, nil
		} else if err != nil {
			return nil, false, err
			// Case 2: Multiple targets e.g. &(Person.name, Person.id)
		} else if targets, ok, err := p.parseList((*Parser).parseGoFullName); ok {
			if starCount(targets) > 1 {
				return nil, false, fmt.Errorf("more than one asterisk in expression near char %d", p.pos)
			}
			return targets, true, nil
		} else if err != nil {
			return nil, false, err
		}
	}
	cp.restore()
	return nil, false, nil
}

// starCount returns the number of FullNames in the argument with a asterisk in
// the Name field.
func starCount(fns []FullName) int {
	s := 0
	for _, fn := range fns {
		if fn.Name == "*" {
			s++
		}
	}
	return s
}

// parseOutputExpression parses all output expressions. The ampersand must be
// preceded by a space and followed by a name byte.
func (p *Parser) parseOutputExpression() (op *OutputPart, ok bool, err error) {
	cp := p.save()
	var cols []FullName
	var targets []FullName

	// Case 1: simple case with no columns e.g. &Person.*
	if targets, ok, err = p.parseTargets(); ok {
		return &OutputPart{cols, targets}, true, nil
	} else if err != nil {
		return nil, false, err
	}
	if cols, ok = p.parseColumns(); ok {
		// Case 2: The expression contains an AS
		// e.g. "p.col1 AS &Person.*".
		numCols := len(cols)
		p.skipSpaces()
		if p.skipString("AS") {
			if targets, ok, err = p.parseTargets(); ok {
				numTargets := len(targets)
				// If the target is not * then check there are equal columns
				// and targets.
				if !(numTargets == 1 && targets[0].Name == "*") &&
					numCols != numTargets {
					return nil, false, fmt.Errorf("number of cols = %d "+
						"but number of targets = %d in expression near %d",
						numCols, numTargets, p.pos)
				}

				return &OutputPart{cols, targets}, true, nil
			} else if err != nil {
				return nil, false, err
			}
		}
	}
	cp.restore()
	return nil, false, nil
}

// parseInputExpression parses an input expression of the form $Type.name.
func (p *Parser) parseInputExpression() (*InputPart, bool, error) {
	cp := p.save()

	if p.skipByte('$') {
		if fn, ok, err := p.parseGoFullName(); ok {
			if fn.Name == "*" {
				return nil, false, fmt.Errorf("asterisk not allowed "+
					"in expression near %d", p.pos)
			}
			return &InputPart{fn}, true, nil
		} else if err != nil {
			return nil, false, err
		}
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
