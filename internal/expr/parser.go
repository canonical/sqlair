// Copyright 2023 Canonical Ltd.
// Licensed under Apache 2.0, see LICENCE file for details.

package expr

import (
	"fmt"
	"strings"
)

func NewParser() *Parser {
	return &Parser{}
}

type Parser struct {
	input string
	pos   int
	// prevExprEnd is the value of pos when we last finished parsing a
	// expression.
	prevExprEnd int
	// currentExprStart is the value of pos just before we started parsing the
	// expression under pos. We maintain currentExprStart >= prevExprEnd.
	currentExprStart int
	exprs            []expression
	// lineNum is the number of the current line of the input.
	lineNum int
	// lineStart is the position of the first byte of the current line in the
	// input.
	lineStart int
}

// Parse takes an SQLair query string and returns a ParsedExpr.
func (p *Parser) Parse(input string) (pe *ParsedExpr, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("cannot parse expression: %s", err)
		}
	}()

	p.init(input)

	for {
		if err := p.advanceToNextExpression(); err != nil {
			return nil, err
		}

		p.currentExprStart = p.pos

		if p.pos == len(p.input) {
			break
		}

		if out, ok, err := p.parseOutputExpr(); err != nil {
			return nil, err
		} else if ok {
			p.add(out)
			continue
		}

		if in, ok, err := p.parseInputExpr(); err != nil {
			return nil, err
		} else if ok {
			p.add(in)
			continue
		}

		// No expression found, advance the parser. This prevents
		// advanceToNextExpression finding the same char again.
		p.advanceByte()
	}

	// Add any remaining unparsed string input to the parser.
	p.add(nil)
	return &ParsedExpr{exprs: p.exprs}, nil
}

// memberAccessor stores information for accessing a keyed Go value. It consists
// of a type name and some value within it to be accessed. For example: a field
// of a struct, or a key of a map.
type memberAccessor struct {
	typeName, memberName string
}

func (ma memberAccessor) String() string {
	return ma.typeName + "." + ma.memberName
}

func (ma memberAccessor) getTypeName() string {
	return ma.typeName
}

type columnAccessor interface {
	String() string
	tableName() string
	columnName() string
}

// basicColumn stores a SQL column name and optionally its table name.
type basicColumn struct {
	table, column string
}

func (sc basicColumn) columnName() string {
	return sc.column
}

func (sc basicColumn) tableName() string {
	return sc.table
}

func (sc basicColumn) String() string {
	if sc.table == "" {
		return sc.column
	}
	return sc.table + "." + sc.column
}

// sqlFunctionCall stores a function call that is used in place of a column.
type sqlFunctionCall struct {
	raw string
}

func (sfc sqlFunctionCall) columnName() string {
	return sfc.raw
}

func (sfc sqlFunctionCall) tableName() string {
	return ""
}

func (sfc sqlFunctionCall) String() string {
	return sfc.raw
}

// init resets the state of the parser and sets the input string.
func (p *Parser) init(input string) {
	p.input = input
	p.pos = 0
	p.prevExprEnd = 0
	p.currentExprStart = 0
	p.exprs = []expression{}
	p.lineNum = 1
	p.lineStart = 0
}

// colNum calculates the current column number taking into account line breaks.
func (p *Parser) colNum() int {
	return p.pos - p.lineStart + 1
}

// advanceByte moves the parser to the next byte in the input. It also takes
// care of updating the line and column numbers if it encounters line breaks.
func (p *Parser) advanceByte() {
	if p.pos >= len(p.input) {
		return
	}
	if p.input[p.pos] == '\n' {
		p.lineStart = p.pos + 1
		p.lineNum++
	}
	p.pos++
}

// errorAt wraps an error with line and column information.
func errorAt(err error, line int, column int, input string) error {
	if strings.ContainsRune(input, '\n') {
		return fmt.Errorf("line %d, column %d: %w", line, column, err)
	} else {
		return fmt.Errorf("column %d: %w", column, err)
	}
}

// A checkpoint struct for saving parser state to restore later. We only use a
// checkpoint within an attempted parsing of an expression, not at a higher
// level since we don't keep track of the expressions in the checkpoint.
type checkpoint struct {
	parser           *Parser
	pos              int
	prevExprEnd      int
	currentExprStart int
	exprs            []expression
	lineNum          int
	lineStart        int
}

// save takes a snapshot of the state of the parser and returns a pointer to a
// checkpoint that represents it.
func (p *Parser) save() *checkpoint {
	return &checkpoint{
		parser:           p,
		pos:              p.pos,
		prevExprEnd:      p.prevExprEnd,
		currentExprStart: p.currentExprStart,
		exprs:            p.exprs,
		lineNum:          p.lineNum,
		lineStart:        p.lineStart,
	}
}

// restore sets the internal state of the parser to the values stored in the
// checkpoint.
func (cp *checkpoint) restore() {
	cp.parser.pos = cp.pos
	cp.parser.prevExprEnd = cp.prevExprEnd
	cp.parser.currentExprStart = cp.currentExprStart
	cp.parser.exprs = cp.exprs
	cp.parser.lineNum = cp.lineNum
	cp.parser.lineStart = cp.lineStart
}

// colNum calculates the current column number taking into account line breaks.
func (cp *checkpoint) colNum() int {
	return cp.pos - cp.lineStart + 1
}

// add pushes the parsed expression to the list of expressions along with the
// bypass chunk that stretches from the end of the previous expression to the
// beginning of this expression.
func (p *Parser) add(expr expression) {
	// Add the string between the previous I/O expression and the current expression.
	if p.prevExprEnd != p.currentExprStart {
		p.exprs = append(p.exprs,
			&bypass{p.input[p.prevExprEnd:p.currentExprStart]})
	}

	if expr != nil {
		p.exprs = append(p.exprs, expr)
	}

	// Save this position at the end of the expression.
	p.prevExprEnd = p.pos
	// Ensure that currentExprStart >= prevExprEnd.
	p.currentExprStart = p.pos
}

// skipComment jumps over comments as defined by the SQLite spec. If no comment
// is found the parser state is left unchanged.
func (p *Parser) skipComment() bool {
	cp := p.save()
	if p.skipByte('-') || p.skipByte('/') {
		c := p.input[p.pos-1]
		if (c == '-' && p.skipByte('-')) || (c == '/' && p.skipByte('*')) {
			var end byte
			if c == '-' {
				end = '\n'
			} else {
				end = '*'
			}
			for p.pos < len(p.input) {
				if p.input[p.pos] == end {
					// if end == '\n' (i.e. its a -- comment) dont consume the newline.
					if end == '*' {
						p.advanceByte()
						if !p.skipByte('/') {
							continue
						}
					}
					return true
				}
				p.advanceByte()
			}
			// Reached end of input (valid comment end).
			return true
		}
		cp.restore()
		return false
	}
	return false
}

// advanceToNextExpression advances the parser until it finds a character that
// could be the start of an expression.
func (p *Parser) advanceToNextExpression() error {
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

		switch p.input[p.pos] {
		// These bytes may be the start of an expression.
		case '(', '*', '$', '&':
			break loop
		// An expression can also start with an initial name byte, e.g. a
		// expression starting with a column name or a SQL function.
		// Rather than testing for every initial name byte (we would stop at
		// every letter of every word), we look for bytes that may precede the
		// start of an expression and then check if the next byte is an initial
		// name byte.
		case ' ', '\t', '\n', '\r', '=', ',', '[', '>', '<', '+', '-', '/', '|', '%':
			p.advanceByte()
			if p.pos >= len(p.input) || isInitialNameByte(p.input[p.pos]) {
				break loop
			}
			continue
		}
		p.advanceByte()
	}

	p.skipBlanks()

	return nil

}

// skipStringLiteral jumps over single and double quoted sections of input.
// Doubled up quotes are escaped.
func (p *Parser) skipStringLiteral() (bool, error) {
	cp := p.save()

	if p.skipByte('"') || p.skipByte('\'') {
		c := p.input[p.pos-1]

		// We keep track of whether the next quote has been previously
		// escaped. If not, it might be a closing quote.
		maybeCloser := true
		for p.skipByteFind(c) {
			// If this looks like a closing quote, check if it might be an
			// escape for a following quote. If not, we're done.
			if maybeCloser && !p.peekByte(c) {
				return true, nil
			}
			maybeCloser = !maybeCloser
		}

		// Reached end of string and didn't find the closing quote
		cp.restore()
		return false, errorAt(fmt.Errorf("missing closing quote in string literal"), p.lineNum, p.colNum(), p.input)
	}
	return false, nil
}

// peekByte returns true if the current byte equals the one passed as
// parameter.
func (p *Parser) peekByte(b byte) bool {
	return p.pos < len(p.input) && p.input[p.pos] == b
}

// skipByte jumps over the current byte if it matches the byte passed as a
// parameter. Returns true in that case, false otherwise.
func (p *Parser) skipByte(b byte) bool {
	if p.pos < len(p.input) && p.input[p.pos] == b {
		p.advanceByte()
		return true
	}
	return false
}

// skipByteFind looks for a byte that matches the one passed as parameter and
// then advances the parser to jump over it. In that case returns true. If the
// end of the string is reached and no matching byte was found, it returns
// false and it does not change the parser.
func (p *Parser) skipByteFind(b byte) bool {
	cp := p.save()
	for p.pos < len(p.input) {
		if p.input[p.pos] == b {
			p.advanceByte()
			return true
		}
		p.advanceByte()
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
		switch p.input[p.pos] {
		case ' ', '\t', '\r', '\n':
			p.advanceByte()
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
		p.advanceByte()
		for p.pos < len(p.input) && isNameByte(p.input[p.pos]) {
			p.advanceByte()
		}
	}
	return p.pos > mark
}

// skipEnclosedParentheses starts from a opening parenthesis '(' and skips until
// its closing ')', taking into account comments, string literals and nested
// parentheses.
func (p *Parser) skipEnclosedParentheses() (bool, error) {
	cp := p.save()

	if !p.skipByte('(') {
		return false, nil
	}

	parenCount := 1
	for parenCount > 0 && p.pos != len(p.input) {
		if ok, err := p.skipStringLiteral(); err != nil {
			cp.restore()
			return false, err
		} else if ok {
			continue
		}
		if ok := p.skipComment(); ok {
			continue
		}

		if p.skipByte('(') {
			parenCount++
			continue
		} else if p.skipByte(')') {
			parenCount--
			continue
		}
		p.advanceByte()
	}

	if parenCount > 0 {
		cp.restore()
		return false, errorAt(fmt.Errorf(`missing closing parenthesis`), cp.lineNum, cp.colNum(), p.input)
	}
	return true, nil
}

// Functions with the prefix parse attempt to parse some construct. They return
// the construct, and an error and/or a bool that indicates if the construct
// was successfully parsed.
//
// Return cases:
//  - bool == true, err == nil
//		The construct was successfully parsed
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

// parseColumnAccessor parses either a column made up of name bytes optionally
// dot-prefixed by its table name or a SQL function call used in place of a
// column.
// parseColumnAccessor returns an error so that it can be used with parseList.
func (p *Parser) parseColumnAccessor() (columnAccessor, bool, error) {
	cp := p.save()

	// asterisk cannot be followed by anything.
	if p.skipByte('*') {
		return basicColumn{column: "*"}, true, nil
	}

	id, ok := p.parseIdentifier()
	if !ok {
		cp.restore()
		return nil, false, nil
	}

	// identifier.<> can only be followed by another identifier or an asterisk.
	if p.skipByte('.') {
		if idCol, ok := p.parseIdentifierAsterisk(); ok {
			return basicColumn{table: id, column: idCol}, true, nil
		}
		cp.restore()
		return nil, false, nil
	}

	// Check if it is a function call instead of a lone identifier.
	if ok, err := p.skipEnclosedParentheses(); err != nil {
		cp.restore()
		return nil, false, err
	} else if ok {
		return sqlFunctionCall{raw: p.input[cp.pos:p.pos]}, true, nil
	}

	return basicColumn{column: id}, true, nil
}

func (p *Parser) parseTargetType() (memberAccessor, bool, error) {
	startLine := p.lineNum
	startCol := p.colNum()

	if p.skipByte('&') {
		// Using a slice as an output is an error, we add the case here to
		// improve the error message.
		if st, ok, err := p.parseSliceAccessor(); ok {
			return memberAccessor{}, false, errorAt(fmt.Errorf(`cannot use slice syntax "%s[:]" in output expression`, st), startLine, startCol, p.input)
		} else if err != nil {
			return memberAccessor{}, false, errorAt(fmt.Errorf("cannot use slice syntax in output expression"), startLine, startCol, p.input)
		}
		return p.parseTypeAndMember()
	}

	return memberAccessor{}, false, nil
}

// parseSliceAccessor parses a slice accessor. A slice accessor is of the form
// "SliceType[:]". It returns the parsed slice type name.
func (p *Parser) parseSliceAccessor() (typeName string, ok bool, err error) {
	cp := p.save()

	id, ok := p.parseIdentifier()
	if !ok {
		return "", false, nil
	}
	if !p.skipByte('[') {
		cp.restore()
		return "", false, nil
	}
	p.skipBlanks()
	if !p.skipByte(':') {
		return "", false, errorAt(fmt.Errorf("invalid slice: expected '%s[:]'", id), cp.lineNum, cp.colNum(), p.input)
	}
	p.skipBlanks()
	if !p.skipByte(']') {
		return "", false, errorAt(fmt.Errorf("invalid slice: expected '%s[:]'", id), cp.lineNum, cp.colNum(), p.input)
	}
	return id, true, nil
}

// parseTypeAndMember parses a Go type name qualified by a tag name (or asterisk)
// of the form "TypeName.col_name".
func (p *Parser) parseTypeAndMember() (memberAccessor, bool, error) {
	cp := p.save()

	// The error points to the skipped & or $.
	identifierCol := p.colNum() - 1
	if id, ok := p.parseIdentifier(); ok {
		if !p.skipByte('.') {
			return memberAccessor{}, false, errorAt(fmt.Errorf("unqualified type, expected %s.* or %s.<db tag> or %s[:]", id, id, id), p.lineNum, identifierCol, p.input)
		}

		idField, ok := p.parseIdentifierAsterisk()
		if !ok {
			return memberAccessor{}, false, errorAt(fmt.Errorf("invalid identifier suffix following %q", id), p.lineNum, p.colNum(), p.input)
		}
		return memberAccessor{typeName: id, memberName: idField}, true, nil
	}

	cp.restore()
	return memberAccessor{}, false, nil
}

// parseList takes a parsing function that returns a T and parses a
// bracketed, comma seperated, list.
func parseList[T any](p *Parser, parseFn func(p *Parser) (T, bool, error)) ([]T, bool, error) {
	cp := p.save()
	if !p.skipByte('(') {
		return nil, false, nil
	}

	// Error points to first parentheses skipped above.
	openingParenCol := p.colNum() - 1
	openingParenLine := p.lineNum
	nextItem := true
	var objs []T
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
			err := errorAt(fmt.Errorf("invalid expression in list"), p.lineNum, p.colNum(), p.input)
			cp.restore()
			return nil, false, err
		}

		p.skipBlanks()
		if p.skipByte(')') {
			return objs, true, nil
		}

		nextItem = p.skipByte(',')
	}
	err := errorAt(fmt.Errorf("missing closing parentheses"), openingParenLine, openingParenCol, p.input)
	cp.restore()
	return nil, false, err
}

// parseColumns parses a single column or a list of columns. Lists must be
// enclosed in parentheses.
func (p *Parser) parseColumns() (cols []columnAccessor, parentheses bool, ok bool) {
	// Case 1: A single column e.g. "p.name".
	if col, ok, _ := p.parseColumnAccessor(); ok {
		return []columnAccessor{col}, false, true
	}

	// Case 2: Multiple columns e.g. "(p.name, p.id)".
	if cols, ok, _ := parseList(p, (*Parser).parseColumnAccessor); ok {
		return cols, true, true
	}

	return nil, false, false
}

// parseTargetTypes parses a single output type or a list of output types.
// Lists of types must be enclosed in parentheses.
func (p *Parser) parseTargetTypes() (types []memberAccessor, parentheses bool, ok bool, err error) {
	// Case 1: A single target e.g. "&Person.name".
	if targetTypes, ok, err := p.parseTargetType(); err != nil {
		return nil, false, false, err
	} else if ok {
		return []memberAccessor{targetTypes}, false, true, nil
	}

	// Case 2: Multiple types e.g. "(&Person.name, &Person.id)".
	if targetTypes, ok, err := parseList(p, (*Parser).parseTargetType); err != nil {
		return nil, true, false, err
	} else if ok {
		return targetTypes, true, true, nil
	}

	return nil, false, false, nil
}

// parseInputMemberAccessor parses an accessor preceded by '$'.
// e.g. "$Type.member".
func (p *Parser) parseInputMemberAccessor() (memberAccessor, bool, error) {
	if p.skipByte('$') {
		return p.parseTypeAndMember()
	}
	return memberAccessor{}, false, nil
}

// parseOutputExpr requires that the ampersand before the identifiers must
// be followed by a name byte.
func (p *Parser) parseOutputExpr() (*outputExpr, bool, error) {
	start := p.pos

	// Case 1: There are no columns e.g. "&Person.*".
	if targetType, ok, err := p.parseTargetType(); err != nil {
		return nil, false, err
	} else if ok {
		return &outputExpr{
			sourceColumns: []columnAccessor{},
			targetTypes:   []memberAccessor{targetType},
			raw:           p.input[start:p.pos],
		}, true, nil
	}

	cp := p.save()

	// Case 2: There are columns e.g. "p.col1 AS &Person.*".
	if cols, parenCols, ok := p.parseColumns(); ok {
		p.skipBlanks()
		if p.skipString("AS") {
			p.skipBlanks()
			parenCol := p.colNum()
			if targetTypes, parenTypes, ok, err := p.parseTargetTypes(); err != nil {
				return nil, false, err
			} else if ok {
				if parenCols && !parenTypes {
					return nil, false, errorAt(fmt.Errorf(`missing parentheses around types after "AS"`), p.lineNum, parenCol, p.input)
				}
				if !parenCols && parenTypes {
					return nil, false, errorAt(fmt.Errorf(`unexpected parentheses around types after "AS"`), p.lineNum, parenCol, p.input)
				}
				if starCountTypes(targetTypes) > 0 {
					for _, c := range cols {
						if _, ok := c.(sqlFunctionCall); ok {
							return nil, false, errorAt(fmt.Errorf(`cannot read function call %q into asterisk`, c), cp.lineNum, cp.colNum(), p.input)
						}
					}
				}
				return &outputExpr{
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

// parseSliceInputExpr parses an input expression of the form "$Type[:]".
func (p *Parser) parseSliceInputExpr() (*sliceInputExpr, bool, error) {
	cp := p.save()
	if !p.skipByte('$') {
		return nil, false, nil
	}

	if st, ok, err := p.parseSliceAccessor(); err != nil {
		cp.restore()
		return nil, false, err
	} else if ok {
		return &sliceInputExpr{sliceTypeName: st, raw: p.input[cp.pos:p.pos]}, true, nil
	}

	cp.restore()
	return nil, false, nil
}

// parseMemberInputExpr parses an input expression of the form "$Type.member".
func (p *Parser) parseMemberInputExpr() (*memberInputExpr, bool, error) {
	cp := p.save()
	ma, ok, err := p.parseInputMemberAccessor()
	if !ok {
		cp.restore()
		return nil, false, err
	}
	if ma.memberName == "*" {
		cp.restore()
		return nil, false, errorAt(fmt.Errorf("invalid asterisk input placement %q", "$"+ma.String()), cp.lineNum, cp.colNum(), p.input)
	}
	return &memberInputExpr{ma: ma, raw: p.input[cp.pos:p.pos]}, true, nil
}

// parseAsteriskInputExpr parses an INSERT statement input expression where
// SQLair generates the columns.
// It is of the form "(*) VALUES ($Type.*, $Type.member,...)".
func (p *Parser) parseAsteriskInputExpr() (*asteriskInputExpr, bool, error) {
	cp := p.save()

	parenCol := p.colNum()
	parenLine := p.lineNum
	if !p.skipByte('(') {
		return nil, false, nil
	}
	p.skipBlanks()
	if !p.skipByte('*') {
		cp.restore()
		return nil, false, nil
	}
	p.skipBlanks()
	if !p.skipByte(')') {
		cp.restore()
		return nil, false, nil
	}

	p.skipBlanks()
	if !p.skipString("VALUES") {
		cp.restore()
		return nil, false, nil
	}
	p.skipBlanks()

	sources, ok, err := parseList(p, (*Parser).parseInputMemberAccessor)
	if err != nil {
		cp.restore()
		return nil, false, err
	} else if !ok {
		// Check for types with missing parentheses.
		if _, ok, _ := p.parseInputMemberAccessor(); ok {
			err = errorAt(fmt.Errorf(`missing parentheses around types after "VALUES"`), parenLine, parenCol, p.input)
		}
		cp.restore()
		return nil, false, err
	}

	return &asteriskInputExpr{sources: sources, raw: p.input[cp.pos:p.pos]}, true, nil
}

// parseColumnsInputExpr parses an INSERT statement input expression where the
// user specifies the columns to insert from a single type.
// It is of the form "(col1, col2,...) VALUES ($Type.*)".
func (p *Parser) parseColumnsInputExpr() (*columnsInputExpr, bool, error) {
	cp := p.save()

	columns, paren, ok := p.parseColumns()
	if !(ok && paren) {
		cp.restore()
		return nil, false, nil
	}

	p.skipBlanks()
	if !p.skipString("VALUES") {
		cp.restore()
		return nil, false, nil
	}
	p.skipBlanks()

	parenCol := p.colNum()
	parenLine := p.lineNum
	// Ignore errors and leave them to be handled by
	// parseMemberInputExpr later.
	sources, ok, _ := parseList(p, (*Parser).parseInputMemberAccessor)
	if !ok {
		// Check for types with missing parentheses.
		if _, ok, _ := p.parseTypeAndMember(); ok {
			cp.restore()
			return nil, false, errorAt(fmt.Errorf(`missing parentheses around types after "VALUES"`), parenLine, parenCol, p.input)
		}
		cp.restore()
		return nil, false, nil
	}
	if starCountTypes(sources) == 0 {
		// If there are no asterisk accessors leave the accessors to be handled
		// by parseMemberInputExpr later.
		cp.restore()
		return nil, false, nil
	}

	return &columnsInputExpr{columns: columns, sources: sources, raw: p.input[cp.pos:p.pos]}, true, nil
}

// parseInputExpr parses all forms of input expressions, that is, expressions
// containing a "$".
func (p *Parser) parseInputExpr() (expression, bool, error) {
	if expr, ok, err := p.parseSliceInputExpr(); err != nil {
		return nil, false, err
	} else if ok {
		return expr, true, nil
	}

	if expr, ok, err := p.parseMemberInputExpr(); err != nil {
		return nil, false, err
	} else if ok {
		return expr, true, nil
	}

	if expr, ok, err := p.parseAsteriskInputExpr(); err != nil {
		return nil, false, err
	} else if ok {
		return expr, true, nil
	}

	if expr, ok, err := p.parseColumnsInputExpr(); err != nil {
		return nil, false, err
	} else if ok {
		return expr, true, nil
	}

	return nil, false, nil
}
