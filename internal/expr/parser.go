// Copyright 2023 Canonical Ltd.
// Licensed under Apache 2.0, see LICENCE file for details.

package expr

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

func NewParser() *Parser {
	return &Parser{}
}

type Parser struct {
	input string
	pos   int
	// nextPos is start of the next char.
	nextPos int
	// char is the rune starting at pos. char is set to 0 when pos reaches the
	// end of input.
	char rune
	// prevExprEnd is the value of pos when we last finished parsing a
	// expression.
	prevExprEnd int
	// currentExprStart is the value of pos just before we started parsing the
	// expression under pos. We maintain currentExprStart >= prevExprEnd.
	currentExprStart int
	// exprs are the output of the parser. Expressions are added as they are
	// parsed.
	exprs []expression
	// lineNum is the number of the current line of the input.
	lineNum int
	// lineStart is the position of the first char of the current line in the
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
		p.advanceChar()
	}

	// Add any remaining unparsed string input to the parser.
	p.add(nil)
	return &ParsedExpr{exprs: p.exprs}, nil
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
	p.nextPos = 0
	p.char = 0
	p.prevExprEnd = 0
	p.currentExprStart = 0
	p.exprs = []expression{}
	p.lineNum = 1
	p.lineStart = 0
	p.advanceChar()
}

// colNum calculates the current column number taking into account line breaks.
func (p *Parser) colNum() int {
	return p.pos - p.lineStart + 1
}

// advanceChar moves the parser to the next character in the input. It also
// takes care of updating the line and column numbers if it encounters line
// breaks.
func (p *Parser) advanceChar() bool {
	if p.nextPos >= len(p.input) {
		p.char = 0
		p.pos = p.nextPos
		return false
	}
	if p.char == '\n' {
		p.lineStart = p.nextPos
		p.lineNum++
	}
	var size int
	p.char, size = utf8.DecodeRuneInString(p.input[p.nextPos:])
	p.pos = p.nextPos
	p.nextPos += size
	return true
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
	nextPos          int
	char             rune
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
		nextPos:          p.nextPos,
		char:             p.char,
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
	cp.parser.nextPos = cp.nextPos
	cp.parser.char = cp.char
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
					// if end == '\n' (i.e. its a -- comment) dont consume the newline.
					if end == '*' {
						p.advanceChar()
						if !p.skipChar('/') {
							continue
						}
					}
					return true
				}
				p.advanceChar()
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
	// If very the first char of the whole input is a nameChar then return as
	// it could be an expression. This case is not covered below.
	if p.pos < len(p.input) && p.pos == 0 && isNameChar(p.char) {
		return nil
	}
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

		switch p.char {
		// These characters may be the start of an expression.
		case '(', '*', '$', '&':
			break loop
		// An expression can also start with a name char, e.g. an expression
		// starting with a column name or a SQL function. Rather than testing
		// for every name char (we would stop at every letter of every word),
		// we look for chars that may come before the start of an expression
		// and then check if the next char is an name char.
		case ' ', '\t', '\n', '\r', '=', ',', '[', '>', '<', '+', '-', '/', '|', '%':
			p.advanceChar()
			if p.pos >= len(p.input) {
				return nil
			}
			if isNameChar(p.char) {
				break loop
			}
			continue
		}
		p.advanceChar()
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
		return false, errorAt(fmt.Errorf("missing closing quote in string literal"), p.lineNum, p.colNum(), p.input)
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
		p.advanceChar()
		return true
	}
	return false
}

// skipCharFind looks for a char that matches the one passed as parameter and
// then advances the parser to jump over it. In that case returns true. If the
// end of the string is reached and no matching char was found, it returns
// false and it does not change the parser.
func (p *Parser) skipCharFind(c rune) bool {
	cp := p.save()
	for p.pos < len(p.input) {
		if p.char == c {
			p.advanceChar()
			return true
		}
		p.advanceChar()
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
			p.advanceChar()
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
	// EqualFold is used here because it is case insensitive.
	if p.pos+len(s) <= len(p.input) &&
		strings.EqualFold(p.input[p.pos:p.pos+len(s)], s) {
		// EqualFold does not advance the parser, so we must manually advance
		// the parser to the end of the string.
		p.pos += len(s)
		var size int
		p.char, size = utf8.DecodeRuneInString(p.input[p.pos:])
		p.nextPos = p.pos + size
		return true
	}
	return false
}

// skipLiteralInList advances the parser to the next comma or closing bracket
// skipping string literals, comments and matching sets of parentheses.
func (p *Parser) skipLiteralInList() (bool, error) {
	for p.pos < len(p.input) {
		if ok, err := p.skipStringLiteral(); err != nil {
			return false, err
		} else if ok {
			continue
		}
		if ok, err := p.skipEnclosedParentheses(); err != nil {
			return false, err
		} else if ok {
			continue
		}
		if ok := p.skipComment(); ok {
			continue
		}

		if p.char == ',' || p.char == ')' {
			return true, nil
		}
		p.advanceChar()
	}
	return false, nil
}

// isNameChar returns true if the given char can be part of a name. It returns
// false otherwise.
func isNameChar(c rune) bool {
	return unicode.IsLetter(c) || unicode.IsDigit(c) || c == '_'
}

// isInitialNameChar returns true if the given char can appear at the start of a
// name. It returns false otherwise.
func isInitialNameChar(c rune) bool {
	return unicode.IsLetter(c) || c == '_'
}

// skipName advances the parser until it is on the first non name char and
// returns true. If the p.pos does not start on a name char it returns false.
func (p *Parser) skipName() bool {
	if p.pos >= len(p.input) {
		return false
	}
	mark := p.pos
	if isInitialNameChar(p.char) {
		p.advanceChar()
		for p.pos < len(p.input) && isNameChar(p.char) {
			p.advanceChar()
		}
	}
	return p.pos > mark
}

// skipEnclosedParentheses starts from a opening parenthesis '(' and skips until
// its closing ')', taking into account comments, string literals and nested
// parentheses.
func (p *Parser) skipEnclosedParentheses() (bool, error) {
	cp := p.save()

	if !p.skipChar('(') {
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

		if p.skipChar('(') {
			parenCount++
			continue
		} else if p.skipChar(')') {
			parenCount--
			continue
		}
		p.advanceChar()
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

// parseIdentifierAsterisk parses an identifier or an asterisk.
func (p *Parser) parseIdentifierAsterisk() (string, bool, error) {
	if p.skipChar('*') {
		return "*", true, nil
	}
	return p.parseIdentifier()
}

// parseIdentifier parses either a name made up of letters, digits and
// underscores or any quoted name. This matches allowed SQL identifiers and db
// tags allowed by SQLair.
func (p *Parser) parseIdentifier() (string, bool, error) {
	mark := p.pos

	// parse quoted column names.
	if ok, err := p.skipStringLiteral(); err != nil {
		return "", false, err
	} else if ok {
		return p.input[mark:p.pos], true, nil
	}

	// parse regular column names, including numeric literals.
	for p.pos < len(p.input) && isNameChar(p.char) {
		p.advanceChar()
	}

	if p.pos > mark {
		return p.input[mark:p.pos], true, nil
	}
	return "", false, nil
}

// parseTypeName parses a name starting with a letter or underscore and
// followed by letters, digits and underscores. This matches the allowed
// characters in Go type names.
func (p *Parser) parseTypeName() (string, bool) {
	mark := p.pos

	if isInitialNameChar(p.char) {
		p.advanceChar()
		for p.pos < len(p.input) && isNameChar(p.char) {
			p.advanceChar()
		}
	}

	if p.pos > mark {
		return p.input[mark:p.pos], true
	}
	return "", false
}

// parseColumnAccessor parses either a column optionally dot-prefixed by its
// table name, or, a SQL function call used in place of a column.
func (p *Parser) parseColumnAccessor() (columnAccessor, bool, error) {
	cp := p.save()

	// asterisk cannot be followed by anything.
	if p.skipChar('*') {
		return basicColumn{column: "*"}, true, nil
	}

	// Parse a SQL identifier. This could be a column or table name.
	id, ok, err := p.parseIdentifier()
	if !ok {
		cp.restore()
		return nil, false, err
	}

	// If we find a '.' assume the previous was a table name, parse the column
	// name.
	if p.skipChar('.') {
		if idCol, ok, err := p.parseIdentifierAsterisk(); err != nil {
			return nil, false, err
		} else if ok {
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

	if p.skipChar('&') {
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

	id, ok := p.parseTypeName()
	if !ok {
		return "", false, nil
	}
	if !p.skipChar('[') {
		cp.restore()
		return "", false, nil
	}
	p.skipBlanks()
	if !p.skipChar(':') {
		return "", false, errorAt(fmt.Errorf("invalid slice: expected '%s[:]'", id), cp.lineNum, cp.colNum(), p.input)
	}
	p.skipBlanks()
	if !p.skipChar(']') {
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
	if id, ok := p.parseTypeName(); ok {
		if !p.skipChar('.') {
			return memberAccessor{}, false, errorAt(fmt.Errorf("unqualified type, expected %s.* or %s.<db tag> or %s[:]", id, id, id), p.lineNum, identifierCol, p.input)
		}

		idField, ok, err := p.parseIdentifierAsterisk()
		if err != nil {
			return memberAccessor{}, false, err
		} else if !ok {
			return memberAccessor{}, false, errorAt(fmt.Errorf("invalid identifier suffix following %q", id), p.lineNum, p.colNum(), p.input)
		}
		return memberAccessor{typeName: id, memberName: idField}, true, nil
	}

	cp.restore()
	return memberAccessor{}, false, nil
}

// parseList takes a parsing function that returns a T and parses a
// bracketed, comma separated, list.
func parseList[T any](p *Parser, parseFn func(p *Parser) (T, bool, error)) ([]T, bool, error) {
	cp := p.save()
	if !p.skipChar('(') {
		return nil, false, nil
	}

	// Error points to first parentheses skipped above.
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
		if p.skipChar(')') {
			return objs, true, nil
		}

		nextItem = p.skipChar(',')
	}
	err := errorAt(fmt.Errorf("missing closing parentheses"), cp.lineNum, cp.colNum(), p.input)
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
	if p.skipChar('$') {
		return p.parseTypeAndMember()
	}
	return memberAccessor{}, false, nil
}

// parseOutputExpr requires that the ampersand before the identifiers must
// be followed by a name char.
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

// parseInputExpr parses all forms of input expressions, that is, expressions
// containing a "$".
func (p *Parser) parseInputExpr() (expression, bool, error) {
	inputExprParsers := []func(*Parser) (expression, bool, error){
		(*Parser).parseSliceInputExpr,
		(*Parser).parseMemberInputExpr,
		(*Parser).parseInsertExpr,
	}
	for _, inputExprParser := range inputExprParsers {
		if expr, ok, err := inputExprParser(p); err != nil {
			return nil, false, err
		} else if ok {
			return expr, true, nil
		}
	}

	return nil, false, nil
}

// parseSliceInputExpr parses an input expression of the form "$Type[:]".
func (p *Parser) parseSliceInputExpr() (expression, bool, error) {
	cp := p.save()
	if !p.skipChar('$') {
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
func (p *Parser) parseMemberInputExpr() (expression, bool, error) {
	cp := p.save()
	ma, ok, err := p.parseInputMemberAccessor()
	if !ok {
		cp.restore()
		return nil, false, err
	}
	if ma.memberName == "*" {
		cp.restore()
		return nil, false, errorAt(fmt.Errorf("invalid asterisk placement in input %q", "$"+ma.String()), cp.lineNum, cp.colNum(), p.input)
	}
	return &memberInputExpr{ma: ma, raw: p.input[cp.pos:p.pos]}, true, nil
}

// parseAsteriskInsertExpr parses an INSERT statement input expression where
// SQLair generates the columns.
// It is of the form "(*) VALUES ($Type.*, $Type.member,...)".
func (p *Parser) parseAsteriskInsertExpr() (expression, bool, error) {
	cp := p.save()
	if !p.skipChar('(') {
		return nil, false, nil
	}
	p.skipBlanks()
	if !p.skipChar('*') {
		cp.restore()
		return nil, false, nil
	}
	p.skipBlanks()
	if !p.skipChar(')') {
		cp.restore()
		return nil, false, nil
	}
	p.skipBlanks()
	if !p.skipString("VALUES") {
		cp.restore()
		return nil, false, nil
	}
	p.skipBlanks()

	sources, ok, err := p.parseComplexInsertValues()
	if ok {
		return &asteriskInsertExpr{sources: sources, raw: p.input[cp.pos:p.pos]}, true, nil
	}
	return nil, false, err
}

// parseInsertExpr parses an INSERT statement input expression.
// e.g. (col1, col2, ...) VALUES (&Type.col1, &Type.*, ...)
func (p *Parser) parseInsertExpr() (expression, bool, error) {
	if expr, ok, err := p.parseAsteriskInsertExpr(); err != nil {
		return nil, false, err
	} else if ok {
		return expr, true, nil
	}

	// Try and parse an insert expression with explict columns.
	cp := p.save()
	// TODO: columns should really be []basicColumn not []columnAccessor
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

	colcp := p.save()
	// Ignore the errors here, let parseBasicInsertValues handle them
	if sources, ok, _ := p.parseComplexInsertValues(); ok && starCountTypes(sources) != 0 {
		// If there are no stars in the sources then it is a basicInsertExpr.
		return &columnsInsertExpr{columns: columns, sources: sources, raw: p.input[cp.pos:p.pos]}, true, nil
	}
	colcp.restore()

	if sources, ok, err := p.parseBasicInsertValues(); err != nil {
		cp.restore()
		return nil, false, err
	} else if ok {
		return &basicInsertExpr{columns: columns, sources: sources, raw: p.input[cp.pos:p.pos]}, true, nil
	}

	cp.restore()
	return nil, false, nil
}

// parseComplexInsertValues parses the values on the right hand side of insert
// expressions. This includes asterisk accessors but not literals.
// e.g. "($Type.*, $Type.col2)"
func (p *Parser) parseComplexInsertValues() ([]memberAccessor, bool, error) {
	cp := p.save()
	sources, ok, err := parseList(p, (*Parser).parseInputMemberAccessor)
	if err != nil {
		cp.restore()
		return nil, false, err
	} else if !ok {
		// Check for types with missing parentheses.
		if _, ok, _ := p.parseInputMemberAccessor(); ok {
			err = errorAt(fmt.Errorf(`missing parentheses around types after "VALUES"`), cp.lineNum, cp.colNum(), p.input)
		}
		cp.restore()
		return nil, false, err
	}
	return sources, true, nil
}

// parseBasicInsertValues parses the right hand side of a basic insert
// expression, this includes literals, but not asterisk accessors.
func (p *Parser) parseBasicInsertValues() ([]valueAccessor, bool, error) {
	cp := p.save()
	if !p.skipChar('(') {
		var err error
		// Check for types with missing parentheses.
		if _, ok, _ := p.parseInputMemberAccessor(); ok {
			err = errorAt(fmt.Errorf(`missing parentheses around types after "VALUES"`), cp.lineNum, cp.colNum(), p.input)
		}
		cp.restore()
		return nil, false, err
	}

	inputParsed := false
	itemStart := p.pos
	var vs []valueAccessor
	// Invariant:
	// - The previous char excluding blanks is ',' or '('.
	// - The loop parser will not pass the matching ')'.
	for {
		p.skipBlanks()
		itemStart = p.pos

		if ma, ok, err := p.parseInputMemberAccessor(); err != nil {
			return nil, false, err
		} else if ok {
			inputParsed = true
			if ma.memberName == "*" {
				return nil, false, fmt.Errorf("internal error: cannot have asterisk accessor in renaming expression")
			}
			vs = append(vs, ma)
		} else if ok, err = p.skipLiteralInList(); err != nil {
			return nil, false, err
		} else if ok {
			lit := literal{p.input[itemStart:p.pos]}
			vs = append(vs, lit)
		} else {
			cp.restore()
			return nil, false, nil
		}

		p.skipBlanks()
		if p.skipChar(')') {
			// If we only parsed literals, and not SQLair inputs, then bypass
			// the parsed expression.
			if !inputParsed {
				return nil, false, nil
			}
			return vs, true, nil
		}

		if !p.skipChar(',') {
			break
		}
	}
	cp.restore()
	return nil, false, nil
}
