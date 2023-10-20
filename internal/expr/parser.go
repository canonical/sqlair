package expr

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

// A QueryPart represents a section of a parsed SQL statement, which forms
// a complete query when processed together with its surrounding parts, in
// their correct order.
type queryPart interface {
	// String returns the part's representation for debugging purposes.
	String() string

	// marker method
	part()
}

// valueAccessor stores information about how to access a Go value. For example:
// by index, by key, or by slice syntax.
type valueAccessor interface {
	getTypeName() string

	// accessor is a marker method.
	accessor()
}

// memberAcessor stores information for accessing a keyed Go value. It consists
// of a type name and some value within it to be accessed. For example: a field
// of a struct, or a key of a map.
type memberAcessor struct {
	typeName, memberName string
}

func (va memberAcessor) String() string {
	return va.typeName + "." + va.memberName
}

func (ma memberAcessor) getTypeName() string {
	return ma.typeName
}

func (ma memberAcessor) accessor() {}

// columnAccessor stores a SQL column name and optionally its table name.
type columnAccessor struct {
	tableName, columnName string
}

func (ca columnAccessor) String() string {
	if ca.tableName == "" {
		return ca.columnName
	}
	return ca.tableName + "." + ca.columnName
}

// sliceRangeAccessor represents the expression typeName[low:high].
type sliceRangeAccessor struct {
	typ string
	// Using pointers to represent that the both low and high are optional.
	low, high *uint64
}

func (st sliceRangeAccessor) getTypeName() string {
	return st.typ
}

func (st sliceRangeAccessor) String() string {
	strLow := ""
	if st.low != nil {
		strLow = strconv.FormatUint(*st.low, 10)
	}
	strHigh := ""
	if st.high != nil {
		strHigh = strconv.FormatUint(*st.high, 10)
	}
	return fmt.Sprintf("%s[%s:%s]", st.typ, strLow, strHigh)
}

func (st sliceRangeAccessor) accessor() {}

// sliceIndexAccessor represents the expression typeName[index].
type sliceIndexAccessor struct {
	typ   string
	index uint64
}

func (st sliceIndexAccessor) getTypeName() string {
	return st.typ
}

func (st sliceIndexAccessor) String() string {
	return fmt.Sprintf("%s[%d]", st.typ, st.index)
}

func (st sliceIndexAccessor) accessor() {}

// inputPart represents a named parameter that will be sent to the database
// while performing the query.
type inputPart struct {
	sourceType valueAccessor
	raw        string
}

func (p *inputPart) String() string {
	return fmt.Sprintf("Input[%+v]", p.sourceType)
}

func (p *inputPart) part() {}

// outputPart represents a named target output variable in the SQL expression,
// as well as the source table and column where it will be read from.
type outputPart struct {
	sourceColumns []columnAccessor
	targetTypes   []memberAcessor
	raw           string
}

func (p *outputPart) String() string {
	return fmt.Sprintf("Output[%+v %+v]", p.sourceColumns, p.targetTypes)
}

func (p *outputPart) part() {}

// bypassPart represents a part of the expression that we want to pass to the
// backend database verbatim.
type bypassPart struct {
	chunk string
}

func (p *bypassPart) String() string {
	return "Bypass[" + p.chunk + "]"
}

func (p *bypassPart) part() {}

type Parser struct {
	input string
	pos   int
	// prevPart is the value of pos when we last finished parsing a part.
	prevPart int
	// partStart is the value of pos just before we started parsing the part
	// under pos. We maintain partStart >= prevPart.
	partStart int
	parts     []queryPart
	// lineNum is the number of the current line of the input.
	lineNum int
	// lineStart is the position of the first byte of the current line in
	// the input.
	lineStart int
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

// A checkpoint struct for saving parser state to restore later. We only use
// a checkpoint within an attempted parsing of an part, not at a higher level
// since we don't keep track of the parts in the checkpoint.
type checkpoint struct {
	parser    *Parser
	pos       int
	prevPart  int
	partStart int
	parts     []queryPart
	lineNum   int
	lineStart int
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
		lineNum:   p.lineNum,
		lineStart: p.lineStart,
	}
}

// restore sets the internal state of the parser to the values stored in the
// checkpoint.
func (cp *checkpoint) restore() {
	cp.parser.pos = cp.pos
	cp.parser.prevPart = cp.prevPart
	cp.parser.partStart = cp.partStart
	cp.parser.parts = cp.parts
	cp.parser.lineNum = cp.lineNum
	cp.parser.lineStart = cp.lineStart
}

// colNum calculates the current column number taking into account line breaks.
func (cp *checkpoint) colNum() int {
	return cp.pos - cp.lineStart + 1
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

		if p.pos == len(p.input) {
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

		switch p.input[p.pos] {
		// If the preceding byte is one of these then we might be at the start
		// of an expression.
		case ' ', '\t', '\n', '\r', '=', ',', '(', '[', '>', '<', '+', '-', '*', '/', '|', '%':
			p.advanceByte()
			break loop
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

// peekByte returns true if the current byte equals the one passed as parameter.
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

// skipNumber consumes one or more consecutive digits.
func (p *Parser) skipNumber() bool {
	found := false
	for p.pos < len(p.input) && '0' <= p.input[p.pos] && p.input[p.pos] <= '9' {
		found = true
		p.advanceByte()
	}
	return found
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
func (p *Parser) parseColumn() (columnAccessor, bool, error) {
	cp := p.save()

	if id, ok := p.parseIdentifierAsterisk(); ok {
		if id != "*" && p.skipByte('.') {
			if idCol, ok := p.parseIdentifierAsterisk(); ok {
				return columnAccessor{tableName: id, columnName: idCol}, true, nil
			}
		} else {
			return columnAccessor{columnName: id}, true, nil
		}
	}

	cp.restore()
	return columnAccessor{}, false, nil
}

func (p *Parser) parseTargetType() (memberAcessor, bool, error) {
	startLine := p.lineNum
	startCol := p.colNum()

	if p.skipByte('&') {
		// Using a slice as an output is an error, we add the case here to
		// improve the error message.
		if st, ok, err := p.parseSliceAccessor(); err != nil {
			return memberAcessor{}, false, err
		} else if ok {
			return memberAcessor{}, false, errorAt(fmt.Errorf("cannot use slice syntax %q in output expression", st), startLine, startCol, p.input)
		}
		return p.parseTypeName()
	}

	return memberAcessor{}, false, nil
}

// parseUNumber parses a non-negative number composed of one or more digits.
func (p *Parser) parseUNumber() (*uint64, bool) {
	mark := p.pos
	if !p.skipNumber() {
		return nil, false
	}
	n, err := strconv.ParseUint(p.input[mark:p.pos], 10, 64)
	if err != nil {
		panic("internal error: skipNumber did not skip a valid number")
	}
	return &n, true
}

// parseSliceAccessor parses a slice range composed of two indexes of the form
// "[low:high]". On success it returns either a sliceIndexAccessor or a
// sliceRangeAccessor.
func (p *Parser) parseSliceAccessor() (va valueAccessor, ok bool, err error) {
	cp := p.save()

	id, ok := p.parseIdentifier()
	if !ok {
		return nil, false, nil
	}
	if !p.skipByte('[') {
		cp.restore()
		return nil, false, nil
	}
	p.skipBlanks()
	low, ok := p.parseUNumber()
	if !ok {
		low = nil
	}
	p.skipBlanks()
	if !p.skipByte(':') {
		if low == nil {
			return nil, false, errorAt(fmt.Errorf("invalid slice: expected index or colon"), p.lineNum, p.colNum(), p.input)
		}
		if p.skipByte(']') {
			return sliceIndexAccessor{typ: id, index: *low}, true, nil
		}
		return nil, false, errorAt(fmt.Errorf("invalid slice: expected ] or colon"), p.lineNum, p.colNum(), p.input)
	}
	p.skipBlanks()
	high, ok := p.parseUNumber()
	if !ok {
		high = nil
	}
	p.skipBlanks()
	if !p.skipByte(']') {
		return nil, false, errorAt(fmt.Errorf("invalid slice: expected ]"), p.lineNum, p.colNum(), p.input)
	}
	if high != nil && low != nil {
		if *low >= *high {
			return nil, false, errorAt(fmt.Errorf("invalid slice: invalid indexes: %d <= %d", *high, *low), cp.lineNum, cp.colNum(), p.input)
		}
	}
	return sliceRangeAccessor{typ: id, low: low, high: high}, true, nil
}

// parseTypeName parses a Go type name qualified by a tag name (or asterisk)
// of the form "TypeName.col_name".
func (p *Parser) parseTypeName() (memberAcessor, bool, error) {
	cp := p.save()

	// The error points to the skipped & or $.
	identifierCol := p.colNum() - 1
	if id, ok := p.parseIdentifier(); ok {
		if !p.skipByte('.') {
			return memberAcessor{}, false, errorAt(fmt.Errorf("unqualified type, expected %s.* or %s.<db tag>", id, id), p.lineNum, identifierCol, p.input)
		}

		idField, ok := p.parseIdentifierAsterisk()
		if !ok {
			return memberAcessor{}, false, errorAt(fmt.Errorf("invalid identifier suffix following %q", id), p.lineNum, p.colNum(), p.input)
		}
		return memberAcessor{typeName: id, memberName: idField}, true, nil
	}

	cp.restore()
	return memberAcessor{}, false, nil
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
			return nil, false, errorAt(fmt.Errorf("invalid expression in list"), p.lineNum, p.colNum(), p.input)
		}

		p.skipBlanks()
		if p.skipByte(')') {
			return objs, true, nil
		}

		nextItem = p.skipByte(',')
	}
	return nil, false, errorAt(fmt.Errorf("missing closing parentheses"), openingParenLine, openingParenCol, p.input)
}

// parseColumns parses a single column or a list of columns. Lists must be
// enclosed in parentheses.
func (p *Parser) parseColumns() (cols []columnAccessor, parentheses bool, ok bool) {
	// Case 1: A single column e.g. "p.name".
	if col, ok, _ := p.parseColumn(); ok {
		return []columnAccessor{col}, false, true
	}

	// Case 2: Multiple columns e.g. "(p.name, p.id)".
	if cols, ok, _ := parseList(p, (*Parser).parseColumn); ok {
		return cols, true, true
	}

	return nil, false, false
}

// parseTargetTypes parses a single output type or a list of output types.
// Lists of types must be enclosed in parentheses.
func (p *Parser) parseTargetTypes() (types []memberAcessor, parentheses bool, ok bool, err error) {
	// Case 1: A single target e.g. "&Person.name".
	if targetTypes, ok, err := p.parseTargetType(); err != nil {
		return nil, false, false, err
	} else if ok {
		return []memberAcessor{targetTypes}, false, true, nil
	}

	// Case 2: Multiple types e.g. "(&Person.name, &Person.id)".
	if targetTypes, ok, err := parseList(p, (*Parser).parseTargetType); err != nil {
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
			sourceColumns: []columnAccessor{},
			targetTypes:   []memberAcessor{targetType},
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
	if !p.skipByte('$') {
		return nil, false, nil
	}

	// Case 1: Slice range, "Type[low:high]".
	if st, ok, err := p.parseSliceAccessor(); err != nil {
		return nil, false, err
	} else if ok {
		return &inputPart{sourceType: st, raw: p.input[cp.pos:p.pos]}, true, nil
	}

	// Case 2: Struct or map, "Type.something".
	if tn, ok, err := p.parseTypeName(); ok {
		if tn.memberName == "*" {
			return nil, false, errorAt(fmt.Errorf("asterisk not allowed in input expression %q", "$"+tn.String()), cp.lineNum, cp.colNum(), p.input)
		}
		return &inputPart{sourceType: tn, raw: p.input[cp.pos:p.pos]}, true, nil
	} else if err != nil {
		return nil, false, err
	}

	cp.restore()
	return nil, false, nil
}
