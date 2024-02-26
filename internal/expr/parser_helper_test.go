// Copyright 2023 Canonical Ltd.
// Licensed under Apache 2.0, see LICENCE file for details.

package expr

import (
	. "gopkg.in/check.v1"
)

type parseSuite struct{}

var _ = Suite(&parseSuite{})

// Type assertions.
var _ columnAccessor = sqlFunctionCall{}
var _ columnAccessor = basicColumn{}

type parseHelperTest struct {
	bytef    func(byte) bool
	stringf  func(string) bool
	stringf0 func() bool
	stringf1 func() (bool, error)
	result   []bool
	input    string
	data     []string
	err      string
}

func (s parseSuite) TestRunTable(c *C) {
	var p = NewParser()
	var parseTests = []parseHelperTest{

		{bytef: p.peekByte, result: []bool{false}, input: "", data: []string{"a"}},
		{bytef: p.peekByte, result: []bool{false}, input: "b", data: []string{"a"}},
		{bytef: p.peekByte, result: []bool{true}, input: "a", data: []string{"a"}},

		{bytef: p.skipByte, result: []bool{false}, input: "", data: []string{"a"}},
		{bytef: p.skipByte, result: []bool{false}, input: "abc", data: []string{"b"}},
		{bytef: p.skipByte, result: []bool{true, true}, input: "abc", data: []string{"a", "b"}},

		{bytef: p.skipByteFind, result: []bool{false}, input: "", data: []string{"a"}},
		{bytef: p.skipByteFind, result: []bool{false, true, true}, input: "abcde", data: []string{"x", "b", "c"}},
		{bytef: p.skipByteFind, result: []bool{true, false}, input: "abcde ", data: []string{" ", " "}},

		{stringf0: p.skipBlanks, result: []bool{false}, input: "", data: []string{}},
		{stringf0: p.skipBlanks, result: []bool{false}, input: "abc    d", data: []string{}},
		{stringf0: p.skipBlanks, result: []bool{true}, input: "     abcd", data: []string{}},
		{stringf0: p.skipBlanks, result: []bool{true}, input: "  \t  abcd", data: []string{}},
		{stringf0: p.skipBlanks, result: []bool{true}, input: "\t  abcd", data: []string{}},
		{stringf0: p.skipBlanks, result: []bool{true}, input: "\n  abcd", data: []string{}},
		{stringf0: p.skipBlanks, result: []bool{true}, input: "\r  abcd", data: []string{}},
		{stringf0: p.skipBlanks, result: []bool{true}, input: "\n\r  abcd", data: []string{}},
		{stringf0: p.skipBlanks, result: []bool{true}, input: "\n\r\t  abcd", data: []string{}},
		{stringf0: p.skipBlanks, result: []bool{true}, input: "   \n\r\t  abcd", data: []string{}},

		{stringf: p.skipString, result: []bool{false}, input: "", data: []string{"a"}},
		{stringf: p.skipString, result: []bool{true, true}, input: "helloworld", data: []string{"hElLo", "w"}},
		{stringf: p.skipString, result: []bool{true, true}, input: "hello world", data: []string{"hello", " "}},

		{stringf0: p.skipName, result: []bool{false}, input: " hi", data: []string{}},
		{stringf0: p.skipName, result: []bool{false}, input: "*", data: []string{}},
		{stringf0: p.skipName, result: []bool{true}, input: "hello", data: []string{}},
		{stringf0: p.skipName, result: []bool{false}, input: "2d3d", data: []string{}},

		{stringf1: p.skipEnclosedParentheses, result: []bool{false}, input: `count *`},
		{stringf1: p.skipEnclosedParentheses, result: []bool{false}, input: `)`},
		{stringf1: p.skipEnclosedParentheses, result: []bool{false}, input: `(")"`, err: `column 1: missing closing parenthesis`},
		{stringf1: p.skipEnclosedParentheses, result: []bool{false}, input: `(--)`, err: `column 1: missing closing parenthesis`},
		{stringf1: p.skipEnclosedParentheses, result: []bool{false}, input: `(/*)*/`, err: `column 1: missing closing parenthesis`},
		{stringf1: p.skipEnclosedParentheses, result: []bool{true}, input: `()`},
		{stringf1: p.skipEnclosedParentheses, result: []bool{true}, input: `(columnName)`},
		{stringf1: p.skipEnclosedParentheses, result: []bool{true}, input: `(/*)(""*/)`},
		{stringf1: p.skipEnclosedParentheses, result: []bool{true}, input: `(")")`},
		{stringf1: p.skipEnclosedParentheses, result: []bool{true}, input: `("/*)*/")`},
	}
	for _, v := range parseTests {
		// Reset the input.
		p.init(v.input)
		for i, _ := range v.result {
			var result bool
			var err error
			if v.bytef != nil {
				result = v.bytef(v.data[i][0])
			}
			if v.stringf != nil {
				result = v.stringf(v.data[i])
			}
			if v.stringf0 != nil {
				result = v.stringf0()
			}
			if v.stringf1 != nil {
				result, err = v.stringf1()
			}
			if v.result[i] != result {
				c.Errorf("Test %#v failed. Expected: '%t', got '%t'\n", v, v.result[i], result)
			}
			if v.err != "" {
				c.Check(err, ErrorMatches, v.err)
			}
		}
	}
}

func (s parseSuite) TestValidQuotes(c *C) {
	var p = NewParser()

	validQuotes := []string{
		`'stringy string'`,
		`'O''Flan'`,
		`"J ""Quickfingers"" Johnson"`,
		`''`,
		`''' '''`,
		`""`,
		`" "" "`,
		`'"""'`,
		`' "''" '`,
	}

	for _, q := range validQuotes {
		p.init(q)
		ok, _ := p.skipStringLiteral()
		if !ok {
			c.Errorf("test failed. %s is a valid quoted string", q)
		}
	}
}

func (s parseSuite) TestInvalidQuote(c *C) {
	var p = NewParser()

	invalidQuote := []string{
		"`name`",
		"unquoted string",
	}

	for _, q := range invalidQuote {
		p.init(q)
		ok, _ := p.skipStringLiteral()
		if ok {
			c.Errorf("test failed. %s is not a valid quoted string but is recognised as one", q)
		}
	}
}

func (s parseSuite) TestUnfinishedQuote(c *C) {
	var p = NewParser()

	unfinishedQuotes := []string{
		`'`,
		`"`,
		`' ''`,
		`'"" ''`,
		`'string`,
		`'string"`,
		`"string`,
	}

	for _, q := range unfinishedQuotes {
		p.init(q)
		_, err := p.skipStringLiteral()
		if err == nil {
			c.Errorf("test failed. the string %s was parsed but is not valid", q)
		}
	}
}

func (s parseSuite) TestRemoveComments(c *C) {
	validComments := []string{
		`-- Single line comment`,
		`-- Single line comment with line break
		`,
		`/* multi
		 line */`,
		`/* unfinished multiline`,
		`/* -- */`,
		`-- */`,
		`--`,
		`/*`}
	invalidComments := []string{
		`- not comment`,
		`- - not comment`,
		`/ * not comment */`,
		`*/ not comment`,
		`/- not comment "`,
		`-* not comment`,
		`/ not comment */`,
	}

	var p = NewParser()
	for _, s := range validComments {
		p.init(s)
		if ok := p.skipComment(); !ok {
			c.Errorf("comment %s not parsed as comment", s)
		}
	}
	for _, s := range invalidComments {
		p.init(s)
		if ok := p.skipComment(); ok {
			c.Errorf("comment %s parsed as comment when it should not be", s)
		}
	}
}

func (s parseSuite) TestParseSliceRange(c *C) {
	sliceRangeTests := []struct {
		input    string
		expected string
		err      string
	}{
		{input: "mySlice[:]", expected: "mySlice"},
		{input: "mySlice[ : ]", expected: "mySlice"},
		{input: "mySlice[]", err: "column 1: invalid slice: expected 'mySlice[:]'"},
		{input: "mySlice[1:10]", err: "column 1: invalid slice: expected 'mySlice[:]'"},
		{input: "mySlice[1:]", err: "column 1: invalid slice: expected 'mySlice[:]'"},
		{input: "mySlice[:10]", err: "column 1: invalid slice: expected 'mySlice[:]'"},
		{input: "mySlice[1]", err: "column 1: invalid slice: expected 'mySlice[:]'"},
	}
	// invalidSliceRanges contains ranges that are invalid but that do not
	// result in an error.
	invalidSliceRanges := []string{"[]", "[:]", "[1:10]", "[1]"}

	var p = NewParser()
	for _, t := range sliceRangeTests {
		p.init(t.input)
		sr, ok, err := p.parseSliceAccessor()
		if err != nil && t.err != "" {
			c.Assert(err.Error(), Equals, t.err)
			c.Assert(ok, Equals, false)
			continue
		}
		c.Assert(err, IsNil)
		c.Assert(ok, Equals, true)
		c.Assert(t.expected, DeepEquals, sr)
	}
	for _, t := range invalidSliceRanges {
		p.init(t)
		_, ok, err := p.parseSliceAccessor()
		if ok {
			c.Errorf("test failed. %s parsed as valid slice range", t)
		}
		if err != nil {
			c.Errorf("test failed. parsing %s returned an error", t)
		}
	}
}

func (s parseSuite) TestAdvanceToNextExpr(c *C) {
	var p = NewParser()

	tests := []struct {
		input   string
		stopPos []int
	}{{
		// advanceToNextExpr stops on the first char if it is the start of the
		// expr.
		input:   `col1`,
		stopPos: []int{0},
	}, {
		input:   `&col`,
		stopPos: []int{0},
	}, {
		input:   `$col`,
		stopPos: []int{0},
	}, {
		input:   `word,&`,
		stopPos: []int{5},
	}, {
		input:   ` ,&`,
		stopPos: []int{2},
	}, {
		input:   ` col1`,
		stopPos: []int{1},
	}, {
		input:   `/* &Person.* */`,
		stopPos: []int{},
	}, {
		input:   `" &Person.*"`,
		stopPos: []int{},
	}, {
		input:   ` ""`,
		stopPos: []int{},
	}, {
		input:   ` /**/`,
		stopPos: []int{},
	}}

	for _, t := range tests {
		p.init(t.input)
		currentStopPos := 0
		for p.pos < len(p.input) {
			err := p.advanceToNextExpression()
			c.Assert(err, IsNil)
			if p.pos >= len(p.input) {
				break
			}

			if len(t.stopPos) <= currentStopPos {
				c.Fatalf("unexpected extra stop at position %d with input %s",
					p.pos, t.input)
			}
			c.Assert(p.pos, Equals, t.stopPos[currentStopPos])
			currentStopPos++

			p.advanceByte()
		}
	}
}
