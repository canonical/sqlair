package expr

import (
	. "gopkg.in/check.v1"
)

type parseHelperTest struct {
	bytef    func(byte) bool
	stringf  func(string) bool
	stringf0 func() bool
	result   []bool
	input    string
	data     []string
}

func (s *ExprInternalSuite) TestRunTable(c *C) {
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

		{stringf0: p.skipNumber, result: []bool{true}, input: "2d3d", data: []string{}},
		{stringf0: p.skipNumber, result: []bool{false}, input: "-2", data: []string{}},
		{stringf0: p.skipNumber, result: []bool{false}, input: "a2", data: []string{}},
		{stringf0: p.skipNumber, result: []bool{true}, input: "2123918", data: []string{}},
		{stringf0: p.skipNumber, result: []bool{true}, input: "2123918as", data: []string{}},
	}
	for _, v := range parseTests {
		// Reset the input.
		p.init(v.input)
		for i, _ := range v.result {
			var result bool
			if v.bytef != nil {
				result = v.bytef(v.data[i][0])
			}
			if v.stringf != nil {
				result = v.stringf(v.data[i])
			}
			if v.stringf0 != nil {
				result = v.stringf0()
			}
			if v.result[i] != result {
				c.Errorf("Test %#v failed. Expected: '%t', got '%t'\n", v, v.result[i], result)
			}
		}
	}
}

func (s *ExprInternalSuite) TestValidQuotes(c *C) {
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

func (s *ExprInternalSuite) TestInvalidQuote(c *C) {
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

func (s *ExprInternalSuite) TestUnfinishedQuote(c *C) {
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

func (s *ExprInternalSuite) TestRemoveComments(c *C) {
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

func (s *ExprInternalSuite) TestParseSliceRange(c *C) {
	validSliceRanges := []struct {
		input  string
		output valueAccessor
	}{
		{"mySlice[:]", sliceRangeAccessor{typ: "mySlice", low: "", high: ""}},
		{"mySlice[ : ]", sliceRangeAccessor{typ: "mySlice", low: "", high: ""}},
		{"mySlice[1020:]", sliceRangeAccessor{typ: "mySlice", low: "1020", high: ""}},
		{"mySlice[:33]", sliceRangeAccessor{typ: "mySlice", low: "", high: "33"}},
		{"mySlice[12:34]", sliceRangeAccessor{typ: "mySlice", low: "12", high: "34"}},
		{"mySlice[ 12  : 34   ]", sliceRangeAccessor{typ: "mySlice", low: "12", high: "34"}},
		{"mySlice[1234]", sliceIndexAccessor{typ: "mySlice", index: 1234}},
		{"mySlice[ 0 ]", sliceIndexAccessor{typ: "mySlice", index: 0}},
	}
	invalidSliceRanges := []struct {
		input  string
		errMsg string
	}{
		{input: "[]"},
		{input: "[:]"},
		{input: "[1:10]"},
		{input: "[1]"},
		{input: "name[:-1]", errMsg: `column 7: invalid slice: expected ]`},
		{input: "name[3:1]", errMsg: `column 1: invalid slice: invalid indexes: "1" <= "3"`},
		{input: "name[1:1]", errMsg: `column 1: invalid slice: invalid indexes: "1" <= "1"`},
		{input: "name[a:]", errMsg: `column 6: invalid slice: expected index or colon`},
		{input: "name[:b]", errMsg: `column 7: invalid slice: expected ]`},
		{input: "name[1a2:]", errMsg: `column 7: invalid slice: expected ] or colon`},
		{input: "name[1 2:]", errMsg: `column 8: invalid slice: expected ] or colon`},
		{input: "name[:1 2]", errMsg: `column 9: invalid slice: expected ]`},
		{input: "name[:1b2]", errMsg: `column 8: invalid slice: expected ]`},
		{input: "name[1a:2b]", errMsg: `column 7: invalid slice: expected ] or colon`},
		{input: "name[1a]", errMsg: `column 7: invalid slice: expected ] or colon`},
		{input: "name[a1]", errMsg: `column 6: invalid slice: expected index or colon`},
		{input: "name[]", errMsg: `column 6: invalid slice: expected index or colon`},
	}

	var p = NewParser()
	for _, t := range validSliceRanges {
		p.init(t.input)
		sr, ok, err := p.parseSliceAccessor()
		if !ok || err != nil {
			c.Errorf("test failed. %s not parsed as valid slice range", t.input)
		}
		c.Assert(t.output, DeepEquals, sr)
	}
	for _, t := range invalidSliceRanges {
		p.init(t.input)
		_, ok, err := p.parseSliceAccessor()
		if ok && err == nil {
			c.Errorf("test failed. %s parsed as valid slice range", t)
		}
		if err != nil {
			c.Assert(err.Error(), Equals, t.errMsg, Commentf(t.input))
		}
	}
}
