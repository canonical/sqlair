package expr

import (
	. "gopkg.in/check.v1"
)

type parseHelperTest struct {
	charf    func(rune) bool
	stringf  func(string) bool
	stringf0 func() bool
	result   []bool
	input    string
	data     []string
}

func (s *ExprInternalSuite) TestRunTable(c *C) {
	var p = NewParser()
	var parseTests = []parseHelperTest{

		{charf: p.peekChar, result: []bool{false}, input: "", data: []string{"a"}},
		{charf: p.peekChar, result: []bool{false}, input: "b", data: []string{"a"}},
		{charf: p.peekChar, result: []bool{true}, input: "a", data: []string{"a"}},

		{charf: p.skipChar, result: []bool{false}, input: "", data: []string{"a"}},
		{charf: p.skipChar, result: []bool{false}, input: "abc", data: []string{"b"}},
		{charf: p.skipChar, result: []bool{true, true}, input: "abc", data: []string{"a", "b"}},

		{charf: p.skipCharFind, result: []bool{false}, input: "", data: []string{"a"}},
		{charf: p.skipCharFind, result: []bool{false, true, true}, input: "abcde", data: []string{"x", "b", "c"}},
		{charf: p.skipCharFind, result: []bool{true, false}, input: "abcde ", data: []string{" ", " "}},

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
	}
	for _, v := range parseTests {
		// Reset the input.
		p.init(v.input)
		for i, _ := range v.result {
			var result bool
			if v.charf != nil {
				result = v.charf(rune(v.data[i][0]))
			}
			if v.stringf != nil {
				result = v.stringf(v.data[i])
			}
			if v.stringf0 != nil {
				result = v.stringf0()
			}
			if v.result[i] != result {
				c.Errorf("Test %#v failed. Expected: '%t', got '%t'\n", v, result, v.result[i])
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
