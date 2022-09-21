package parse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type parseHelperTest struct {
	bytef    func(byte) bool
	stringf  func(string) bool
	stringf0 func() bool
	result   []bool
	input    string
	data     []string
}

var p = NewParser()

var parseTests = []parseHelperTest{
	{bytef: isNameByte, result: []bool{true}, input: "", data: []string{"f"}},
	{bytef: isNameByte, result: []bool{true}, input: "", data: []string{"0"}},
	{bytef: isNameByte, result: []bool{true}, input: "", data: []string{"5"}},

	{bytef: p.peekByte, result: []bool{false}, input: "", data: []string{"a"}},
	{bytef: p.peekByte, result: []bool{false}, input: "b", data: []string{"a"}},
	{bytef: p.peekByte, result: []bool{true}, input: "a", data: []string{"a"}},

	{bytef: p.skipByte, result: []bool{false}, input: "", data: []string{"a"}},
	{bytef: p.skipByte, result: []bool{false}, input: "abc", data: []string{"b"}},
	{bytef: p.skipByte, result: []bool{true, true}, input: "abc", data: []string{"a", "b"}},

	{bytef: p.skipByteFind, result: []bool{false}, input: "", data: []string{"a"}},
	{bytef: p.skipByteFind, result: []bool{false, true, true}, input: "abcde", data: []string{"x", "b", "c"}},
	{bytef: p.skipByteFind, result: []bool{true, false}, input: "abcde ", data: []string{" ", " "}},

	{stringf0: p.skipSpaces, result: []bool{false}, input: "", data: []string{}},
	{stringf0: p.skipSpaces, result: []bool{false}, input: "abc    d", data: []string{}},
	{stringf0: p.skipSpaces, result: []bool{true}, input: "     abcd", data: []string{}},
	{stringf0: p.skipSpaces, result: []bool{true}, input: "  \t  abcd", data: []string{}},
	{stringf0: p.skipSpaces, result: []bool{false}, input: "\t  abcd", data: []string{}},

	{stringf: p.skipString, result: []bool{false}, input: "", data: []string{"a"}},
	{stringf: p.skipString, result: []bool{true, true}, input: "helloworld", data: []string{"hElLo", "w"}},
	{stringf: p.skipString, result: []bool{true, true}, input: "hello world", data: []string{"hello", " "}},
}

func TestRunTable(t *testing.T) {
	for _, v := range parseTests {
		p.Parse(v.input)
		for i, _ := range v.result {
			if v.bytef != nil {
				assert.Equal(t, v.result[i], v.bytef(v.data[i][0]))
			}
			if v.stringf != nil {
				assert.Equal(t, v.result[i], v.stringf(v.data[i]))
			}
			if v.stringf0 != nil {
				assert.Equal(t, v.result[i], v.stringf0())
			}
		}
	}
}

func TestInit(t *testing.T) {
	p := NewParser()
	expr, err := p.Parse("select foo from bar")
	assert.Equal(t, nil, err)
	assert.Equal(t, (*ParsedExpr)(nil), expr)
}
