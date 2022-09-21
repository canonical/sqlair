package parse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type parseHelperTest struct {
	bytef   func(byte) bool
	stringf func(string) bool
	result  bool
	input   string
	data    string
}

var p = NewParser()

var parseTests = []parseHelperTest{
	{bytef: isNameByte, result: true, input: "", data: "f"},
	{bytef: isNameByte, result: true, input: "", data: "0"},
	{bytef: isNameByte, result: true, input: "", data: "5"},
	{bytef: p.peekByte, result: false, input: "", data: "a"},
	{bytef: p.peekByte, result: false, input: "b", data: "a"},
	{bytef: p.peekByte, result: true, input: "a", data: "a"},
}

func TestRunTable(t *testing.T) {
	for _, v := range parseTests {
		p.Parse(v.input)
		assert.Equal(t, v.result, v.bytef(v.data[0]))
	}
}

func TestInit(t *testing.T) {
	p := NewParser()
	expr, err := p.Parse("select foo from bar")
	assert.Equal(t, nil, err)
	assert.Equal(t, (*ParsedExpr)(nil), expr)
}

func TestSkipByte(t *testing.T) {
	p := NewParser()
	p.Parse("")
	assert.Equal(t, false, p.skipByte('a'))
	p.Parse("abc")
	assert.Equal(t, false, p.skipByte('b'))
	p.Parse("abc")
	assert.Equal(t, true, p.skipByte('a'))
	assert.Equal(t, true, p.peekByte('b'))
}

func TestSkipByteFind(t *testing.T) {
	p := NewParser()
	p.Parse("")
	assert.Equal(t, false, p.skipByteFind('a'))
	p.Parse("abcde")
	assert.Equal(t, false, p.skipByteFind('x'))
	assert.Equal(t, true, p.skipByteFind('b'))
	assert.Equal(t, true, p.peekByte('c'))
	p.Parse("abcde ")
	assert.Equal(t, true, p.skipByteFind(' '))
	assert.Equal(t, false, p.skipByteFind(' '))
}

func TestSkipSpaces(t *testing.T) {
	p := NewParser()
	p.Parse("")
	assert.Equal(t, false, p.skipSpaces())
	p.Parse("abc    d")
	assert.Equal(t, false, p.skipSpaces())
	p.Parse("     abcd")
	assert.Equal(t, true, p.skipSpaces())
	assert.Equal(t, true, p.peekByte('a'))
	p.Parse("  \t  abcd")
	assert.Equal(t, true, p.skipSpaces())
	assert.Equal(t, true, p.peekByte('\t'))
}

func TestSkipString(t *testing.T) {
	p := NewParser()
	p.Parse("")
	assert.Equal(t, false, p.skipString("a"))
	p.Parse("helloworld")
	assert.Equal(t, true, p.skipString("hElLo"))
	assert.Equal(t, true, p.peekByte('w'))
	p.Parse("hello world")
	assert.Equal(t, true, p.skipString("hello"))
	assert.Equal(t, true, p.peekByte(' '))
}
