package parse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInit(t *testing.T) {
	p := NewParser()
	expr, err := p.Parse("select foo from bar")
	assert.Equal(t, nil, err)
	assert.Equal(t, (*ParsedExpr)(nil), expr)
}

func TestPeekByte(t *testing.T) {
	p := NewParser()
	p.Parse("")
	assert.Equal(t, false, p.peekByte('a'))
	p.Parse("b")
	assert.Equal(t, false, p.peekByte('a'))
	p.Parse("a")
	assert.Equal(t, true, p.peekByte('a'))
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

func TestIsNameByte(t *testing.T) {
	assert.Equal(t, true, isNameByte('f'))
	assert.Equal(t, true, isNameByte('0'))
	assert.Equal(t, true, isNameByte('5'))
	assert.Equal(t, false, isNameByte('à'))
	assert.Equal(t, false, isNameByte('\n'))
	assert.Equal(t, false, isNameByte('\r'))
	assert.Equal(t, false, isNameByte('\t'))
}
