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
