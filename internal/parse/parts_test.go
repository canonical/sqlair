package parse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInputPart(t *testing.T) {
	i, err := NewInputPart("mytype", "mytag")
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, i)
	assert.Equal(t, "mytype", i.Prefix)
	assert.Equal(t, "mytag", i.Name)
}

func TestOutputPart(t *testing.T) {
	// Fully specified part
	p, err := NewOutputPart("mytable", "mycolumn", "mytype", "mytag")
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, p)
	assert.Equal(t, "mytable", p.Source.Prefix)
	assert.Equal(t, "mycolumn", p.Source.Name)
	assert.Equal(t, "mytype", p.Target.Prefix)
	assert.Equal(t, "mytag", p.Target.Name)
	// Having column name without table is OK
	p, err = NewOutputPart("", "mycolumn", "mytype", "mytag")
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, p)
	assert.Equal(t, "", p.Source.Prefix)
	assert.Equal(t, "mycolumn", p.Source.Name)
	assert.Equal(t, "mytype", p.Target.Prefix)
	assert.Equal(t, "mytag", p.Target.Name)
	// Having type name without tag name is OK
	p, err = NewOutputPart("mytable", "mycolumn", "mytype", "")
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, p)
	assert.Equal(t, "mytable", p.Source.Prefix)
	assert.Equal(t, "mycolumn", p.Source.Name)
	assert.Equal(t, "mytype", p.Target.Prefix)
	assert.Equal(t, "", p.Target.Name)
}
