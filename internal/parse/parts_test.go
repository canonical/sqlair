package parse

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInputPart(t *testing.T) {
	i, err := NewInputPart("mytype", "mytag")
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, i)
	assert.Equal(t, "mytype", i.TypeName())
	assert.Equal(t, "mytag", i.TagName())
	i, err = NewInputPart("", "mytag")
	assert.Equal(t, (*InputPart)(nil), i)
	assert.Equal(t, fmt.Errorf("malformed InputPart"), err)
}

func TestOutputPart(t *testing.T) {
	// Fully specified part
	i, err := NewOutputPart("mytable", "mycolumn", "mytype", "mytag")
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, i)
	assert.Equal(t, "mytable", i.Column.TableName())
	assert.Equal(t, "mycolumn", i.Column.ColumnName())
	assert.Equal(t, "mytype", i.GoType.TypeName())
	assert.Equal(t, "mytag", i.GoType.TagName())
	// Having table name but not column name is an error
	i, err = NewOutputPart("mytable", "", "mytype", "mytag")
	assert.Equal(t, (*OutputPart)(nil), i)
	assert.Equal(t, fmt.Errorf("malformed OutputPart"), err)
	// Having column name without table is OK
	i, err = NewOutputPart("", "mycolumn", "mytype", "mytag")
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, i)
	assert.Equal(t, "", i.Column.TableName())
	assert.Equal(t, "mycolumn", i.Column.ColumnName())
	assert.Equal(t, "mytype", i.GoType.TypeName())
	assert.Equal(t, "mytag", i.GoType.TagName())
	// Having tag name but no type name is an error
	i, err = NewOutputPart("mytag", "mycolumn", "", "mytag")
	assert.Equal(t, (*OutputPart)(nil), i)
	assert.Equal(t, fmt.Errorf("malformed OutputPart"), err)
	// Having type name without tag name is OK
	i, err = NewOutputPart("mytable", "mycolumn", "mytype", "")
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, i)
	assert.Equal(t, "mytable", i.Column.TableName())
	assert.Equal(t, "mycolumn", i.Column.ColumnName())
	assert.Equal(t, "mytype", i.GoType.TypeName())
	assert.Equal(t, "", i.GoType.TagName())
}
