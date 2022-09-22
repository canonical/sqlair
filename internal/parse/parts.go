package parse

import "fmt"

// queryPart is an interface to be implemented by all parts.
type queryPart interface {
	// String-like output for debugging purposes
	String() string

	// Translate part to its Sql equivalent
	ToSql() string
}

// qualifiedExpression represents a qualified expression
// qualified. Examples are:
//	... column as &Person ... 	<--- "column" is unqualified
//	... t.column as &Person.name	<--- "name" and "column" and qualified
type qualifiedExpression struct {
	qualifier string
	name      string
}

// InputPart represents an expression used as input in the SDL query.
// Examples:
//	$Address
//	$Address.postal_code
type InputPart struct {
	goType
}

// NewInputPart is a constructor that returns a pointer to an InputPart.
func NewInputPart(typeName string, tagName string) (*InputPart, error) {
	if typeName == "" && tagName != "" {
		return nil, fmt.Errorf("malformed InputPart")
	}
	return &InputPart{
		goType{
			qualifiedExpression{qualifier: typeName,
				name: tagName,
			},
		},
	}, nil
}

func (ip *InputPart) String() string {
	return ""
}

func (ip *InputPart) ToSql() string {
	return ""
}

// TypeName returns the name of the type in the input part or empty string if
// none is present.
func (ip *InputPart) TypeName() string {
	return ip.qualifier
}

// TagName returns the name of the tag in the input part or empty string if
// none is present.
func (ip *InputPart) TagName() string {
	return ip.name
}

// A column is an internal type that represents the columns selected in the
// query.
type column struct {
	qualifiedExpression
}

// TableName returns the name of the table that qualifies a column or empty
// string if there is none. Examples:
//
// select t.column	<--- TableName() returns "t"
// select column	<--- TableName() returns ""
func (c *column) TableName() string {
	return c.qualifier
}

// ColumnName returns the name of the column that is selected in a query or an
// empty string if there is none. Examples:
//
// select t.column	<--- TableName() returns "column"
func (c *column) ColumnName() string {
	return c.name
}

// A goType is an internal type that represents the type part of an output
// expression in our SDL
type goType struct {
	qualifiedExpression
}

// TypeName returns the name of the type in the output part or empty string if
// none is present.
func (gt *goType) TypeName() string {
	return gt.qualifier
}

// TagName returns the name of the type in the output part or empty string if
// none is present.
func (gt *goType) TagName() string {
	return gt.name
}

// OutputPart represents an expression to be used as output in our SDL.
// Examples:
// 	&Person
//	&Person.name
type OutputPart struct {
	Column column
	GoType goType
}

// NewOutputPart is a constructor that returns a pointer to an OutputPart.
func NewOutputPart(tableName string, colName string,
	typeName string, tagName string) (*OutputPart, error) {
	if (tableName != "" && colName == "") || (typeName == "" && tagName != "") {
		return nil, fmt.Errorf("malformed OutputPart")
	}
	return &OutputPart{
		column{
			qualifiedExpression{qualifier: tableName,
				name: colName,
			},
		},
		goType{
			qualifiedExpression{qualifier: typeName,
				name: tagName,
			},
		},
	}, nil
}

func (op *OutputPart) String() string {
	return ""
}

func (op *OutputPart) ToSql() string {
	return ""
}

// passthroughPart represents a part of the SDL that we want to pass to the
// backend database verbatim.
type PassthroughPart struct {
	Chunk string
}

func (pt *PassthroughPart) String() string {
	return ""
}

func (pt *PassthroughPart) ToSql() string {
	return ""
}
