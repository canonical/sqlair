package parse

import (
	"bytes"
	"strings"
)

// Expression describes a token or tokens in a Sqlair DSL statement
// that represent a coherent, discrete subset of the DSL grammar.
type Expression interface {
	// Expressions returns the child expressions
	// that constitute this parent expression.
	Expressions() []Expression

	// Begin returns the starting position of the expression.
	Begin() Position

	// End returns the end position of the expression.
	End() Position

	// String returns the string that constitutes the expression.
	String() string
}

// TypeMappingExpression describes an expression that
// is for mapping inputs or outputs to Go types.
type TypeMappingExpression interface {
	Expression

	// TypeName returns the type name used in this expression,
	// such as "Person" in "&Person.*" or "$Person.id".
	TypeName() Expression
}

// parentExpressionBase implements base functionality for working
// with expressions that are parents of other expressions.
type parentExpressionBase struct {
	children []Expression
}

// Expressions returns all the child expressions for this parent.
func (e *parentExpressionBase) Expressions() []Expression {
	return e.children
}

// Begin implements Expression by returning the start
// Position of this Expression's first Token.
func (e *parentExpressionBase) Begin() Position {
	if len(e.children) > 0 {
		return e.children[0].Begin()
	}
	return Position{}
}

// End implements Expression by returning the end
// Position of this Expression's last Token.
func (e *parentExpressionBase) End() Position {
	if l := len(e.children); l > 0 {
		return e.children[l-1].End()
	}
	return Position{}
}

// AppendExpression appends the input expression to this parent's children.
func (e *parentExpressionBase) AppendExpression(child Expression) {
	e.children = append(e.children, child)
}

// SQLExpression is a parent expression representing
// a full structured query language query.
type SQLExpression struct {
	parentExpressionBase
}

func (e *SQLExpression) String() string {
	var sb strings.Builder
	for i, exp := range e.Expressions() {
		if i > 0 {
			sb.WriteByte(' ')
		}
		sb.WriteString(exp.String())
	}
	return sb.String()
}

// DMLExpression is a parent expression representing a data modification
// language statement, i.e insert, update or delete.
type DMLExpression struct {
	parentExpressionBase
}

func (e *DMLExpression) String() string {
	var sb bytes.Buffer
	for _, exp := range e.Expressions() {
		sb.WriteString(exp.String())
	}
	return sb.String()
}

// DDLExpression is a parent expression representing a data definition
// language statement such as a table creation.
type DDLExpression struct {
	parentExpressionBase
}

func (e *DDLExpression) String() string {
	var sb bytes.Buffer
	for _, exp := range e.Expressions() {
		sb.WriteString(exp.String())
	}
	return sb.String()
}

// GroupedColumnsExpression is an expression representing a list of columns
// to be selected into a struct reference, as found within a query.
// Example:
// "(id, name)" in "SELECT (id, name) AS &Person.* FROM person;"
type GroupedColumnsExpression struct {
	parentExpressionBase
}

func (e *GroupedColumnsExpression) String() string {
	var sb strings.Builder
	sb.WriteByte('(')
	for i, exp := range e.Expressions() {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(exp.String())
	}
	sb.WriteByte(')')
	return sb.String()
}

// OutputTargetExpression is an expression representing a type
// into which the output of a SQL query is to be mapped.
// Example:
// "&Person.*" in "SELECT &Person.* FROM person;"
type OutputTargetExpression struct {
	marker Token
	name   *IdentityExpression
	field  *IdentityExpression
}

// NewOutputTargetExpression returns a reference to a new
// OutputTargetExpression based on the input arguments.
func NewOutputTargetExpression(
	marker Token, name *IdentityExpression, field *IdentityExpression,
) *OutputTargetExpression {
	return &OutputTargetExpression{
		marker: marker,
		name:   name,
		field:  field,
	}
}

// Expressions implements Expression by returning the child Expressions.
func (e *OutputTargetExpression) Expressions() []Expression {
	return []Expression{e.name, e.field}
}

// Begin implements Expression by returning the
// Position of this Expression's first Token.
func (e *OutputTargetExpression) Begin() Position {
	return e.marker.Pos
}

func (e *OutputTargetExpression) End() Position {
	return e.field.End()
}

func (e *OutputTargetExpression) String() string {
	return strings.Join([]string{e.marker.Literal, e.name.String(), ".", e.field.String()}, "")
}

func (e *OutputTargetExpression) TypeName() Expression {
	return e.name
}

// InputSourceExpression is an expression representing a type
// from which parameters of a statement are to be sourced.
// Example:
// "$Person.id" in "UPDATE person SET surname='Hitchens' WHERE id=$Person.id;"
type InputSourceExpression struct {
	marker Token
	name   Expression
	field  Expression
}

// NewInputSourceExpression returns a reference to a new
// InputSourceExpression based on the input arguments.
func NewInputSourceExpression(
	marker Token, name *IdentityExpression, field *IdentityExpression,
) *InputSourceExpression {
	return &InputSourceExpression{
		marker: marker,
		name:   name,
		field:  field,
	}
}

// Expressions implements Expression by returning the child Expressions.
func (e *InputSourceExpression) Expressions() []Expression {
	return []Expression{e.name, e.field}
}

// Begin implements Expression by returning the
// Position of this Expression's first Token.
func (e *InputSourceExpression) Begin() Position {
	return e.marker.Pos
}

func (e *InputSourceExpression) End() Position {
	return e.field.End()
}

func (e *InputSourceExpression) String() string {
	return strings.Join([]string{e.marker.Literal, e.name.String(), ".", e.field.String()}, "")
}

func (e *InputSourceExpression) TypeName() Expression {
	return e.name
}

// IdentityExpression is an expression that identifies a single entity.
type IdentityExpression struct {
	token Token
}

// NewIdentityExpression returns a reference to a new
// IdentityExpression based on the input Token.
func NewIdentityExpression(token Token) *IdentityExpression {
	return &IdentityExpression{token: token}
}

// Expressions implements Expression by returning the child Expressions.
func (e *IdentityExpression) Expressions() []Expression {
	return nil
}

// Begin implements Expression by returning the
// Position of this Expression's first Token.
func (e *IdentityExpression) Begin() Position {
	return e.token.Pos
}

func (e *IdentityExpression) End() Position {
	return Position{
		Offset: e.token.Pos.Offset + len(e.token.Literal),
	}
}

func (e *IdentityExpression) String() string {
	return e.token.Literal
}

// PassThroughExpression is an expression representing a chunk of SQL, DML
// or SQL that Sqlair will effectively ignore and pass to the DB as is.
type PassThroughExpression struct {
	parentExpressionBase
}

func (e *PassThroughExpression) String() string {
	var sb strings.Builder
	for _, exp := range e.Expressions() {
		sb.WriteString(exp.String())
	}
	return sb.String()
}

// Walk recursively iterates depth-first over the input expression tree,
// calling the input function for each visited expression.
// If it returns an error, the iteration terminates.
func Walk(parent Expression, visit func(Expression) error) error {
	if err := visit(parent); err != nil {
		return err
	}
	for _, child := range parent.Expressions() {
		if err := Walk(child, visit); err != nil {
			return err
		}
	}
	return nil
}
