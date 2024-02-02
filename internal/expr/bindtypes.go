// Copyright 2023 Canonical Ltd.
// Licensed under Apache 2.0, see LICENCE file for details.

package expr

import (
	"bytes"
	"fmt"

	"github.com/canonical/sqlair/internal/typeinfo"
)

// ParsedExpr is the AST representation of SQLair query. It contains only
// information encoded in the SQLair query string.
type ParsedExpr struct {
	exprs []expression
}

// String returns a textual representation of the AST contained in the
// ParsedExpr for debugging and testing purposes.
func (pe *ParsedExpr) String() string {
	var out bytes.Buffer
	out.WriteString("[")
	for i, p := range pe.exprs {
		if i > 0 {
			out.WriteString(" ")
		}
		out.WriteString(p.String())
	}
	out.WriteString("]")
	return out.String()
}

// BindTypes takes samples of all types mentioned in the SQLair expressions of
// the query. The expressions are checked for validity and required information
// is generated from the types.
func (pe *ParsedExpr) BindTypes(args ...any) (tbe *TypeBoundExpr, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("cannot prepare statement: %s", err)
		}
	}()

	argInfo, err := typeinfo.GenerateArgInfo(args)
	if err != nil {
		return nil, err
	}

	// Bind types to each expression.
	var typedExprs TypeBoundExpr
	outputUsed := map[string]bool{}
	for _, expr := range pe.exprs {
		te, err := expr.bindTypes(argInfo)
		if err != nil {
			return nil, err
		}

		if toe, ok := te.(*typedOutputExpr); ok {
			for _, oc := range toe.outputColumns {
				if ok := outputUsed[oc.output.Identifier()]; ok {
					return nil, fmt.Errorf("%s appears more than once in output expressions", oc.output.Desc())
				}
				outputUsed[oc.output.Identifier()] = true
			}
		}
		typedExprs = append(typedExprs, te)
	}

	return &typedExprs, nil
}

// expression represents a parsed node of the SQLair query's AST.
type expression interface {
	// String returns a text representation for debugging and testing purposes.
	String() string

	// bindTypes binds the types to the expression to generate either a
	// *typedInputExpr or *typedOutputExpr.
	bindTypes(typeinfo.ArgInfo) (any, error)
}

// bypass represents part of the expression that we want to pass to the backend
// database verbatim.
type bypass struct {
	chunk string
}

// String returns a text representation for debugging and testing purposes.
func (b *bypass) String() string {
	return "Bypass[" + b.chunk + "]"
}

// bindTypes returns the bypass part itself since it contains no references to
// types.
func (b *bypass) bindTypes(typeinfo.ArgInfo) (any, error) {
	return b, nil
}

// memberInputExpr is an input expression of the form "$Type.member" which
// represents a query parameter contained in a member of a type.
type memberInputExpr struct {
	raw string
	ma  memberAccessor
}

// String returns a text representation for debugging and testing purposes.
func (e *memberInputExpr) String() string {
	return fmt.Sprintf("Input[%+v]", e.ma)
}

// bindTypes generates a *typedInputExpr containing type information about the
// Go object and its member.
func (e *memberInputExpr) bindTypes(argInfo typeinfo.ArgInfo) (any, error) {
	input, err := argInfo.InputMember(e.ma.typeName, e.ma.memberName)
	if err != nil {
		return nil, fmt.Errorf("input expression: %s: %s", err, e.raw)
	}
	return &typedInputExpr{input: input}, nil
}

// insertIntoAsteriskExpr is a form of input expression for insert statements e.g.
// "(*) VALUES ($Type1.member, $Type2.*)".
type insertIntoAsteriskExpr struct {
	sources []memberAccessor
	raw     string
}

func (e *insertIntoAsteriskExpr) String() string {
	return fmt.Sprintf("AsteriskInsert[[*] %v]", e.sources)
}

func (e *insertIntoAsteriskExpr) bindTypes(argInfo typeinfo.ArgInfo) (any, error) {
	return nil, fmt.Errorf("insert input expression not implemented")
}

// insertIntoColumnsExpr is a form of input expression for insert statements e.g.
// "(col1, col2, col3) VALUES ($Type.*)".
type insertIntoColumnsExpr struct {
	columns  []columnAccessor
	typeName string
	raw      string
}

func (e *insertIntoColumnsExpr) String() string {
	return fmt.Sprintf("ColumnInsert[%v [%v.*]]", e.columns, e.typeName)
}

func (e *insertIntoColumnsExpr) bindTypes(argInfo typeinfo.ArgInfo) (any, error) {
	return nil, fmt.Errorf("insert input expression not implemented")
}

// sliceInputExpr is an input expression of the form "$S[:]" that represents a
// slice of query parameters.
type sliceInputExpr struct {
	raw           string
	sliceTypeName string
}

// String returns a text representation for debugging and testing purposes.
func (e *sliceInputExpr) String() string {
	return fmt.Sprintf("Input[%s[:]]", e.sliceTypeName)
}

// bindTypes generates a *typedInputExpr containing type information about the
// slice.
func (e *sliceInputExpr) bindTypes(argInfo typeinfo.ArgInfo) (any, error) {
	input, err := argInfo.InputSlice(e.sliceTypeName)
	if err != nil {
		return nil, fmt.Errorf("input expression: %s: %s", err, e.raw)
	}
	return &typedInputExpr{input}, nil
}

// outputExpr represents columns to be read from the database and Go values to
// scan them into.
type outputExpr struct {
	sourceColumns []columnAccessor
	targetTypes   []memberAccessor
	raw           string
}

// String returns a text representation for debugging and testing purposes.
func (e *outputExpr) String() string {
	return fmt.Sprintf("Output[%+v %+v]", e.sourceColumns, e.targetTypes)
}

// bindTypes binds the output expression to concrete types. It then checks the
// expression is valid with respect to its bound types and returns a
// *typedOutputExpr.
func (e *outputExpr) bindTypes(argInfo typeinfo.ArgInfo) (te any, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("output expression: %s: %s", err, e.raw)
		}
	}()

	numTypes := len(e.targetTypes)
	numColumns := len(e.sourceColumns)
	starTypes := starCountTypes(e.targetTypes)
	starColumns := starCountColumns(e.sourceColumns)

	toe := &typedOutputExpr{}

	// Case 1: Generated columns e.g. "* AS (&P.*, &A.id)" or "&P.*".
	if numColumns == 0 || (numColumns == 1 && starColumns == 1) {
		pref := ""
		// Prepend table name. E.g. "t" in "t.* AS &P.*".
		if numColumns > 0 {
			pref = e.sourceColumns[0].tableName()
		}

		for _, t := range e.targetTypes {
			if t.memberName == "*" {
				// Generate asterisk columns.
				outputs, memberNames, err := argInfo.AllStructOutputs(t.typeName)
				if err != nil {
					return nil, err
				}
				for i, output := range outputs {
					oc := newOutputColumn(pref, memberNames[i], output)
					toe.outputColumns = append(toe.outputColumns, oc)
				}
			} else {
				// Generate explicit columns.
				output, err := argInfo.OutputMember(t.typeName, t.memberName)
				if err != nil {
					return nil, err
				}
				oc := newOutputColumn(pref, t.memberName, output)
				toe.outputColumns = append(toe.outputColumns, oc)
			}
		}
		return toe, nil
	} else if numColumns > 1 && starColumns > 0 {
		return nil, fmt.Errorf("invalid asterisk in columns")
	}

	// Case 2: Explicit columns, single asterisk type e.g. "(col1, t.col2) AS &P.*".
	if starTypes == 1 && numTypes == 1 {
		for _, c := range e.sourceColumns {
			output, err := argInfo.OutputMember(e.targetTypes[0].typeName, c.columnName())
			if err != nil {
				return nil, err
			}
			oc := newOutputColumn(c.tableName(), c.columnName(), output)
			toe.outputColumns = append(toe.outputColumns, oc)
		}
		return toe, nil
	} else if starTypes > 0 && numTypes > 1 {
		return nil, fmt.Errorf("invalid asterisk in types")
	}

	// Case 3: Explicit columns and types e.g. "(col1, col2) AS (&P.name, &P.id)".
	if numColumns == numTypes {
		for i, c := range e.sourceColumns {
			t := e.targetTypes[i]
			output, err := argInfo.OutputMember(t.typeName, t.memberName)
			if err != nil {
				return nil, err
			}
			oc := newOutputColumn(c.tableName(), c.columnName(), output)
			toe.outputColumns = append(toe.outputColumns, oc)
		}
	} else {
		return nil, fmt.Errorf("mismatched number of columns and target types")
	}

	return toe, nil
}

// starCountColumns counts the number of asterisks in a list of columns.
func starCountColumns(cs []columnAccessor) int {
	s := 0
	for _, c := range cs {
		if c.columnName() == "*" {
			s++
		}
	}
	return s
}

// starCountTypes counts the number of asterisks in a list of types.
func starCountTypes(vs []memberAccessor) int {
	s := 0
	for _, v := range vs {
		if v.memberName == "*" {
			s++
		}
	}
	return s
}
