// Copyright 2023 Canonical Ltd.
// Licensed under Apache 2.0, see LICENCE file for details.

package expr

import (
	"bytes"
	"fmt"
	"reflect"

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
	teb := newTypedExprBuilder(argInfo)
	for _, expr := range pe.exprs {
		if err := expr.bindTypes(teb); err != nil {
			return nil, err
		}
	}

	return teb.Build()
}

// expression represents a parsed node of the SQLair query's AST.
type expression interface {
	// String returns a text representation for debugging and testing purposes.
	String() string

	// bindTypes binds the types to the expression to generate a typedExpr and
	// adds it to the typedExprBuilder.
	bindTypes(*typedExprBuilder) error
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

// bindTypes adds the bypass part, unchanged, to the typedExprBuilder.
func (b *bypass) bindTypes(teb *typedExprBuilder) error {
	teb.AddBypass(b)
	return nil
}

// addToQuery adds the bypass part to the query builder.
func (b *bypass) addToQuery(qb *queryBuilder, _ typeinfo.TypeToValue) error {
	return qb.addBypass(b)
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

// bindTypes generates a typed input expression containing type information
// about the Go object and its member. This is then added to the
// typedExprBuilder.
func (e *memberInputExpr) bindTypes(teb *typedExprBuilder) error {
	input, err := teb.InputMember(e.ma.typeName, e.ma.memberName)
	if err != nil {
		return fmt.Errorf("input expression: %s: %s", err, e.raw)
	}
	teb.AddTypedInputExpr(input)
	return nil
}

// asteriskInsertExpr is an input expression occurring within an INSERT
// statement that consists of an asterisk on the left and explicit type accessors
// on the right. This means that SQLair generates the columns.
// e.g. "(*) VALUES ($Type1.col1, $Type2.*)".
type asteriskInsertExpr struct {
	sources []memberAccessor
	raw     string
}

// String returns a text representation for debugging and testing purposes.
func (e *asteriskInsertExpr) String() string {
	return fmt.Sprintf("AsteriskInsert[[*] %v]", e.sources)
}

// bindTypes generates a typed insert expression containing type information
// about the asteriskInsertExpr. This is added to the typedExprBuilder.
func (e *asteriskInsertExpr) bindTypes(teb *typedExprBuilder) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("input expression: %s: %s", err, e.raw)
		}
	}()

	var cols []typedColumn
	for _, source := range e.sources {
		if source.memberName == "*" {
			inputs, tags, err := teb.AllStructInputs(source.typeName)
			if err != nil {
				return err
			}
			for i, input := range inputs {
				c := newInsertColumn(input, tags[i], false)
				cols = append(cols, c)
			}
		} else {
			input, err := teb.InputMember(source.typeName, source.memberName)
			if err != nil {
				return err
			}
			c := newInsertColumn(input, source.memberName, true)
			cols = append(cols, c)
		}
	}
	teb.AddTypedInsertExpr(cols)
	return nil
}

// columnsInsertExpr is an input expression occurring within an INSERT statement
// that consists of explicit columns on the left and type accessors on the right.
// e.g. "(col1, col2, col3) VALUES ($Type.*, $Type2.col1)".
type columnsInsertExpr struct {
	columns []columnAccessor
	sources []memberAccessor
	raw     string
}

// String returns a text representation for debugging and testing purposes.
func (e *columnsInsertExpr) String() string {
	return fmt.Sprintf("ColumnInsert[%v %v]", e.columns, e.sources)
}

// bindTypes generates a typed insert expression containing type information
// about the columnsInsertExpr. It checks that all the listed columns are
// provided by the supplied types. If a map with an asterisk is passed, the
// spare columns are taken from that map.
//
// The generated typed expression is then added to the typedExprBuilder.
func (e *columnsInsertExpr) bindTypes(teb *typedExprBuilder) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("input expression: %s: %s", err, e.raw)
		}
	}()

	// 1. Work out all the columns available on the right hand side of the insert
	// expression. For each valueAccessor on the right, save all the columns names
	// that can be found in the type it specifies.
	// We want to store a list of inputs instead of throwing out an error early.
	// Because if the column is not used, the query would still be valid even
	// if two structs clash.
	colToInput := make(map[string][]typeinfo.Input)
	// remainingMap stores the map with an asterisk if passed, the remaining
	// columns are taken from it later.
	var remainingMap *string
	for _, source := range e.sources {
		if source.memberName == "*" {
			kind, err := teb.Kind(source.typeName)
			if err != nil {
				return err
			}
			// If we find a map save it for later to match the spare columns.
			if kind == reflect.Map {
				if remainingMap != nil {
					return fmt.Errorf("cannot use more than one map with asterisk")
				}
				remainingMapTypeName := source.typeName
				remainingMap = &remainingMapTypeName
				continue
			}
			inps, tags, err := teb.AllStructInputs(source.typeName)
			if err != nil {
				return err
			}
			for i := range tags {
				colToInput[tags[i]] = append(colToInput[tags[i]], inps[i])
			}
		} else {
			inp, err := teb.InputMember(source.typeName, source.memberName)
			if err != nil {
				return err
			}
			colToInput[source.memberName] = []typeinfo.Input{inp}
		}
	}

	// 2. We go over all the columns listed on the left of the insert expression
	// and match them against the columns provided by the types on the right.
	// If a map with an asterisk is present, we use it to supply the spare
	// columns. If it is not present, a spare column is an error.
	var cols []typedColumn
	for _, column := range e.columns {
		columnStr := column.String()
		input, ok := colToInput[columnStr]
		if !ok && remainingMap != nil {
			// The spare columns must belong to the map.
			inp, err := teb.InputMember(*remainingMap, columnStr)
			if err != nil {
				// unreachable
				return err
			}
			input = []typeinfo.Input{inp}
		} else if !ok {
			return fmt.Errorf("missing type that provides column %q", columnStr)
		}
		if len(input) > 1 {
			return fmt.Errorf("more than one type provides column %q", columnStr)
		}

		c := newInsertColumn(input[0], columnStr, true)
		cols = append(cols, c)
	}
	teb.AddTypedInsertExpr(cols)
	return nil
}

// basicInsertExpr is an input expression occurring within an INSERT statement
// that consists of columns on the left and type accessors or literals on the
// right. Unlike the columnInsertExpr, the values on the right are independent
// of the columns on the left and are matched by position rather than by name.
// e.g. (col1, col2, col3) VALUES ($M.key, "literal value", $T.value).
type basicInsertExpr struct {
	columns []columnAccessor
	sources []valueAccessor
	raw     string
}

// String returns a text representation for debugging and testing purposes.
func (e *basicInsertExpr) String() string {
	return fmt.Sprintf("BasicInsert[%v %v]", e.columns, e.sources)
}

// bindTypes generates a typed insert expression containing type information
// about the values to be inserted in the basicInsertExpr. The typed expression
// is added to the typedExprBuilder.
func (e *basicInsertExpr) bindTypes(teb *typedExprBuilder) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("input expression: %s: %s", err, e.raw)
		}
	}()
	if len(e.columns) != len(e.sources) {
		return fmt.Errorf("mismatched number of columns and values: %d != %d", len(e.columns), len(e.sources))
	}
	var cols []typedColumn
	for i, source := range e.sources {
		col, err := source.typedColumn(teb, e.columns[i].columnName())
		if err != nil {
			return err
		}
		cols = append(cols, col)
	}
	teb.AddTypedInsertExpr(cols)
	return nil
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

// bindTypes generates a typed input expression containing type information
// about the slice. This is then added to the typedExprBuilder.
func (e *sliceInputExpr) bindTypes(teb *typedExprBuilder) error {
	input, err := teb.InputSlice(e.sliceTypeName)
	if err != nil {
		return fmt.Errorf("input expression: %s: %s", err, e.raw)
	}
	teb.AddTypedInputExpr(input)
	return nil
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
// expression is valid with respect to its bound types and adds the typed
// expression to the typedExprBuilder.
func (e *outputExpr) bindTypes(teb *typedExprBuilder) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("output expression: %s: %s", err, e.raw)
		}
	}()

	numTypes := len(e.targetTypes)
	numColumns := len(e.sourceColumns)
	starTypes := starCountTypes(e.targetTypes)
	starColumns := starCountColumns(e.sourceColumns)

	var outputColumns []outputColumn

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
				outputs, memberNames, err := teb.AllStructOutputs(t.typeName)
				if err != nil {
					return err
				}
				for i, output := range outputs {
					oc := newOutputColumn(pref, memberNames[i], output)
					outputColumns = append(outputColumns, oc)
				}
			} else {
				// Generate explicit columns.
				output, err := teb.OutputMember(t.typeName, t.memberName)
				if err != nil {
					return err
				}
				oc := newOutputColumn(pref, t.memberName, output)
				outputColumns = append(outputColumns, oc)
			}
		}
		teb.AddTypedOutputExpr(outputColumns)
		return nil
	} else if numColumns > 1 && starColumns > 0 {
		return fmt.Errorf("invalid asterisk in columns")
	}

	// Case 2: Explicit columns, single asterisk type e.g. "(col1, t.col2) AS &P.*".
	if starTypes == 1 && numTypes == 1 {
		for _, c := range e.sourceColumns {
			output, err := teb.OutputMember(e.targetTypes[0].typeName, c.columnName())
			if err != nil {
				return err
			}
			oc := newOutputColumn(c.tableName(), c.columnName(), output)
			outputColumns = append(outputColumns, oc)
		}
		teb.AddTypedOutputExpr(outputColumns)
		return nil
	} else if starTypes > 0 && numTypes > 1 {
		return fmt.Errorf("invalid asterisk in types")
	}

	// Case 3: Explicit columns and types e.g. "(col1, col2) AS (&P.name, &P.id)".
	if numColumns == numTypes {
		for i, c := range e.sourceColumns {
			t := e.targetTypes[i]
			output, err := teb.OutputMember(t.typeName, t.memberName)
			if err != nil {
				return err
			}
			oc := newOutputColumn(c.tableName(), c.columnName(), output)
			outputColumns = append(outputColumns, oc)
		}
	} else {
		return fmt.Errorf("mismatched number of columns and target types")
	}

	teb.AddTypedOutputExpr(outputColumns)
	return nil
}

// valueAccessor defines an accessor that can be used to generate a typedColumn
// with the given column name.
type valueAccessor interface {
	// typedColumn generates a typedColumn that associates the given colum name
	// with the value specified by the valueAccessor.
	typedColumn(teb *typedExprBuilder, columnName string) (typedColumn, error)
}

// memberAccessor stores information for accessing a keyed Go value. It consists
// of a type name and some value within it to be accessed. For example: a field
// of a struct, or a key of a map.
type memberAccessor struct {
	typeName, memberName string
}

// literal represents a literal expression be pasted verbatim as the value in an
// insert column.
type literal struct {
	value string
}

func (l literal) String() string {
	return l.value
}

// typedColumn generates a typedColumn with the given name from a literal.
func (l literal) typedColumn(_ *typedExprBuilder, columnName string) (typedColumn, error) {
	lc := newLiteralColumn(columnName, l.value)
	return lc, nil
}

func (ma memberAccessor) String() string {
	return ma.typeName + "." + ma.memberName
}

// typedColumn generates a typedColumn with the input specified by the member
// accessor and the given column name.
func (ma memberAccessor) typedColumn(teb *typedExprBuilder, columnName string) (typedColumn, error) {
	input, err := teb.InputMember(ma.typeName, ma.memberName)
	if err != nil {
		return nil, err
	}
	ic := newInsertColumn(input, columnName, true)
	return ic, nil
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
