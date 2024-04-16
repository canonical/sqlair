// Copyright 2023 Canonical Ltd.
// Licensed under Apache 2.0, see LICENCE file for details.

package expr

import (
	"fmt"

	"github.com/canonical/sqlair/internal/typeinfo"
)

// typedExpr represents a SQLair expression bound to a type that can be added to
// a query.
type typedExpr interface {
	addToQuery(*queryBuilder, typeinfo.TypeToValue) error
}

// TypeBoundExpr represents a SQLair statement bound to concrete Go types. It
// contains information used to generate the underlying SQL query and map it to
// the SQLair query.
type TypeBoundExpr struct {
	typedExprs []typedExpr
}

// BindInputs takes the SQLair input arguments and returns the PrimedQuery ready
// for use with the database.
func (tbe *TypeBoundExpr) BindInputs(args ...any) (pq *PrimedQuery, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("invalid input parameter: %s", err)
		}
	}()

	typeToValue, err := typeinfo.ValidateInputs(args)
	if err != nil {
		return nil, err
	}

	qb := newQueryBuilder()
	for _, te := range tbe.typedExprs {
		if err := te.addToQuery(qb, typeToValue); err != nil {
			return nil, err
		}
	}

	if err := qb.checkAllArgsUsed(typeToValue); err != nil {
		return nil, err
	}

	return &PrimedQuery{outputs: qb.outputs, sql: qb.sqlBuilder.getSQL(), params: qb.namedInputs}, nil
}

// typedInputExpr stores information about a Go value to use as a standalone query
// input.
type typedInputExpr struct {
	input typeinfo.Input
}

// addToQuery adds the typed input expressions to the query builder.
func (te *typedInputExpr) addToQuery(qb *queryBuilder, typeToValue typeinfo.TypeToValue) error {
	params, err := te.input.LocateParams(typeToValue)
	if err != nil {
		return err
	}
	if params.Omit {
		return omitEmptyInputError(te.input.Desc())
	}
	if params.Bulk {
		return fmt.Errorf("cannot use bulk inputs outside an insert statement")
	}
	qb.markArgUsed(params.ArgTypeUsed)

	qb.addInputs(params.Vals)
	return nil
}

// typedColumn represents a column and input locator in an insert statement.
type typedColumn interface {
	// bindInputs binds a concrete value to a typedColumn to generate a
	// boundInsertColumn.
	bindInputs(tv typeinfo.TypeToValue, ia *inputAssigner) (*boundInsertColumn, error)
}

// typedInsertExpr stores information about the Go values to use as inputs inside
// an INSERT statement.
type typedInsertExpr struct {
	insertColumns []typedColumn
}

// addToQuery adds the typed insert expressions to the query builder.
func (te *typedInsertExpr) addToQuery(qb *queryBuilder, typeToValue typeinfo.TypeToValue) error {
	var boundColumns []*boundInsertColumn
	bulk := false
	numRows := 1
	// firstBulkColumn stores the type name of the first column used in
	// a bulk insert. This is used for error messages.
	var firstBulkColumn string
	for _, ic := range te.insertColumns {
		bc, err := ic.bindInputs(typeToValue, qb.inputAssigner)
		if err != nil {
			return err
		}

		if bc.bulk {
			if !bulk {
				// First bulk row.
				bulk = true
				firstBulkColumn = bc.inputName
				numRows = len(bc.vals)
			} else if len(bc.vals) != numRows {
				return mismatchedBulkLengthsError(firstBulkColumn, numRows, bc.inputName, len(bc.vals))
			}
		}

		if bc.argType != nil {
			qb.markArgUsed(bc.argType)
		}

		boundColumns = append(boundColumns, bc)
	}
	return qb.addInsert(boundColumns, numRows)
}

// typedOutputExpr contains the columns to fetch from the database and
// information about the Go values to read the query results into.
type typedOutputExpr struct {
	outputColumns []outputColumn
}

// addToQuery adds the typed output expressions to the query builder.
func (te *typedOutputExpr) addToQuery(qb *queryBuilder, _ typeinfo.TypeToValue) error {
	var columns []string
	var outputs []typeinfo.Output
	for _, oc := range te.outputColumns {
		outputs = append(outputs, oc.output)
		columns = append(columns, oc.column)
	}
	qb.addOutput(columns, outputs)
	return nil
}

// insertColumn stores information about a single column of a row in an insert
// statement.
type insertColumn struct {
	input  typeinfo.Input
	column string
	// explicit is true if the column is explicitly inserted in the SQLair
	// query. If the column is inserted via an asterisk type, it is false.
	explicit bool
}

// newInsertColumn builds an insert column.
func newInsertColumn(input typeinfo.Input, column string, explicit bool) insertColumn {
	return insertColumn{
		input:    input,
		column:   column,
		explicit: explicit,
	}
}

// bindInputs generates and verifies the query parameters corresponding to the
// insertColumn and returns them as a boundInsertColumn.
func (ic insertColumn) bindInputs(tv typeinfo.TypeToValue, ia *inputAssigner) (*boundInsertColumn, error) {
	params, err := ic.input.LocateParams(tv)
	if err != nil {
		return nil, err
	}
	if !params.Bulk && len(params.Vals) > 1 {
		// Only slices and bulk inserts return multiple values and
		// slices are not allowed in insert expressions.
		return nil, fmt.Errorf("internal error: types in insert expressions cannot return multiple values")
	}
	if params.Omit && ic.explicit {
		return nil, omitEmptyInputError(ic.input.Desc())
	}
	var firstInputNum int
	if !params.Omit {
		firstInputNum = ia.assignInputs(len(params.Vals))
	}
	bc := &boundInsertColumn{
		vals:          params.Vals,
		firstInputNum: firstInputNum,
		omit:          params.Omit,
		bulk:          params.Bulk,
		argType:       params.ArgTypeUsed,
		inputName:     ic.input.ArgType().Name(),
		literal:       "",
		column:        ic.column,
	}
	return bc, nil
}

// literalColumn represents a column in an insert statement populated with a
// literal value.
type literalColumn struct {
	column  string
	literal string
}

// bindInputs creates a boundInsertColumn from a literalColumn. It is part of the
// typedColumn interface.
func (lc literalColumn) bindInputs(_ typeinfo.TypeToValue, _ *inputAssigner) (*boundInsertColumn, error) {
	bc := &boundInsertColumn{
		column:        lc.column,
		vals:          []any{},
		firstInputNum: 0,
		omit:          false,
		bulk:          false,
		literal:       lc.literal,
		inputName:     "",
		argType:       nil,
	}
	return bc, nil
}

// newLiteralColumn builds a literal column.
func newLiteralColumn(column, literal string) literalColumn {
	return literalColumn{
		column:  column,
		literal: literal,
	}
}

// outputColumn stores the name of a column to fetch from the database and the
// output type location specifying the value to scan the result into.
type outputColumn struct {
	output typeinfo.Output
	column string
}

// newOutputColumn generates an output column with the correct column string to
// write in the generated query.
func newOutputColumn(tableName string, columnName string, output typeinfo.Output) outputColumn {
	if tableName == "" {
		return outputColumn{column: columnName, output: output}
	}
	return outputColumn{column: tableName + "." + columnName, output: output}
}

func omitEmptyInputError(valueDesc string) error {
	return fmt.Errorf("%s has zero value and has the omitempty flag but the value is explicitly input", valueDesc)
}

func mismatchedBulkLengthsError(firstBulkColumn string, expectedNumRows int, badTypeName string, badRowLength int) error {
	return fmt.Errorf("expected slices of matching length in bulk insert: slice of %q has length %d but slice of %q has length %d",
		firstBulkColumn, expectedNumRows, badTypeName, badRowLength)
}
