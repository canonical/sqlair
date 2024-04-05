// Copyright 2023 Canonical Ltd.
// Licensed under Apache 2.0, see LICENCE file for details.

package expr

import (
	"bytes"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/canonical/sqlair/internal/typeinfo"
)

// TypeBoundExpr represents a SQLair statement bound to concrete Go types. It
// contains information used to generate the underlying SQL query and map it to
// the SQLair query.
type TypeBoundExpr []any

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

	// Generate SQL and query parameters.
	var namedInputs []any
	var outputs []typeinfo.Output
	// argUsed is used to check that all the query parameters are
	// referenced in the query.
	argUsed := map[reflect.Type]bool{}
	inputCount := 0
	outputCount := 0
	var sqlBuilder sqlBuilder
	for _, te := range *tbe {
		switch te := te.(type) {
		case *typedInputExpr:
			params, err := te.input.LocateParams(typeToValue)
			if err != nil {
				return nil, err
			}
			if params.Omit {
				return nil, omitEmptyInputError(te.input.Desc())
			}
			if params.Bulk {
				return nil, fmt.Errorf("cannot use bulk inputs outside an insert statement")
			}

			argUsed[params.ArgTypeUsed] = true
			sqlBuilder.writeInput(inputCount, len(params.Vals))
			for _, val := range params.Vals {
				namedInput := sql.Named("sqlair_"+strconv.Itoa(inputCount), val)
				namedInputs = append(namedInputs, namedInput)
				inputCount++
			}
		case *typedInsertExpr:
			var boundColumns []*boundInsertColumn
			var columnNames []string
			bulk := false
			numRows := 1
			// firstBulkColumn stores the type name of the first column used in
			// a bulk insert. This is used for error messages.
			var firstBulkColumn string
			for _, ic := range te.insertColumns {
				bc, err := ic.bindInputs(typeToValue, inputCount)
				if err != nil {
					return nil, err
				}
				if bc.argType != nil {
					argUsed[bc.argType] = true
				}

				if bc.bulk {
					if !bulk {
						// First bulk row.
						bulk = true
						firstBulkColumn = bc.inputName
						numRows = len(bc.vals)
					} else if len(bc.vals) != numRows {
						return nil, mismatchedBulkLengthsError(firstBulkColumn, numRows, bc.inputName, len(bc.vals))
					}
				}
				boundColumns = append(boundColumns, bc)
				if !bc.omit {
					columnNames = append(columnNames, bc.column)
					inputCount += len(bc.vals)
				}
			}

			var rowsSQL [][]string
			for rowNum := 0; rowNum < numRows; rowNum++ {
				var rowSQL []string
				for _, bc := range boundColumns {
					if !bc.omit {
						valueSQL, namedInput, newParam, err := bc.parameter(rowNum)
						if err != nil {
							return nil, err
						}
						rowSQL = append(rowSQL, valueSQL)
						if newParam {
							namedInputs = append(namedInputs, namedInput)
						}
					}
				}
				rowsSQL = append(rowsSQL, rowSQL)
			}
			sqlBuilder.writeInsert(columnNames, rowsSQL)
		case *typedOutputExpr:
			var columns []string
			for _, oc := range te.outputColumns {
				outputs = append(outputs, oc.output)
				columns = append(columns, oc.column)
			}
			sqlBuilder.writeOutput(outputCount, columns)
			outputCount += len(columns)
		case *bypass:
			sqlBuilder.write(te.chunk)
		default:
			return nil, fmt.Errorf("internal error: unknown expression type %T", te)
		}
	}

	for argType := range typeToValue {
		if !argUsed[argType] {
			return nil, notReferencedInQueryError(argType)
		}
	}

	return &PrimedQuery{outputs: outputs, sql: sqlBuilder.getSQL(), params: namedInputs}, nil
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

// typedInputExpr stores information about a Go value to use as a standalone query
// input.
type typedInputExpr struct {
	input typeinfo.Input
}

// typedColumn represents a column and input locator in an insert statement.
type typedColumn interface {
	// bindInputs binds a concrete value to a typedColumn to generate a
	// boundInsertColumn.
	bindInputs(tv typeinfo.TypeToValue, inputCount int) (*boundInsertColumn, error)
}

// insertColumn stores information about a single column of a row in an insert
// statement.
type insertColumn struct {
	input  typeinfo.Input
	column string
	// explicit is true if the column is explicity inserted in the SQLair
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
func (ic insertColumn) bindInputs(tv typeinfo.TypeToValue, inputCount int) (*boundInsertColumn, error) {
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
	bc := &boundInsertColumn{
		column:    ic.column,
		vals:      params.Vals,
		inputNum:  inputCount,
		omit:      params.Omit,
		bulk:      params.Bulk,
		literal:   "",
		inputName: ic.input.ArgType().Name(),
		argType:   params.ArgTypeUsed,
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
func (lc literalColumn) bindInputs(_ typeinfo.TypeToValue, inputCount int) (*boundInsertColumn, error) {
	bc := &boundInsertColumn{
		column:    lc.column,
		vals:      []any{},
		inputNum:  inputCount,
		omit:      false,
		bulk:      false,
		literal:   lc.literal,
		inputName: "",
		argType:   nil,
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

// boundInsertColumn represents a column in an insert statement along with the values
// to be inserted into it.
type boundInsertColumn struct {
	// vals contains the query parameters. There may be multiple for a bulk
	// insert, or none for a literal.
	vals []any
	// inputNum is the number associated with the first named input for this
	// column. The numbers inputNum to inputNum + len(vals) can be used by this
	// boundInsertColumn.
	inputNum int
	// omit indicates if the column and values should be ommited.
	omit bool
	// bulk is true if the list of values should be inserted in a bulk insert
	// expression.
	bulk bool
	// argType is the type of the argument that was used to generate the
	// params.
	argType reflect.Type
	// inputName is the type name of the input parameter.
	inputName string
	// literal is set if the value to insert is a literal.
	literal string
	// column is the column name.
	column string
}

// parameter returns the value to be inserted into the boundInsertColumn in the given
// row. The inputCount it used to generate the parameter name.
func (bc *boundInsertColumn) parameter(row int) (name string, v any, newParam bool, err error) {
	switch {
	case len(bc.vals) == 0:
		return bc.literal, nil, false, nil
	case len(bc.vals) == 1:
		name = "sqlair_" + strconv.Itoa(bc.inputNum)
		newParam = false
		if row == 0 {
			newParam = true
		}
		return "@" + name, sql.Named(name, bc.vals[0]), newParam, nil
	case row < len(bc.vals):
		name = "sqlair_" + strconv.Itoa(bc.inputNum+row)
		return "@" + name, sql.Named(name, bc.vals[row]), true, nil
	default:
		return "", nil, false, fmt.Errorf("internal error: no bulk insert value for row %d, only have %d values", row, len(bc.vals))
	}
}

// typedInsertExpr stores information about the Go values to use as inputs inside
// an INSERT statement.
type typedInsertExpr struct {
	insertColumns []typedColumn
}

// typedOutputExpr contains the columns to fetch from the database and
// information about the Go values to read the query results into.
type typedOutputExpr struct {
	outputColumns []outputColumn
}

// sqlBuilder is used to generate SQL string piece by piece using the struct
// methods.
type sqlBuilder struct {
	buf bytes.Buffer
}

// writeInsert writes the SQL for INSERT statements to the sqlBuilder.
func (b *sqlBuilder) writeInsert(columns []string, rows [][]string) {
	// Write out the columns.
	b.buf.WriteString("(")
	b.writeCommaSeparatedList(columns, func(_ int, column string) string {
		return column
	})
	b.buf.WriteString(") VALUES ")
	// Write out the values.
	for i, row := range rows {
		if i != 0 {
			b.buf.WriteString(", ")
		}
		b.buf.WriteString("(")
		b.writeCommaSeparatedList(row, func(_ int, value string) string {
			return value
		})
		b.buf.WriteString(")")
	}
}

// writeInput writes the SQL for input placeholders to the sqlBuilder.
func (b *sqlBuilder) writeInput(inputCount, num int) {
	b.writeCommaSeparatedList(make([]string, num), func(i int, column string) string {
		return "@sqlair_" + strconv.Itoa(inputCount+i)
	})
}

// writeOutput writes the SQL for output columns to the sqlBuilder.
func (b *sqlBuilder) writeOutput(outputCount int, columns []string) {
	b.writeCommaSeparatedList(columns, func(i int, column string) string {
		return column + " AS " + markerName(outputCount+i)
	})
}

// writeCommaSeparatedList writes out the provided list using the writer to
// write each element into the SQL.
func (b *sqlBuilder) writeCommaSeparatedList(list []string, writer func(i int, s string) string) {
	for i, s := range list {
		if i != 0 {
			b.buf.WriteString(", ")
		}
		b.buf.WriteString(writer(i, s))
	}
}

// write writes the SQL to the sqlBuilder.
func (b *sqlBuilder) write(sql string) {
	b.buf.WriteString(sql)
}

// getSQL returns the generated SQL string
func (b *sqlBuilder) getSQL() string {
	return b.buf.String()
}

const markerPrefix = "_sqlair_"

func markerName(n int) string {
	return markerPrefix + strconv.Itoa(n)
}

// markerIndex returns the int X from the string "_sqlair_X".
func markerIndex(s string) (int, bool) {
	if strings.HasPrefix(s, markerPrefix) {
		n, err := strconv.Atoi(s[len(markerPrefix):])
		if err == nil {
			return n, true
		}
	}
	return 0, false
}

func omitEmptyInputError(valueDesc string) error {
	return fmt.Errorf("%s has zero value and has the omitempty flag but the value is explicitly input", valueDesc)
}

func mismatchedBulkLengthsError(firstBulkColumn string, expectedNumRows int, badTypeName string, badRowLength int) error {
	return fmt.Errorf("expected slices of matching length in bulk insert: slice of %q has length %d but slice of %q has length %d",
		firstBulkColumn, expectedNumRows, badTypeName, badRowLength)
}

func notReferencedInQueryError(t reflect.Type) error {
	return fmt.Errorf(`argument of type %q not used by query`, typeinfo.PrettyTypeName(t))
}
