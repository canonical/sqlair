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
	// argTypeUsed is used to check that all the query parameters are
	// referenced in the query.
	argTypeUsed := map[reflect.Type]bool{}
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

			argTypeUsed[params.ArgTypeUsed] = true
			sqlBuilder.writeInput(inputCount, len(params.Vals))
			for _, val := range params.Vals {
				namedInput := sql.Named("sqlair_"+strconv.Itoa(inputCount), val.Interface())
				namedInputs = append(namedInputs, namedInput)
				inputCount++
			}
		case *typedInsertExpr:
			var columnNames []string
			var paramsToInsert []*typeinfo.Params
			bulk := false
			// bulkRows is the number of rows in the bulk insert. If it is not a
			// bulk insert, it remains 1.
			bulkRows := 1
			// firstBulkColumn stores the type name of the first inputs that are
			// part of a bulk insert. This is used for error messages.
			var firstBulkColumn string
			for _, ic := range te.insertColumns {
				params, err := ic.input.LocateParams(typeToValue)
				if err != nil {
					return nil, err
				}
				argTypeUsed[params.ArgTypeUsed] = true

				if params.Bulk {
					if bulk {
						if len(params.Vals) != bulkRows {
							return nil, mismatchedBulkLengthsError(firstBulkColumn, bulkRows, ic.input.ArgType(), len(params.Vals))
						}
					} else {
						bulk = true
						firstBulkColumn = ic.input.ArgType().Name()
						bulkRows = len(params.Vals)
					}
				} else if len(params.Vals) > 1 {
					// Only slices and bulk inserts return multiple values and
					// slices are not allowed in insert expressions.
					return nil, fmt.Errorf("internal error: types in insert expressions cannot return multiple values")
				}

				// We check for params.Omit after params.Bulk because we still
				// want to do a bulk insert even if all bulk inputs are omitted.
				if params.Omit {
					if ic.explicit {
						return nil, omitEmptyInputError(ic.input.Desc())
					}
					continue
				}
				columnNames = append(columnNames, ic.column)
				paramsToInsert = append(paramsToInsert, params)
			}

			sqlBuilder.writeInsert(inputCount, bulkRows, columnNames)
			for i := 0; i < bulkRows; i++ {
				for _, params := range paramsToInsert {
					index := i
					if !params.Bulk {
						index = 0
					}
					namedInputs = append(namedInputs, sql.Named("sqlair_"+strconv.Itoa(inputCount), params.Vals[index].Interface()))
					inputCount++
				}
			}
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
		if !argTypeUsed[argType] {
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

// insertColumn stores information about a single column of a row in an INSERT
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

// typedInsertExpr stores information about the Go values to use as inputs inside
// an INSERT statement.
type typedInsertExpr struct {
	insertColumns []insertColumn
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
func (b *sqlBuilder) writeInsert(inputCount, bulkRows int, columns []string) {
	// Write out the columns.
	b.buf.WriteString("(")
	b.writeCommaSeperatedList(columns, func(_ int, column string) string {
		return column
	})
	b.buf.WriteString(") VALUES ")
	// Write out the values.
	for i := 0; i < bulkRows; i++ {
		if i != 0 {
			b.buf.WriteString(", ")
		}
		b.buf.WriteString("(")
		b.writeCommaSeperatedList(columns, func(j int, _ string) string {
			return "@sqlair_" + strconv.Itoa(inputCount+(i*len(columns))+j)
		})
		b.buf.WriteString(")")
	}
}

// writeInput writes the SQL for input placeholders to the sqlBuilder.
func (b *sqlBuilder) writeInput(inputCount, num int) {
	b.writeCommaSeperatedList(make([]string, num), func(i int, column string) string {
		return "@sqlair_" + strconv.Itoa(inputCount+i)
	})
}

// writeOutput writes the SQL for output columns to the sqlBuilder.
func (b *sqlBuilder) writeOutput(outputCount int, columns []string) {
	b.writeCommaSeperatedList(columns, func(i int, column string) string {
		return column + " AS " + markerName(outputCount+i)
	})
}

// writeCommaSeperatedList writes out the provided list using the writer to
// write each element into the SQL.
func (b *sqlBuilder) writeCommaSeperatedList(list []string, writer func(i int, s string) string) {
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

func mismatchedBulkLengthsError(firstBulkColumn string, expectedNumRows int, badType reflect.Type, badRowLength int) error {
	return fmt.Errorf("different slices sizes in bulk insert: slice of %q has length %d but slice of %q has length %d",
		firstBulkColumn, expectedNumRows, badType.Name(), badRowLength)
}

func notReferencedInQueryError(t reflect.Type) error {
	name := t.Name()
	if name == "" && (t.Kind() == reflect.Slice || t.Kind() == reflect.Pointer) {
		t = t.Elem()
		name = t.Name()
		if name == "" && t.Kind() == reflect.Pointer {
			name = t.Elem().Name()
		}
	}
	return fmt.Errorf(`%q not referenced in query`, name)
}
