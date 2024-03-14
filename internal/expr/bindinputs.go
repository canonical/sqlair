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
	var params []any
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
			vals, omit, err := te.input.LocateParams(typeToValue)
			if err != nil {
				return nil, err
			}

			if omit {
				return nil, omitEmptyInputError(te.input.Desc())
			}
			argTypeUsed[te.input.ArgType()] = true
			sqlBuilder.writeInput(inputCount, len(vals))
			for _, val := range vals {
				namedInput := sql.Named("sqlair_"+strconv.Itoa(inputCount), val.Interface())
				params = append(params, namedInput)
				inputCount++
			}
		case *typedInsertExpr:
			var columns []string
			for _, ic := range te.insertColumns {
				vals, omit, err := ic.input.LocateParams(typeToValue)
				if err != nil {
					return nil, err
				}
				if len(vals) > 1 {
					// Only slices return multiple values and they are not
					// allowed in insert expressions.
					return nil, fmt.Errorf("internal error: unexpected values")
				}

				argTypeUsed[ic.input.ArgType()] = true
				if omit {
					if ic.explicit {
						return nil, omitEmptyInputError(ic.input.Desc())
					}
					continue
				}
				namedInput := sql.Named("sqlair_"+strconv.Itoa(inputCount), vals[0].Interface())
				params = append(params, namedInput)
				columns = append(columns, ic.column)
				inputCount++
			}
			sqlBuilder.writeInsert(inputCount-len(columns), columns)
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
			return nil, fmt.Errorf("%q not referenced in query", argType.Name())
		}
	}

	return &PrimedQuery{outputs: outputs, sql: sqlBuilder.getSQL(), params: params}, nil
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

type sqlBuilder struct {
	buf bytes.Buffer
}

// writeInsert writes the SQL for INSERT statements to the sqlBuilder.
func (b *sqlBuilder) writeInsert(inputCount int, columns []string) {
	// Write out the columns.
	b.buf.WriteString("(")
	for i, col := range columns {
		if i != 0 {
			b.buf.WriteString(", ")
		}
		b.buf.WriteString(col)
	}
	b.buf.WriteString(") VALUES (")
	// Write out the values.
	for i := range columns {
		if i != 0 {
			b.buf.WriteString(", ")
		}
		b.buf.WriteString("@sqlair_" + strconv.Itoa(inputCount+i))
	}
	b.buf.WriteString(")")
}

// writeInput writes the SQL for input placeholders to the sqlBuilder.
func (b *sqlBuilder) writeInput(inputCount, num int) {
	for i := 0; i < num; i++ {
		if i != 0 {
			b.buf.WriteString(", ")
		}
		b.buf.WriteString("@sqlair_" + strconv.Itoa(inputCount+i))
	}
}

// writeOutput writes the SQL for output columns to the sqlBuilder.
func (b *sqlBuilder) writeOutput(outputCount int, columns []string) {
	for i, column := range columns {
		if i != 0 {
			b.buf.WriteString(", ")
		}
		b.buf.WriteString(column + " AS " + markerName(outputCount+i))
	}
}

// write writes the SQL to the sqlBuilder.
func (b *sqlBuilder) write(sql string) {
	b.buf.WriteString(sql)
}

// write writes the SQL to the sqlBuilder.
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
