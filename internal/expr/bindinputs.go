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
	sqlStr := bytes.Buffer{}
	for _, te := range *tbe {
		switch te := te.(type) {
		case *typedInputExpr:
			vals, err := te.input.LocateParams(typeToValue)
			if err != nil {
				return nil, err
			}
			argTypeUsed[te.input.ArgType()] = true
			for i, val := range vals {
				if i != 0 {
					sqlStr.WriteString(", ")
				}
				namedInput := sql.Named("sqlair_"+strconv.Itoa(inputCount), val.Interface())
				params = append(params, namedInput)
				sqlStr.WriteString("@sqlair_" + strconv.Itoa(inputCount))
				inputCount++
			}
		case *typedInsertExpr:
			// Write out the columns.
			sqlStr.WriteString("(")
			for i, col := range te.columns {
				if i != 0 {
					sqlStr.WriteString(", ")
				}
				sqlStr.WriteString(col)
			}
			sqlStr.WriteString(") VALUES (")
			// Write out the values.
			for i, input := range te.inputs {
				if i != 0 {
					sqlStr.WriteString(", ")
				}
				vals, err := input.LocateParams(typeToValue)
				if err != nil {
					return nil, err
				}
				argTypeUsed[input.ArgType()] = true
				for j, val := range vals {
					if j != 0 {
						sqlStr.WriteString(", ")
					}
					namedInput := sql.Named("sqlair_"+strconv.Itoa(inputCount), val.Interface())
					params = append(params, namedInput)
					sqlStr.WriteString("@sqlair_" + strconv.Itoa(inputCount))
					inputCount++
				}
			}
			sqlStr.WriteString(")")
		case *typedOutputExpr:
			for i, oc := range te.outputColumns {
				if i != 0 {
					sqlStr.WriteString(", ")
				}
				sqlStr.WriteString(oc.sql(outputCount))
				outputCount++
				outputs = append(outputs, oc.output)
			}
		case *bypass:
			sqlStr.WriteString(te.chunk)
		default:
			return nil, fmt.Errorf("internal error: unknown expression type %T", te)
		}
	}

	for argType := range typeToValue {
		if !argTypeUsed[argType] {
			return nil, fmt.Errorf("%q not referenced in query", argType.Name())
		}
	}

	return &PrimedQuery{outputs: outputs, sql: sqlStr.String(), params: params}, nil
}

// outputColumn stores the name of a column to fetch from the database and the
// output type location specifying the value to scan the result into.
type outputColumn struct {
	output typeinfo.Output
	column string
}

// sql generates the SQL for a single output column.
func (oc *outputColumn) sql(outputCount int) string {
	return oc.column + " AS " + markerName(outputCount)
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

// typedInsertExpr stores information about the Go values to use as inputs inside
// an INSERT statement.
type typedInsertExpr struct {
	columns []string
	inputs  []typeinfo.Input
}

// typedOutputExpr contains the columns to fetch from the database and
// information about the Go values to read the query results into.
type typedOutputExpr struct {
	outputColumns []outputColumn
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
