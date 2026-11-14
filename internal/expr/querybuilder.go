// Copyright 2024 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package expr

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/canonical/sqlair/internal/typeinfo"
)

// queryBuilder is used to build up a query to be passed to the database from a
// type bound expression
type queryBuilder struct {
	// outputCount tracks the number of query outputs.
	outputCount int
	// argUsed is used to check that all the arguments provided by the caller of
	// BindInputs are referenced in the query.
	argUsed map[reflect.Type]bool

	// sqlBuilder is used to accumulate the generated SQL.
	sqlBuilder sqlBuilder
	// params are the query parameters that will be passed to the database at
	// query time. The placeholder for params[i] is $(i+1).
	params []any
	// outputs are the output value locators to be used when the SQL is scanned.
	outputs []typeinfo.Output
}

// newQueryBuilder builds a new queryBuilder with the inputs in typeToValue.
func newQueryBuilder() *queryBuilder {
	return &queryBuilder{
		sqlBuilder:  sqlBuilder{},
		outputCount: 0,
		argUsed:     map[reflect.Type]bool{},
		params:      []any{},
		outputs:     []typeinfo.Output{},
	}
}

// markArgUsed marks the argument passed by the caller of BindInputs as used.
func (qb *queryBuilder) markArgUsed(t reflect.Type) {
	qb.argUsed[t] = true
}

// addParam adds a query parameter and returns the numbered placeholder that
// refers to it in the generated SQL.
func (qb *queryBuilder) addParam(val any) string {
	qb.params = append(qb.params, val)
	return "$" + strconv.Itoa(len(qb.params))
}

// addInputs adds input placeholders and argument values to the query.
func (qb *queryBuilder) addInputs(inputVals []any) {
	placeholders := make([]string, len(inputVals))
	for i, val := range inputVals {
		placeholders[i] = qb.addParam(val)
	}
	qb.sqlBuilder.writeInputs(placeholders)
}

// addInsert adds a typedInsertExpr to the queryBuilder
func (qb *queryBuilder) addInsert(boundColumns []*boundInsertColumn, numRows int) error {
	var rowsSQL [][]string
	var columnNames []string
	for rowNum := 0; rowNum < numRows; rowNum++ {
		var rowSQL []string
		for _, bc := range boundColumns {
			if !bc.omit {
				valueSQL, err := bc.parameter(rowNum, qb)
				if err != nil {
					return err
				}
				rowSQL = append(rowSQL, valueSQL)
			}
		}
		rowsSQL = append(rowsSQL, rowSQL)
	}
	for _, bc := range boundColumns {
		if !bc.omit {
			columnNames = append(columnNames, bc.column)
		}
	}
	qb.sqlBuilder.writeInsert(columnNames, rowsSQL)
	return nil
}

// addOutput adds a typedOutputExpr to the queryBuilder
func (qb *queryBuilder) addOutput(columns []string, outputs []typeinfo.Output) {
	qb.sqlBuilder.writeOutput(qb.outputCount, columns)
	qb.outputCount += len(columns)
	qb.outputs = append(qb.outputs, outputs...)
}

// addBypass adds a bypass part to the queryBuilder
func (qb *queryBuilder) addBypass(b *bypass) error {
	qb.sqlBuilder.write(b.chunk)
	return nil
}

// checkAllArgsUsed goes through all the arguments contained in typeToValue and
// checks that they were used somewhere during the building of the query.
func (qb *queryBuilder) checkAllArgsUsed(typeToValue typeinfo.TypeToValue) error {
	for argType := range typeToValue {
		if !qb.argUsed[argType] {
			return notReferencedInQueryError(argType)
		}
	}
	return nil
}

// boundInsertColumn represents a column in an insert statement along with the values
// to be inserted into it.
type boundInsertColumn struct {
	// vals contains the query parameters. There may be multiple for a bulk
	// insert, or none for a literal.
	vals []any
	// placeholder refers to the parameter of a single value column. It is set
	// when the column is first added to the query and reused in later rows.
	placeholder string
	// omit indicates if the column and values should be omitted.
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

// parameter returns the SQL for the value to be inserted into the
// boundInsertColumn in the given row. New query parameters are added to qb.
func (bc *boundInsertColumn) parameter(row int, qb *queryBuilder) (string, error) {
	switch {
	case len(bc.vals) == 0:
		return bc.literal, nil
	case len(bc.vals) == 1:
		if bc.placeholder == "" {
			bc.placeholder = qb.addParam(bc.vals[0])
		}
		return bc.placeholder, nil
	case row < len(bc.vals):
		return qb.addParam(bc.vals[row]), nil
	default:
		return "", fmt.Errorf("internal error: no bulk insert value for row %d, only have %d values", row, len(bc.vals))
	}
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

// writeInputs writes the SQL for input placeholders to the sqlBuilder.
func (b *sqlBuilder) writeInputs(placeholders []string) {
	b.writeCommaSeparatedList(placeholders, func(_ int, placeholder string) string {
		return placeholder
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

func notReferencedInQueryError(t reflect.Type) error {
	return fmt.Errorf(`argument of type %q not used by query`, typeinfo.PrettyTypeName(t))
}
