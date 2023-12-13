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

	typeToValue := map[reflect.Type]reflect.Value{}
	for _, arg := range args {
		v := reflect.ValueOf(arg)
		if v.Kind() == reflect.Invalid || (v.Kind() == reflect.Pointer && v.IsNil()) {
			return nil, fmt.Errorf("need struct or map, got nil")
		}
		v = reflect.Indirect(v)
		t := v.Type()
		if v.Kind() != reflect.Struct && v.Kind() != reflect.Map {
			return nil, fmt.Errorf("need struct or map, got %s", t.Kind())
		}
		if _, ok := typeToValue[t]; ok {
			return nil, fmt.Errorf("type %q provided more than once", t.Name())
		}
		typeToValue[t] = v
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
			for _, val := range vals {
				namedInput := sql.Named("sqlair_"+strconv.Itoa(inputCount), val.Interface())
				params = append(params, namedInput)
				sqlStr.WriteString("@sqlair_" + strconv.Itoa(inputCount))
				inputCount++
			}
		case *typedOutputExpr:
			for i, oc := range te.outputColumns {
				sqlStr.WriteString(oc.sql(outputCount))
				if i != len(te.outputColumns)-1 {
					sqlStr.WriteString(", ")
				}
				outputCount++
				outputs = append(outputs, oc.output)
			}
		case *bypass:
			sqlStr.WriteString(te.chunk)
		default:
			return nil, fmt.Errorf("internal error: unknown type %T", te)
		}
	}

	for argType := range typeToValue {
		if !argTypeUsed[argType] {
			return nil, fmt.Errorf("%s not referenced in query", argType.Name())
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

// typedInputExpr stores information about a Go value to use as a query input.
type typedInputExpr struct {
	input typeinfo.Input
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
