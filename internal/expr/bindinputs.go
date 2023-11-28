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

// TypeBoundExpr represents a SQLair query bound to concrete Go types. It
// contains information used to generate the underlying SQL query and map it to
// the SQLair query.
type TypeBoundExpr []typedExpression

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
	params := []any{}
	outputs := []typeinfo.Member{}
	// argTypeUsed records the types of the input arguments used in the query.
	argTypeUsed := map[reflect.Type]bool{}
	inCount := 0
	outCount := 0
	sqlStr := bytes.Buffer{}
	for _, te := range *tbe {
		switch te := te.(type) {
		case *typedInputExpr:
			typeMember := te.input
			outerType := typeMember.OuterType()
			v, ok := typeToValue[outerType]
			if !ok {
				return nil, missingInputArgError(outerType, typeToValue)
			}
			argTypeUsed[outerType] = true

			val, err := typeMember.ValueFromOuter(v)
			if err != nil {
				return nil, err
			}
			params = append(params, sql.Named("sqlair_"+strconv.Itoa(inCount), val.Interface()))

			sqlStr.WriteString("@sqlair_" + strconv.Itoa(inCount))
			inCount++
		case *typedOutputExpr:
			for i, oc := range te.outputColumns {
				sqlStr.WriteString(oc.sql)
				sqlStr.WriteString(" AS ")
				sqlStr.WriteString(markerName(outCount))
				if i != len(te.outputColumns)-1 {
					sqlStr.WriteString(", ")
				}
				outCount++
				outputs = append(outputs, oc.tm)
			}
		case *bypass:
			sqlStr.WriteString(te.chunk)
		}
	}

	for argType := range typeToValue {
		if !argTypeUsed[argType] {
			return nil, fmt.Errorf("%s not referenced in query", argType.Name())
		}
	}

	return &PrimedQuery{outputs: outputs, sql: sqlStr.String(), params: params}, nil
}

// typedExpression represents a expression with the type names bound to Go
// types.
type typedExpression interface {
	// typedExpr is a marker method.
	typedExpr()
}

// outputColumn stores the name of a column to fetch from the database and the
// type to scan the result into.
type outputColumn struct {
	sql string
	tm  typeinfo.Member
}

// typedOutputExpr contains the columns to fetch from the database and
// information about the Go values to read the query results into.
type typedOutputExpr struct {
	outputColumns []outputColumn
}

// typedExpr is a marker method.
func (*typedOutputExpr) typedExpr() {}

// typedInputExpr stores information about a Go value to use as a query input.
type typedInputExpr struct {
	input typeinfo.Member
}

// typedExpr is a marker method.
func (*typedInputExpr) typedExpr() {}

// typedExpr is a marker method.
func (*bypass) typedExpr() {}

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

// missingInputArgError is used when no matching input argument can be found
// for a type in a type bound expression of the query.
func missingInputArgError(missingType reflect.Type, typeToValue map[reflect.Type]reflect.Value) error {
	// check if the missing type and some argument type have the same name but
	// are from different packages.
	typeNames := []string{}
	for argType := range typeToValue {
		if argType.Name() == missingType.Name() {
			return fmt.Errorf("parameter with type %q missing, have type with same name: %q", missingType.String(), argType.String())
		}
		typeNames = append(typeNames, argType.Name())
	}
	return typeMissingError(missingType.Name(), typeNames)
}
