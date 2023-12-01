package expr

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/canonical/sqlair/internal/typeinfo"
)

// TypedExpr represents a SQLair query bound to concrete Go types. It contains
// all the type information needed by SQLair.
type TypedExpr struct {
	outputs []typeinfo.Output
	inputs  []typeinfo.Input
	sql     string
}

// SQL returns the SQL ready for execution.
func (te *TypedExpr) SQL() string {
	return te.sql
}

// BindInputs takes the SQLair input arguments and returns the PrimedQuery ready
// for use with the database.
func (te *TypedExpr) BindInputs(args ...any) (pq *PrimedQuery, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("invalid input parameter: %s", err)
		}
	}()

	inQuery := map[reflect.Type]bool{}
	for _, input := range te.inputs {
		inQuery[input.ArgType()] = true
	}

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
		if !inQuery[t] {
			// Check if we have a type with the same name from a different package.
			for _, input := range te.inputs {
				if t.Name() == input.ArgType().Name() {
					return nil, fmt.Errorf("parameter with type %q missing, have type with same name: %q", input.ArgType().String(), t.String())
				}
			}
			return nil, fmt.Errorf("%s not referenced in query", t.Name())
		}
	}

	// Query parameters.
	var params []any
	inputCount := 0
	for _, input := range te.inputs {
		vals, err := input.LocateParams(typeToValue)
		if err != nil {
			return nil, err
		}
		for _, val := range vals {
			params = append(params, sql.Named("sqlair_"+strconv.Itoa(inputCount), val.Interface()))
			inputCount++
		}
	}
	return &PrimedQuery{outputs: te.outputs, sql: te.sql, params: params}, nil
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
