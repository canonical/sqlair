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
	outputs []typeinfo.Member
	inputs  []typeinfo.Member
	sql     string
}

// SQL returns the SQL ready for execution.
func (te *TypedExpr) SQL() string {
	return te.sql
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

func typeMissingError(missingType string, existingTypes []string) error {
	if len(existingTypes) == 0 {
		return fmt.Errorf(`parameter with type %q missing`, missingType)
	}
	// "%s" is used instead of %q to correctly print double quotes within the joined string.
	return fmt.Errorf(`parameter with type %q missing (have "%s")`, missingType, strings.Join(existingTypes, `", "`))
}

// BindInputs takes the SQLair input arguments and returns the PrimedQuery ready
// for use with the database.
func (te *TypedExpr) BindInputs(args ...any) (pq *PrimedQuery, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("invalid input parameter: %s", err)
		}
	}()

	var inQuery = make(map[reflect.Type]bool)
	for _, typeMember := range te.inputs {
		inQuery[typeMember.OuterType()] = true
	}

	var typeValue = make(map[reflect.Type]reflect.Value)
	var typeNames []string
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
		if _, ok := typeValue[t]; ok {
			return nil, fmt.Errorf("type %q provided more than once", t.Name())
		}
		typeValue[t] = v
		typeNames = append(typeNames, t.Name())
		if !inQuery[t] {
			// Check if we have a type with the same name from a different package.
			for _, typeMember := range te.inputs {
				if t.Name() == typeMember.OuterType().Name() {
					return nil, fmt.Errorf("parameter with type %q missing, have type with same name: %q", typeMember.OuterType().String(), t.String())
				}
			}
			return nil, fmt.Errorf("%s not referenced in query", t.Name())
		}
	}

	// Query parameters.
	var params []any
	for i, typeMember := range te.inputs {
		outerType := typeMember.OuterType()
		v, ok := typeValue[outerType]
		if !ok {
			return nil, typeMissingError(outerType.Name(), typeNames)
		}

		val, err := typeMember.ValueFromOuter(v)
		if err != nil {
			return nil, err
		}

		params = append(params, sql.Named("sqlair_"+strconv.Itoa(i), val.Interface()))
	}
	return &PrimedQuery{outputs: te.outputs, sql: te.sql, params: params}, nil
}
