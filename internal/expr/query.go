package expr

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// QuerySQL returns the SQL string for running on the database.
func (qe *QueryExpr) QuerySQL() string {
	return qe.sql
}

// QueryArgs returns the query parameters to be passed to the database.
func (qe *QueryExpr) QueryArgs() []any {
	return qe.args
}

// HasOutputs is true if the query contains one or more SQLair output
// expressions.
func (qe *QueryExpr) HasOutputs() bool {
	return len(qe.outputs) > 0
}

// QueryExpr represents a SQLair query that is ready for execution and then
// scanning of query results.
type QueryExpr struct {
	sql     string
	args    []any
	outputs []typeMember
}

// Query takes all maps and structs mentioned in the input expressions of the
// PreparedExpr and builds a QueryExpr ready for running on a database.
func (pe *PreparedExpr) Query(args ...any) (ce *QueryExpr, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("invalid input parameter: %s", err)
		}
	}()

	var inQuery = make(map[reflect.Type]bool)
	for _, typeMember := range pe.inputs {
		inQuery[typeMember.outerType()] = true
	}

	// Check args are as expected using reflection.
	var typeValue = make(map[reflect.Type]reflect.Value)
	var typeNames []string
	for _, arg := range args {
		v := reflect.ValueOf(arg)
		k := v.Kind()
		if k == reflect.Invalid || (k == reflect.Pointer && v.IsNil()) {
			return nil, fmt.Errorf("need struct or map, got nil")
		}
		v = reflect.Indirect(v)
		k = v.Kind()
		t := v.Type()
		if k != reflect.Struct && k != reflect.Map {
			return nil, fmt.Errorf("need struct or map, got %s", k)
		}
		if _, ok := typeValue[t]; ok {
			return nil, fmt.Errorf("type %q provided more than once", t.Name())
		}
		typeValue[t] = v
		typeNames = append(typeNames, t.Name())
		if !inQuery[t] {
			// Check if we have a type with the same name from a different
			// package.
			for _, typeMember := range pe.inputs {
				if t.Name() == typeMember.outerType().Name() {
					return nil, fmt.Errorf("type %s not passed as a parameter, have %s", typeMember.outerType().String(), t.String())
				}
			}
			return nil, fmt.Errorf("%s not referenced in query", t.Name())
		}
	}

	// Extract query parameters from arguments.
	qargs := []any{}
	for i, typeMember := range pe.inputs {
		outerType := typeMember.outerType()
		v, ok := typeValue[outerType]
		if !ok {
			if len(typeNames) == 0 {
				return nil, fmt.Errorf(`type %q not passed as a parameter`, outerType.Name())
			} else {
				return nil, fmt.Errorf(`type %q not passed as a parameter, have: %s`, outerType.Name(), strings.Join(typeNames, ", "))
			}
		}
		var val reflect.Value
		switch tm := typeMember.(type) {
		case *structField:
			val = v.Field(tm.index)
		case *mapKey:
			val = v.MapIndex(reflect.ValueOf(tm.name))
			if val.Kind() == reflect.Invalid {
				return nil, fmt.Errorf(`map %q does not contain key %q`, outerType.Name(), tm.name)
			}
		}
		qargs = append(qargs, sql.Named("sqlair_"+strconv.Itoa(i), val.Interface()))
	}
	return &QueryExpr{outputs: pe.outputs, sql: pe.sql, args: qargs}, nil
}

var scannerInterface = reflect.TypeOf((*sql.Scanner)(nil)).Elem()

// ScanArgs takes the result column names and all the target structs/maps that
// the query results will be scanned into.
// It returns a list of pointers for use with sql.Rows.Scan.
// The onSuccess function should be called after the row has been scanned into
// scanArgs.
func (qe *QueryExpr) ScanArgs(columns []string, outputArgs []any) (scanArgs []any, onSuccess func(), err error) {
	var typesInQuery = []string{}
	var inQuery = make(map[reflect.Type]bool)
	for _, typeMember := range qe.outputs {
		outerType := typeMember.outerType()
		if ok := inQuery[outerType]; !ok {
			inQuery[outerType] = true
			typesInQuery = append(typesInQuery, outerType.Name())
		}
	}

	// scanProxy represents a result value that is scanned into a proxy
	// variable. The original target is set in onSuccess.
	type scanProxy struct {
		original reflect.Value
		scan     reflect.Value
		key      reflect.Value
	}
	var scanProxies []scanProxy

	// Check outputArgs are as expected using reflection.
	var typeDest = make(map[reflect.Type]reflect.Value)
	outputVals := []reflect.Value{}
	for _, outputArg := range outputArgs {
		if outputArg == nil {
			return nil, nil, fmt.Errorf("need map or pointer to struct, got nil")
		}
		outputVal := reflect.ValueOf(outputArg)
		k := outputVal.Kind()
		if k != reflect.Map {
			if k != reflect.Pointer {
				return nil, nil, fmt.Errorf("need map or pointer to struct, got %s", k)
			}
			if outputVal.IsNil() {
				return nil, nil, fmt.Errorf("got nil pointer")
			}
			outputVal = outputVal.Elem()
			k = outputVal.Kind()
			if k != reflect.Struct && k != reflect.Map {
				return nil, nil, fmt.Errorf("need map or pointer to struct, got pointer to %s", k)
			}
		}
		if !inQuery[outputVal.Type()] {
			return nil, nil, fmt.Errorf("type %q does not appear in query, have: %s", outputVal.Type().Name(), strings.Join(typesInQuery, ", "))
		}
		if _, ok := typeDest[outputVal.Type()]; ok {
			return nil, nil, fmt.Errorf("type %q provided more than once, rename one of them", outputVal.Type().Name())
		}
		typeDest[outputVal.Type()] = outputVal
		outputVals = append(outputVals, outputVal)
	}

	// Generate the pointers for sql.Rows.Scan.
	var ptrs = []any{}
	for _, column := range columns {
		idx, ok := markerIndex(column)
		if !ok {
			// Columns not mentioned in output expressions are scanned into a
			// placeholder variable.
			var x any
			ptrs = append(ptrs, &x)
			continue
		}
		if idx >= len(qe.outputs) {
			return nil, nil, fmt.Errorf("internal error: sqlair column not in outputs (%d>=%d)", idx, len(qe.outputs))
		}
		typeMember := qe.outputs[idx]
		outputVal, ok := typeDest[typeMember.outerType()]
		if !ok {
			return nil, nil, fmt.Errorf("type %q found in query but not passed to get", typeMember.outerType().Name())
		}
		switch tm := typeMember.(type) {
		case *structField:
			val := outputVal.Field(tm.index)
			if !val.CanSet() {
				return nil, nil, fmt.Errorf("internal error: cannot set field %s of struct %s", tm.name, tm.structType.Name())
			}
			pt := reflect.PointerTo(val.Type())
			if val.Type().Kind() != reflect.Pointer && !pt.Implements(scannerInterface) {
				// For any scan targets that are not pointers and do not
				// implement the Scanner interface, e.g. int, string, ect., a
				// pointer to a new proxy variable will be added to ptrs.
				// The type of this new proxy will be a pointer to the target
				// type.
				// This is done because sql.Rows.Scan will panic when trying to
				// scan a NULL from the database into a type that is not a
				// pointer. If the type is a pointer then sql.Rows.Scan will
				// set it to nil.
				// The onSuccess function sets the original targets according
				// to the values in the proxies. If the pointer is nil then the
				// target will be zeroed.
				scanVal := reflect.New(pt).Elem()
				ptrs = append(ptrs, scanVal.Addr().Interface())
				scanProxies = append(scanProxies, scanProxy{
					original: val,
					scan:     scanVal,
				})
			} else {
				ptrs = append(ptrs, val.Addr().Interface())
			}
		case *mapKey:
			// If a result column is to be scanned into a map at a specific key
			// then a pointer to a new proxy variable of type any is added to
			// ptrs.
			// The onSuccess function will then populate the target map with
			// the values in these proxies.
			// Proxy values are used for map values because it is not possible
			// to generate a pointer to a value in a map.
			scanVal := reflect.New(tm.mapType.Elem()).Elem()
			ptrs = append(ptrs, scanVal.Addr().Interface())
			scanProxies = append(scanProxies, scanProxy{
				original: outputVal,
				scan:     scanVal,
				key:      reflect.ValueOf(tm.name),
			})
		}
	}

	onSuccess = func() {
		for _, sp := range scanProxies {
			if sp.key.IsValid() {
				sp.original.SetMapIndex(sp.key, sp.scan)
			} else {
				var val reflect.Value
				if !sp.scan.IsNil() {
					val = sp.scan.Elem()
				} else {
					val = reflect.Zero(sp.original.Type())
				}
				sp.original.Set(val)
			}
		}
	}

	return ptrs, onSuccess, nil
}
