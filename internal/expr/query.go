package expr

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

// PrimedQuery contains all concrete values needed to run a SQLair query on a
// database.
type PrimedQuery struct {
	sql string
	// params are the query parameters to pass to the database.
	params []any
	// outputs specifies where to scan the query results.
	outputs []typeMember
}

// Params returns the query parameters to pass with the SQL to a database.
func (pq *PrimedQuery) Params() []any {
	return pq.params
}

// HasOutputs returns true if the SQLair query contains at least one output
// expression.
func (pq *PrimedQuery) HasOutputs() bool {
	return len(pq.outputs) > 0
}

var scannerInterface = reflect.TypeOf((*sql.Scanner)(nil)).Elem()

// ScanArgs produces a list of pointers to be passed to rows.Scan. After a
// successful call, the onSuccess function must be invoked. The outputArgs will
// be populated with the query results. All the structs/maps/slices mentioned in
// the query must be in outputArgs.
func (pq *PrimedQuery) ScanArgs(columnNames []string, outputArgs []any) (scanArgs []any, onSuccess func(), err error) {
	var typesInQuery = []string{}
	var inQuery = make(map[reflect.Type]bool)
	for _, typeMember := range pq.outputs {
		outerType := typeMember.outerType()
		if ok := inQuery[outerType]; !ok {
			inQuery[outerType] = true
			typesInQuery = append(typesInQuery, outerType.Name())
		}
	}

	type scanProxy struct {
		original reflect.Value
		scan     reflect.Value
		key      reflect.Value
	}
	var scanProxies []scanProxy

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

	// Generate the pointers.
	var ptrs = []any{}
	var columnInResult = make([]bool, len(columnNames))
	for _, column := range columnNames {
		idx, ok := markerIndex(column)
		if !ok {
			// Columns not mentioned in output expressions are scanned into x.
			var x any
			ptrs = append(ptrs, &x)
			continue
		}
		if idx >= len(pq.outputs) {
			return nil, nil, fmt.Errorf("internal error: sqlair column not in outputs (%d>=%d)", idx, len(pq.outputs))
		}
		columnInResult[idx] = true
		typeMember := pq.outputs[idx]
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
				// Rows.Scan will return an error if it tries to scan NULL into a type that cannot be set to nil.
				// For types that are not a pointer and do not implement sql.Scanner a pointer to them is generated
				// and passed to Rows.Scan. If Scan has set this pointer to nil the value is zeroed.
				scanVal := reflect.New(pt).Elem()
				ptrs = append(ptrs, scanVal.Addr().Interface())
				scanProxies = append(scanProxies, scanProxy{original: val, scan: scanVal})
			} else {
				ptrs = append(ptrs, val.Addr().Interface())
			}
		case *mapKey:
			scanVal := reflect.New(tm.mapType.Elem()).Elem()
			ptrs = append(ptrs, scanVal.Addr().Interface())
			scanProxies = append(scanProxies, scanProxy{original: outputVal, scan: scanVal, key: reflect.ValueOf(tm.name)})
		}
	}

	for i := 0; i < len(pq.outputs); i++ {
		if !columnInResult[i] {
			return nil, nil, fmt.Errorf(`query uses "&%s" outside of result context`, pq.outputs[i].outerType().Name())
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
