package expr

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

func (qe *QueryExpr) QueryArgs() []any {
	return qe.args
}

func (qe *QueryExpr) HasOutputs() bool {
	return len(qe.outputs) > 0
}

type QueryExpr struct {
	sql     string
	args    []any
	outputs []typeMember
}

// Query returns a query expression ready for execution, using the provided values to
// substitute the input placeholders in the prepared expression. These placeholders use
// the syntax "$Person.fullname", where Person would be a type such as:
//
//	type Person struct {
//	        Name string `db:"fullname"`
//	}
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
			for _, typeMember := range pe.inputs {
				if t.Name() == typeMember.outerType().Name() {
					return nil, fmt.Errorf("parameter with type %q missing, have type with same name: %q", typeMember.outerType().String(), t.String())
				}
			}
			return nil, fmt.Errorf("%s not referenced in query", t.Name())
		}
	}

	// Query parameteres.
	qargs := []any{}
	for i, typeMember := range pe.inputs {
		outerType := typeMember.outerType()
		v, ok := typeValue[outerType]
		if !ok {
			return nil, typeMissingError(outerType.Name(), typeNames)
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
		default:
			return nil, fmt.Errorf(`internal error: unknown type: %T`, tm)
		}
		qargs = append(qargs, sql.Named("sqlair_"+strconv.Itoa(i), val.Interface()))
	}
	return &QueryExpr{outputs: pe.outputs, sql: pe.sql, args: qargs}, nil
}

var scannerInterface = reflect.TypeOf((*sql.Scanner)(nil)).Elem()

// ScanArgs returns list of pointers to the struct fields that are listed in qe.outputs.
// All the structs and maps mentioned in the query must be in outputArgs.
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
	var columnInResult = make([]bool, len(columns))
	for _, column := range columns {
		idx, ok := markerIndex(column)
		if !ok {
			// Columns not mentioned in output expressions are scanned into x.
			var x any
			ptrs = append(ptrs, &x)
			continue
		}
		if idx >= len(qe.outputs) {
			return nil, nil, fmt.Errorf("internal error: sqlair column not in outputs (%d>=%d)", idx, len(qe.outputs))
		}
		columnInResult[idx] = true
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
		default:
			return nil, nil, fmt.Errorf(`internal error: unknown type: %T`, tm)
		}
	}

	for i := 0; i < len(qe.outputs); i++ {
		if !columnInResult[i] {
			return nil, nil, fmt.Errorf(`query uses "&%s" outside of result context`, qe.outputs[i].outerType().Name())
		}
	}

	for i := 0; i < len(qe.outputs); i++ {
		if !columnInResult[i] {
			return nil, nil, fmt.Errorf(`query uses "&%s" outside of result context`, qe.outputs[i].outerType().Name())
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
