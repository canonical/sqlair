package expr

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

func (ce *QueryExpr) QuerySQL() string {
	return ce.sql
}

func (ce *QueryExpr) QueryArgs() []any {
	return ce.args
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
		if arg == nil {
			return nil, fmt.Errorf("need map or struct, got nil")
		}
		v := reflect.Indirect(reflect.ValueOf(arg))
		t := v.Type()
		if v.Kind() != reflect.Struct && v.Kind() != reflect.Map {
			return nil, fmt.Errorf("need map or struct, got %s", t.Kind())
		}

		typeValue[t] = v
		typeNames = append(typeNames, t.Name())
		if !inQuery[t] {
			// Check if we have a type with the same name from a different package.
			for _, typeMember := range pe.inputs {
				if t.Name() == typeMember.outerType().Name() {
					return nil, fmt.Errorf("type %s not found, have %s", typeMember.outerType().String(), t.String())
				}
			}
			return nil, fmt.Errorf("%s not referenced in query", t.Name())
		}
	}

	// Query parameteres.
	qargs := []any{}
	for i, typeMember := range pe.inputs {
		v, ok := typeValue[typeMember.outerType()]
		if !ok {
			return nil, fmt.Errorf(`type %s not found, have: %s`, typeMember.outerType().Name(), strings.Join(typeNames, ", "))
		}
		switch tm := typeMember.(type) {
		case field:
			qargs = append(qargs, sql.Named("sqlair_"+strconv.Itoa(i), v.FieldByIndex(tm.index).Interface()))
		case mapKey:
			// MapIndex returns a zero value if the key is not in the map so we
			// need to check the MapKeys().
			var val reflect.Value
			for _, key := range v.MapKeys() {
				if key.String() == tm.name {
					val = v.MapIndex(reflect.ValueOf(tm.name))
					break
				}
			}
			if !val.IsValid() {
				return nil, fmt.Errorf(`map does not contain key %q`, tm.name)
			}
			qargs = append(qargs, sql.Named("sqlair_"+strconv.Itoa(i), val.Interface()))
		}
	}
	return &QueryExpr{outputs: pe.outputs, sql: pe.sql, args: qargs}, nil
}

type MapDecodeInfo struct {
	m      reflect.Value
	keys   []string
	values []reflect.Value
}

// ScanArgs returns list of pointers to the struct fields that are listed in qe.outputs.
// All the structs and maps mentioned in the query must be in outputArgs.
func (qe *QueryExpr) ScanArgs(columns []string, outputArgs []any) ([]any, []*MapDecodeInfo, error) {
	var typesInQuery = []string{}
	var inQuery = make(map[reflect.Type]bool)
	for _, typeMember := range qe.outputs {
		if ok := inQuery[typeMember.outerType()]; !ok {
			inQuery[typeMember.outerType()] = true
			typesInQuery = append(typesInQuery, typeMember.outerType().Name())
		}
	}

	var mapDecodeInfos = make(map[reflect.Type]*MapDecodeInfo)
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
		if outputVal.Kind() == reflect.Map {
			mapDecodeInfos[outputVal.Type()] = &MapDecodeInfo{m: outputVal}
		} else {
			typeDest[outputVal.Type()] = outputVal
		}
		outputVals = append(outputVals, outputVal)
	}

	// Generate the pointers.
	var ptrs = []any{}
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
		typeMember := qe.outputs[idx]
		switch tm := typeMember.(type) {
		case field:
			outputVal, ok := typeDest[tm.structType]
			if !ok {
				return nil, nil, fmt.Errorf("type %q found in query but not passed to decode", tm.structType.Name())
			}
			val := outputVal.FieldByIndex(tm.index)
			if !val.CanSet() {
				return nil, nil, fmt.Errorf("internal error: cannot set field %s of struct %s", tm.name, tm.structType.Name())
			}
			ptrs = append(ptrs, val.Addr().Interface())
		case mapKey:
			mapDecodeInfo, ok := mapDecodeInfos[tm.mapType]
			if !ok {
				return nil, nil, fmt.Errorf("type %q found in query but not passed to decode", tm.mapType.Name())
			}
			// Scan in to a new value with the type of the maps value
			mapDecodeInfo.keys = append(mapDecodeInfo.keys, tm.name)
			mapDecodeInfo.values = append(mapDecodeInfo.values, reflect.New(tm.mapType.Elem()).Elem())
			ptrs = append(ptrs, mapDecodeInfo.values[len(mapDecodeInfo.values)-1].Addr().Interface())
		}
	}
	var mapInfoSlice = []*MapDecodeInfo{}
	for _, v := range mapDecodeInfos {
		mapInfoSlice = append(mapInfoSlice, v)
	}
	return ptrs, mapInfoSlice, nil
}

// Populate puts scanned values into the map.
func (mapDecodeInfo *MapDecodeInfo) Populate() {
	for i, k := range mapDecodeInfo.keys {
		mapDecodeInfo.m.SetMapIndex(reflect.ValueOf(k), mapDecodeInfo.values[i])
	}
}
