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
	outputs []typeElement
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
	for _, typeElement := range pe.inputs {
		inQuery[typeElement.outerType()] = true
	}

	var typeValue = make(map[reflect.Type]reflect.Value)
	var typeNames []string
	for _, arg := range args {
		if arg == nil {
			return nil, fmt.Errorf("need map or struct, got nil")
		}
		v := reflect.ValueOf(arg)
		v = reflect.Indirect(v)
		t := v.Type()
		if v.Kind() != reflect.Struct && v.Kind() != reflect.Map {
			return nil, fmt.Errorf("need map or struct, got %s", t.Kind())
		}

		typeValue[t] = v
		typeNames = append(typeNames, t.Name())
		if !inQuery[t] {
			// Check if we have a type with the same name from a different package.
			for _, typeElement := range pe.inputs {
				if t.Name() == typeElement.outerType().Name() {
					return nil, fmt.Errorf("type %s not found, have %s", typeElement.outerType().String(), t.String())
				}
			}
			return nil, fmt.Errorf("%s not referenced in query", t.Name())
		}
	}

	// Query parameteres.
	qargs := []any{}
	for i, typeElement := range pe.inputs {
		v, ok := typeValue[typeElement.outerType()]
		if !ok {
			return nil, fmt.Errorf(`type %s not found, have: %s`, typeElement.outerType().Name(), strings.Join(typeNames, ", "))
		}
		switch te := typeElement.(type) {
		case field:
			qargs = append(qargs, sql.Named("sqlair_"+strconv.Itoa(i), v.FieldByIndex(te.index).Interface()))
		case mapKey:
			// MapIndex returns a zero value of the key is not in the map so we
			// need to check the MapKeys().
			var val reflect.Value
			for _, key := range v.MapKeys() {
				if key.String() == te.name {
					val = v.MapIndex(reflect.ValueOf(te.name))
					break
				}
			}
			if !val.IsValid() {
				return nil, fmt.Errorf(`map does not contain key %q`, te.name)
			}
			qargs = append(qargs, sql.Named("sqlair_"+strconv.Itoa(i), val.Interface()))
		}
	}
	return &QueryExpr{outputs: pe.outputs, sql: pe.sql, args: qargs}, nil
}

type MapDecodeInfo struct {
	m         reflect.Value
	valuePtrs []any
	keyIndex  map[string]int
}

// ScanArgs returns list of pointers to the struct fields that are listed in qe.outputs.
// All the structs mentioned in the query must be in outputArgs.
// All outputArgs must be structs.
func (qe *QueryExpr) ScanArgs(columns []string, outputArgs []any) ([]any, []*MapDecodeInfo, error) {
	var typesInQuery = []string{}
	var inQuery = make(map[reflect.Type]bool)
	for _, typeElement := range qe.outputs {
		if ok := inQuery[typeElement.outerType()]; !ok {
			inQuery[typeElement.outerType()] = true
			typesInQuery = append(typesInQuery, typeElement.outerType().Name())
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
			mapDecodeInfos[outputVal.Type()] = &MapDecodeInfo{m: outputVal, keyIndex: map[string]int{}}
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
		typeElement := qe.outputs[idx]
		switch te := typeElement.(type) {
		case field:
			outputVal, ok := typeDest[te.structType]
			if !ok {
				return nil, nil, fmt.Errorf("type %q found in query but not passed to decode", te.structType.Name())
			}
			val := outputVal.FieldByIndex(te.index)
			if !val.CanSet() {
				return nil, nil, fmt.Errorf("internal error: cannot set field %s of struct %s", te.name, te.structType.Name())
			}
			ptrs = append(ptrs, val.Addr().Interface())
		case mapKey:
			mapDecodeInfo, ok := mapDecodeInfos[te.mapType]
			if !ok {
				return nil, nil, fmt.Errorf("type %q found in query but not passed to decode", te.mapType.Name())
			}
			// Scan in to a new variable with the type of the maps value
			val := reflect.New(te.mapType.Elem()).Elem()
			addr := val.Addr().Interface()
			ptrs = append(ptrs, addr)
			mapDecodeInfo.valuePtrs = append(mapDecodeInfo.valuePtrs, addr)
			mapDecodeInfo.keyIndex[te.name] = len(mapDecodeInfo.valuePtrs) - 1
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
	for k, i := range mapDecodeInfo.keyIndex {
		// Is there a better way to pass the set values here than their
		// pointers?
		mapDecodeInfo.m.SetMapIndex(reflect.ValueOf(k), reflect.ValueOf(mapDecodeInfo.valuePtrs[i]).Elem())
	}
}
