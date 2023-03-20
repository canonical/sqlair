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
		switch te := typeElement.(type) {
		case field:
			inQuery[te.structType] = true
		}
	}

	var typeValue = make(map[reflect.Type]reflect.Value)
	var typeNames []string
	var m map[string]any
	for _, arg := range args {
		if arg == nil {
			return nil, fmt.Errorf("need a map or struct, got nil")
		}
		v := reflect.ValueOf(arg)
		v = reflect.Indirect(v)
		t := v.Type()

		switch t.Kind() {
		case reflect.Map:
			if t.Key().Kind() != reflect.String {
				return nil, fmt.Errorf(`map type %s must have key type string, found type %s`, t.Name(), t.Key().Kind())
			}
			if m != nil {
				return nil, fmt.Errorf(`found multiple map types`)
			}
			switch mtype := arg.(type) {
			case map[string]any:
				m = mtype
			case *map[string]any:
				m = *mtype
			default:
				return nil, fmt.Errorf(`internal error: cannot cast map type to map[string]any, have type %T`, mtype)
			}
		case reflect.Struct:
			typeValue[t] = v
			typeNames = append(typeNames, t.Name())
			if !inQuery[t] {
				// Check if we have a type with the same name from a different package.
				for _, typeElement := range pe.inputs {
					switch te := typeElement.(type) {
					case field:
						if t.Name() == te.structType.Name() {
							return nil, fmt.Errorf("type %s not found, have %s", te.structType.String(), t.String())
						}
					}
				}
				return nil, fmt.Errorf("%s not referenced in query", t.Name())
			}
		default:
			return nil, fmt.Errorf("need struct, got %s", t.Kind())
		}
	}

	// Query parameteres.
	qargs := []any{}
	for i, typeElement := range pe.inputs {
		switch te := typeElement.(type) {
		case field:
			v, ok := typeValue[te.structType]
			if !ok {
				return nil, fmt.Errorf(`type %s not found, have: %s`, te.structType.Name(), strings.Join(typeNames, ", "))
			}
			qargs = append(qargs, sql.Named("sqlair_"+strconv.Itoa(i), v.FieldByIndex(te.index).Interface()))
		case mapKey:
			v, ok := m[te.name]
			if !ok {
				return nil, fmt.Errorf(`map does not contain key %s`, te.name)
			}
			qargs = append(qargs, sql.Named("sqlair_"+strconv.Itoa(i), v))
		default:
			return nil, fmt.Errorf("internal error: field type %T not supported", te)
		}

	}
	return &QueryExpr{outputs: pe.outputs, sql: pe.sql, args: qargs}, nil
}

type MapDecodeInfo struct {
	valueSlice []any
	mapPtrs    []any
	keyIndex   map[string]int
	m          map[string]any
}

// ScanArgs returns list of pointers to the struct fields that are listed in qe.outputs.
// All the structs mentioned in the query must be in outputArgs.
// All outputArgs must be structs.
func (qe *QueryExpr) ScanArgs(columns []string, outputArgs []any) ([]any, *MapDecodeInfo, error) {
	var inQuery = make(map[reflect.Type]bool)
	for _, typeElement := range qe.outputs {
		switch te := typeElement.(type) {
		case field:
			inQuery[te.structType] = true
		}
	}

	var typeDest = make(map[reflect.Type]reflect.Value)
	var m map[string]any
	outputVals := []reflect.Value{}
	for _, outputArg := range outputArgs {
		if outputArg == nil {
			return nil, nil, fmt.Errorf("need map or pointer to struct, got nil")
		}
		outputVal := reflect.ValueOf(outputArg)
		if outputVal.Kind() != reflect.Pointer {
			return nil, nil, fmt.Errorf("need map or pointer to struct, got %s", outputVal.Kind())
		}
		if outputVal.IsNil() {
			return nil, nil, fmt.Errorf("got nil pointer")
		}
		outputVal = reflect.Indirect(outputVal)
		switch k := outputVal.Kind(); k {
		case reflect.Struct:
			if !inQuery[outputVal.Type()] {
				return nil, nil, fmt.Errorf("type %q does not appear as an output type in the query", outputVal.Type().Name())
			}
			typeDest[outputVal.Type()] = outputVal
		case reflect.Map:
			// Should we do these two checks or just try the type assertion?
			if outputVal.Type().Key().Kind() != reflect.String {
				return nil, nil, fmt.Errorf(`map type %s must have key type string, found type %s`, outputVal.Type().Name(), outputVal.Type().Key().Kind())
			}
			if !outputVal.Type().Elem().Implements(reflect.TypeOf((*any)(nil)).Elem()) {
				return nil, nil, fmt.Errorf(`map type %s must have value type any`, outputVal.Type().Name())
			}
			if m != nil {
				return nil, nil, fmt.Errorf(`found multiple map types`)
			}
			switch arg := outputArg.(type) {
			case map[string]any:
				m = arg
			case *map[string]any:
				m = *arg
			default:
				return nil, nil, fmt.Errorf(`internal error: cannot cast map type to map[string]any, have type %T`, outputArg)
			}
		default:
			return nil, nil, fmt.Errorf("need map or pointer to struct, got pointer to %s", k)
		}
		outputVals = append(outputVals, outputVal)
	}

	// Generate the pointers.
	var keyIndex = map[string]int{}
	var ptrs = []any{}
	var mapPtrs = []any{}
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
			v, ok := m[te.name]
			if ok {
				val := reflect.New(reflect.TypeOf(v)).Elem()
				addr := val.Addr().Interface()
				ptrs = append(ptrs, addr)
				mapPtrs = append(mapPtrs, addr)
			} else {
				var x any
				ptrs = append(ptrs, &x)
				mapPtrs = append(mapPtrs, &x)
			}
			keyIndex[te.name] = len(mapPtrs) - 1
		}
	}
	return ptrs, &MapDecodeInfo{mapPtrs: mapPtrs, keyIndex: keyIndex, m: m}, nil
}

// PopulateMap enters the scanned values into the map.
func (mapDecodeInfo *MapDecodeInfo) PopulateMap() {
	for k, i := range mapDecodeInfo.keyIndex {
		mapDecodeInfo.m[k] = reflect.ValueOf(mapDecodeInfo.mapPtrs[i]).Elem().Interface()
	}
}
