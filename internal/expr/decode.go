package expr

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

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

type MapDecodeInfo struct {
	valueSlice []any
	mapPtrs    []any
	keyIndex   map[string]int
	m          map[string]any
}

// ScanArgs returns list of pointers to the struct fields that are listed in qe.outputs.
// All the structs mentioned in the query must be in outputVals.
// All outputVals must be of kind reflect.Struct.
func (qe *QueryExpr) ScanArgs(columns []string, outputArgs []any) ([]any, *MapDecodeInfo, error) {
	// Check that each outputVal is in the query.
	var inQuery = make(map[reflect.Type]bool)
	for _, typeElement := range qe.outputs {
		switch te := typeElement.(type) {
		case field:
			inQuery[te.structType] = true
		}
	}

	var typeDest = make(map[reflect.Type]reflect.Value)
	var m map[string]any
	var outputVals = []reflect.Value{}
	for _, outputArg := range outputArgs {
		if outputArg == nil {
			return nil, nil, fmt.Errorf("need map or pointer to struct, got nil")
		}
		outputVal := reflect.ValueOf(outputArg)
		if k := outputVal.Kind(); k != reflect.Pointer && k != reflect.Map {
			return nil, nil, fmt.Errorf("need map or pointer to struct, got %s", outputVal.Kind())
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
				return nil, nil, fmt.Errorf(`internal error: cannot cast map type to *map[string]any, have type %T`, outputArg)
			}
		default:
			return nil, nil, fmt.Errorf("need map or pointer to struct, got pointer to %s", k)
		}
		outputVals = append(outputVals, outputVal)
	}

	var keyIndex = map[string]int{}
	var ptrs = []any{}
	var mapPtrs = []any{}
	// Generate the pointers.
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
