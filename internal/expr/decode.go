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

// ScanArgs returns list of pointers to the struct fields that are listed in qe.outputs.
// All the structs mentioned in the query must be in outputVals.
// All outputVals must be of kind reflect.Struct.
func (qe *QueryExpr) ScanArgs(columns []string, outputVals []reflect.Value) ([]any, error) {
	// Check that each outputVal is in the query.
	var inQuery = make(map[reflect.Type]bool)
	for _, field := range qe.outputs {
		inQuery[field.structType] = true
	}
	var typeDest = make(map[reflect.Type]reflect.Value)
	for _, outputVal := range outputVals {
		if !inQuery[outputVal.Type()] {
			return nil, fmt.Errorf("type %q does not appear as an output type in the query", outputVal.Type().Name())
		}
		typeDest[outputVal.Type()] = outputVal
	}

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
			return nil, fmt.Errorf("internal error: sqlair column not in outputs (%d>=%d)", idx, len(qe.outputs))
		}
		field := qe.outputs[idx]
		outputVal, ok := typeDest[field.structType]
		if !ok {
			return nil, fmt.Errorf("type %q found in query but not passed to decode", field.structType.Name())
		}

		val := outputVal.FieldByIndex(field.index)
		if !val.CanSet() {
			return nil, fmt.Errorf("internal error: cannot set field %s of struct %s", field.name, field.structType.Name())
		}
		ptrs = append(ptrs, val.Addr().Interface())
	}
	return ptrs, nil
}
