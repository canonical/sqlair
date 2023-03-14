package expr

import (
	"fmt"
	"reflect"
	"strconv"
)

// OutputAddrs returns list of pointers to struct fields specified in outputs.
// All the structs mentioned in the query must be in dests.
// All dests must be of kind reflect.Struct and must be addressable and settable.
func OutputAddrs(cols []string, outputs Outputs, dests []reflect.Value) ([]any, error) {
	// Check that each dest is in the query.
	var inQuery = make(map[reflect.Type]bool)
	for _, field := range outputs.structFields {
		inQuery[field.structType] = true
	}
	var typeDest = make(map[reflect.Type]reflect.Value)
	for _, dest := range dests {
		if !inQuery[dest.Type()] {
			return []any{}, fmt.Errorf("type %q does not appear as an output type in the query", dest.Type().Name())
		}
		typeDest[dest.Type()] = dest
	}

	//  SQLair columns in the results are named as _sqlair_X, where X = 0, 1, 2,...
	//  offset is the difference between X and the index i of cols where the column _sqlair_X is located.
	var offset = 0
	var ptrs = []any{}
	for i, col := range cols {
		if col == "_sqlair_"+strconv.Itoa(i-offset) {
			field := outputs.structFields[i-offset]
			dest, ok := typeDest[field.structType]
			if !ok {
				return []any{}, fmt.Errorf("type %s found in query but not passed to decode", field.structType.Name())
			}

			val := dest.FieldByIndex(field.index)
			if !val.CanSet() {
				return []any{}, fmt.Errorf("cannot set field %s of struct %s", field.name, field.structType.Name())
			}
			ptrs = append(ptrs, val.Addr().Interface())
		} else {
			// Columns not mentioned in output expressions are scanned into x.
			// TODO: Add M type and save these columns in the map.
			var x any
			ptrs = append(ptrs, &x)
			offset++
		}
	}
	return ptrs, nil
}
