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
	inQuery := make(map[reflect.Type]bool)
	for _, out := range outputs.structFields {
		inQuery[out.structType] = true
	}

	// Check that each dest is in the query.
	for _, dest := range dests {
		if !inQuery[dest.Type()] {
			return []any{}, fmt.Errorf("type %q does not appear as an output type in the query", dest.Type().Name())
		}
	}

	var typeDest = make(map[reflect.Type]reflect.Value)
	for _, dest := range dests {
		typeDest[dest.Type()] = dest
	}

	//  SQLair columns are named as _sqlair_X, where X = 0, 1, 2,...
	//  offset is the difference between X and the index i and where column _sqlair_X is located in cols.
	//  It allows non-sqlair columns to be returned in the results.
	offset := 0
	ptrs := []any{}
	for i, col := range cols {
		if col == "_sqlair_"+strconv.Itoa(i-offset) {
			field := outputs.structFields[i-offset]
			dest, ok := typeDest[field.structType]
			if !ok {
				return []any{}, fmt.Errorf("type %s found in query but not passed to decode", field.structType.Name())
			}

			val := dest.FieldByIndex(field.index)
			if !val.CanAddr() {
				return []any{}, fmt.Errorf("cannot address field %s of struct %s", field.name, field.structType.Name())
			}
			ptrs = append(ptrs, val.Addr().Interface())
		} else {
			var x any
			ptrs = append(ptrs, &x)
			offset++
		}
	}
	return ptrs, nil
}
