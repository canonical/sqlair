package expr

import (
	"fmt"
	"reflect"
	"strconv"
)

// FabricatedOutputAddrs generates a new instace for each output struct and
// returns the pointers to their fields for scanning the database columns into.
func FabricatedOutputAddrs(cols []string, outputs []field) ([]any, []any, error) {
	return generateAddrs(cols, outputs, []reflect.Value{}, true)
}

// OutputAddrs gets the pointers to the output struct fields for scanning the database columns into.
func OutputAddrs(cols []string, outputs []field, dests []reflect.Value) ([]any, error) {
	addrs, _, err := generateAddrs(cols, outputs, dests, false)
	return addrs, err
}

// generateAddrs gets the pointers to the output struct fields for scanning the database columns into.
// It can fabricate the structs if fabricate=true.
// If fabricate=false all the structs mentioned in the query must be in dests.
// All dests must be of kind reflect.Struct and must be addressable and settable.
func generateAddrs(cols []string, outputs []field, dests []reflect.Value, fabricate bool) ([]any, []any, error) {
	inQuery := make(map[reflect.Type]bool)
	queryTypes := []reflect.Type{}
	for _, out := range outputs {
		if t := out.structType; !inQuery[t] {
			inQuery[t] = true
			queryTypes = append(queryTypes, t)
		}
	}

	var newStructs = []any{}
	if fabricate {
		// Generate a new instance of each output type in the query.
		for _, t := range queryTypes {
			newValue := reflect.New(t)
			newStructs = append(newStructs, newValue.Interface())
			dests = append(dests, newValue.Elem())
		}
	} else {
		// Check that each dest is in the query.
		for _, dest := range dests {
			if !inQuery[dest.Type()] {
				return []any{}, []any{}, fmt.Errorf("type %q does not appear as an output type in the query", dest.Type().Name())
			}
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
			field := outputs[i-offset]

			dest, ok := typeDest[field.structType]
			if !ok {
				return []any{}, []any{}, fmt.Errorf("type %s found in query but not passed to decode", field.structType.Name())
			}

			val := dest.FieldByIndex(field.index)
			if !val.CanAddr() {
				return []any{}, []any{}, fmt.Errorf("cannot address field %s of struct %s", field.name, field.structType.Name())
			}
			ptrs = append(ptrs, val.Addr().Interface())
		} else {
			var x any
			ptrs = append(ptrs, &x)
			offset++
		}
	}
	return ptrs, newStructs, nil
}
