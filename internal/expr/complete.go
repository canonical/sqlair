package expr

import (
	"fmt"
	"reflect"
)

// Complete gathers the query arguments that are specified in inputParts from
// structs passed as parameters.
func (pe *PreparedExpr) Complete(args ...any) ([]any, error) {
	var tv = make(map[reflect.Type]reflect.Value)
	for _, arg := range args {
		if arg == nil {
			return nil, fmt.Errorf("nil parameter")
		}
		v := reflect.ValueOf(arg)
		tv[v.Type()] = v
	}

	// Query parameteres.
	qargs := []any{}

	for _, in := range pe.inputs {
		v, ok := tv[in.inputType]
		if !ok {
			return nil, fmt.Errorf(`type %s not passed as a parameter`, in.inputType.Name())
		}
		qargs = append(qargs, v.Field(in.field.index).Interface())
	}

	return qargs, nil
}
