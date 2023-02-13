package expr

import (
	"fmt"
	"reflect"
)

type typeNameToValue = map[string]any

// Complete gathers the query arguments that are specified in inputParts from
// structs passed as parameters.
func (pe *PreparedExpr) Complete(args ...any) ([]any, error) {
	var tv = make(typeNameToValue)
	for _, arg := range args {
		if arg == (any)(nil) {
			return nil, fmt.Errorf("nil parameter")
		}
		tv[reflect.TypeOf(arg).Name()] = arg
	}

	// Query parameteres.
	qargs := []any{}

	for _, p := range pe.inputs {
		v, ok := tv[p.source.prefix]
		if !ok {
			return nil, fmt.Errorf(`type %s not passed as a parameter`, p.source.prefix)
		}
		qp, err := fieldValue(v, p.source)
		if err != nil {
			return nil, err
		}
		qargs = append(qargs, qp)
	}

	return qargs, nil
}
