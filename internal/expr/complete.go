package expr

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type CompletedExpr struct {
	outputs []loc
	SQL     string
	Args    []any
}

// Complete gathers the query arguments that are specified in inputParts from
// structs passed as parameters.
func (pe *PreparedExpr) Complete(args ...any) (ce *CompletedExpr, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("parameter issue: %s", err)
		}
	}()

	var tv = make(map[reflect.Type]reflect.Value)
	var typeNames []string
	for _, arg := range args {
		if arg == nil {
			return nil, fmt.Errorf("nil parameter")
		}
		v := reflect.ValueOf(arg)
		tv[v.Type()] = v
		typeNames = append(typeNames, v.Type().String())
	}

	// Query parameteres.
	qargs := []any{}

	for i, in := range pe.inputs {
		v, ok := tv[in.typ]
		if !ok {
			return nil, fmt.Errorf(`type %s not found, have: %s`, in.typ.String(), strings.Join(typeNames, ", "))
		}
		named := sql.Named("sqlair_"+strconv.Itoa(i), v.FieldByIndex(in.field.index).Interface())
		qargs = append(qargs, named)
	}

	return &CompletedExpr{outputs: pe.outputs, SQL: pe.SQL, Args: qargs}, nil
}
