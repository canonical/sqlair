package expr

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

func (qe *QueryExpr) QuerySQL() string {
	return qe.sql
}

func (qe *QueryExpr) QueryArgs() []any {
	return qe.args
}

func (qe *QueryExpr) HasOutputs() bool {
	return len(qe.outputs) > 0
}

type QueryExpr struct {
	sql     string
	args    []any
	outputs []field
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

	var typeInQuery = make(map[reflect.Type]bool)
	for _, in := range pe.inputs {
		typeInQuery[in.structType] = true
	}

	var typeValue = make(map[reflect.Type]reflect.Value)
	var typeNames []string
	for _, arg := range args {
		v := reflect.ValueOf(arg)
		if v.Kind() == reflect.Invalid || (v.Kind() == reflect.Pointer && v.IsNil()) {
			return nil, fmt.Errorf("need struct, got nil")
		}
		v = reflect.Indirect(v)
		t := v.Type()
		if t.Kind() != reflect.Struct {
			return nil, fmt.Errorf("need struct, got %s", t.Kind())
		}
		if _, ok := typeValue[t]; ok {
			return nil, fmt.Errorf("type %q provided more than once, rename one of them", t.Name())
		}
		typeValue[t] = v
		typeNames = append(typeNames, t.Name())

		if !typeInQuery[t] {
			// Check if we have a type with the same name from a different package.
			for _, in := range pe.inputs {
				if t.Name() == in.structType.Name() {
					return nil, fmt.Errorf("type %s not passed as a parameter, have %s", in.structType.String(), t.String())
				}
			}
			return nil, fmt.Errorf("%s not referenced in query", t.Name())
		}
	}

	// Query parameteres.
	qargs := []any{}

	for i, in := range pe.inputs {
		v, ok := typeValue[in.structType]
		if !ok {
			if len(typeNames) == 0 {
				return nil, fmt.Errorf(`type %q not passed as a parameter`, in.structType.Name())
			} else {
				return nil, fmt.Errorf(`type %q not passed as a parameter, have: %s`, in.structType.Name(), strings.Join(typeNames, ", "))
			}
		}
		named := sql.Named("sqlair_"+strconv.Itoa(i), v.FieldByIndex(in.index).Interface())
		qargs = append(qargs, named)
	}

	return &QueryExpr{outputs: pe.outputs, sql: pe.sql, args: qargs}, nil
}

// ScanArgs returns list of pointers to the struct fields that are listed in qe.outputs.
// All the structs mentioned in the query must be in outputArgs.
// All outputArgs must be structs.
func (qe *QueryExpr) ScanArgs(columns []string, outputArgs []any) ([]any, error) {
	outputVals := []reflect.Value{}
	for _, outputArg := range outputArgs {
		if outputArg == nil {
			return nil, fmt.Errorf("need pointer to struct, got nil")
		}
		outputVal := reflect.ValueOf(outputArg)
		if outputVal.Kind() != reflect.Pointer {
			return nil, fmt.Errorf("need pointer to struct, got %s", outputVal.Kind())
		}
		if outputVal.IsNil() {
			return nil, fmt.Errorf("got nil pointer")
		}
		outputVal = reflect.Indirect(outputVal)
		if outputVal.Kind() != reflect.Struct {
			return nil, fmt.Errorf("need pointer to struct, got pointer to %s", outputVal.Kind())
		}
		outputVals = append(outputVals, outputVal)
	}

	// Check that each outputVal is in the query.
	var typesInQuery = []string{}
	var inQuery = make(map[reflect.Type]bool)
	for _, field := range qe.outputs {
		if ok := inQuery[field.structType]; !ok {
			inQuery[field.structType] = true
			typesInQuery = append(typesInQuery, field.structType.Name())
		}
	}
	var typeDest = make(map[reflect.Type]reflect.Value)
	for _, outputVal := range outputVals {
		t := outputVal.Type()
		if !inQuery[t] {
			return nil, fmt.Errorf("type %q does not appear in query, have: %s", t.Name(), strings.Join(typesInQuery, ", "))
		}
		if _, ok := typeDest[t]; ok {
			return nil, fmt.Errorf("type %q provided more than once, rename one of them", t.Name())
		}
		typeDest[t] = outputVal
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
