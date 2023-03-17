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
