package expr

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
)

type ResultExpr struct {
	outputs []field
	rows    *sql.Rows
}

func NewResultExpr(pe *PreparedExpr, rows *sql.Rows) *ResultExpr {
	return &ResultExpr{outputs: pe.outputs, rows: rows}
}

func (re *ResultExpr) One(args ...any) error {
	if ok, err := re.Next(); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("cannot return one row: no results")
	}
	err := re.Decode(args...)
	if err != nil {
		return err
	}
	re.Close()
	return nil
}

// getTypes returns the types mentioned in SQLair expression of the query in the order they appear.
func getTypes(outs []field) []reflect.Type {
	isDup := make(map[reflect.Type]bool)
	ts := make([]reflect.Type, 0)
	for _, out := range outs {
		if t := out.structType; !isDup[t] {
			isDup[t] = true
			ts = append(ts, t)
		}
	}
	return ts
}

// All iterates over the query and decodes all the rows.
// It fabricates all struct instansiations needed.
func (re *ResultExpr) All() ([][]any, error) {
	var rss [][]any

	ts := getTypes(re.outputs)

	for {
		if ok, err := re.Next(); err != nil {
			return [][]any{}, err
		} else if !ok {
			break
		}

		newStructs := make([]reflect.Value, len(ts))

		for i, t := range ts {
			rp := reflect.New(t)
			// We need to unwrap the struct inside the interface{}.
			r := rp.Elem()

			newStructs[i] = r
		}

		err := re.decodeRow(newStructs...)
		if err != nil {
			return [][]any{}, err
		}

		rs := make([]any, len(ts))
		for i, r := range newStructs {
			rs[i] = r.Interface()
		}

		rss = append(rss, rs)
	}

	re.Close()
	return rss, nil
}

// Next prepares the next result row for reading with the Scan method.
func (re *ResultExpr) Next() (bool, error) {
	ok := re.rows.Next()
	if !ok && re.rows.Err() != nil {
		return false, re.rows.Err()
	}
	return ok, nil
}

// Decode copies the columns in the current row into the fields of the structs in dests.
// dests must contain all the structs mentioned in the query.
func (re *ResultExpr) Decode(dests ...any) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("cannot decode expression: %s", err)
		}
	}()

	vals := []reflect.Value{}

	for _, dest := range dests {
		if dest == nil {
			return fmt.Errorf("need valid struct, got nil")
		}

		v := reflect.ValueOf(dest)
		if v.Kind() != reflect.Pointer {
			return fmt.Errorf("need pointer to struct, got non-pointer")
		}

		v = reflect.Indirect(v)

		if v.Kind() != reflect.Struct {
			return fmt.Errorf("need struct, got %s", v.Kind())
		}

		t := v.Type()

		typeFound := false
		for _, f := range re.outputs {
			if f.structType == t {
				typeFound = true
				break
			}
		}

		if !typeFound {
			return fmt.Errorf("no output expression of type %s", t.Name())
		}

		vals = append(vals, v)
	}
	if err := re.decodeRow(vals...); err != nil {
		return err
	}

	return nil
}

// decodeRow copies a result row into the fields of the reflect.Values in dests.
// All the structs mentioned in the query must be in dests.
// The reflect.Value must be of Kind reflect.Struct and must be addressable and settable.
func (re *ResultExpr) decodeRow(dests ...reflect.Value) error {
	cols, err := re.rows.Columns()
	if err != nil {
		return err
	}

	var typeDest = make(map[reflect.Type]reflect.Value)
	for _, dest := range dests {
		typeDest[dest.Type()] = dest
	}

	ptrs := []any{}

	// 	sqlair columns are named as _sqlair_X, where X = 0, 1, 2,...
	//  offset is the difference between X and the index i and where column _sqlair_X is located in cols.
	//  It allows non-sqlair columns to be returned in the results.
	offset := 0

	for i, col := range cols {
		if col == "_sqlair_"+strconv.Itoa(i-offset) {
			field := re.outputs[i-offset]

			dest, ok := typeDest[field.structType]
			if !ok {
				return fmt.Errorf("type %s found in query but not passed to decode", field.structType.Name())
			}

			val := dest.FieldByIndex(field.index)
			if !val.CanAddr() {
				return fmt.Errorf("cannot address field %s of struct %s", field.name, field.structType.Name())
			}
			ptrs = append(ptrs, val.Addr().Interface())
		} else {
			var x any
			ptrs = append(ptrs, &x)
			offset++
		}
	}

	if err := re.rows.Scan(ptrs...); err != nil {
		return err
	}

	return nil
}

func (re *ResultExpr) Close() error {
	return re.rows.Close()
}
