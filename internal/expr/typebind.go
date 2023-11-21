package expr

import (
	"bytes"
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

// TypedExpr represents a SQLair query bound to concrete Go types. It contains
// all the type information needed by SQLair.
type TypedExpr struct {
	outputs []typeMember
	inputs  []typeMember
	sql     string
}

// SQL returns the SQL ready for execution.
func (te *TypedExpr) SQL() string {
	return te.sql
}

// BindInputs takes the SQLair input arguments and returns the PrimedQuery ready
// for use with the database.
func (te *TypedExpr) BindInputs(args ...any) (pq *PrimedQuery, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("invalid input parameter: %s", err)
		}
	}()

	var inQuery = make(map[reflect.Type]bool)
	for _, typeMember := range te.inputs {
		inQuery[typeMember.outerType()] = true
	}

	var typeValue = make(map[reflect.Type]reflect.Value)
	var typeNames []string
	for _, arg := range args {
		v := reflect.ValueOf(arg)
		if v.Kind() == reflect.Invalid || (v.Kind() == reflect.Pointer && v.IsNil()) {
			return nil, fmt.Errorf("need struct or map, got nil")
		}
		v = reflect.Indirect(v)
		t := v.Type()
		if v.Kind() != reflect.Struct && v.Kind() != reflect.Map {
			return nil, fmt.Errorf("need struct or map, got %s", t.Kind())
		}
		if _, ok := typeValue[t]; ok {
			return nil, fmt.Errorf("type %q provided more than once", t.Name())
		}
		typeValue[t] = v
		typeNames = append(typeNames, t.Name())
		if !inQuery[t] {
			// Check if we have a type with the same name from a different package.
			for _, typeMember := range te.inputs {
				if t.Name() == typeMember.outerType().Name() {
					return nil, fmt.Errorf("parameter with type %q missing, have type with same name: %q", typeMember.outerType().String(), t.String())
				}
			}
			return nil, fmt.Errorf("%s not referenced in query", t.Name())
		}
	}

	// Query parameters.
	params := []any{}
	for i, typeMember := range te.inputs {
		outerType := typeMember.outerType()
		v, ok := typeValue[outerType]
		if !ok {
			return nil, typeMissingError(outerType.Name(), typeNames)
		}
		var val reflect.Value
		switch tm := typeMember.(type) {
		case *structField:
			val = v.Field(tm.index)
		case *mapKey:
			val = v.MapIndex(reflect.ValueOf(tm.name))
			if val.Kind() == reflect.Invalid {
				return nil, fmt.Errorf(`map %q does not contain key %q`, outerType.Name(), tm.name)
			}
		}
		params = append(params, sql.Named("sqlair_"+strconv.Itoa(i), val.Interface()))
	}
	return &PrimedQuery{outputs: te.outputs, sql: te.sql, params: params}, nil
}

const markerPrefix = "_sqlair_"

func markerName(n int) string {
	return markerPrefix + strconv.Itoa(n)
}

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

// getKeys returns the keys of a string map in a deterministic order.
func getKeys[T any](m map[string]T) []string {
	i := 0
	keys := make([]string, len(m))
	for k := range m {
		keys[i] = k
		i++
	}
	sort.Strings(keys)
	return keys
}

// starCountColumns counts the number of asterisks in a list of columns.
func starCountColumns(cs []columnAccessor) int {
	s := 0
	for _, c := range cs {
		if c.columnName == "*" {
			s++
		}
	}
	return s
}

// starCountTypes counts the number of asterisks in a list of types.
func starCountTypes(vs []valueAccessor) int {
	s := 0
	for _, v := range vs {
		if v.memberName == "*" {
			s++
		}
	}
	return s
}

func typeMissingError(missingType string, existingTypes []string) error {
	if len(existingTypes) == 0 {
		return fmt.Errorf(`parameter with type %q missing`, missingType)
	}
	// "%s" is used instead of %q to correctly print double quotes within the joined string.
	return fmt.Errorf(`parameter with type %q missing (have "%s")`, missingType, strings.Join(existingTypes, `", "`))
}

// bindInputTypes binds the input expression to a type and returns the
// typeMember represented by the expression.
func bindInputTypes(ti typeNameToInfo, e *inputExpr) (tm typeMember, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("input expression: %s: %s", err, e.raw)
		}
	}()

	info, ok := ti[e.sourceType.typeName]
	if !ok {
		return nil, typeMissingError(e.sourceType.typeName, getKeys(ti))
	}

	tm, err = info.typeMember(e.sourceType.memberName)
	if err != nil {
		return nil, err
	}
	return tm, nil
}

// bindOutputTypes binds the output expression to concrete types. It then checks
// the expression is formatted correctly and generates the columns for the query
// and the typeMembers the columns correspond to.
func bindOutputTypes(ti typeNameToInfo, e *outputExpr) (outCols []columnAccessor, typeMembers []typeMember, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("output expression: %s: %s", err, e.raw)
		}
	}()

	numTypes := len(e.targetTypes)
	numColumns := len(e.sourceColumns)
	starTypes := starCountTypes(e.targetTypes)
	starColumns := starCountColumns(e.sourceColumns)

	// Case 1: Generated columns e.g. "* AS (&P.*, &A.id)" or "&P.*".
	if numColumns == 0 || (numColumns == 1 && starColumns == 1) {
		pref := ""
		// Prepend table name. E.g. "t" in "t.* AS &P.*".
		if numColumns > 0 {
			pref = e.sourceColumns[0].tableName
		}

		for _, t := range e.targetTypes {
			info, ok := ti[t.typeName]
			if !ok {
				return nil, nil, typeMissingError(t.typeName, getKeys(ti))
			}
			if t.memberName == "*" {
				// Generate asterisk columns.
				allMembers, err := info.getAllMembers()
				if err != nil {
					return nil, nil, err
				}
				typeMembers = append(typeMembers, allMembers...)
				for _, tm := range allMembers {
					outCols = append(outCols, columnAccessor{pref, tm.memberName()})
				}
			} else {
				// Generate explicit columns.
				tm, err := info.typeMember(t.memberName)
				if err != nil {
					return nil, nil, err
				}
				typeMembers = append(typeMembers, tm)
				outCols = append(outCols, columnAccessor{pref, t.memberName})
			}
		}
		return outCols, typeMembers, nil
	} else if numColumns > 1 && starColumns > 0 {
		return nil, nil, fmt.Errorf("invalid asterisk in columns")
	}

	// Case 2: Explicit columns, single asterisk type e.g. "(col1, t.col2) AS &P.*".
	if starTypes == 1 && numTypes == 1 {
		info, ok := ti[e.targetTypes[0].typeName]
		if !ok {
			return nil, nil, typeMissingError(e.targetTypes[0].typeName, getKeys(ti))
		}
		for _, c := range e.sourceColumns {
			tm, err := info.typeMember(c.columnName)
			if err != nil {
				return nil, nil, err
			}
			typeMembers = append(typeMembers, tm)
			outCols = append(outCols, c)
		}
		return outCols, typeMembers, nil
	} else if starTypes > 0 && numTypes > 1 {
		return nil, nil, fmt.Errorf("invalid asterisk in types")
	}

	// Case 3: Explicit columns and types e.g. "(col1, col2) AS (&P.name, &P.id)".
	if numColumns == numTypes {
		for i, c := range e.sourceColumns {
			t := e.targetTypes[i]
			info, ok := ti[t.typeName]
			if !ok {
				return nil, nil, typeMissingError(t.typeName, getKeys(ti))
			}
			tm, err := info.typeMember(t.memberName)
			if err != nil {
				return nil, nil, err
			}
			typeMembers = append(typeMembers, tm)
			outCols = append(outCols, c)
		}
	} else {
		return nil, nil, fmt.Errorf("mismatched number of columns and target types")
	}

	return outCols, typeMembers, nil
}

type typeNameToInfo map[string]typeInfo

// BindTypes takes samples of all types mentioned in the SQLair expressions of
// the query. The expressions are checked for validity and required information
// is generated from the types.
func (pe *ParsedExpr) BindTypes(args ...any) (te *TypedExpr, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("cannot prepare statement: %s", err)
		}
	}()

	var ti = make(typeNameToInfo)

	// Generate and save reflection info.
	for _, arg := range args {
		if arg == nil {
			return nil, fmt.Errorf("need struct or map, got nil")
		}
		t := reflect.TypeOf(arg)
		switch t.Kind() {
		case reflect.Struct, reflect.Map:
			if t.Name() == "" {
				return nil, fmt.Errorf("cannot use anonymous %s", t.Kind())
			}
			info, err := getTypeInfo(arg)
			if err != nil {
				return nil, err
			}
			if dupeInfo, ok := ti[t.Name()]; ok {
				if dupeInfo.typ() == t {
					return nil, fmt.Errorf("found multiple instances of type %q", t.Name())
				}
				return nil, fmt.Errorf("two types found with name %q: %q and %q", t.Name(), dupeInfo.typ().String(), t.String())
			}
			ti[t.Name()] = info
		case reflect.Pointer:
			return nil, fmt.Errorf("need struct or map, got pointer to %s", t.Elem().Kind())
		default:
			return nil, fmt.Errorf("need struct or map, got %s", t.Kind())
		}
	}

	var sql bytes.Buffer

	var inCount int
	var outCount int

	var outputs = make([]typeMember, 0)
	var inputs = make([]typeMember, 0)

	var typeMemberPresent = make(map[typeMember]bool)

	// Check and expand each query expr.
	for _, expr := range pe.exprs {
		switch e := expr.(type) {
		case *inputExpr:
			inLoc, err := bindInputTypes(ti, e)
			if err != nil {
				return nil, err
			}
			sql.WriteString("@sqlair_" + strconv.Itoa(inCount))
			inCount++
			inputs = append(inputs, inLoc)
		case *outputExpr:
			outCols, typeMembers, err := bindOutputTypes(ti, e)
			if err != nil {
				return nil, err
			}

			for _, tm := range typeMembers {
				if ok := typeMemberPresent[tm]; ok {
					return nil, fmt.Errorf("member %q of type %q appears more than once in output expressions", tm.memberName(), tm.outerType().Name())
				}
				typeMemberPresent[tm] = true
			}

			for i, c := range outCols {
				sql.WriteString(c.String())
				sql.WriteString(" AS ")
				sql.WriteString(markerName(outCount))
				if i != len(outCols)-1 {
					sql.WriteString(", ")
				}
				outCount++
			}
			outputs = append(outputs, typeMembers...)
		case *bypass:
			sql.WriteString(e.chunk)
		default:
			return nil, fmt.Errorf("internal error: unknown query expr type %T", expr)
		}
	}

	return &TypedExpr{inputs: inputs, outputs: outputs, sql: sql.String()}, nil
}
