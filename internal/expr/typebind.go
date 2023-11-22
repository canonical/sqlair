package expr

import (
	"bytes"
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/canonical/sqlair/internal/typeinfo"
)

// TypedExprs represents a SQLair query bound to concrete Go types. It contains
// all the type information needed by SQLair.
type TypedExprs []typedExpression

// typedExpression represents a expression bound to a type. It contains
// information to generate the SQL for the part and to access Go types
// referenced in the part.
type typedExpression interface {
	// typedExpr is a marker method.
	typedExpr()
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

// BindInputs takes the SQLair input arguments and returns the PrimedQuery ready
// for use with the database.
func (tes *TypedExprs) BindInputs(args ...any) (pq *PrimedQuery, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("invalid input parameter: %s", err)
		}
	}()

	var typeToValue = make(map[reflect.Type]reflect.Value)
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
		if _, ok := typeToValue[t]; ok {
			return nil, fmt.Errorf("type %q provided more than once", t.Name())
		}
		typeToValue[t] = v
	}

	// Generate SQL and query parameters.
	params := []any{}
	outputs := []typeinfo.Member{}
	argTypeUsed := map[reflect.Type]bool{}
	inCount := 0
	outCount := 0
	sqlStr := bytes.Buffer{}
	for _, te := range *tes {
		switch te := te.(type) {
		case *typedInputExpr:
			typeMember := te.input
			outerType := typeMember.OuterType()
			v, ok := typeToValue[outerType]
			if !ok {
				return nil, missingInputError(outerType, typeToValue)
			}
			argTypeUsed[outerType] = true

			val, err := typeMember.ValueFromOuter(v)
			if err != nil {
				return nil, err
			}
			params = append(params, sql.Named("sqlair_"+strconv.Itoa(inCount), val.Interface()))

			sqlStr.WriteString("@sqlair_" + strconv.Itoa(inCount))
			inCount++
		case *typedOutputExpr:
			for i, oc := range te.outputColumns {
				sqlStr.WriteString(oc.sql)
				sqlStr.WriteString(" AS ")
				sqlStr.WriteString(markerName(outCount))
				if i != len(te.outputColumns)-1 {
					sqlStr.WriteString(", ")
				}
				outCount++
				outputs = append(outputs, oc.tm)
			}
		case *bypass:
			sqlStr.WriteString(te.chunk)
		}
	}

	for argType := range typeToValue {
		if !argTypeUsed[argType] {
			return nil, fmt.Errorf("%s not referenced in query", argType.Name())
		}
	}

	return &PrimedQuery{outputs: outputs, sql: sqlStr.String(), params: params}, nil
}

// missingInputError returns an error message for a missing input type.
func missingInputError(missingType reflect.Type, typeToValue map[reflect.Type]reflect.Value) error {
	// check if the missing type and some argument type have the same name but
	// are from different packages.
	typeNames := []string{}
	for argType := range typeToValue {
		if argType.Name() == missingType.Name() {
			return fmt.Errorf("parameter with type %q missing, have type with same name: %q", missingType.String(), argType.String())
		}
		typeNames = append(typeNames, argType.Name())
	}
	return typeMissingError(missingType.Name(), typeNames)
}

// outputColumn stores the name of a column to fetch from the database and the
// type member to scan the result into.
type outputColumn struct {
	sql string
	tm  typeinfo.Member
}

// typedOutputExpr contains the columns to fetch from the database and
// information about the Go values to read the query results into.
type typedOutputExpr struct {
	outputColumns []outputColumn
}

// typedExpr is a marker method.
func (*typedOutputExpr) typedExpr() {}

// typedInputExpr stores information about a Go value to use as a query input.
type typedInputExpr struct {
	input typeinfo.Member
}

// typedExpr is a marker method.
func (*typedInputExpr) typedExpr() {}

// typedExpr is a marker method.
func (*bypass) typedExpr() {}

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
func bindInputTypes(ti typeNameToInfo, e *inputExpr) (tie *typedInputExpr, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("input expression: %s: %s", err, e.raw)
		}
	}()

	info, ok := ti[e.sourceType.typeName]
	if !ok {
		return nil, typeMissingError(e.sourceType.typeName, getKeys(ti))
	}

	tm, err := info.TypeMember(e.sourceType.memberName)
	if err != nil {
		return nil, err
	}
	return &typedInputExpr{tm}, nil
}

// bindOutputTypes binds the output expression to concrete types. It then checks
// the expression is formatted correctly and generates the columns for the query
// and the typeMembers the columns correspond to.
func bindOutputTypes(ti typeNameToInfo, e *outputExpr) (toe *typedOutputExpr, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("output expression: %s: %s", err, e.raw)
		}
	}()

	numTypes := len(e.targetTypes)
	numColumns := len(e.sourceColumns)
	starTypes := starCountTypes(e.targetTypes)
	starColumns := starCountColumns(e.sourceColumns)

	toe = &typedOutputExpr{}

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
				return nil, typeMissingError(t.typeName, getKeys(ti))
			}
			if t.memberName == "*" {
				// Generate asterisk columns.
				allMembers, err := info.GetAllMembers()
				if err != nil {
					return nil, err
				}
				for _, tm := range allMembers {
					oc := outputColumn{sql: colString(pref, tm.MemberName()), tm: tm}
					toe.outputColumns = append(toe.outputColumns, oc)
				}
			} else {
				// Generate explicit columns.
				tm, err := info.TypeMember(t.memberName)
				if err != nil {
					return nil, err
				}
				oc := outputColumn{sql: colString(pref, t.memberName), tm: tm}
				toe.outputColumns = append(toe.outputColumns, oc)
			}
		}
		return toe, nil
	} else if numColumns > 1 && starColumns > 0 {
		return nil, fmt.Errorf("invalid asterisk in columns")
	}

	// Case 2: Explicit columns, single asterisk type e.g. "(col1, t.col2) AS &P.*".
	if starTypes == 1 && numTypes == 1 {
		info, ok := ti[e.targetTypes[0].typeName]
		if !ok {
			return nil, typeMissingError(e.targetTypes[0].typeName, getKeys(ti))
		}
		for _, c := range e.sourceColumns {
			tm, err := info.TypeMember(c.columnName)
			if err != nil {
				return nil, err
			}
			oc := outputColumn{sql: c.String(), tm: tm}
			toe.outputColumns = append(toe.outputColumns, oc)
		}
		return toe, nil
	} else if starTypes > 0 && numTypes > 1 {
		return nil, fmt.Errorf("invalid asterisk in types")
	}

	// Case 3: Explicit columns and types e.g. "(col1, col2) AS (&P.name, &P.id)".
	if numColumns == numTypes {
		for i, c := range e.sourceColumns {
			t := e.targetTypes[i]
			info, ok := ti[t.typeName]
			if !ok {
				return nil, typeMissingError(t.typeName, getKeys(ti))
			}
			tm, err := info.TypeMember(t.memberName)
			if err != nil {
				return nil, err
			}
			oc := outputColumn{sql: c.String(), tm: tm}
			toe.outputColumns = append(toe.outputColumns, oc)
		}
	} else {
		return nil, fmt.Errorf("mismatched number of columns and target types")
	}

	return toe, nil
}

type typeNameToInfo map[string]typeinfo.Info

// BindTypes takes samples of all types mentioned in the SQLair expressions of
// the query. The expressions are checked for validity and required information
// is generated from the types.
func (pe *ParsedExprs) BindTypes(args ...any) (te *TypedExprs, err error) {
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
			info, err := typeinfo.GetTypeInfo(arg)
			if err != nil {
				return nil, err
			}
			if dupeInfo, ok := ti[t.Name()]; ok {
				if dupeInfo.Typ() == t {
					return nil, fmt.Errorf("found multiple instances of type %q", t.Name())
				}
				return nil, fmt.Errorf("two types found with name %q: %q and %q", t.Name(), dupeInfo.Typ().String(), t.String())
			}
			ti[t.Name()] = info
		case reflect.Pointer:
			return nil, fmt.Errorf("need struct or map, got pointer to %s", t.Elem().Kind())
		default:
			return nil, fmt.Errorf("need struct or map, got %s", t.Kind())
		}
	}

	typeMemberPresent := make(map[typeinfo.Member]bool)
	typedExprs := make([]typedExpression, 0)

	// Check and expand each query expr.
	for _, expr := range *pe {
		switch e := expr.(type) {
		case *inputExpr:
			tie, err := bindInputTypes(ti, e)
			if err != nil {
				return nil, err
			}
			typedExprs = append(typedExprs, tie)
		case *outputExpr:
			toe, err := bindOutputTypes(ti, e)
			if err != nil {
				return nil, err
			}

			for _, oc := range toe.outputColumns {
				tm := oc.tm
				if ok := typeMemberPresent[tm]; ok {
					return nil, fmt.Errorf("member %q of type %q appears more than once in output expressions", tm.MemberName(), tm.OuterType().Name())
				}
				typeMemberPresent[tm] = true
			}
			typedExprs = append(typedExprs, toe)
		case *bypass:
			typedExprs = append(typedExprs, e)
		default:
			return nil, fmt.Errorf("internal error: unknown query expr type %T", expr)
		}
	}
	typedExpr := TypedExprs(typedExprs)
	return &typedExpr, nil
}
