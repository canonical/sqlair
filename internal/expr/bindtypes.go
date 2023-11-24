package expr

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"

	"github.com/canonical/sqlair/internal/typeinfo"
)

// ParsedExpr is the AST representation of SQLair query. It contains only
// information encoded in the SQLair query string.
type ParsedExpr struct {
	exprs []expression
}

// String returns a textual representation of the AST contained in the
// ParsedExpr for debugging and testing purposes.
func (pe *ParsedExpr) String() string {
	var out bytes.Buffer
	out.WriteString("[")
	for i, p := range pe.exprs {
		if i > 0 {
			out.WriteString(" ")
		}
		out.WriteString(p.String())
	}
	out.WriteString("]")
	return out.String()
}

// BindTypes takes samples of all types mentioned in the SQLair expressions of
// the query. The expressions are checked for validity and required information
// is generated from the types.
func (pe *ParsedExpr) BindTypes(args ...any) (te *TypedExpr, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("cannot prepare statement: %s", err)
		}
	}()

	var argInfos = make(typeinfo.ArgInfos)

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
			info, err := typeinfo.GetArgInfo(arg)
			if err != nil {
				return nil, err
			}
			if dupeInfo, ok := argInfos[t.Name()]; ok {
				if dupeInfo.Typ() == t {
					return nil, fmt.Errorf("found multiple instances of type %q", t.Name())
				}
				return nil, fmt.Errorf("two types found with name %q: %q and %q", t.Name(), dupeInfo.Typ().String(), t.String())
			}
			argInfos[t.Name()] = info
		case reflect.Pointer:
			return nil, fmt.Errorf("need struct or map, got pointer to %s", t.Elem().Kind())
		default:
			return nil, fmt.Errorf("need struct or map, got %s", t.Kind())
		}
	}

	var sql bytes.Buffer

	var inCount int
	var outCount int

	var outputs = make([]typeinfo.Output, 0)
	var inputs = make([]typeinfo.Input, 0)

	var outputUsed = make(map[typeinfo.Output]bool)

	// Check and expand each query expr.
	for _, expr := range pe.exprs {
		switch e := expr.(type) {
		case *inputExpr:
			inLoc, err := bindInputTypes(argInfos, e)
			if err != nil {
				return nil, err
			}
			sql.WriteString("@sqlair_" + strconv.Itoa(inCount))
			inCount++
			inputs = append(inputs, inLoc)
		case *outputExpr:
			outCols, os, err := bindOutputTypes(argInfos, e)
			if err != nil {
				return nil, err
			}

			for _, o := range os {
				if ok := outputUsed[o]; ok {
					return nil, fmt.Errorf("%s appears more than once in output expressions", o.String())
				}
				outputUsed[o] = true
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
			outputs = append(outputs, os...)
		case *bypass:
			sql.WriteString(e.chunk)
		default:
			return nil, fmt.Errorf("internal error: unknown query expr type %T", expr)
		}
	}

	return &TypedExpr{inputs: inputs, outputs: outputs, sql: sql.String()}, nil
}

// bindInputTypes binds the input expression to a type and returns the
// typeMember represented by the expression.
func bindInputTypes(argInfos typeinfo.ArgInfos, e *inputExpr) (vl typeinfo.Input, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("input expression: %s: %s", err, e.raw)
		}
	}()

	info, err := argInfos.GetArgWithMembers(e.sourceType.typeName)
	if err != nil {
		return nil, err
	}

	m, err := info.InputMember(e.sourceType.memberName)
	if err != nil {
		return nil, err
	}
	return m, nil
}

// bindOutputTypes binds the output expression to concrete types. It then checks
// the expression is formatted correctly and generates the columns for the query
// and the typeMembers the columns correspond to.
func bindOutputTypes(argInfos typeinfo.ArgInfos, e *outputExpr) (outCols []columnAccessor, outputs []typeinfo.Output, err error) {
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

			info, err := argInfos.GetArgWithMembers(t.typeName)
			if err != nil {
				return nil, nil, err
			}
			if t.memberName == "*" {
				// Generate asterisk columns.
				allMembers, err := info.AllOutputMembers()
				if err != nil {
					return nil, nil, err
				}
				outputs = append(outputs, allMembers...)
				memberNames, err := info.AllMemberNames()
				if err != nil {
					return nil, nil, err
				}
				for _, memberName := range memberNames {
					outCols = append(outCols, columnAccessor{pref, memberName})
				}
			} else {
				// Generate explicit columns.
				tm, err := info.OutputMember(t.memberName)
				if err != nil {
					return nil, nil, err
				}
				outputs = append(outputs, tm)
				outCols = append(outCols, columnAccessor{pref, t.memberName})
			}
		}
		return outCols, outputs, nil
	} else if numColumns > 1 && starColumns > 0 {
		return nil, nil, fmt.Errorf("invalid asterisk in columns")
	}

	// Case 2: Explicit columns, single asterisk type e.g. "(col1, t.col2) AS &P.*".
	if starTypes == 1 && numTypes == 1 {
		info, err := argInfos.GetArgWithMembers(e.targetTypes[0].typeName)
		if err != nil {
			return nil, nil, err
		}
		for _, c := range e.sourceColumns {
			tm, err := info.OutputMember(c.columnName)
			if err != nil {
				return nil, nil, err
			}
			outputs = append(outputs, tm)
			outCols = append(outCols, c)
		}
		return outCols, outputs, nil
	} else if starTypes > 0 && numTypes > 1 {
		return nil, nil, fmt.Errorf("invalid asterisk in types")
	}

	// Case 3: Explicit columns and types e.g. "(col1, col2) AS (&P.name, &P.id)".
	if numColumns == numTypes {
		for i, c := range e.sourceColumns {
			t := e.targetTypes[i]
			info, err := argInfos.GetArgWithMembers(t.typeName)
			if err != nil {
				return nil, nil, err
			}
			tm, err := info.OutputMember(t.memberName)
			if err != nil {
				return nil, nil, err
			}
			outputs = append(outputs, tm)
			outCols = append(outCols, c)
		}
	} else {
		return nil, nil, fmt.Errorf("mismatched number of columns and target types")
	}

	return outCols, outputs, nil
}

/*
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
*/

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
