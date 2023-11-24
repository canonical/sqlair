package expr

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/canonical/sqlair/internal/typeinfo"
)

// ParsedExprs is the AST representation of SQLair query. It contains only
// information encoded in the SQLair query string.
type ParsedExprs []expression

// String returns a textual representation of the AST contained in the
// ParsedExprs for debugging and testing purposes.
func (pe *ParsedExprs) String() string {
	var out bytes.Buffer
	out.WriteString("[")
	for i, p := range *pe {
		if i > 0 {
			out.WriteString(" ")
		}
		out.WriteString(p.String())
	}
	out.WriteString("]")
	return out.String()
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

	// Bind types to each expression.
	typedExprs := []typedExpression{}
	outputMemberUsed := map[typeinfo.Member]bool{}
	for _, expr := range *pe {
		te, err := expr.bindTypes(ti)
		if err != nil {
			return nil, err
		}

		if toe, ok := te.(*typedOutputExpr); ok {
			err = toe.checkUsed(outputMemberUsed)
			if err != nil {
				return nil, err
			}
		}

		typedExprs = append(typedExprs, te)
	}

	typedExpr := TypedExprs(typedExprs)
	return &typedExpr, nil
}

// expression represents a parsed node of the SQLair query's AST.
type expression interface {
	// String returns a text representation for debugging and testing purposes.
	String() string

	// bindTypes generates a typed expression from the query type information.
	bindTypes(typeNameToInfo) (typedExpression, error)
}

// bypass represents part of the expression that we want to pass to the backend
// database verbatim.
type bypass struct {
	chunk string
}

func (e *bypass) String() string {
	return "Bypass[" + e.chunk + "]"
}

// bindTypes is part of the expression interface. bypass expressions have no
// types so the same expression is returned.
func (e *bypass) bindTypes(typeNameToInfo) (typedExpression, error) {
	return e, nil
}

// inputExpr represents a named parameter that will be sent to the database
// while performing the query.
type inputExpr struct {
	sourceType valueAccessor
	raw        string
}

func (e *inputExpr) String() string {
	return fmt.Sprintf("Input[%+v]", e.sourceType)
}

// bindTypes binds the input expression to a query type and
// returns a typed input expression.
func (e *inputExpr) bindTypes(ti typeNameToInfo) (te typedExpression, err error) {
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

// outputExpr represents a named target output variable in the SQL expression,
// as well as the source table and column where it will be read from.
type outputExpr struct {
	sourceColumns []columnAccessor
	targetTypes   []valueAccessor
	raw           string
}

func (e *outputExpr) String() string {
	return fmt.Sprintf("Output[%+v %+v]", e.sourceColumns, e.targetTypes)
}

// bindTypes binds the output expression to concrete types. It then checks the
// expression is formatted correctly and generates a typed output expression.
func (e *outputExpr) bindTypes(ti typeNameToInfo) (te typedExpression, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("output expression: %s: %s", err, e.raw)
		}
	}()

	numTypes := len(e.targetTypes)
	numColumns := len(e.sourceColumns)
	starTypes := starCountTypes(e.targetTypes)
	starColumns := starCountColumns(e.sourceColumns)

	toe := &typedOutputExpr{}

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
