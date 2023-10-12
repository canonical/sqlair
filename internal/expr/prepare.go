package expr

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// PreparedExpr represents a valid SQLair statement.
type PreparedExpr struct {
	preparedParts []preparedPart
}

// preparedPart represents a part of a SQLair statement. It contains
// information to generate the SQL for the part and to access Go types
// referenced in the part.
type preparedPart interface {
	preparedPart()
}

// preparedOutput contains the columns to fetch from the database and
// information about the Go values to read the results into.
type preparedOutput struct {
	columns []columnAccessor
	outputs []typeMember
}

// preparedPart is a marker method.
func (*preparedOutput) preparedPart() {}

// preparedInput stores information about a Go value to use as a query input.
type preparedInput struct {
	input typeMember
}

// preparedPart is a marker method.
func (*preparedInput) preparedPart() {}

type preparedBypass struct {
	chunk string
}

// preparedPart is a marker method.
func (*preparedBypass) preparedPart() {}

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

// prepareInput checks that the input expression corresponds to a known type.
func prepareInput(ti typeNameToInfo, p *inputPart) (pi *preparedInput, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("input expression: %s: %s", err, p.raw)
		}
	}()

	info, ok := ti[p.sourceType.typeName]
	if !ok {
		return nil, typeMissingError(p.sourceType.typeName, getKeys(ti))
	}

	tm, err := info.typeMember(p.sourceType.memberName)
	if err != nil {
		return nil, err
	}
	return &preparedInput{tm}, nil
}

// prepareOutput checks that the output expressions correspond to known types.
// It then checks they are formatted correctly and finally generates the columns for the query.
func prepareOutput(ti typeNameToInfo, p *outputPart) (po *preparedOutput, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("output expression: %s: %s", err, p.raw)
		}
	}()

	numTypes := len(p.targetTypes)
	numColumns := len(p.sourceColumns)
	starTypes := starCountTypes(p.targetTypes)
	starColumns := starCountColumns(p.sourceColumns)

	po = &preparedOutput{}

	// Case 1: Generated columns e.g. "* AS (&P.*, &A.id)" or "&P.*".
	if numColumns == 0 || (numColumns == 1 && starColumns == 1) {
		pref := ""
		// Prepend table name. E.g. "t" in "t.* AS &P.*".
		if numColumns > 0 {
			pref = p.sourceColumns[0].tableName
		}

		for _, t := range p.targetTypes {
			info, ok := ti[t.typeName]
			if !ok {
				return nil, typeMissingError(t.typeName, getKeys(ti))
			}
			if t.memberName == "*" {
				// Generate asterisk columns.
				allMembers, err := info.getAllMembers()
				if err != nil {
					return nil, err
				}
				po.outputs = append(po.outputs, allMembers...)
				for _, tm := range allMembers {
					po.columns = append(po.columns, columnAccessor{pref, tm.memberName()})
				}
			} else {
				// Generate explicit columns.
				tm, err := info.typeMember(t.memberName)
				if err != nil {
					return nil, err
				}
				po.outputs = append(po.outputs, tm)
				po.columns = append(po.columns, columnAccessor{pref, t.memberName})
			}
		}
		return po, nil
	} else if numColumns > 1 && starColumns > 0 {
		return nil, fmt.Errorf("invalid asterisk in columns")
	}

	// Case 2: Explicit columns, single asterisk type e.g. "(col1, t.col2) AS &P.*".
	if starTypes == 1 && numTypes == 1 {
		info, ok := ti[p.targetTypes[0].typeName]
		if !ok {
			return nil, typeMissingError(p.targetTypes[0].typeName, getKeys(ti))
		}
		for _, c := range p.sourceColumns {
			tm, err := info.typeMember(c.columnName)
			if err != nil {
				return nil, err
			}
			po.outputs = append(po.outputs, tm)
			po.columns = append(po.columns, c)
		}
		return po, nil
	} else if starTypes > 0 && numTypes > 1 {
		return nil, fmt.Errorf("invalid asterisk in types")
	}

	// Case 3: Explicit columns and types e.g. "(col1, col2) AS (&P.name, &P.id)".
	if numColumns == numTypes {
		for i, c := range p.sourceColumns {
			t := p.targetTypes[i]
			info, ok := ti[t.typeName]
			if !ok {
				return nil, typeMissingError(t.typeName, getKeys(ti))
			}
			tm, err := info.typeMember(t.memberName)
			if err != nil {
				return nil, err
			}
			po.outputs = append(po.outputs, tm)
			po.columns = append(po.columns, c)
		}
	} else {
		return nil, fmt.Errorf("mismatched number of columns and target types")
	}

	return po, nil
}

type typeNameToInfo map[string]typeInfo

// Prepare takes a parsed expression and struct instantiations of all the types
// mentioned in it.
// The IO parts of the statement are checked for validity against the types
// and expanded if necessary.
func (pe *ParsedExpr) Prepare(args ...any) (expr *PreparedExpr, err error) {
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

	typeMemberPresent := make(map[typeMember]bool)
	preparedParts := make([]preparedPart, 0)

	// Check and expand each query part.
	for _, part := range pe.queryParts {
		switch p := part.(type) {
		case *inputPart:
			pi, err := prepareInput(ti, p)
			if err != nil {
				return nil, err
			}
			preparedParts = append(preparedParts, pi)
		case *outputPart:
			po, err := prepareOutput(ti, p)
			if err != nil {
				return nil, err
			}

			for _, tm := range po.outputs {
				if ok := typeMemberPresent[tm]; ok {
					return nil, fmt.Errorf("member %q of type %q appears more than once in output expressions", tm.memberName(), tm.outerType().Name())
				}
				typeMemberPresent[tm] = true
			}
			preparedParts = append(preparedParts, po)
		case *bypassPart:
			preparedParts = append(preparedParts, &preparedBypass{p.chunk})
		default:
			return nil, fmt.Errorf("internal error: unknown query part type %T", part)
		}
	}
	return &PreparedExpr{preparedParts: preparedParts}, nil
}
