package expr

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

// PreparedExpr contains an SQL expression that is ready for execution.
type PreparedExpr struct {
	outputs []typeMember
	inputs  []typeMember
	sql     string
}

// SQL returns the SQL ready for execution.
func (pe *PreparedExpr) SQL() string {
	return pe.sql
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
func starCountColumns(cs []columnExpr) int {
	s := 0
	for _, c := range cs {
		if ca, ok := c.(columnAccessor); ok {
			if ca.columnName == "*" {
				s++
			}
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
func (pe *PreparedExpr) prepareInput(ti typeNameToInfo, p *inputPart) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("input expression: %s: %s", err, p.raw)
		}
	}()

	info, ok := ti[p.sourceType.typeName]
	if !ok {
		return typeMissingError(p.sourceType.typeName, getKeys(ti))
	}

	tm, err := info.typeMember(p.sourceType.memberName)
	if err != nil {
		return err
	}
	pe.sql += "@sqlair_" + strconv.Itoa(len(pe.inputs))
	pe.inputs = append(pe.inputs, tm)
	return nil
}

// prepareOutput checks that the output expressions correspond to known types.
// It then checks they are formatted correctly and finally generates the columns for the query.
func (pe *PreparedExpr) prepareOutput(ti typeNameToInfo, p *outputPart) (err error) {
	defer func() {
		// Remove trailing comma and space from list of columns.
		if err == nil && len(pe.sql) > 1 {
			pe.sql = pe.sql[:len(pe.sql)-2]
		}

		if err != nil {
			err = fmt.Errorf("output expression: %s: %s", err, p.raw)
		}
	}()

	numTypes := len(p.targetTypes)
	numColumns := len(p.sourceColumns)
	starTypes := starCountTypes(p.targetTypes)
	starColumns := starCountColumns(p.sourceColumns)

	// Case 1: Generated columns e.g. "* AS (&P.*, &A.id)" or "&P.*".
	if numColumns == 0 || (numColumns == 1 && starColumns == 1) {
		pref := ""
		// Prepend table name. E.g. "t" in "t.* AS &P.*".
		if numColumns > 0 {
			starCol, ok := p.sourceColumns[0].(columnAccessor)
			if !ok {
				return fmt.Errorf("internal error: expected starCol to be of type columnAccessor, got %T", starCol)
			}
			pref = starCol.tableName
		}

		for _, t := range p.targetTypes {
			info, ok := ti[t.typeName]
			if !ok {
				return typeMissingError(t.typeName, getKeys(ti))
			}
			if t.memberName == "*" {
				// Generate asterisk columns.
				allMembers, err := info.getAllMembers()
				if err != nil {
					return err
				}
				for _, tm := range allMembers {
					pe.sql += columnAccessor{pref, tm.memberName()}.String() + " AS " + markerName(len(pe.outputs)) + ", "
					pe.outputs = append(pe.outputs, tm)
				}
			} else {
				// Generate explicit columns.
				tm, err := info.typeMember(t.memberName)
				if err != nil {
					return err
				}
				pe.sql += columnAccessor{pref, t.memberName}.String() + " AS " + markerName(len(pe.outputs)) + ", "
				pe.outputs = append(pe.outputs, tm)
			}
		}
		return nil
	} else if numColumns > 1 && starColumns > 0 {
		return fmt.Errorf("invalid asterisk in columns")
	}

	// Case 2: Explicit columns, single asterisk type e.g. "(col1, t.col2) AS &P.*".
	if starTypes == 1 && numTypes == 1 {
		info, ok := ti[p.targetTypes[0].typeName]
		if !ok {
			return typeMissingError(p.targetTypes[0].typeName, getKeys(ti))
		}
		for _, c := range p.sourceColumns {
			switch c := c.(type) {
			case funcExpr:
				return fmt.Errorf(`cannot use function with star type`)
			case columnAccessor:
				tm, err := info.typeMember(c.columnName)
				if err != nil {
					return err
				}
				pe.sql += c.String() + " AS " + markerName(len(pe.outputs)) + ", "
				pe.outputs = append(pe.outputs, tm)
			}
		}
		return nil
	} else if starTypes > 0 && numTypes > 1 {
		return fmt.Errorf("invalid asterisk in types")
	}

	// Case 3: Explicit columns and types e.g. "(col1, col2) AS (&P.name, &P.id)".
	if numColumns == numTypes {
		for i, c := range p.sourceColumns {
			t := p.targetTypes[i]
			info, ok := ti[t.typeName]
			if !ok {
				return typeMissingError(t.typeName, getKeys(ti))
			}

			var column string
			switch c := c.(type) {
			case funcExpr:
				if err = pe.prepareParts(ti, c.pe.queryParts); err != nil {
					return err
				}
				// We leave the column black since it has already been written
				// by prepareParts.
				column = ""
			case columnAccessor:
				column = c.String()
			}
			tm, err := info.typeMember(t.memberName)
			if err != nil {
				return err
			}
			pe.sql += column + " AS " + markerName(len(pe.outputs)) + ", "
			pe.outputs = append(pe.outputs, tm)
		}
	} else {
		return fmt.Errorf("mismatched number of columns and target types")
	}

	return nil
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
	var preparedExpr = &PreparedExpr{}
	err = preparedExpr.prepareParts(ti, pe.queryParts)
	return preparedExpr, err
}

func (pe *PreparedExpr) prepareParts(ti typeNameToInfo, qp []queryPart) error {
	// Check expression, and generate SQL for each query part.
	for _, part := range qp {
		switch p := part.(type) {
		case *inputPart:
			err := pe.prepareInput(ti, p)
			if err != nil {
				return err
			}
		case *outputPart:
			err := pe.prepareOutput(ti, p)
			if err != nil {
				return err
			}
		case *bypassPart:
			pe.sql += p.chunk
		default:
			return fmt.Errorf("internal error: unknown query part type %T", part)
		}
	}

	var typeMemberPresent = make(map[typeMember]bool)
	for _, tm := range pe.outputs {
		if _, ok := typeMemberPresent[tm]; ok {
			return fmt.Errorf(
				"member %q of type %q appears more than once in output expressions",
				tm.memberName(), tm.outerType().Name(),
			)
		}
		typeMemberPresent[tm] = true
	}

	return nil
}
