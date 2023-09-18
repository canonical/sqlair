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

func starCount[T any](ts []T) int {
	s := 0
	for _, t := range ts {
		if fn, ok := any(t).(fullName); ok {
			if fn.name == "*" {
				s++
			}
		}
	}
	return s
}

// prepareInput checks that the input expression corresponds to a known type.
func (pe *PreparedExpr) prepareInput(ti typeNameToInfo, p *inputPart) error {
	info, ok := ti[p.sourceType.prefix]
	if !ok {
		ts := getKeys(ti)
		if len(ts) == 0 {
			return fmt.Errorf(`type %q not passed as a parameter`, p.sourceType.prefix)
		} else {
			return fmt.Errorf(`type %q not passed as a parameter, have: %s`, p.sourceType.prefix, strings.Join(ts, ", "))
		}
	}
	tm, err := info.typeMember(p.sourceType.name)
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
		if err == nil && len(pe.sql) > 1 {
			pe.sql = pe.sql[:len(pe.sql)-2]
		}
	}()
	numTypes := len(p.targetTypes)
	numColumns := len(p.sourceColumns)
	starTypes := starCount(p.targetTypes)
	starColumns := starCount(p.sourceColumns)

	// Check target struct type and its tags are valid.
	var info typeInfo

	fetchInfo := func(typeName string) (typeInfo, error) {
		info, ok := ti[typeName]
		if !ok {
			ts := getKeys(ti)
			if len(ts) == 0 {
				return nil, fmt.Errorf(`type %q not passed as a parameter`, typeName)
			} else {
				return nil, fmt.Errorf(`type %q not passed as a parameter, have: %s`, typeName, strings.Join(ts, ", "))
			}
		}
		return info, nil
	}

	addColumn := func(column string, tm typeMember) error {
		for _, output := range pe.outputs {
			if tm == output {
				return fmt.Errorf("member %q of type %q appears more than once", tm.memberName(), tm.outerType().Name())
			}
		}
		pe.sql += column + " AS " + markerName(len(pe.outputs)) + ", "
		pe.outputs = append(pe.outputs, tm)
		return nil
	}

	// Case 1: Generated columns e.g. "* AS (&P.*, &A.id)" or "&P.*".
	if numColumns == 0 || (numColumns == 1 && starColumns == 1) {
		pref := ""
		// Prepend table name. E.g. "t" in "t.* AS &P.*".
		if numColumns > 0 {
			starCol, ok := p.sourceColumns[0].(fullName)
			if !ok {
				return fmt.Errorf("internal error: expected starCol to be of type fullName, got %T", starCol)
			}
			pref = starCol.prefix
		}

		for _, t := range p.targetTypes {
			if info, err = fetchInfo(t.prefix); err != nil {
				return err
			}
			if t.name == "*" {
				// Generate asterisk columns.
				allMembers, err := info.getAllMembers()
				if err != nil {
					return err
				}
				for _, tm := range allMembers {
					err := addColumn(fullName{pref, tm.memberName()}.String(), tm)
					if err != nil {
						return err
					}
				}
			} else {
				// Generate explicit columns.
				tm, err := info.typeMember(t.name)
				if err == nil {
					err = addColumn(fullName{prefix: pref, name: t.name}.String(), tm)
				}
				if err != nil {
					return err
				}
			}
		}
		return nil
	} else if numColumns > 1 && starColumns > 0 {
		return fmt.Errorf("invalid asterisk in output expression columns: %s", p.raw)
	}

	// Case 2: Explicit columns, single asterisk type e.g. "(col1, t.col2) AS &P.*".
	if starTypes == 1 && numTypes == 1 {
		if info, err = fetchInfo(p.targetTypes[0].prefix); err != nil {
			return err
		}
		for _, c := range p.sourceColumns {
			switch c := c.(type) {
			case funcExpr:
				return fmt.Errorf(`cannot use function %q with asterisk output expression: %q`, c.raw, p.raw)
			case fullName:
				tm, err := info.typeMember(c.name)
				if err == nil {
					err = addColumn(c.String(), tm)
				}
				if err != nil {
					return err
				}
			}
		}
		return nil
	} else if starTypes > 0 && numTypes > 1 {
		return fmt.Errorf("invalid asterisk in output expression types: %s", p.raw)
	}

	// Case 3: Explicit columns and types e.g. "(col1, col2) AS (&P.name, &P.id)".
	if numColumns == numTypes {
		for i, c := range p.sourceColumns {
			t := p.targetTypes[i]
			if info, err = fetchInfo(t.prefix); err != nil {
				return err
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
			case fullName:
				column = c.String()
			}
			tm, err := info.typeMember(t.name)
			if err == nil {
				err = addColumn(column, tm)
			}
			if err != nil {
				return err
			}
		}
	} else {
		return fmt.Errorf("mismatched number of columns and targets in output expression: %s", p.raw)
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
			err = fmt.Errorf("cannot prepare expression: %s", err)
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
	return nil
}
