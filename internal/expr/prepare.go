package expr

import (
	"bytes"
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

func starCount(fns []fullName) int {
	s := 0
	for _, fn := range fns {
		if fn.name == "*" {
			s++
		}
	}
	return s
}

func findTypeInfo(ti typeNameToInfo, typeName string) (typeInfo, error) {
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

// prepareInput checks that the input expression is correctly formatted,
// corresponds to known types, and then generates input columns and values.
func prepareInput(ti typeNameToInfo, p *inputPart) (inCols []fullName, typeMembers []typeMember, err error) {
	addColumns := func(info typeInfo, tag string, column fullName) error {
		var tm typeMember
		var ok bool
		switch info := info.(type) {
		case *structInfo:
			tm, ok = info.tagToField[tag]
			if !ok {
				return fmt.Errorf(`type %q has no %q db tag`, info.typ().Name(), tag)
			}
		case *mapInfo:
			tm = &mapKey{name: tag, mapType: info.typ()}
		default:
			return fmt.Errorf(`internal error: unknown info type: %T`, info)
		}
		typeMembers = append(typeMembers, tm)
		inCols = append(inCols, column)
		return nil
	}

	numTypes := len(p.sourceTypes)
	numColumns := len(p.targetColumns)
	starTypes := starCount(p.sourceTypes)
	starColumns := starCount(p.targetColumns)

	// Generate columns to inject into SQL query.
	// Case 0: A simple standalone input expression e.g. "$P.name".
	if numColumns == 0 {
		if numTypes != 1 {
			return nil, nil, fmt.Errorf("internal error: cannot group standalone input expressions")
		}
		if starTypes > 0 {
			return nil, nil, fmt.Errorf("invalid asterisk in input expression: %s", p.raw)
		}
		info, err := findTypeInfo(ti, p.sourceTypes[0].prefix)
		if err != nil {
			return nil, nil, err
		}
		if err := addColumns(info, p.sourceTypes[0].name, fullName{}); err != nil {
			return nil, nil, err
		}
		return inCols, typeMembers, nil
	}

	// Case 1: Generate input columns e.g. "(*) VALUES ($P.*, $A.id)".
	if numColumns == 1 && starColumns == 1 {
		for _, t := range p.sourceTypes {
			info, err := findTypeInfo(ti, t.prefix)
			if err != nil {
				return nil, nil, err
			}

			if t.name == "*" {
				// Generate asterisk columns.
				switch info := info.(type) {
				case *mapInfo:
					return nil, nil, fmt.Errorf(`map type %q cannot be used with asterisk in input expression: %s`, info.typ().Name(), p.raw)
				case *structInfo:
					for _, tag := range info.tags {
						inCols = append(inCols, fullName{name: tag})
						typeMembers = append(typeMembers, info.tagToField[tag])
					}
				default:
					return nil, nil, fmt.Errorf(`internal error: unknown info type: %T`, info)
				}
			} else {
				// Generate explicit columns.
				if err = addColumns(info, t.name, fullName{name: t.name}); err != nil {
					return nil, nil, err
				}
			}
		}
		return inCols, typeMembers, nil
	} else if numColumns > 1 && starColumns > 0 {
		return nil, nil, fmt.Errorf("invalid asterisk in input expression: %s", p.raw)
	}

	// Case 2: Explicit columns, single asterisk type e.g. "(col1, col2) VALUES ($P.*)".
	if starTypes == 1 && numTypes == 1 {
		info, err := findTypeInfo(ti, p.sourceTypes[0].prefix)
		if err != nil {
			return nil, nil, err
		}
		for _, c := range p.targetColumns {
			if err = addColumns(info, c.name, c); err != nil {
				return nil, nil, err
			}
		}
		return inCols, typeMembers, nil
	} else if starTypes > 0 && numTypes > 1 {
		return nil, nil, fmt.Errorf("invalid asterisk in input expression types: %s", p.raw)
	}

	// Case 3: Explicit columns and types e.g. "(col1, col2) VALUES ($P.name, $A.id)".
	if numColumns == numTypes {
		for i, c := range p.targetColumns {
			t := p.sourceTypes[i]
			info, err := findTypeInfo(ti, t.prefix)
			if err != nil {
				return nil, nil, err
			}

			if err = addColumns(info, t.name, c); err != nil {
				return nil, nil, err
			}
		}
	} else {
		return nil, nil, fmt.Errorf("mismatched number of columns and targets in input expression: %s", p.raw)
	}

	return inCols, typeMembers, nil
}

// prepareOutput checks that the output expressions correspond to known types.
// It then checks they are formatted correctly and finally generates the columns for the query.
func prepareOutput(ti typeNameToInfo, p *outputPart) ([]fullName, []typeMember, error) {
	var outCols = make([]fullName, 0)
	var typeMembers = make([]typeMember, 0)

	numTypes := len(p.targetTypes)
	numColumns := len(p.sourceColumns)
	starTypes := starCount(p.targetTypes)
	starColumns := starCount(p.sourceColumns)

	// Check target struct type and its tags are valid.
	var info typeInfo
	var ok bool
	var err error

	addColumns := func(info typeInfo, tag string, column fullName) error {
		var tm typeMember
		switch info := info.(type) {
		case *structInfo:
			tm, ok = info.tagToField[tag]
			if !ok {
				return fmt.Errorf(`type %q has no %q db tag`, info.typ().Name(), tag)
			}
		case *mapInfo:
			tm = &mapKey{name: tag, mapType: info.typ()}
		}
		typeMembers = append(typeMembers, tm)
		outCols = append(outCols, column)
		return nil
	}

	// Case 1: Generated columns e.g. "* AS (&P.*, &A.id)" or "&P.*".
	if numColumns == 0 || (numColumns == 1 && starColumns == 1) {
		pref := ""
		// Prepend table name. E.g. "t" in "t.* AS &P.*".
		if numColumns > 0 {
			pref = p.sourceColumns[0].prefix
		}

		for _, t := range p.targetTypes {
			if info, err = findTypeInfo(ti, t.prefix); err != nil {
				return nil, nil, err
			}
			// Generate asterisk columns.
			if t.name == "*" {
				switch info := info.(type) {
				case *mapInfo:
					return nil, nil, fmt.Errorf(`&%s.* cannot be used for maps when no column names are specified`, info.typ().Name())
				case *structInfo:
					if len(info.tags) == 0 {
						return nil, nil, fmt.Errorf("type %q in %q does not have any db tags", info.typ().Name(), p.raw)
					}
					for _, tag := range info.tags {
						outCols = append(outCols, fullName{pref, tag})
						typeMembers = append(typeMembers, info.tagToField[tag])
					}
				}
			} else {
				// Generate explicit columns.
				if err = addColumns(info, t.name, fullName{pref, t.name}); err != nil {
					return nil, nil, err
				}
			}
		}
		return outCols, typeMembers, nil
	} else if numColumns > 1 && starColumns > 0 {
		return nil, nil, fmt.Errorf("invalid asterisk in output expression columns: %s", p.raw)
	}

	// Case 2: Explicit columns, single asterisk type e.g. "(col1, t.col2) AS &P.*".
	if starTypes == 1 && numTypes == 1 {
		if info, err = findTypeInfo(ti, p.targetTypes[0].prefix); err != nil {
			return nil, nil, err
		}
		for _, c := range p.sourceColumns {
			if err = addColumns(info, c.name, c); err != nil {
				return nil, nil, err
			}
		}
		return outCols, typeMembers, nil
	} else if starTypes > 0 && numTypes > 1 {
		return nil, nil, fmt.Errorf("invalid asterisk in output expression types: %s", p.raw)
	}

	// Case 3: Explicit columns and types e.g. "(col1, col2) AS (&P.name, &P.id)".
	if numColumns == numTypes {
		for i, c := range p.sourceColumns {
			t := p.targetTypes[i]
			if info, err = findTypeInfo(ti, t.prefix); err != nil {
				return nil, nil, err
			}

			if err = addColumns(info, t.name, c); err != nil {
				return nil, nil, err
			}
		}
	} else {
		return nil, nil, fmt.Errorf("mismatched number of columns and targets in output expression: %s", p.raw)
	}

	return outCols, typeMembers, nil
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

	var sql bytes.Buffer

	var inCount int
	var outCount int

	var outputs = make([]typeMember, 0)
	var inputs = make([]typeMember, 0)

	var typeMemberPresentInOuts = make(map[typeMember]bool)

	// Check and expand each query part.
	for _, part := range pe.queryParts {
		switch p := part.(type) {
		case *inputPart:
			columnInInput := make(map[fullName]bool)
			inCols, typeMembers, err := prepareInput(ti, p)
			if err != nil {
				return nil, err
			}

			for _, col := range inCols {
				if ok := columnInInput[col]; ok {
					return nil, fmt.Errorf("column %q is set more than once in input expression: %s", col.name, p.raw)
				}
				columnInInput[col] = true
			}

			if len(p.targetColumns) == 0 {
				sql.WriteString("@sqlair_" + strconv.Itoa(inCount))
				inCount += 1
			} else {
				sql.WriteString(formatColNames(inCols))
				sql.WriteString(" VALUES ")
				sql.WriteString(generateNamedParams(inCount, len(inCols)))
				inCount += len(inCols)
			}
			inputs = append(inputs, typeMembers...)
		case *outputPart:
			outCols, typeMembers, err := prepareOutput(ti, p)
			if err != nil {
				return nil, err
			}

			for _, tm := range typeMembers {
				if ok := typeMemberPresentInOuts[tm]; ok {
					return nil, fmt.Errorf("member %q of type %q appears more than once in outputs", tm.memberName(), tm.outerType().Name())
				}
				typeMemberPresentInOuts[tm] = true
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
		case *bypassPart:
			sql.WriteString(p.chunk)
		default:
			return nil, fmt.Errorf("internal error: unknown query part type %T", part)
		}
	}

	return &PreparedExpr{inputs: inputs, outputs: outputs, sql: sql.String()}, nil
}

// formatColNames prints a parenthesised, comma separated list of columns including
// the table name if present.
func formatColNames(cs []fullName) string {
	var s bytes.Buffer
	s.WriteString("(")
	for i, c := range cs {
		s.WriteString(c.String())
		if i < len(cs)-1 {
			s.WriteString(", ")
		}
	}
	s.WriteString(")")
	return s.String()
}

// generateNamedParams returns n incrementing named parameters beginning at
// n=start.
// For example:
// 	generateNamedParams(0, 2) == "(@sqlair_0, @sqlair_1)"
func generateNamedParams(start int, n int) string {
	var s bytes.Buffer
	s.WriteString("(")
	for i := start; i < start+n; i++ {
		s.WriteString("@sqlair_")
		s.WriteString(strconv.Itoa(i))
		if i < start+n-1 {
			s.WriteString(", ")
		}
	}
	s.WriteString(")")
	return s.String()
}
