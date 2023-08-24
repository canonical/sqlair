package expr

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

// maxSliceLen is the maximum size of an argument slice allowed in an IN statement
const maxSliceLen = 8

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

func starCount(fns []fullName) int {
	s := 0
	for _, fn := range fns {
		if fn.name == "*" {
			s++
		}
	}
	return s
}

// prepareInput checks that the input expression corresponds to a known type.
func prepareInput(ti typeNameToInfo, p *inputPart) (typeMember, error) {
	info, ok := ti[p.sourceType.prefix]
	if !ok {
		ts := getKeys(ti)
		if len(ts) == 0 {
			return nil, fmt.Errorf(`type %q not passed as a parameter`, p.sourceType.prefix)
		} else {
			return nil, fmt.Errorf(`type %q not passed as a parameter, have: %s`, p.sourceType.prefix, strings.Join(ts, ", "))
		}
	}
	switch info := info.(type) {
	case *mapInfo:
		return &mapKey{name: p.sourceType.name, mapType: info.typ()}, nil
	case *structInfo:
		f, ok := info.tagToField[p.sourceType.name]
		if !ok {
			return nil, fmt.Errorf(`type %q has no %q db tag`, info.typ().Name(), p.sourceType.name)
		}
		return f, nil
	default:
		return nil, fmt.Errorf(`internal error: unknown info type: %T`, info)
	}
}

// prepareIn check input expressions following IN statements correspond to known
// types and generates information about the query arguments.
func prepareIn(ti typeNameToInfo, p *inPart) ([]typeMember, error) {
	var typeMembers = make([]typeMember, 0)
	for _, t := range p.types {
		info, ok := ti[t.prefix]
		if !ok {
			ts := getKeys(ti)
			if len(ts) == 0 {
				return nil, fmt.Errorf(`type %q not passed as a parameter`, t.prefix)
			} else {
				return nil, fmt.Errorf(`type %q not passed as a parameter, have: %s`, t.prefix, strings.Join(ts, ", "))
			}
		}
		if t.name == "*" {
			return nil, fmt.Errorf("invalid asterisk in input expression: %s", p.raw)
		}
		switch info := info.(type) {
		case *structInfo:
			field, ok := info.tagToField[t.name]
			if !ok {
				return nil, fmt.Errorf(`type %q has no %q db tag`, info.typ().Name(), t.name)
			}
			typeMembers = append(typeMembers, field)
		case *mapInfo:
			typeMembers = append(typeMembers, &mapKey{
				name:        t.name,
				mapType:     info.typ(),
				listAllowed: true,
			})
		}
	}
	return typeMembers, nil
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
			if info, err = fetchInfo(t.prefix); err != nil {
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
		if info, err = fetchInfo(p.targetTypes[0].prefix); err != nil {
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
			if info, err = fetchInfo(t.prefix); err != nil {
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

	var typeMemberPresent = make(map[typeMember]bool)

	// Check and expand each query part.
	for _, part := range pe.queryParts {
		switch p := part.(type) {
		case *inputPart:
			inLoc, err := prepareInput(ti, p)
			if err != nil {
				return nil, err
			}
			sql.WriteString("@sqlair_" + strconv.Itoa(inCount))
			inCount++
			inputs = append(inputs, inLoc)
		case *inPart:
			typeMembers, err := prepareIn(ti, p)
			if err != nil {
				return nil, err
			}
			sql.WriteString("IN (")
			for i, tm := range typeMembers {
				switch tm := tm.(type) {
				case *structField:
					sql.WriteString("@sqlair_")
					sql.WriteString(strconv.Itoa(inCount))
					inCount++
				case *mapKey:
					length := 1
					if tm.listAllowed {
						length = maxSliceLen
					}
					for j := 0; j < length; j++ {
						sql.WriteString("@sqlair_")
						sql.WriteString(strconv.Itoa(inCount))
						if j < length-1 {
							sql.WriteString(", ")
						}
						inCount++
					}
				default:
					return nil, fmt.Errorf("internal error: invalid type: %T", tm)
				}
				if i < len(typeMembers)-1 {
					sql.WriteString(", ")
				}
			}
			sql.WriteString(")")
			inputs = append(inputs, typeMembers...)
		case *outputPart:
			outCols, typeMembers, err := prepareOutput(ti, p)
			if err != nil {
				return nil, err
			}

			for _, tm := range typeMembers {
				if ok := typeMemberPresent[tm]; ok {
					return nil, fmt.Errorf("member %q of type %q appears more than once", tm.memberName(), tm.outerType().Name())
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
		case *bypassPart:
			sql.WriteString(p.chunk)
		default:
			return nil, fmt.Errorf("internal error: unknown query part type %T", part)
		}
	}

	return &PreparedExpr{inputs: inputs, outputs: outputs, sql: sql.String()}, nil
}
