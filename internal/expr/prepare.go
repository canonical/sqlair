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
	info, ok := lookupType(ti, p.sourceType.prefix)
	if !ok {
		ts := getKeys(ti)
		if len(ts) == 0 {
			return nil, fmt.Errorf(`type %q not passed as a parameter`, p.sourceType.prefix)
		} else {
			return nil, fmt.Errorf(`type %q not passed as a parameter, have: %s`, p.sourceType.prefix, strings.Join(ts, ", "))
		}
	}
	switch info := info.(type) {
	case *simpleTypeInfo:
		if p.sourceType.name != "" {
			return nil, fmt.Errorf(`cannot specify member of primative type %q`, info.typ().Name())
		}
		return simpleType{simpleType: info.typ()}, nil
	case *mapInfo:
		if p.sourceType.name == "" {
			return nil, fmt.Errorf(`type %q missing map key`, info.typ().Name())
		}
		return mapKey{name: p.sourceType.name, mapType: info.typ()}, nil
	case *structInfo:
		if p.sourceType.name == "" {
			return nil, fmt.Errorf(`type %q missing struct db tag`, info.typ().Name())
		}
		f, ok := info.tagToField[p.sourceType.name]
		if !ok {
			return nil, fmt.Errorf(`type %q has no %q db tag`, info.typ().Name(), p.sourceType.name)
		}
		return f, nil
	default:
		return nil, fmt.Errorf(`internal error: unknown info type: %T`, info)
	}
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
		info, ok := lookupType(ti, typeName)
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

	addColumns := func(info typeInfo, member string, column fullName) error {
		var tm typeMember
		switch info := info.(type) {
		case *simpleTypeInfo:
			if member != "" {
				return fmt.Errorf(`cannot specify member of primative type %q`, info.typ().Name())
			}
			tm = simpleType{simpleType: info.typ()}
		case *structInfo:
			if member == "" {
				return fmt.Errorf(`type %q missing struct db tag`, info.typ().Name())
			}
			tm, ok = info.tagToField[member]
			if !ok {
				return fmt.Errorf(`type %q has no %q db tag`, info.typ().Name(), member)
			}
		case *mapInfo:
			if member == "" {
				return fmt.Errorf(`type %q missing map key`, info.typ())
			}
			tm = mapKey{name: member, mapType: info.typ()}
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
			if _, ok := info.(*simpleTypeInfo); ok {
				return nil, nil, fmt.Errorf(`explicit columns required for primative type e.g. "col AS &%s"`, info.typ().Name())
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
				default:
					return nil, nil, fmt.Errorf(`internal error: unexpected type: %T`, info)
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
		if _, ok := info.(*simpleTypeInfo); ok {
			return nil, nil, fmt.Errorf(`cannot use asterisk with primative type %s`, info.typ().Name())
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
			return nil, fmt.Errorf("need struct, map, or primative type, got nil")
		}
		t := reflect.TypeOf(arg)
		switch t.Kind() {
		case reflect.Struct, reflect.Map, reflect.String, reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
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
			return nil, fmt.Errorf("need struct, map, or primative type, got pointer to %s", t.Elem().Kind())
		default:
			return nil, fmt.Errorf("need struct, map, or primative type, got %s", t.Kind())
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
		case *outputPart:
			outCols, typeMembers, err := prepareOutput(ti, p)
			if err != nil {
				return nil, err
			}
			//fmt.Printf("typeMembers: %#v\npart: %#v\n", typeMembers, part)

			for _, tm := range typeMembers {
				if ok := typeMemberPresent[tm]; ok {
					switch tm.(type) {
					case structField, mapKey:
						return nil, fmt.Errorf("member %q of type %q appears more than once", tm.memberName(), tm.outerType().Name())
					case simpleType:
						return nil, fmt.Errorf("type %q appears more than once", tm.outerType().Name())
					default:
						return nil, fmt.Errorf(`internal error: unknown type: %T`, tm)
					}
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
