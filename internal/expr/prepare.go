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
	outputs    []typeMember
	inputs     []typeMember
	queryParts []queryPart
}

func (pe *PreparedExpr) SQL() string {
	return generateSQL(pe.queryParts, nil)
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
func starCountColumns(columns []columnName) int {
	s := 0
	for _, column := range columns {
		if column.name == "*" {
			s++
		}
	}
	return s
}

// starCountTypes counts the number of asterisks in a list of types.
func starCountTypes(types []typeName) int {
	s := 0
	for _, t := range types {
		if t.member == "*" {
			s++
		}
	}
	return s
}

// prepareInput checks that the input expression corresponds to a known type.
func prepareInput(ti typeNameToInfo, p *inputPart) (tm typeMember, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("input expression: %s: %s", err, p.raw)
		}
	}()
	info, ok := ti[p.sourceType.name]
	if !ok {
		ts := getKeys(ti)
		if len(ts) == 0 {
			return nil, fmt.Errorf(`type %q not passed as a parameter`, p.sourceType.name)
		} else {
			// "%s" is used instead of %q to correctly print double quotes within the joined string.
			return nil, fmt.Errorf(`type %q not passed as a parameter (have "%s")`, p.sourceType.name, strings.Join(ts, `", "`))
		}
	}
	if p.sourceType.member == "*" {
		switch info := info.(type) {
		case *structInfo, *mapInfo:
			return nil, fmt.Errorf(`cannot use %s %q with asterisk in input expression`, info.typ().Kind(), p.sourceType.name)
		case *sliceInfo:
			tms, err := info.getAllMembers()
			if err != nil {
				return nil, err
			}
			p.isSlice = true
			tm = tms[0]
		default:
			return nil, fmt.Errorf(`internal error: unknown type: %T`, info)
		}
	} else {
		tm, err = info.typeMember(p.sourceType.member)
		if err != nil {
			return nil, err
		}
	}
	return tm, nil
}

// prepareOutput checks that the output expressions correspond to known types.
// It then checks they are formatted correctly and finally generates the columns for the query.
func prepareOutput(ti typeNameToInfo, p *outputPart) (typeMembers []typeMember, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("output expression: %s: %s", err, p.raw)
		}
	}()

	numTypes := len(p.targetTypes)
	numColumns := len(p.sourceColumns)
	starTypes := starCountTypes(p.targetTypes)
	starColumns := starCountColumns(p.sourceColumns)

	// Check target struct type and its tags are valid.
	var info typeInfo

	fetchInfo := func(typeName string) (typeInfo, error) {
		info, ok := ti[typeName]
		if !ok {
			ts := getKeys(ti)
			if len(ts) == 0 {
				return nil, fmt.Errorf(`type %q not passed as a parameter`, typeName)
			} else {
				// "%s" is used instead of %q to correctly print double quotes within the joined string.
				return nil, fmt.Errorf(`type %q not passed as a parameter (have "%s")`, typeName, strings.Join(ts, `", "`))
			}
		}
		if _, ok = info.(*sliceInfo); ok {
			return nil, fmt.Errorf(`cannot use slice type %q in output expression`, info.typ().Name())
		}
		return info, nil
	}

	// Case 1: Generated columns e.g. "* AS (&P.*, &A.id)" or "&P.*".
	if numColumns == 0 || (numColumns == 1 && starColumns == 1) {
		pref := ""
		// Prepend table name. E.g. "t" in "t.* AS &P.*".
		if numColumns > 0 {
			pref = p.sourceColumns[0].table
		}

		for _, t := range p.targetTypes {
			if info, err = fetchInfo(t.name); err != nil {
				return nil, err
			}
			if t.member == "*" {
				// Generate asterisk columns.
				allMembers, err := info.getAllMembers()
				if err != nil {
					return nil, err
				}
				typeMembers = append(typeMembers, allMembers...)
				for _, tm := range allMembers {
					p.sqlColumns = append(p.sqlColumns, columnName{pref, tm.memberName()})
				}
			} else {
				// Generate explicit columns.
				tm, err := info.typeMember(t.member)
				if err != nil {
					return nil, err
				}
				typeMembers = append(typeMembers, tm)
				p.sqlColumns = append(p.sqlColumns, columnName{pref, t.member})
			}
		}
		return typeMembers, nil
	} else if numColumns > 1 && starColumns > 0 {
		return nil, fmt.Errorf("invalid asterisk in columns")
	}

	// Case 2: Explicit columns, single asterisk type e.g. "(col1, t.col2) AS &P.*".
	if starTypes == 1 && numTypes == 1 {
		if info, err = fetchInfo(p.targetTypes[0].name); err != nil {
			return nil, err
		}
		for _, c := range p.sourceColumns {
			tm, err := info.typeMember(c.name)
			if err != nil {
				return nil, err
			}
			typeMembers = append(typeMembers, tm)
			p.sqlColumns = append(p.sqlColumns, c)
		}
		return typeMembers, nil
	} else if starTypes > 0 && numTypes > 1 {
		return nil, fmt.Errorf("invalid asterisk in types")
	}

	// Case 3: Explicit columns and types e.g. "(col1, col2) AS (&P.name, &P.id)".
	if numColumns == numTypes {
		for i, c := range p.sourceColumns {
			t := p.targetTypes[i]
			if info, err = fetchInfo(t.name); err != nil {
				return nil, err
			}
			tm, err := info.typeMember(t.member)
			if err != nil {
				return nil, err
			}
			typeMembers = append(typeMembers, tm)
			p.sqlColumns = append(p.sqlColumns, c)
		}
	} else {
		return nil, fmt.Errorf("mismatched number of columns and target types")
	}

	return typeMembers, nil
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
			return nil, fmt.Errorf("need valid value, got nil")
		}
		t := reflect.TypeOf(arg)
		switch t.Kind() {
		case reflect.Struct, reflect.Map, reflect.Slice, reflect.Array:
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
			return nil, fmt.Errorf("unsupported type: pointer to %s", t.Elem().Kind())
		default:
			return nil, fmt.Errorf("unsupported type: %s", t.Kind())
		}
	}

	var outputs = make([]typeMember, 0)
	var inputs = make([]typeMember, 0)
	var typeMemberPresent = make(map[typeMember]bool)

	// Check and expand each query part.
	for _, part := range pe.queryParts {
		switch p := part.(type) {
		case *inputPart:
			tm, err := prepareInput(ti, p)
			if err != nil {
				return nil, err
			}
			inputs = append(inputs, tm)
		case *outputPart:
			typeMembers, err := prepareOutput(ti, p)
			if err != nil {
				return nil, err
			}

			for _, tm := range typeMembers {
				if ok := typeMemberPresent[tm]; ok {
					return nil, fmt.Errorf("%q appears more than once in output expressions", tm.string())
				}
				typeMemberPresent[tm] = true
			}
			outputs = append(outputs, typeMembers...)
		case *bypassPart:
		default:
			return nil, fmt.Errorf("internal error: unknown query part type %T", part)
		}
	}
	return &PreparedExpr{inputs: inputs, outputs: outputs, queryParts: pe.queryParts}, nil
}

func generateSQL(queryParts []queryPart, sliceLens []int) string {
	var sql bytes.Buffer
	var inCount int
	var outCount int
	var sliceCount int

	for _, part := range queryParts {
		switch p := part.(type) {
		case *inputPart:
			if p.isSlice {
				length := maxSliceLen
				if sliceLens != nil && sliceLens[sliceCount] > maxSliceLen {
					length = sliceLens[sliceCount]
				}
				for i := 0; i < length; i++ {
					sql.WriteString("@sqlair_")
					sql.WriteString(strconv.Itoa(inCount))
					if i < length-1 {
						sql.WriteString(", ")
					}
					inCount++
					sliceCount++
				}
			} else {
				sql.WriteString("@sqlair_")
				sql.WriteString(strconv.Itoa(inCount))
				inCount++
			}
		case *outputPart:
			for i, c := range p.sqlColumns {
				sql.WriteString(c.String())
				sql.WriteString(" AS ")
				sql.WriteString(markerName(outCount))
				if i != len(p.sqlColumns)-1 {
					sql.WriteString(", ")
				}
				outCount++
			}
		case *bypassPart:
			sql.WriteString(p.chunk)
		default:
			panic(fmt.Sprintf("internal error: unknown query part type %T", part))
		}
	}
	return sql.String()
}
