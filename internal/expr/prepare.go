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

type typeNameToInfo map[string]typeInfo

func (ti typeNameToInfo) lookupInfo(typeName string) (typeInfo, error) {
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

func prepareColumnsAndTypes(ti typeNameToInfo, columns []fullName, types []fullName) ([]fullName, []typeMember, error) {
	numTypes := len(types)
	numColumns := len(columns)
	starTypes := starCount(types)
	starColumns := starCount(columns)

	typeMembers := []typeMember{}
	genCols := []fullName{}

	// Case 1: SQLair generated columns.
	// For example the expressions:
	//  "(*) VALUES ($P.*, $A.id)"
	//  "(*) AS (&P.*, &A.id)"
	//  "&P.*"
	if numColumns == 0 || (numColumns == 1 && starColumns == 1) {
		pref := ""
		// Prepend table name. E.g. "t" in "t.* AS &P.*".
		if numColumns > 0 {
			pref = columns[0].prefix
		}
		for _, t := range types {
			info, err := ti.lookupInfo(t.prefix)
			if err != nil {
				return nil, nil, err
			}

			if t.name == "*" {
				// Generate asterisk columns.
				allMembers, err := info.getAllMembers()
				if err != nil {
					return nil, nil, err
				}
				typeMembers = append(typeMembers, allMembers...)
				for _, tm := range allMembers {
					genCols = append(genCols, fullName{pref, tm.memberName()})
				}
			} else {
				// Generate explicit columns.
				tm, err := info.typeMember(t.name)
				if err != nil {
					return nil, nil, err
				}
				typeMembers = append(typeMembers, tm)
				genCols = append(genCols, fullName{pref, t.name})
			}
		}
		return genCols, typeMembers, nil
	} else if numColumns > 1 && starColumns > 0 {
		return nil, nil, fmt.Errorf("invalid asterisk in expression columns")
	}

	// Case 2: Explicit columns, single asterisk type.
	// For example the expressions:
	//  "(col1, col2) VALUES ($P.*)"
	//  "(col1, t.col2) AS (&P.*)"
	if starTypes == 1 && numTypes == 1 {
		info, err := ti.lookupInfo(types[0].prefix)
		if err != nil {
			return nil, nil, err
		}
		for _, c := range columns {
			tm, err := info.typeMember(c.name)
			if err != nil {
				return nil, nil, err
			}
			typeMembers = append(typeMembers, tm)
			genCols = append(genCols, c)
		}
		return genCols, typeMembers, nil
	} else if starTypes > 0 && numTypes > 1 {
		return nil, nil, fmt.Errorf("invalid asterisk in expression types")
	}

	// Case 3: Explicit columns and types.
	// For example the expressions:
	//  "(col1, col2) VALUES ($P.name, $A.id)"
	//  "(col1, col2) AS (&P.name, &P.id)"
	if numColumns == numTypes {
		for i, c := range columns {
			t := types[i]
			info, err := ti.lookupInfo(t.prefix)
			if err != nil {
				return nil, nil, err
			}
			tm, err := info.typeMember(t.name)
			if err != nil {
				return nil, nil, err
			}
			typeMembers = append(typeMembers, tm)
			genCols = append(genCols, c)
		}
	} else {
		return nil, nil, fmt.Errorf("mismatched number of columns and targets")
	}
	return genCols, typeMembers, nil
}

// prepareOutput checks that the output expressions correspond to known types.
// It then checks they are formatted correctly and finally generates the columns for the query.
func prepareOutput(ti typeNameToInfo, p *outputPart) (outCols []fullName, typeMembers []typeMember, err error) {
	return prepareColumnsAndTypes(ti, p.sourceColumns, p.targetTypes)
}

// prepareInput checks that the input expression is correctly formatted,
// corresponds to known types, and then generates input columns and values.
func prepareInput(ti typeNameToInfo, p *inputPart) (inCols []fullName, typeMembers []typeMember, err error) {
	numTypes := len(p.sourceTypes)
	numColumns := len(p.targetColumns)
	starTypes := starCount(p.sourceTypes)

	// Check for standalone input expression.
	// For example:
	//  "$P.name"
	if numColumns == 0 {
		if numTypes != 1 {
			return nil, nil, fmt.Errorf("internal error: cannot group standalone input expressions")
		}
		if starTypes > 0 {
			return nil, nil, fmt.Errorf("invalid asterisk in input expression: %s", p.raw)
		}
		info, err := ti.lookupInfo(p.sourceTypes[0].prefix)
		if err != nil {
			return nil, nil, err
		}
		tm, err := info.typeMember(p.sourceTypes[0].name)
		if err != nil {
			return nil, nil, err
		}
		return []fullName{}, []typeMember{tm}, nil
	}

	return prepareColumnsAndTypes(ti, p.targetColumns, p.sourceTypes)
}

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
