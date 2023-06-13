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

// starCheckOutput checks that the output expression is well formed.
func starCheckOutput(p *outputPart) error {
	numColumns := len(p.sourceColumns)
	numTypes := len(p.targetTypes)

	typeStars := starCount(p.targetTypes)
	columnsStars := starCount(p.sourceColumns)
	starType := typeStars == 1
	starColumn := columnsStars == 1

	if typeStars > 1 || columnsStars > 1 || (columnsStars == 1 && typeStars == 0) ||
		(starType && numTypes > 1) || (starColumn && numColumns > 1) {
		return fmt.Errorf("invalid asterisk in output expression: %s", p.raw)
	}
	if !starType && (numColumns > 0 && (numTypes != numColumns)) {
		return fmt.Errorf("cannot match columns to types in output expression: %s", p.raw)
	}
	return nil
}

// starCheckInput checks that the input expression is well formed.
func starCheckInput(p *inputPart) error {
	numTypes := len(p.sourceTypes)
	numCols := len(p.targetColumns)
	starTypes := starCount(p.sourceTypes)
	starCols := starCount(p.targetColumns)

	if numCols == 1 && starCols == 1 {
		return nil
	}

	// Input types grouped togther not in VALUES expression
	if numCols == 0 && numTypes > 1 {
		return fmt.Errorf("internal error: cannot group standalone input expressions")
	}

	// Cannot have multiple star columns or multiple star types
	if (numCols > 1 && starCols > 0) || numCols == 0 && starTypes > 0 {
		return fmt.Errorf("invalid asterisk in input expression: %s", p.raw)
	}

	// Explicit columns and not star type and the number of columns does not equal number of fields specified.
	if numCols > 0 && starCols == 0 && !((numTypes == 1 && starTypes == 1) || (starTypes == 0 && numTypes == numCols)) {
		return fmt.Errorf("cannot match columns to types in input expression: %s", p.raw)
	}
	return nil
}

// prepareInput checks that the input expression corresponds to a known type.
func prepareInput(ti typeNameToInfo, p *inputPart) ([]fullName, []typeMember, error) {
	var inCols = make([]fullName, 0)
	var typeMembers = make([]typeMember, 0)

	// Check the asterisks are well formed (if present).
	if err := starCheckInput(p); err != nil {
		return nil, nil, err
	}

	// Check target struct type and its tags are valid.
	var info typeInfo
	var ok bool

	// Check the input structs and their tags are valid.
	for _, t := range p.sourceTypes {
		info, ok = ti[t.prefix]
		if !ok {
			ts := getKeys(ti)
			if len(ts) == 0 {
				return nil, nil, fmt.Errorf(`type %q not passed as a parameter`, t.prefix)
			} else {
				return nil, nil, fmt.Errorf(`type %q not passed as a parameter, have: %s`, t.prefix, strings.Join(ts, ", "))
			}
		}
		if t.name != "*" {
			// For a none star expression we record output destinations here.
			// For a star expression we fill out the destinations as we generate the columns.
			switch info := info.(type) {
			case *mapInfo:
				typeMembers = append(typeMembers, &mapKey{name: t.name, mapType: info.typ()})
			case *structInfo:
				f, ok := info.tagToField[t.name]
				if !ok {
					return nil, nil, fmt.Errorf(`type %q has no %q db tag`, info.typ().Name(), t.name)
				}
				typeMembers = append(typeMembers, f)
			default:
				return nil, nil, fmt.Errorf(`internal error: unknown info type: %T`, info)
			}
		}
	}

	// Generate columns to inject into SQL query.

	// Case 0: A simple standalone input expression e.g. "$P.name".
	if len(p.targetColumns) == 0 {
		return []fullName{}, typeMembers, nil
	}

	// Case 1: Star type cases e.g. "... VALUES $P.*".
	if p.sourceTypes[0].name == "*" {
		info, _ := ti[p.sourceTypes[0].prefix]

		// Case 1.1: Single star i.e. "* VALUES $P.*"
		if p.targetColumns[0].name == "*" {
			switch info := info.(type) {
			case *mapInfo:
				return nil, nil, fmt.Errorf(`&%s.* cannot be used for maps when no column names are specified`, info.typ().Name())
			case *structInfo:
				for _, tag := range info.tags {
					inCols = append(inCols, fullName{name: tag})
					typeMembers = append(typeMembers, info.tagToField[tag])
				}
				return inCols, typeMembers, nil
			default:
				return nil, nil, fmt.Errorf(`internal error: unknown info type: %T`, info)
			}
		}

		switch info := info.(type) {
		case *mapInfo:
			for _, c := range p.targetColumns {
				inCols = append(inCols, c)
				typeMembers = append(typeMembers, &mapKey{name: c.name, mapType: info.typ()})
			}
			return inCols, typeMembers, nil
		case *structInfo:
			// Case 1.2: Explicit columns e.g. "(col1, col2) VALUES $P.*".
			for _, c := range p.targetColumns {
				f, ok := info.tagToField[c.name]
				if !ok {
					return nil, nil, fmt.Errorf(`type %q has no %q db tag`, info.typ().Name(), c.name)
				}
				inCols = append(inCols, c)
				typeMembers = append(typeMembers, f)
			}
			return inCols, typeMembers, nil
		default:
			return nil, nil, fmt.Errorf(`internal error: unknown info type: %T`, info)
		}
	}

	// Case 2: None star type cases e.g. "... VALUES ($P.name, $P.id)".

	// Case 2.1: Star column e.g. "* VALUES ($P.name, $P.id)".
	if p.targetColumns[0].name == "*" {
		for _, t := range p.sourceTypes {
			inCols = append(inCols, fullName{name: t.name})
		}
		return inCols, typeMembers, nil
	}

	// Case 2.2: Renamed explicit columns e.g. "(name_1) VALUES $P.name".
	for _, c := range p.targetColumns {
		inCols = append(inCols, c)
	}
	return inCols, typeMembers, nil
}

// prepareOutput checks that the output expressions correspond to known types.
// It then checks they are formatted correctly and finally generates the columns for the query.
func prepareOutput(ti typeNameToInfo, p *outputPart) ([]fullName, []typeMember, error) {
	var outCols = make([]fullName, 0)
	var typeMembers = make([]typeMember, 0)

	// Check the asterisks are well formed (if present).
	if err := starCheckOutput(p); err != nil {
		return nil, nil, err
	}

	// Check target struct type and its tags are valid.
	var info typeInfo
	var ok bool

	for _, t := range p.targetTypes {
		info, ok = ti[t.prefix]
		if !ok {
			return nil, nil, fmt.Errorf(`type %q not passed as a parameter, have: %s`, t.prefix, strings.Join(getKeys(ti), ", "))
		}
		if t.name != "*" {
			// For a none star expression we record output destinations here.
			// For a star expression we fill out the destinations as we generate the columns.
			switch info := info.(type) {
			case *mapInfo:
				typeMembers = append(typeMembers, &mapKey{name: t.name, mapType: info.typ()})
			case *structInfo:
				f, ok := info.tagToField[t.name]
				if !ok {
					return nil, nil, fmt.Errorf(`type %q has no %q db tag`, info.typ().Name(), t.name)
				}
				typeMembers = append(typeMembers, f)
			default:
				return nil, nil, fmt.Errorf(`internal error: unknown info type: %T`, info)
			}
		}
	}

	// Generate columns to inject into SQL query.

	// Case 1: Star types cases e.g. "...&P.*".
	if p.targetTypes[0].name == "*" {
		info, _ := ti[p.targetTypes[0].prefix]
		// Case 1.1: Single star i.e. "t.* AS &P.*" or "&P.*"
		if len(p.sourceColumns) == 0 || p.sourceColumns[0].name == "*" {
			switch info := info.(type) {
			case *mapInfo:
				return nil, nil, fmt.Errorf(`&%s.* cannot be used for maps when no column names are specified`, info.typ().Name())
			case *structInfo:
				pref := ""

				// Prepend table name. E.g. "t" in "t.* AS &P.*".
				if len(p.sourceColumns) > 0 {
					pref = p.sourceColumns[0].prefix
				}

				for _, tag := range info.tags {
					outCols = append(outCols, fullName{pref, tag})
					typeMembers = append(typeMembers, info.tagToField[tag])
				}
				return outCols, typeMembers, nil
			default:
				return nil, nil, fmt.Errorf(`internal error: unknown info type: %T`, info)
			}
		}

		// Case 1.2: Explicit columns e.g. "(col1, t.col2) AS &P.*".
		if len(p.sourceColumns) > 0 {
			switch info := info.(type) {
			case *mapInfo:
				for _, c := range p.sourceColumns {
					outCols = append(outCols, c)
					typeMembers = append(typeMembers, &mapKey{name: c.name, mapType: info.typ()})
				}
				return outCols, typeMembers, nil
			case *structInfo:
				for _, c := range p.sourceColumns {
					f, ok := info.tagToField[c.name]
					if !ok {
						return nil, nil, fmt.Errorf(`type %q has no %q db tag`, info.typ().Name(), c.name)
					}
					outCols = append(outCols, c)
					typeMembers = append(typeMembers, f)
				}
				return outCols, typeMembers, nil
			default:
				return nil, nil, fmt.Errorf(`internal error: unknown info type: %T`, info)
			}
		}
	}

	// Case 2: None star types cases e.g. "...(&P.name, &P.id)".

	// Case 2.1: Explicit columns e.g. "name_1 AS P.name".
	if len(p.sourceColumns) > 0 {
		for _, c := range p.sourceColumns {
			outCols = append(outCols, c)
		}
		return outCols, typeMembers, nil
	}

	// Case 2.2: No columns e.g. "(&P.name, &P.id)".
	for _, t := range p.targetTypes {
		outCols = append(outCols, fullName{name: t.name})
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

	// Check and expand each query part.
	for _, part := range pe.queryParts {
		switch p := part.(type) {
		case *inputPart:
			inCols, fields, err := prepareInput(ti, p)
			if err != nil {
				return nil, err
			}
			if len(p.targetColumns) == 0 {
				sql.WriteString("@sqlair_" + strconv.Itoa(inCount))
				inCount += 1
			} else {
				sql.WriteString(printCols(inCols))
				sql.WriteString(" VALUES ")
				sql.WriteString(namedParams(inCount, len(inCols)))
				inCount += len(inCols)
			}
			inputs = append(inputs, fields...)
		case *outputPart:
			outCols, fields, err := prepareOutput(ti, p)
			if err != nil {
				return nil, err
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
			outputs = append(outputs, fields...)

		case *bypassPart:
			sql.WriteString(p.chunk)
		default:
			return nil, fmt.Errorf("internal error: unknown query part type %T", part)
		}
	}

	return &PreparedExpr{inputs: inputs, outputs: outputs, sql: sql.String()}, nil
}

// printCols prints a bracketed, comma seperated list of fullNames.
func printCols(cs []fullName) string {
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

// namedParams returns n incrementing parameters with the first index being start.
func namedParams(start int, n int) string {
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
