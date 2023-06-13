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

// starCheckOutput checks that the statement is well formed with regard to
// asterisks and the number of columns and types.
func starCheckOutput(p *outputPart) error {
	numColumns := len(p.sourceColumns)
	numTypes := len(p.targetTypes)

	typeStars := starCount(p.targetTypes)
	columnStars := starCount(p.sourceColumns)
	starTypes := typeStars == 1
	starColumns := columnStars == 1

	if typeStars > 1 || columnStars > 1 || (columnStars == 1 && typeStars == 0) ||
		(starTypes && numTypes > 1) || (starColumns && numColumns > 1) {
		return fmt.Errorf("invalid asterisk in output expression: %s", p.raw)
	}
	if !starTypes && (numColumns > 0 && (numTypes != numColumns)) {
		return fmt.Errorf("mismatched number of columns and targets in output expression: %s", p.raw)
	}
	return nil
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
					outCols = append(outCols, fullName{prefix: pref, name: tag})
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
					if c.isFunc {
						return nil, nil, fmt.Errorf(`invalid tag/column name %q in %q`, c.name, p.raw)
					}
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

	// Case 2: None star target cases e.g. "...(&P.name, &P.id)".

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
			inLoc, err := prepareInput(ti, p)
			if err != nil {
				return nil, err
			}
			sql.WriteString("@sqlair_" + strconv.Itoa(inCount))
			inCount++
			inputs = append(inputs, inLoc)
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
