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
	outputs []field
	inputs  []field
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
	numColumns := len(p.columns)
	numTypes := len(p.types)

	typeStars := starCount(p.types)
	columnStars := starCount(p.columns)
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
func prepareInput(ti typeNameToInfo, p *inputPart) (field, error) {
	info, ok := ti[p.typ.prefix]
	if !ok {
		ts := getKeys(ti)
		if len(ts) == 0 {
			return field{}, fmt.Errorf(`type %q not passed as a parameter`, p.typ.prefix)
		} else {
			return field{}, fmt.Errorf(`type %q not passed as a parameter, have: %s`, p.typ.prefix, strings.Join(ts, ", "))
		}
	}
	f, ok := info.tagToField[p.typ.name]
	if !ok {
		return field{}, fmt.Errorf(`type %q has no %q db tag`, info.typ.Name(), p.typ.name)
	}

	return f, nil
}

// prepareOutput checks that the output expressions correspond to known types.
// It then checks they are formatted correctly and finally generates the columns for the query.
func prepareOutput(ti typeNameToInfo, p *outputPart) ([]fullName, []field, error) {

	var outCols = make([]fullName, 0)
	var fields = make([]field, 0)

	// Check the asterisks are well formed (if present).
	if err := starCheckOutput(p); err != nil {
		return nil, nil, err
	}

	// Check target struct type and its tags are valid.
	var info *info
	var ok bool

	for _, t := range p.types {
		info, ok = ti[t.prefix]
		if !ok {
			return nil, nil, fmt.Errorf(`type %q not passed as a parameter, have: %s`, t.prefix, strings.Join(getKeys(ti), ", "))
		}

		if t.name != "*" {
			f, ok := info.tagToField[t.name]
			if !ok {
				return nil, nil, fmt.Errorf(`type %q has no %q db tag`, info.typ.Name(), t.name)
			}
			// For a none star expression we record output destinations here.
			// For a star expression we fill out the destinations as we generate the columns.
			fields = append(fields, f)
		}
	}

	// Generate columns to inject into SQL query.

	// Case 1: Star type cases e.g. "...&P.*".
	if p.types[0].name == "*" {
		info, _ := ti[p.types[0].prefix]

		// Case 1.1: Single star i.e. "t.* AS &P.*" or "&P.*"
		if len(p.columns) == 0 || p.columns[0].name == "*" {
			pref := ""

			// Prepend table name. E.g. "t" in "t.* AS &P.*".
			if len(p.columns) > 0 {
				pref = p.columns[0].prefix
			}

			for _, tag := range info.tags {
				outCols = append(outCols, fullName{pref, tag})
				fields = append(fields, info.tagToField[tag])
			}
			return outCols, fields, nil
		}

		// Case 1.2: Explicit columns e.g. "(col1, t.col2) AS &P.*".
		if len(p.columns) > 0 {
			for _, c := range p.columns {
				f, ok := info.tagToField[c.name]
				if !ok {
					return nil, nil, fmt.Errorf(`type %q has no %q db tag`, info.typ.Name(), c.name)
				}
				outCols = append(outCols, c)
				fields = append(fields, f)
			}
			return outCols, fields, nil
		}
	}

	// Case 2: None star type cases e.g. "...(&P.name, &P.id)".

	// Case 2.1: Explicit columns e.g. "name_1 AS P.name".
	if len(p.columns) > 0 {
		for _, c := range p.columns {
			outCols = append(outCols, c)
		}
		return outCols, fields, nil
	}

	// Case 2.2: No columns e.g. "(&P.name, &P.id)".
	for _, t := range p.types {
		outCols = append(outCols, fullName{name: t.name})
	}
	return outCols, fields, nil
}

type typeNameToInfo map[string]*info

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
			return nil, fmt.Errorf("need struct, got nil")
		}
		t := reflect.TypeOf(arg)
		if t.Kind() != reflect.Struct {
			if t.Kind() == reflect.Pointer {
				return nil, fmt.Errorf("need struct, got pointer to %s", t.Elem().Kind())
			}
			return nil, fmt.Errorf("need struct, got %s", t.Kind())
		}
		if t.Name() == "" {
			return nil, fmt.Errorf("cannot use anonymous %s", t.Kind())
		}
		info, err := typeInfo(arg)
		if err != nil {
			return nil, err
		}
		ti[info.typ.Name()] = info
	}

	var sql bytes.Buffer

	var inCount int
	var outCount int

	var outputs = make([]field, 0)
	var inputs = make([]field, 0)

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
