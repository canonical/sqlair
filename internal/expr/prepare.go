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

// checkValidOutput checks that the statement is well formed with regard to
// asterisks and the number of columns and types.
func checkValidOutput(p *outputPart) error {
	numTypes := len(p.types)
	numColumns := len(p.columns)
	starTypes := starCount(p.types)
	starColumns := starCount(p.columns)

	if (numColumns == 1 && starColumns == 1) || numColumns == 0 {
		return nil
	}

	if numColumns > 1 && starColumns > 0 {
		return fmt.Errorf("invalid asterisk in output expression: %s", p.raw)
	}

	if numColumns > 0 && starColumns == 0 && !((numTypes == 1 && starTypes == 1) || (starTypes == 0 && numTypes == numColumns)) {
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
	// Check the asterisks are well formed (if present).
	if err := checkValidOutput(p); err != nil {
		return nil, nil, err
	}

	// Generate columns to inject into SQL query.
	var outCols = make([]fullName, 0)
	var fields = make([]field, 0)

	var info *structInfo
	var err error

	fetchInfo := func(typeName string) (*structInfo, error) {
		info, ok := ti[typeName]
		if !ok {
			return nil, fmt.Errorf(`type %q not passed as a parameter, have: %s`, typeName, strings.Join(getKeys(ti), ", "))
		}
		return info, nil
	}

	addColumns := func(info *structInfo, tag string, column fullName) error {
		f, ok := info.tagToField[tag]
		if !ok {
			return fmt.Errorf(`type %q has no %q db tag`, info.typ.Name(), tag)
		}
		outCols = append(outCols, column)
		fields = append(fields, f)
		return nil
	}

	// Case 1: Generated columns e.g. "* AS (&P.*, &A.id)" or "&P.*" or
	if len(p.columns) == 0 || (len(p.columns) == 1 && p.columns[0].name == "*") {
		pref := ""
		// Prepend table name. E.g. "t" in "t.* AS &P.*".
		if len(p.columns) > 0 {
			pref = p.columns[0].prefix
		}

		for _, t := range p.types {
			if info, err = fetchInfo(t.prefix); err != nil {
				return nil, nil, err
			}
			// Generate asterisk columns.
			if t.name == "*" {
				for _, tag := range info.tags {
					outCols = append(outCols, fullName{pref, tag})
					fields = append(fields, info.tagToField[tag])
				}
			} else {
				// Generate explicit columns.
				if err = addColumns(info, t.name, fullName{pref, t.name}); err != nil {
					return nil, nil, err
				}
			}
		}
		return outCols, fields, nil
	}

	// Case 2: Explicit columns, single asterisk type e.g. "(col1, t.col2) AS &P.*".
	if t := p.types[0]; t.name == "*" {
		if info, err = fetchInfo(t.prefix); err != nil {
			return nil, nil, err
		}
		for _, c := range p.columns {
			if err = addColumns(info, c.name, c); err != nil {
				return nil, nil, err
			}
		}
		return outCols, fields, nil
	}

	// Case 3: Explicit columns and types e.g. "(col1, col2) AS (&P.name, &P.id)".
	for i, c := range p.columns {
		t := p.types[i]
		if info, err = fetchInfo(t.prefix); err != nil {
			return nil, nil, err
		}

		if err = addColumns(info, t.name, c); err != nil {
			return nil, nil, err
		}
	}
	return outCols, fields, nil
}

type typeNameToInfo map[string]*structInfo

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

	var fieldPresent = make(map[field]bool)

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

			for _, f := range fields {
				if ok := fieldPresent[f]; ok {
					return nil, fmt.Errorf("field with tag %q of struct %q appears more than once", f.tag, f.structType.Name())
				}
				fieldPresent[f] = true
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
