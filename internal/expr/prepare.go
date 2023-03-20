package expr

import (
	"bytes"
	"fmt"
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
// asterisks and the number of sources and targets.
func starCheckOutput(p *outputPart) error {
	numSources := len(p.source)
	numTargets := len(p.target)

	targetStars := starCount(p.target)
	sourceStars := starCount(p.source)
	starTarget := targetStars == 1
	starSource := sourceStars == 1

	if targetStars > 1 || sourceStars > 1 || (sourceStars == 1 && targetStars == 0) ||
		(starTarget && numTargets > 1) || (starSource && numSources > 1) {
		return fmt.Errorf("invalid asterisk in output expression: %s", p)
	}
	if !starTarget && (numSources > 0 && (numTargets != numSources)) {
		return fmt.Errorf("mismatched number of cols and targets in output expression: %s", p)
	}
	return nil
}

// prepareInput checks that the input expression corresponds to a known type.
func prepareInput(ti typeNameToInfo, p *inputPart) (field, error) {
	info, ok := ti[p.source.prefix]
	if !ok {
		return field{}, fmt.Errorf(`type %s unknown, have: %s`, p.source.prefix, strings.Join(getKeys(ti), ", "))
	}
	f, ok := info.tagToField[p.source.name]
	if !ok {
		return field{}, fmt.Errorf(`type %s has no %q db tag`, info.typ.Name(), p.source.name)
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

	for _, t := range p.target {
		info, ok = ti[t.prefix]
		if !ok {
			return nil, nil, fmt.Errorf(`type %s unknown, have: %s`, t.prefix, strings.Join(getKeys(ti), ", "))
		}

		if t.name != "*" {
			f, ok := info.tagToField[t.name]
			if !ok {
				return nil, nil, fmt.Errorf(`type %s has no %q db tag`, info.typ.Name(), t.name)
			}
			// For a none star expression we record output destinations here.
			// For a star expression we fill out the destinations as we generate the columns.
			fields = append(fields, f)
		}
	}

	// Generate columns to inject into SQL query.

	// Case 1: Star target cases e.g. "...&P.*".
	if p.target[0].name == "*" {
		info, _ := ti[p.target[0].prefix]

		// Case 1.1: Single star i.e. "t.* AS &P.*" or "&P.*"
		if len(p.source) == 0 || p.source[0].name == "*" {
			pref := ""

			// Prepend table name. E.g. "t" in "t.* AS &P.*".
			if len(p.source) > 0 {
				pref = p.source[0].prefix
			}

			for _, tag := range info.tags {
				outCols = append(outCols, fullName{pref, tag})
				fields = append(fields, info.tagToField[tag])
			}
			return outCols, fields, nil
		}

		// Case 1.2: Explicit columns e.g. "(col1, t.col2) AS &P.*".
		if len(p.source) > 0 {
			for _, c := range p.source {
				f, ok := info.tagToField[c.name]
				if !ok {
					return nil, nil, fmt.Errorf(`type %s has no %q db tag`, info.typ.Name(), c.name)
				}
				outCols = append(outCols, c)
				fields = append(fields, f)
			}
			return outCols, fields, nil
		}
	}

	// Case 2: None star target cases e.g. "...(&P.name, &P.id)".

	// Case 2.1: Explicit columns e.g. "name_1 AS P.name".
	if len(p.source) > 0 {
		for _, c := range p.source {
			outCols = append(outCols, c)
		}
		return outCols, fields, nil
	}

	// Case 2.2: No columns e.g. "(&P.name, &P.id)".
	for _, t := range p.target {
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
