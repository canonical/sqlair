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
	outputs typeToCols
	inputs  []*inputPart
	SQL     string
}

// typeToCols maps the output types to the columns they are assosiated with.
type typeToCols map[reflect.Type]numRange

type numRange struct {
	firstCol int
	lastCol  int
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
func prepareInput(ti typeNameToInfo, p *inputPart) error {
	info, ok := ti[p.source.prefix]
	if !ok {
		return fmt.Errorf(`type %s unknown, have: %s`, p.source.prefix, strings.Join(getKeys(ti), ", "))
	}

	if _, ok = info.tagToField[p.source.name]; !ok {
		return fmt.Errorf(`type %s has no %q db tag`,
			info.structType.Name(), p.source.name)
	}

	return nil
}

// prepareOutput checks that the output expressions are correspond to a known types.
// It then checks they are formatted correctly and finally generates the columns for the query.
func prepareOutput(ti typeNameToInfo, p *outputPart) ([]fullName, error) {

	var outCols []fullName = make([]fullName, 0)

	// Check target struct type and its tags are valid.
	var info *info
	var ok bool

	for i, t := range p.target {
		if i == 0 {
			info, ok = ti[t.prefix]
			if !ok {
				return nil, fmt.Errorf(`type %s unknown, have: %s`, t.prefix, strings.Join(getKeys(ti), ", "))
			}
		} else if t.prefix != info.structType.Name() {
			return nil, fmt.Errorf("multiple target types in: %s", p.String())
		}

		_, ok = info.tagToField[t.name]
		if !ok && t.name != "*" {
			return nil, fmt.Errorf(`type %s has no %q db tag`, info.structType.Name(), t.name)
		}
	}

	if err := starCheckOutput(p); err != nil {
		return nil, err
	}

	// Generate columns to inject into SQL query.

	// Case 1: Star target cases e.g. "...&P.*".
	if p.target[0].name == "*" {
		info, _ := ti[p.target[0].prefix]

		// Case 1.1: Single star e.g. "t.* AS &P.*" or "&P.*"
		if len(p.source) == 0 || p.source[0].name == "*" {
			pref := ""

			// Prepend table name. E.g. "t" in "t.* AS &P.*".
			if len(p.source) > 0 {
				pref = p.source[0].prefix
			}

			tags := getKeys(info.tagToField)
			for _, tag := range tags {
				outCols = append(outCols, fullName{prefix: pref, name: tag})
			}
			return outCols, nil
		}

		// Case 1.2: Explicit columns e.g. "(col1, t.col2) AS &P.*".
		if len(p.source) > 0 {
			for _, c := range p.source {
				if _, ok := info.tagToField[c.name]; !ok {
					return nil, fmt.Errorf(`type %s has no %q db tag`, info.structType.Name(), c.name)
				}
				outCols = append(outCols, c)
			}
			return outCols, nil
		}
	}

	// Case 2: None star target cases e.g. "...&(P.name, P.id)".

	// Case 2.1: Explicit columns e.g. "name_1 AS P.name".
	if len(p.source) > 0 {
		for _, c := range p.source {
			outCols = append(outCols, c)
		}
		return outCols, nil
	}

	// Case 2.2: No columns e.g. "&(P.name, P.id)".
	for _, t := range p.target {
		outCols = append(outCols, fullName{name: t.name})
	}
	return outCols, nil
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
		ti[info.structType.Name()] = info
	}

	var sql bytes.Buffer
	var n int

	var outs = make(typeToCols)
	var ins = make([]*inputPart, 0)

	// Check and expand each query part.
	for _, part := range pe.queryParts {
		switch p := part.(type) {
		case *inputPart:
			err := prepareInput(ti, p)
			if err != nil {
				return nil, err
			}
			sql.WriteString("?")
			ins = append(ins, p)
		case *outputPart:
			outCols, err := prepareOutput(ti, p)
			if err != nil {
				return nil, err
			}
			startCol := n
			for i, c := range outCols {
				sql.WriteString(c.String())
				sql.WriteString(" AS _sqlair_")
				sql.WriteString(c.name)
				sql.WriteString("_")
				sql.WriteString(strconv.Itoa(n))
				if i != len(outCols)-1 {
					sql.WriteString(", ")
				}
				n++
			}
			outs[ti[p.target[0].prefix].structType] = numRange{startCol, n - 1}

		case *bypassPart:
			sql.WriteString(p.chunk)
		default:
			return nil, fmt.Errorf("internal error: unknown query part type %T", part)
		}
	}

	return &PreparedExpr{inputs: ins, outputs: outs, SQL: sql.String()}, nil
}
