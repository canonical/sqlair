package expr

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
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

func starCount(fns []fullName) int {
	s := 0
	for _, fn := range fns {
		if fn.name == "*" {
			s++
		}
	}
	return s
}

// prepareOutput checks that the output expressions corresponds to a known type.
// It then checks the asterisk are in the right place and finally generates the
// columns needed from the database.
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

	// Check asterisk are in correct places.
	sct := starCount(p.target)
	scs := starCount(p.source)

	if sct > 1 || scs > 1 || (scs == 1 && sct == 0) {
		return nil, fmt.Errorf("invalid asterisk in output expression")
	}

	starTarget := sct == 1
	starSource := scs == 1
	numSources := len(p.source)
	numTargets := len(p.target)

	if (starTarget && numTargets > 1) || (starSource && numSources > 1) {
		return nil, fmt.Errorf("invalid asterisk columns in: %s", p.String())
	}

	if !starTarget && (numSources > 0 && (numTargets != numSources)) {
		return nil, fmt.Errorf("mismatched number of cols and targets in: %s", p.String())
	}

	// Generate columns to inject into SQL query.

	// Case 1: Star target cases e.g. "...&P.*".
	if starTarget {
		info, _ := ti[p.target[0].prefix]

		// Case 1.1: Single star e.g. "t.* AS &P.*" or "&P.*"
		if starSource || numSources == 0 {
			pref := ""

			// Prepend table name. E.g. "t" in "t.* AS &P.*".
			if numSources > 0 {
				pref = p.source[0].prefix
			}

			for tag := range info.tagToField {
				outCols = append(outCols, fullName{pref, tag})
			}

			// The strings are sorted to give a deterministic order for
			// testing.
			sort.Slice(outCols, func(i, j int) bool { return outCols[i].String() < outCols[j].String() })
			return outCols, nil
		}

		// Case 1.2: Explicit columns e.g. "(col1, t.col2) AS &P.*".
		if numSources > 0 {
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
	if numSources > 0 {
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
				sql.WriteString(fmt.Sprintf("_%d", n))
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

	return &PreparedExpr{inputs: ins, SQL: sql.String()}, nil
}
