package expr

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
)

// PreparedExpr contains an SQL expression that is ready for execution.
type PreparedExpr struct {
	ParsedExpr *ParsedExpr
	SQL        string
}

type typeNameToInfo map[string]*info

func getKeys(m map[string]*info) []string {
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

func prepareOutput(ti typeNameToInfo, p *outputPart) ([]string, error) {

	var outCols []string = make([]string, 0)

	// Check target struct type and its tags are valid.
	var inf *info
	var ok bool

	for i, t := range p.target {
		if i == 0 {
			inf, ok = ti[t.prefix]
			if !ok {
				return nil, fmt.Errorf("unknown type: %s", t.prefix)
			}
		} else if t.prefix != inf.structType.Name() {
			return nil, fmt.Errorf("multiple types in single output expression")
		}

		_, ok = inf.tagToField[t.name]
		if !ok && t.name != "*" {
			return nil, fmt.Errorf(`no tag with name "%s" in "%s"`, t.name, inf.structType.Name())
		}
	}

	// Check asterisk are in correct places.

	sct := starCount(p.target)
	scc := starCount(p.source)

	if sct > 1 || scc > 1 || (scc == 1 && sct == 0) {
		return nil, fmt.Errorf("invalid asterisk in output expression")
	}

	starTarget := sct == 1
	starSource := scc == 1

	numSources := len(p.source)
	numTargets := len(p.target)

	if (starTarget && numTargets > 1) || (starSource && numSources > 1) {
		return nil, fmt.Errorf("invalid mix of asterisk and none asterisk columns in output expression")
	}

	if !starTarget && (numSources > 0 && (numTargets != numSources)) {
		return nil, fmt.Errorf("mismatched number of cols and targets in output expression")
	}

	// Case 1: Star target cases e.g. "...&P.*".
	if starTarget {
		inf, _ := ti[p.target[0].prefix]

		// Case 1.1: Single star e.g. "t.* AS &P.*" or "&P.*"
		if starSource || numSources == 0 {
			pref := ""

			// Prepend table name. E.g. "t" in "t.* AS &P.*".
			if numSources > 0 && p.source[0].prefix != "" {
				pref = p.source[0].prefix + "."
			}

			for tag := range inf.tagToField {
				outCols = append(outCols, pref+tag)
			}

			// The strings are sorted to give a deterministic order for
			// testing.
			sort.Strings(outCols)
			return outCols, nil
		}

		// Case 1.2: Explicit columns e.g. "(col1, t.col2) AS &P.*".
		if numSources > 0 {
			for _, c := range p.source {
				if _, ok := inf.tagToField[c.name]; !ok {
					return nil, fmt.Errorf(`no tag with name "%s" in "%s"`,
						c.name, inf.structType.Name())
				}
				outCols = append(outCols, c.String())
			}
			return outCols, nil
		}
	}

	// Case 2: None star target cases e.g. "...&(P.name, P.id)".

	// Case 2.1: Explicit columns e.g. "name_1 AS P.name".
	if numSources > 0 {
		for _, c := range p.source {
			outCols = append(outCols, c.String())
		}
		return outCols, nil
	}

	// Case 2.2: No columns e.g. "&(P.name, P.id)".
	for _, t := range p.target {
		outCols = append(outCols, t.name)
	}
	return outCols, nil
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
		info, err := typeInfo(arg)
		if err != nil {
			return nil, err
		}
		ti[info.structType.Name()] = info
	}

	var sql bytes.Buffer
	// Check and expand each query part.

	ins := []*inputPart{}

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
			n += len(outCols)
			sql.WriteString(p.toSQL(outCols, n))
			continue
		case *bypassPart:
			sql.WriteString(p.chunk)
		default:
			return nil, fmt.Errorf("internal error: unknown query part type %T", part)
		}
	}

	return &PreparedExpr{inputs: ins, SQL: sql.String()}, nil
}
