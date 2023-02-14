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
	outputs []loc
	inputs  []loc
	SQL     string
}

// loc stores the type and field in which you can find an IO part.
type loc struct {
	typ   reflect.Type
	field field
}

type typeNameToInfo map[string]*info

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

type ioPart struct {
	cols  []fullName
	types []fullName
	isOut bool
}

func (p *ioPart) raw() string {
	var midWord string
	var symb string
	if p.isOut {
		midWord = ") AS "
		symb = "&"
	} else {
		midWord = ") VALUES "
		symb = "$"
	}
	var b bytes.Buffer
	if len(p.cols) > 0 {
		b.WriteString("(")
		for i, c := range p.cols {
			b.WriteString(c.String())
			if i < len(p.cols)-1 {
				b.WriteString(", ")
			}
		}
		b.WriteString(midWord)
	}

	b.WriteString("(")
	for i, s := range p.types {
		b.WriteString(symb)
		b.WriteString(s.String())
		if i < len(p.types)-1 {
			b.WriteString(", ")
		}
	}
	b.WriteString(")")
	return b.String()
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

// nParams returns "num" incrementing parameters with the first index being
// "start".
func nParams(start int, num int) string {
	var s bytes.Buffer
	s.WriteString("(")
	for i := start; i < start+num; i++ {
		s.WriteString("@sqlair_")
		s.WriteString(strconv.Itoa(i))
		if i < start+num-1 {
			s.WriteString(", ")
		}
	}
	s.WriteString(")")
	return s.String()
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

// checkValid ensures that the expression is formatted correctly.
func checkValid(p ioPart) error {
	numTypes := len(p.types)
	numCols := len(p.cols)
	starTypes := starCount(p.types)
	starCols := starCount(p.cols)

	if (numCols == 1 && starCols == 1) || (p.isOut && numCols == 0) {
		return nil
	}

	if !p.isOut && numCols == 0 && numTypes > 1 {
		return fmt.Errorf("internal error: cannot group standalone input expressions")
	}

	if (numCols > 1 && starCols > 0) || (!p.isOut && numCols == 0 && starTypes > 0) {
		return fmt.Errorf("invalid asterisk in: %s", p.raw())
	}

	if numCols > 0 && starCols == 0 && !((numTypes == 1 && starTypes == 1) || (starTypes == 0 && numTypes == numCols)) {
		return fmt.Errorf("cannot match columns to types in: %s", p.raw())
	}

	return nil
}

type retBuilder struct {
	cols []fullName
	locs []loc
}

// prepareExpr checks that an input or output part is correctly formatted, that
// it corrosponds to known types and then generates the columns to go in the query.
func prepareExpr(ti typeNameToInfo, part queryPart) ([]fullName, []loc, error) {

	var info *info
	var ok bool

	var io ioPart
	switch p := part.(type) {
	case *inputPart:
		io = ioPart{cols: p.cols, types: p.source, isOut: false}
	case *outputPart:
		io = ioPart{cols: p.source, types: p.target, isOut: true}
	case *bypassPart:
		return nil, nil, fmt.Errorf("internal error: cannot prepare bypass part")
	}

	// res stores the list of columns to put in the query and their locations.
	res := retBuilder{}

	// add prepares a location and column.
	add := func(typeName string, tag string, col fullName) error {
		info, ok = ti[typeName]
		if !ok {
			return fmt.Errorf(`type %s unknown, have: %s`, typeName, strings.Join(getKeys(ti), ", "))
		}

		f, ok := info.tagToField[tag]
		if !ok {
			return fmt.Errorf(`type %s has no %q db tag`, info.structType.Name(), tag)
		}
		res.cols = append(res.cols, col)
		res.locs = append(res.locs, loc{info.structType, f})
		return nil
	}

	// Check the expression is valid.
	if err := checkValid(io); err != nil {
		return nil, nil, err
	}

	// Generate columns to inject into SQL query.

	// Case 0: A simple standalone input expression e.g. "$P.name".
	if !io.isOut && len(io.cols) == 0 {
		if len(io.types) != 1 {
			return []fullName{fullName{}}, nil, fmt.Errorf("internal error: cannot group standalone input expressions")
		}
		if err := add(io.types[0].prefix, io.types[0].name, fullName{}); err != nil {
			return nil, nil, err
		}
		return res.cols, res.locs, nil
	}

	// Case 1: sqlair generates columns e.g. "* AS (&P.*, &A.id)" or "&P.*" or
	// 		   "(*) VALUES ($P.*)".
	if (io.isOut && len(io.cols) == 0) || (len(io.cols) == 1 && (io.cols[0].name == "*")) {
		pref := ""
		// Prepend table name. E.g. the "t" in "t.* AS &P.*".
		if len(io.cols) > 0 {
			pref = io.cols[0].prefix
		}
		for _, t := range io.types {
			if t.name == "*" {
				// Generate columns for Star types.
				info, ok = ti[t.prefix]
				if !ok {
					return nil, nil, fmt.Errorf(`type %s unknown, have: %s`, t.prefix, strings.Join(getKeys(ti), ", "))
				}
				for _, tag := range info.tags {
					if err := add(t.prefix, tag, fullName{pref, tag}); err != nil {
						return nil, nil, err
					}
				}
			} else {
				// Generate Columns for none star types.
				if err := add(t.prefix, t.name, fullName{pref, t.name}); err != nil {
					return nil, nil, err
				}
			}
		}
		return res.cols, res.locs, nil
	}
	// Case 2: Explicit columns with star e.g. "(name, id) AS (&P.*)" or
	// 		   "(col1, col2) VALUES ($P.*)".
	// There must only be a single type in this case.
	if io.types[0].name == "*" {
		for _, c := range io.cols {
			if err := add(io.types[0].prefix, c.name, c); err != nil {
				return nil, nil, err
			}
		}
		return res.cols, res.locs, nil
	}

	// Case 3: Explicit columns and targets e.g. "(col1, col2) AS (&P.name, &P.id)" or
	// 		   "(col1, col2) VALUES ($P.name, $P.id)".
	// The number of each must be equal here.
	for i, c := range io.cols {
		if err := add(io.types[i].prefix, io.types[i].name, c); err != nil {
			return nil, nil, err
		}
	}
	return res.cols, res.locs, nil
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
	// n counts the inputs.
	var n int
	// m counts the outputs.
	var m int

	var outputs = make([]loc, 0)
	var inputs = make([]loc, 0)

	// Check and expand each query part.
	for _, part := range pe.queryParts {
		switch p := part.(type) {
		case *inputPart:
			cols, locs, err := prepareExpr(ti, p)
			if err != nil {
				return nil, err
			}
			if len(p.cols) == 0 {
				sql.WriteString("@sqlair_" + strconv.Itoa(n))
			} else {
				sql.WriteString(printCols(cols))
				sql.WriteString(" VALUES ")
				sql.WriteString(nParams(n, len(cols)))
			}
			n += len(cols)
			inputs = append(inputs, locs...)
		case *outputPart:
			cols, locs, err := prepareExpr(ti, p)
			if err != nil {
				return nil, err
			}
			for i, c := range cols {
				sql.WriteString(c.String())
				sql.WriteString(" AS _sqlair_")
				sql.WriteString(strconv.Itoa(m))
				if i != len(cols)-1 {
					sql.WriteString(", ")
				}
				m++
			}
			outputs = append(outputs, locs...)

		case *bypassPart:
			sql.WriteString(p.chunk)
		default:
			return nil, fmt.Errorf("internal error: unknown query part type %T", part)
		}
	}

	return &PreparedExpr{inputs: inputs, outputs: outputs, SQL: sql.String()}, nil
}
