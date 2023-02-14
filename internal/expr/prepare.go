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
func checkValid(p *ioPart) error {
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
		return fmt.Errorf("invalid asterisk in: %s", p.rawString)
	}

	if numCols > 0 && starCols == 0 && !((numTypes == 1 && starTypes == 1) || (starTypes == 0 && numTypes == numCols)) {
		return fmt.Errorf("cannot match columns to types in: %s", p.rawString)
	}

	return nil
}

type retBuilder struct {
	cols []fullName
	locs []loc
}

// prepareExpr checks that an input or output part is correctly formatted, that
// it corrosponds to known types and then generates the columns to go in the query.
func prepareExpr(ti typeNameToInfo, p *ioPart) ([]fullName, []loc, error) {

	var info *info
	var ok bool

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
			return fmt.Errorf(`type %s has no %q db tag`, info.typ.Name(), tag)
		}
		res.cols = append(res.cols, col)
		res.locs = append(res.locs, loc{info.typ, f})
		return nil
	}

	// Check the expression is valid.
	if err := checkValid(p); err != nil {
		return nil, nil, err
	}

	// Generate columns to inject into SQL query.

	// Case 0: A simple standalone input expression e.g. "$P.name".
	if !p.isOut && len(p.cols) == 0 {
		if len(p.types) != 1 {
			return []fullName{fullName{}}, nil, fmt.Errorf("internal error: cannot group standalone input expressions")
		}
		if err := add(p.types[0].prefix, p.types[0].name, fullName{}); err != nil {
			return nil, nil, err
		}
		return res.cols, res.locs, nil
	}

	// Case 1: sqlair generates columns e.g. "* AS (&P.*, &A.id)" or "&P.*" or
	// 		   "(*) VALUES ($P.*)".
	if (p.isOut && len(p.cols) == 0) || (len(p.cols) == 1 && (p.cols[0].name == "*")) {
		pref := ""
		// Prepend table name. E.g. the "t" in "t.* AS &P.*".
		if len(p.cols) > 0 {
			pref = p.cols[0].prefix
		}
		for _, t := range p.types {
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
	if p.types[0].name == "*" {
		for _, c := range p.cols {
			if err := add(p.types[0].prefix, c.name, c); err != nil {
				return nil, nil, err
			}
		}
		return res.cols, res.locs, nil
	}

	// Case 3: Explicit columns and targets e.g. "(col1, col2) AS (&P.name, &P.id)" or
	// 		   "(col1, col2) VALUES ($P.name, $P.id)".
	// The number of each must be equal here.
	for i, c := range p.cols {
		if err := add(p.types[i].prefix, p.types[i].name, c); err != nil {
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
		ti[info.typ.Name()] = info
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
		case *ioPart:
			cols, locs, err := prepareExpr(ti, p)
			if err != nil {
				return nil, err
			}
			if p.isOut {
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
			} else {
				if len(p.cols) == 0 {
					sql.WriteString("@sqlair_" + strconv.Itoa(n))
				} else {
					sql.WriteString(printCols(cols))
					sql.WriteString(" VALUES ")
					sql.WriteString(nParams(n, len(cols)))
				}
				n += len(cols)
				inputs = append(inputs, locs...)
			}
		case *bypassPart:
			sql.WriteString(p.chunk)
		default:
			return nil, fmt.Errorf("internal error: unknown query part type %T", part)
		}
	}

	return &PreparedExpr{inputs: inputs, outputs: outputs, SQL: sql.String()}, nil
}
