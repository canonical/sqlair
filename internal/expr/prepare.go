package expr

import (
	"bytes"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// PreparedExpr contains an SQL expression that is ready for execution.
type PreparedExpr struct {
	inputs []inputLocation
	SQL    string
}

// We get the position in the query from its position in inputLocation
type inputLocation struct {
	inputType reflect.Type
	field     field
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

// starCheckInput checks that the input is well formed with regard to
// asterisks and the number of sources and targets.
func starCheckInput(p *inputPart) error {
	numColumns := len(p.cols)
	numSources := len(p.source)

	sourceStars := starCount(p.source)
	columnStars := 0
	for _, col := range p.cols {
		if col == "*" {
			columnStars++
		}
	}
	starSource := sourceStars == 1
	starColumn := columnStars == 1

	if sourceStars > 1 || columnStars > 1 || (columnStars == 1 && sourceStars == 0) ||
		(starColumn && numColumns > 1) || (starSource && numSources > 1) || (starSource && numColumns == 0) {
		return fmt.Errorf("invalid asterisk in input expression: %s", p)
	}
	if !starSource && (numColumns > 0 && (numColumns != numSources)) {
		return fmt.Errorf("mismatched number of inputs and cols in input expression: %s", p)
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

func printList(xs []string) string {
	var s bytes.Buffer
	s.WriteString("(")
	s.WriteString(strings.Join(xs, ", "))
	s.WriteString(")")
	return s.String()
}

func nParams(start int, num int) string {
	var s bytes.Buffer
	s.WriteString("(")
	for i := start; i < start+num; i++ {
		s.WriteString("@_sqlair_")
		s.WriteString(strconv.Itoa(i))
		if i < start+num-1 {
			s.WriteString(", ")
		}
	}
	s.WriteString(")")
	return s.String()
}

// prepareInput first checks the types mentioned in the expression are known, it
// then checks the expression is valid and generates the SQL to print in place
// of it.
// As well as the SQL string it also returns a list of fullNames containing the
// type and tag of each input parameter. These are used in the complete stage to
// extract the arguments from the relevent structs.
func prepareInput(ti typeNameToInfo, p *inputPart, n int) (string, []inputLocation, error) {

	var inLocs = make([]inputLocation, 0)

	// Check the input structs and their tags are valid.
	for _, s := range p.source {
		info, ok := ti[s.prefix]
		if !ok {
			return "", nil, fmt.Errorf(`type %s unknown, have: %s`, s.prefix, strings.Join(getKeys(ti), ", "))
		}
		if s.name != "*" {
			f, ok := info.tagToField[s.name]
			if !ok {
				return "", nil, fmt.Errorf(`type %s has no %q db tag`, info.structType.Name(), s.name)
			}
			// For a none star expression we record output destinations here.
			// For a star expression we fill out the destinations as we generate the columns.
			inLocs = append(inLocs, inputLocation{info.structType, f})
		}
	}

	if err := starCheckInput(p); err != nil {
		return "", nil, err
	}

	// Case 1: A simple standalone input expression e.g. "$P.name".
	if len(p.cols) == 0 {
		if len(p.source) != 1 {
			return "", nil, fmt.Errorf("internal error: cannot group standalone input expressions")
		}
		return "@_sqlair_" + strconv.Itoa(n), inLocs, nil
	}

	// Case 2: A VALUES expression (probably inside an INSERT)
	cols := []string{}
	// Case 2.1: An Asterisk VALUES expression e.g. "... VALUES $P.*".
	if p.source[0].name == "*" {
		info, _ := ti[p.source[0].prefix]
		// Case 2.1.1 e.g. "(*) VALUES ($P.*)"
		if p.cols[0] == "*" {
			for _, tag := range getKeys(info.tagToField) {
				cols = append(cols, tag)
				inLocs = append(inLocs, inputLocation{info.structType, info.tagToField[tag]})
			}
			return printList(cols) + " VALUES " + nParams(n, len(cols)), inLocs, nil
		}
		// Case 2.1.2 e.g. "(col1, col2, col3) VALUES ($P.*)"
		for _, col := range p.cols {
			f, ok := info.tagToField[col]
			if !ok {
				return "", nil, fmt.Errorf(`type %s has no %q db tag`, info.structType.Name(), col)
			}
			cols = append(cols, col)
			inLocs = append(inLocs, inputLocation{info.structType, f})
		}
		return printList(cols) + " VALUES " + nParams(n, len(cols)), inLocs, nil
	}
	// Case 2.2: explicit for both e.g. (mycol1, mycol2) VALUES ($Person.col1, $Address.col1)
	cols = p.cols
	return printList(p.cols) + " VALUES " + nParams(n, len(p.cols)), inLocs, nil
}

var alphaNum = regexp.MustCompile("[^a-zA-Z0-9]+")

// fullNameToNamedParam converts a to a named paramter.
// The includeAt flag will prefix the name with an "@" symbol.
// e.g. fullName{"Prefix", "name") =>  "@Prefixname".
func fullNameToNamedParam(fn fullName, includeAt bool) string {
	str := ""
	if includeAt {
		str = "@"
	}
	return str + alphaNum.ReplaceAllString(fn.String(), "")
}

// Prepare takes a parsed expression and all the Go objects mentioned in it.
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

	inputs := []inputLocation{}
	n := 0

	for _, part := range pe.queryParts {
		switch p := part.(type) {
		case *inputPart:
			s, ins, err := prepareInput(ti, p, n)
			n += len(ins)
			if err != nil {
				return nil, err
			}
			sql.WriteString(s)
			inputs = append(inputs, ins...)
		case *outputPart:
		case *bypassPart:
			sql.WriteString(p.chunk)
		default:
			return nil, fmt.Errorf("internal error: unknown query part type %T", part)
		}
	}

	return &PreparedExpr{inputs: inputs, SQL: sql.String()}, nil
}
