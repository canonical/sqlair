package expr

import (
	"bytes"
	"fmt"
	"log"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

// PreparedExpr contains an SQL expression that is ready for execution.
type PreparedExpr struct {
	outputs          []typeMember
	inputs           []typeMember
	prepedQueryParts []prepedQueryPart
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
func prepareInput(ti typeNameToInfo, p *inputPart) (*prepedInputPart, []typeMember, error) {
	info, ok := ti[p.sourceType.prefix]
	if !ok {
		ts := getKeys(ti)
		if len(ts) == 0 {
			return nil, nil, fmt.Errorf(`type %q not passed as a parameter`, p.sourceType.prefix)
		} else {
			return nil, nil, fmt.Errorf(`type %q not passed as a parameter, have: %s`, p.sourceType.prefix, strings.Join(ts, ", "))
		}
	}
	switch info := info.(type) {
	case *mapInfo:
		return &prepedInputPart{}, []typeMember{&mapKey{name: p.sourceType.name, mapType: info.typ()}}, nil
	case *structInfo:
		f, ok := info.tagToField[p.sourceType.name]
		if !ok {
			return nil, nil, fmt.Errorf(`type %q has no %q db tag`, info.typ().Name(), p.sourceType.name)
		}
		return &prepedInputPart{}, []typeMember{f}, nil
	default:
		return nil, nil, fmt.Errorf(`internal error: unknown info type: %T`, info)
	}
}

// prepareIn generates the values to fetch for sqlair input expressions following an IN statement.
func prepareIn(ti typeNameToInfo, p *inPart) (*prepedInPart, []typeMember, error) {
	var typeMembers = make([]typeMember, 0)
	for _, t := range p.types {
		info, ok := ti[t.prefix]
		if !ok {
			ts := getKeys(ti)
			if len(ts) == 0 {
				return nil, nil, fmt.Errorf(`type %q not passed as a parameter`, t.prefix)
			} else {
				return nil, nil, fmt.Errorf(`type %q not passed as a parameter, have: %s`, t.prefix, strings.Join(ts, ", "))
			}
		}
		if t.name == "*" {
			return nil, nil, fmt.Errorf("invalid asterisk in input expression: %s", p.raw)
		}
		switch info := info.(type) {
		case *structInfo:
			field, ok := info.tagToField[t.name]
			if !ok {
				return nil, nil, fmt.Errorf(`type %q has no %q db tag`, info.typ().Name(), t.name)
			}
			typeMembers = append(typeMembers, field)
		case *mapInfo:
			typeMembers = append(typeMembers, &mapKey{
				name:         t.name,
				mapType:      info.typ(),
				sliceAllowed: &sliceInfo{length: 1},
			})
		}
	}
	return &prepedInPart{typeMembers}, typeMembers, nil
}

// prepareOutput checks that the output expressions correspond to known types.
// It then checks they are formatted correctly and finally generates the columns for the query.
func prepareOutput(ti typeNameToInfo, p *outputPart) (*prepedOutputPart, []typeMember, error) {
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
					outCols = append(outCols, fullName{pref, tag})
					typeMembers = append(typeMembers, info.tagToField[tag])
				}
				return &prepedOutputPart{outCols}, typeMembers, nil
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
				return &prepedOutputPart{outCols}, typeMembers, nil
			case *structInfo:
				for _, c := range p.sourceColumns {
					f, ok := info.tagToField[c.name]
					if !ok {
						return nil, nil, fmt.Errorf(`type %q has no %q db tag`, info.typ().Name(), c.name)
					}
					outCols = append(outCols, c)
					typeMembers = append(typeMembers, f)
				}
				return &prepedOutputPart{outCols}, typeMembers, nil
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
		return &prepedOutputPart{outCols}, typeMembers, nil
	}

	// Case 2.2: No columns e.g. "(&P.name, &P.id)".
	for _, t := range p.targetTypes {
		outCols = append(outCols, fullName{name: t.name})
	}
	return &prepedOutputPart{outCols}, typeMembers, nil
}

type typeNameToInfo map[string]typeInfo

type ioCounter struct {
	outputCount int
	inputCount  int
}

type prepedQueryPart interface {
	sql(*ioCounter) string
}

type prepedOutputPart struct {
	outCols []fullName
}

func (oc *prepedOutputPart) sql(c *ioCounter) string {
	var sql bytes.Buffer
	for i, col := range oc.outCols {
		sql.WriteString(col.String())
		sql.WriteString(" AS ")
		sql.WriteString(markerName(c.outputCount))
		if i != len(oc.outCols)-1 {
			sql.WriteString(", ")
		}
		c.outputCount++
	}
	return sql.String()
}

type prepedInputPart struct{}

func (ic *prepedInputPart) sql(c *ioCounter) string {
	c.inputCount++
	return "@sqlair_" + strconv.Itoa(c.inputCount-1)
}

type prepedInPart struct {
	typeMembers []typeMember
}

func (ic *prepedInPart) sql(c *ioCounter) string {
	var sql bytes.Buffer
	sql.WriteString("IN (")
	for i, tm := range ic.typeMembers {
		switch tm := tm.(type) {
		case *structField:
			sql.WriteString("@sqlair_")
			sql.WriteString(strconv.Itoa(c.inputCount))
			c.inputCount++
		case *mapKey:
			length := 1
			if tm.sliceAllowed != nil {
				log.Printf("length is %d", tm.sliceAllowed.length)
				length = tm.sliceAllowed.length
			}
			for j := 0; j < length; j++ {
				sql.WriteString("@sqlair_")
				sql.WriteString(strconv.Itoa(c.inputCount))
				if j < length-1 {
					sql.WriteString(", ")
				}
				c.inputCount++
			}
		default:
			panic(fmt.Sprintf("invalid type: %T", tm))
		}
		if i < len(ic.typeMembers)-1 {
			sql.WriteString(", ")
		}
	}
	sql.WriteString(")")
	return sql.String()
}

type prepedBypassPart struct {
	str string
}

func (bc *prepedBypassPart) sql(*ioCounter) string {
	return bc.str
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

	var outputs = make([]typeMember, 0)
	var inputs = make([]typeMember, 0)

	var prepedQueryParts []prepedQueryPart
	var typeMembers []typeMember
	var prepedQueryPart prepedQueryPart

	// Check and expand each query part.
	for _, part := range pe.queryParts {
		switch p := part.(type) {
		case *inputPart:
			prepedQueryPart, typeMembers, err = prepareInput(ti, p)
			if err != nil {
				return nil, err
			}
			inputs = append(inputs, typeMembers...)
		case *outputPart:
			prepedQueryPart, typeMembers, err = prepareOutput(ti, p)
			if err != nil {
				return nil, err
			}
			outputs = append(outputs, typeMembers...)
		case *inPart:
			prepedQueryPart, typeMembers, err = prepareIn(ti, p)
			if err != nil {
				return nil, err
			}
			inputs = append(inputs, typeMembers...)
		case *bypassPart:
			prepedQueryPart = &prepedBypassPart{p.chunk}
		default:
			return nil, fmt.Errorf("internal error: unknown query part type %T", part)
		}
		prepedQueryParts = append(prepedQueryParts, prepedQueryPart)
	}

	return &PreparedExpr{inputs: inputs, outputs: outputs, prepedQueryParts: prepedQueryParts}, nil
}

func (pe *PreparedExpr) sql() string {
	var c = &ioCounter{}
	var sql bytes.Buffer
	for _, prepedQueryPart := range pe.prepedQueryParts {
		sql.WriteString(prepedQueryPart.sql(c))
	}
	return sql.String()
}
