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
	outputs   []typeMember
	inputs    []typeMember
	sqlChunks []sqlChunk
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
		return fmt.Errorf("invalid asterisk in output expression: %s", p.raw)
	}
	if !starTarget && (numSources > 0 && (numTargets != numSources)) {
		return fmt.Errorf("mismatched number of columns and targets in output expression: %s", p.raw)
	}
	return nil
}

// prepareInput checks that the input expression corresponds to a known type.
func prepareInput(ti typeNameToInfo, p *inputPart) (*inputChunk, []typeMember, error) {
	info, ok := ti[p.source.prefix]
	if !ok {
		ts := getKeys(ti)
		if len(ts) == 0 {
			return nil, nil, fmt.Errorf(`type %q not passed as a parameter`, p.source.prefix)
		} else {
			return nil, nil, fmt.Errorf(`type %q not passed as a parameter, have: %s`, p.source.prefix, strings.Join(ts, ", "))
		}
	}
	switch info := info.(type) {
	case *mapInfo:
		return &inputChunk{}, []typeMember{&mapKey{name: p.source.name, mapType: info.typ()}}, nil
	case *structInfo:
		f, ok := info.tagToField[p.source.name]
		if !ok {
			return nil, nil, fmt.Errorf(`type %q has no %q db tag`, info.typ().Name(), p.source.name)
		}
		return &inputChunk{}, []typeMember{f}, nil
	default:
		return nil, nil, fmt.Errorf(`internal error: unknown info type: %T`, info)
	}
}

// prepareIn generates the values to fetch for sqlair input expressions following an IN statement.
func prepareIn(ti typeNameToInfo, p *inPart) (*inChunk, []typeMember, error) {
	var typeMembers = make([]typeMember, 0)
	for _, t := range p.types {
		info, ok := ti[t.prefix]
		if !ok {
			return nil, nil, fmt.Errorf(`type %q not passed as a parameter, have: %s`, t.prefix, strings.Join(getKeys(ti), ", "))
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
				name:    t.name,
				mapType: info.typ(),
				slice:   &sliceInfo{length: 1},
			})
		}
	}
	return &inChunk{typeMembers}, typeMembers, nil
}

// prepareOutput checks that the output expressions correspond to known types.
// It then checks they are formatted correctly and finally generates the columns for the query.
func prepareOutput(ti typeNameToInfo, p *outputPart) (*outputChunk, []typeMember, error) {
	var outCols = make([]fullName, 0)
	var typeMembers = make([]typeMember, 0)

	// Check the asterisks are well formed (if present).
	if err := starCheckOutput(p); err != nil {
		return nil, nil, err
	}

	// Check target struct type and its tags are valid.
	var info typeInfo
	var ok bool

	for _, t := range p.target {
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

	// Case 1: Star target cases e.g. "...&P.*".
	if p.target[0].name == "*" {
		info, _ := ti[p.target[0].prefix]
		// Case 1.1: Single star i.e. "t.* AS &P.*" or "&P.*"
		if len(p.source) == 0 || p.source[0].name == "*" {
			switch info := info.(type) {
			case *mapInfo:
				return nil, nil, fmt.Errorf(`&%s.* cannot be used for maps when no column names are specified`, info.typ().Name())
			case *structInfo:
				pref := ""

				// Prepend table name. E.g. "t" in "t.* AS &P.*".
				if len(p.source) > 0 {
					pref = p.source[0].prefix
				}

				for _, tag := range info.tags {
					outCols = append(outCols, fullName{pref, tag})
					typeMembers = append(typeMembers, info.tagToField[tag])
				}
				return &outputChunk{outCols}, typeMembers, nil
			default:
				return nil, nil, fmt.Errorf(`internal error: unknown info type: %T`, info)
			}
		}

		// Case 1.2: Explicit columns e.g. "(col1, t.col2) AS &P.*".
		if len(p.source) > 0 {
			switch info := info.(type) {
			case *mapInfo:
				for _, c := range p.source {
					outCols = append(outCols, c)
					typeMembers = append(typeMembers, &mapKey{name: c.name, mapType: info.typ()})
				}
				return &outputChunk{outCols}, typeMembers, nil
			case *structInfo:
				for _, c := range p.source {
					f, ok := info.tagToField[c.name]
					if !ok {
						return nil, nil, fmt.Errorf(`type %q has no %q db tag`, info.typ().Name(), c.name)
					}
					outCols = append(outCols, c)
					typeMembers = append(typeMembers, f)
				}
				return &outputChunk{outCols}, typeMembers, nil
			default:
				return nil, nil, fmt.Errorf(`internal error: unknown info type: %T`, info)
			}
		}
	}

	// Case 2: None star target cases e.g. "...(&P.name, &P.id)".

	// Case 2.1: Explicit columns e.g. "name_1 AS P.name".
	if len(p.source) > 0 {
		for _, c := range p.source {
			outCols = append(outCols, c)
		}
		return &outputChunk{outCols}, typeMembers, nil
	}

	// Case 2.2: No columns e.g. "(&P.name, &P.id)".
	for _, t := range p.target {
		outCols = append(outCols, fullName{name: t.name})
	}
	return &outputChunk{outCols}, typeMembers, nil
}

type typeNameToInfo map[string]typeInfo

type sqlChunk interface {
	chunk()
}

type outputChunk struct {
	outCols []fullName
}

func (outputChunk) chunk() {}

type inputChunk struct{}

func (inputChunk) chunk() {}

type inChunk struct {
	typeMembers []typeMember
}

func (inChunk) chunk() {}

type bypassChunk struct {
	str string
}

func (bypassChunk) chunk() {}

func generateInputSQL(inCount int) (int, string) {
	return inCount + 1, "@sqlair_" + strconv.Itoa(inCount)
}

func generateOutputSQL(outCount int, outCols []fullName) (int, string) {
	var sql bytes.Buffer
	for i, c := range outCols {
		sql.WriteString(c.String())
		sql.WriteString(" AS ")
		sql.WriteString(markerName(outCount))
		if i != len(outCols)-1 {
			sql.WriteString(", ")
		}
		outCount++
	}
	return outCount, sql.String()
}

func generateInSQL(inCount int, typeMembers []typeMember) (int, string) {
	var sql bytes.Buffer
	sql.WriteString("IN (")
	for i, tm := range typeMembers {
		switch tm := tm.(type) {
		case *structField:
			sql.WriteString("@sqlair_")
			sql.WriteString(strconv.Itoa(inCount))
			if i < len(typeMembers)-1 {
				sql.WriteString(", ")
			}
			inCount++
		case *mapKey:
			length := 1
			if tm.slice != nil {
				log.Printf("length is %d", tm.slice.length)
				length = tm.slice.length
			}
			for j := 0; j < length; j++ {
				sql.WriteString("@sqlair_")
				sql.WriteString(strconv.Itoa(inCount))
				if i < len(typeMembers)-1 || j < length-1 {
					sql.WriteString(", ")
				}
				inCount++
			}
		default:
			panic(fmt.Sprintf("%T", tm))
		}
	}
	sql.WriteString(")")
	return inCount, sql.String()
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

	var sqlChunks []sqlChunk
	var typeMembers []typeMember
	var chunk sqlChunk

	// Check and expand each query part.
	for _, part := range pe.queryParts {
		switch p := part.(type) {
		case *inputPart:
			chunk, typeMembers, err = prepareInput(ti, p)
			if err != nil {
				return nil, err
			}
			inputs = append(inputs, typeMembers...)
		case *outputPart:
			chunk, typeMembers, err = prepareOutput(ti, p)
			if err != nil {
				return nil, err
			}
			outputs = append(outputs, typeMembers...)
		case *inPart:
			chunk, typeMembers, err = prepareIn(ti, p)
			if err != nil {
				return nil, err
			}
			inputs = append(inputs, typeMembers...)
		case *bypassPart:
			chunk = &bypassChunk{p.chunk}
		default:
			return nil, fmt.Errorf("internal error: unknown query part type %T", part)
		}
		sqlChunks = append(sqlChunks, chunk)
	}

	return &PreparedExpr{inputs: inputs, outputs: outputs, sqlChunks: sqlChunks}, nil
}

func (pe *PreparedExpr) sql() string {
	var inCount int
	var outCount int
	var s string
	var sql bytes.Buffer

	for _, chunk := range pe.sqlChunks {
		switch chunk := chunk.(type) {
		case *outputChunk:
			outCount, s = generateOutputSQL(outCount, chunk.outCols)
		case *inputChunk:
			inCount, s = generateInputSQL(inCount)
		case *inChunk:
			inCount, s = generateInSQL(inCount, chunk.typeMembers)
		case *bypassChunk:
			s = chunk.str
		default:
			panic("not valid type")
		}
		sql.WriteString(s)
	}
	return sql.String()
}
