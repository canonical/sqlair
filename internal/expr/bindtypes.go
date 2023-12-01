package expr

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/canonical/sqlair/internal/typeinfo"
)

// ParsedExpr is the AST representation of SQLair query. It contains only
// information encoded in the SQLair query string.
type ParsedExpr struct {
	exprs []expression
}

// String returns a textual representation of the AST contained in the
// ParsedExpr for debugging and testing purposes.
func (pe *ParsedExpr) String() string {
	var out bytes.Buffer
	out.WriteString("[")
	for i, p := range pe.exprs {
		if i > 0 {
			out.WriteString(" ")
		}
		out.WriteString(p.String())
	}
	out.WriteString("]")
	return out.String()
}

// BindTypes takes samples of all types mentioned in the SQLair expressions of
// the query. The expressions are checked for validity and required information
// is generated from the types.
func (pe *ParsedExpr) BindTypes(args ...any) (te *TypedExpr, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("cannot prepare statement: %s", err)
		}
	}()

	argInfo, err := typeinfo.GenerateArgInfo(args...)
	if err != nil {
		return nil, err
	}

	var sql bytes.Buffer

	var inCount int
	var outCount int

	var outputs = make([]typeinfo.Output, 0)
	var inputs = make([]typeinfo.Input, 0)

	var outputUsed = make(map[typeinfo.Output]bool)

	// Check and expand each query expr.
	for _, expr := range pe.exprs {
		switch e := expr.(type) {
		case *inputExpr:
			inLoc, err := bindInputTypes(argInfo, e)
			if err != nil {
				return nil, err
			}
			sql.WriteString("@sqlair_" + strconv.Itoa(inCount))
			inCount++
			inputs = append(inputs, inLoc)
		case *outputExpr:
			outCols, os, err := bindOutputTypes(argInfo, e)
			if err != nil {
				return nil, err
			}

			for _, o := range os {
				if ok := outputUsed[o]; ok {
					return nil, fmt.Errorf("%s appears more than once in output expressions", o.String())
				}
				outputUsed[o] = true
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
			outputs = append(outputs, os...)
		case *bypass:
			sql.WriteString(e.chunk)
		default:
			return nil, fmt.Errorf("internal error: unknown query expr type %T", expr)
		}
	}

	return &TypedExpr{inputs: inputs, outputs: outputs, sql: sql.String()}, nil
}

// bindInputTypes binds the input expression to a type and returns the
// typeMember represented by the expression.
func bindInputTypes(argInfo typeinfo.ArgInfo, e *inputExpr) (input typeinfo.Input, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("input expression: %s: %s", err, e.raw)
		}
	}()

	input, err = argInfo.InputMember(e.sourceType.typeName, e.sourceType.memberName)
	if err != nil {
		return nil, err
	}
	return input, nil
}

// bindOutputTypes binds the output expression to concrete types. It then checks
// the expression is formatted correctly and generates the columns for the query
// and the typeMembers the columns correspond to.
func bindOutputTypes(argInfo typeinfo.ArgInfo, e *outputExpr) (outCols []columnAccessor, outputs []typeinfo.Output, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("output expression: %s: %s", err, e.raw)
		}
	}()

	numTypes := len(e.targetTypes)
	numColumns := len(e.sourceColumns)
	starTypes := starCountTypes(e.targetTypes)
	starColumns := starCountColumns(e.sourceColumns)

	// Case 1: Generated columns e.g. "* AS (&P.*, &A.id)" or "&P.*".
	if numColumns == 0 || (numColumns == 1 && starColumns == 1) {
		pref := ""
		// Prepend table name. E.g. "t" in "t.* AS &P.*".
		if numColumns > 0 {
			pref = e.sourceColumns[0].tableName
		}

		for _, t := range e.targetTypes {
			if t.memberName == "*" {
				// Generate asterisk columns.
				members, memberNames, err := argInfo.AllStructOutputs(t.typeName)
				if err != nil {
					return nil, nil, err
				}
				outputs = append(outputs, members...)
				for _, memberName := range memberNames {
					outCols = append(outCols, columnAccessor{pref, memberName})
				}
			} else {
				// Generate explicit columns.
				member, err := argInfo.OutputMember(t.typeName, t.memberName)
				if err != nil {
					return nil, nil, err
				}
				outputs = append(outputs, member)
				outCols = append(outCols, columnAccessor{pref, t.memberName})
			}
		}
		return outCols, outputs, nil
	} else if numColumns > 1 && starColumns > 0 {
		return nil, nil, fmt.Errorf("invalid asterisk in columns")
	}

	// Case 2: Explicit columns, single asterisk type e.g. "(col1, t.col2) AS &P.*".
	if starTypes == 1 && numTypes == 1 {
		for _, c := range e.sourceColumns {
			ml, err := argInfo.OutputMember(e.targetTypes[0].typeName, c.columnName)
			if err != nil {
				return nil, nil, err
			}
			outputs = append(outputs, ml)
			outCols = append(outCols, c)
		}
		return outCols, outputs, nil
	} else if starTypes > 0 && numTypes > 1 {
		return nil, nil, fmt.Errorf("invalid asterisk in types")
	}

	// Case 3: Explicit columns and types e.g. "(col1, col2) AS (&P.name, &P.id)".
	if numColumns == numTypes {
		for i, c := range e.sourceColumns {
			t := e.targetTypes[i]
			ml, err := argInfo.OutputMember(t.typeName, t.memberName)
			if err != nil {
				return nil, nil, err
			}
			outputs = append(outputs, ml)
			outCols = append(outCols, c)
		}
	} else {
		return nil, nil, fmt.Errorf("mismatched number of columns and target types")
	}

	return outCols, outputs, nil
}

// starCountColumns counts the number of asterisks in a list of columns.
func starCountColumns(cs []columnAccessor) int {
	s := 0
	for _, c := range cs {
		if c.columnName == "*" {
			s++
		}
	}
	return s
}

// starCountTypes counts the number of asterisks in a list of types.
func starCountTypes(vs []valueAccessor) int {
	s := 0
	for _, v := range vs {
		if v.memberName == "*" {
			s++
		}
	}
	return s
}
