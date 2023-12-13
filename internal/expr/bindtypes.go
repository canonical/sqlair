package expr

import (
	"bytes"
	"fmt"

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
func (pe *ParsedExpr) BindTypes(args ...any) (tbe *TypeBoundExpr, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("cannot prepare statement: %s", err)
		}
	}()

	argInfo, err := typeinfo.GenerateArgInfo(args)
	if err != nil {
		return nil, err
	}

	// Bind types to each expression.
	typedExprs := []any{}
	outputUsed := map[typeinfo.Output]bool{}
	var typedExpr any
	for _, expr := range pe.exprs {
		typedExpr, err = expr.bindTypes(argInfo)
		if err != nil {
			return nil, err
		}
		if toe, ok := typedExpr.(*typedOutputExpr); ok {
			for _, oc := range toe.outputColumns {
				if ok := outputUsed[oc.output]; ok {
					return nil, fmt.Errorf("%s appears more than once in output expressions", oc.output.String())
				}
				outputUsed[oc.output] = true
			}
		}
		typedExprs = append(typedExprs, typedExpr)
	}

	typeBoundExpr := TypeBoundExpr(typedExprs)
	return &typeBoundExpr, nil
}

// expression represents a parsed node of the SQLair query's AST.
type expression interface {
	// String returns a text representation for debugging and testing purposes.
	String() string

	// bindTypes generates a typed expression from the query argument type information.
	bindTypes(typeinfo.ArgInfo) (any, error)
}

// inputExpr represents a named parameter that will be sent to the database
// while performing the query.
type inputExpr struct {
	sourceType valueAccessor
	raw        string
}

func (p *inputExpr) String() string {
	return fmt.Sprintf("Input[%+v]", p.sourceType)
}

// bindTypes binds the input expression to a query type and returns a typed
// input expression.
func (e *inputExpr) bindTypes(argInfo typeinfo.ArgInfo) (typedExpr any, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("input expression: %s: %s", err, e.raw)
		}
	}()

	input, err := argInfo.InputMember(e.sourceType.typeName, e.sourceType.memberName)
	if err != nil {
		return nil, err
	}
	return &typedInputExpr{input}, nil
}

// outputExpr represents a named target output variable in the SQL expression,
// as well as the source table and column where it will be read from.
type outputExpr struct {
	sourceColumns []columnAccessor
	targetTypes   []valueAccessor
	raw           string
}

func (p *outputExpr) String() string {
	return fmt.Sprintf("Output[%+v %+v]", p.sourceColumns, p.targetTypes)
}

// bindTypes binds the output expression to concrete types. It then checks the
// expression valid with respect to its bound types and generates a typed
// output expression.
func (e *outputExpr) bindTypes(argInfo typeinfo.ArgInfo) (typedExpr any, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("output expression: %s: %s", err, e.raw)
		}
	}()

	numTypes := len(e.targetTypes)
	numColumns := len(e.sourceColumns)
	starTypes := starCountTypes(e.targetTypes)
	starColumns := starCountColumns(e.sourceColumns)

	toe := &typedOutputExpr{}

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
				outputs, memberNames, err := argInfo.AllStructOutputs(t.typeName)
				if err != nil {
					return nil, err
				}
				for i, output := range outputs {
					oc := newOutputColumn(pref, memberNames[i], output)
					toe.outputColumns = append(toe.outputColumns, oc)
				}
			} else {
				// Generate explicit columns.
				output, err := argInfo.OutputMember(t.typeName, t.memberName)
				if err != nil {
					return nil, err
				}
				oc := newOutputColumn(pref, t.memberName, output)
				toe.outputColumns = append(toe.outputColumns, oc)
			}
		}
		return toe, nil
	} else if numColumns > 1 && starColumns > 0 {
		return nil, fmt.Errorf("invalid asterisk in columns")
	}

	// Case 2: Explicit columns, single asterisk type e.g. "(col1, t.col2) AS &P.*".
	if starTypes == 1 && numTypes == 1 {
		for _, c := range e.sourceColumns {
			output, err := argInfo.OutputMember(e.targetTypes[0].typeName, c.columnName)
			if err != nil {
				return nil, err
			}
			oc := newOutputColumn(c.tableName, c.columnName, output)
			toe.outputColumns = append(toe.outputColumns, oc)
		}
		return toe, nil
	} else if starTypes > 0 && numTypes > 1 {
		return nil, fmt.Errorf("invalid asterisk in types")
	}

	// Case 3: Explicit columns and types e.g. "(col1, col2) AS (&P.name, &P.id)".
	if numColumns == numTypes {
		for i, c := range e.sourceColumns {
			t := e.targetTypes[i]
			output, err := argInfo.OutputMember(t.typeName, t.memberName)
			if err != nil {
				return nil, err
			}
			oc := newOutputColumn(c.tableName, c.columnName, output)
			toe.outputColumns = append(toe.outputColumns, oc)
		}
	} else {
		return nil, fmt.Errorf("mismatched number of columns and target types")
	}

	return toe, nil
}

// bypass represents part of the expression that we want to pass to the backend
// database verbatim.
type bypass struct {
	chunk string
}

func (b *bypass) String() string {
	return "Bypass[" + b.chunk + "]"
}

// bindTypes is part of the expression interface. bypass expressions have no
// types so the same expression is returned.
func (b *bypass) bindTypes(argInfo typeinfo.ArgInfo) (any, error) {
	return b, nil
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
