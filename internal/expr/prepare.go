package expr

import (
	"bytes"
	"fmt"
)

// PreparedExpr contains an SQL expression that is ready for execution.
type PreparedExpr struct {
	ParsedExpr *ParsedExpr
	SQL        string
}

type typeNameToInfo map[string]*info

// prepareInput checks that the input expression corresponds to a known type.
func prepareInput(ti typeNameToInfo, p *inputPart) error {
	inf, ok := ti[p.source.prefix]
	if !ok {
		return fmt.Errorf(`unknown type: "%s"`, p.source.prefix)
	}

	if _, ok = inf.tagToField[p.source.name]; !ok {
		return fmt.Errorf(`no tag with name "%s" in "%s"`,
			p.source.name, inf.structType.Name())
	}

	return nil
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
		inf, err := typeInfo(arg)
		if err != nil {
			return nil, err
		}
		ti[inf.structType.Name()] = inf
	}

	var sql bytes.Buffer
	// Check and expand each query part.
	for _, part := range pe.queryParts {
		if p, ok := part.(*inputPart); ok {
			err := prepareInput(ti, p)
			if err != nil {
				return nil, err
			}
			sql.WriteString(p.toSQL())
			continue
		}
		if p, ok := part.(*outputPart); ok {
			// Do nothing for now.
			sql.WriteString(p.toSQL())
			continue
		}
		p := part.(*bypassPart)
		sql.WriteString(p.toSQL())
	}

	return &PreparedExpr{ParsedExpr: pe, SQL: sql.String()}, nil
}
