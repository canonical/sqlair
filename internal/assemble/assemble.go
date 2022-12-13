package assemble

import (
	"fmt"
	"strings"

	"github.com/canonical/sqlair/internal/parse"
	"github.com/canonical/sqlair/internal/typeinfo"
)

type AssembledExpr struct {
	Parsed *parse.ParsedExpr
	SQL    string
}

type typeNameToInfo map[string]*typeinfo.Info

// assembleInput prepares an input part. Since input parts do not need any
// special transformations, this function just checks that the used name
// corresponds to a tag in the struct. Returns an error otherwise.
func assembleInput(ti typeNameToInfo, p *parse.InputPart) error {
	if inf, ok := ti[p.Source.Prefix]; ok {
		if _, ok := inf.TagToField[p.Source.Name]; ok {
			return nil
		}
		return fmt.Errorf("there is no tag with name %s in %s",
			p.Source.Name, inf.Type.Name())
	}
	return fmt.Errorf("unknown type: %s", p.Source.Prefix)
}

// Assemble prepares a parsed expression.
// Returns a pointer to AssembledExpr on success.
// Returns nil and an error otherwise.
func Assemble(pe *parse.ParsedExpr, args ...any) (expr *AssembledExpr, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("cannot assemble expression: %s", err)
		}
	}()

	var ti = make(typeNameToInfo)

	// Generate and save reflection info.
	for _, arg := range args {
		i, err := typeinfo.TypeInfo(arg)
		if err != nil {
			return nil, err
		}
		ti[i.Type.Name()] = i
	}

	sql := ""

	// Check and expand each query part.
	for _, part := range pe.QueryParts {
		if p, ok := part.(*parse.InputPart); ok {
			err := assembleInput(ti, p)
			if err != nil {
				return nil, err
			}
			sql = sql + p.ToSQL([]string{})
			continue
		}
		if p, ok := part.(*parse.OutputPart); ok {
			sql = sql + p.ToSQL([]string{"DUMMY_OUTPUT"})
			continue
		}
		p := part.(*parse.BypassPart)
		sql = sql + p.ToSQL([]string{})
	}

	sql = strings.TrimSpace(sql)
	// We will probably need to save the outcols and in cols.
	return &AssembledExpr{Parsed: pe, SQL: sql}, nil
}
