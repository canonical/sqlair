package parse

import (
	"bytes"
	"fmt"
)

// A QueryPart represents a section of a parsed SQL statement, which forms
// a complete query when processed together with its surrounding parts, in
// their correct order.
type QueryPart interface {
	// String returns the part's representation for debugging purposes.
	String() string

	// ToSQL returns the SQL representation of the part.
	ToSQL([]string) string
}

// FullName represents a table column or a Go type identifier.
type FullName struct {
	Prefix, Name string
}

func (fn FullName) String() string {
	if fn.Prefix == "" {
		return fn.Name
	} else if fn.Name == "" {
		return fn.Prefix
	}
	return fn.Prefix + "." + fn.Name
}

// InputPart represents a named parameter that will be sent to the database
// while performing the query.
type InputPart struct {
	Source FullName
}

func (p *InputPart) String() string {
	return fmt.Sprintf("InputPart[%+v]", p.Source)
}

func (p *InputPart) ToSQL([]string) string {
	return "?"
}

// OutputPart represents a named target output variable in the SQL expression,
// as well as the source table and column where it will be read from.
type OutputPart struct {
	Source []FullName
	Target []FullName
}

func (p *OutputPart) String() string {
	return fmt.Sprintf("OutputPart[%+v %+v]", p.Source, p.Target)
}

func (p *OutputPart) ToSQL(cs []string) string {
	var out bytes.Buffer
	out.WriteString(" ")
	for i, c := range cs {
		if i > 0 {
			out.WriteString(", ")
		}
		out.WriteString(c)
	}
	return out.String()
}

// BypassPart represents a part of the expression that we want to pass to the
// backend database verbatim.
type BypassPart struct {
	Chunk string
}

func (p *BypassPart) String() string {
	return "BypassPart[" + p.Chunk + "]"
}

func (p *BypassPart) ToSQL([]string) string {
	return p.Chunk
}
