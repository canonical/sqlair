package expr

import (
	"fmt"
)

// A queryPart represents a section of a parsed SQL statement. The parsed query
// is represented as a list of queryParts.
type queryPart interface {
	// String returns a string representation of the part for debugging and
	// testing purposes.
	String() string

	// part is a marker method.
	part()
}

// fullName represents a table column or a Go type identifier.
type fullName struct {
	prefix, name string
}

func (fn fullName) String() string {
	if fn.prefix == "" {
		return fn.name
	} else if fn.name == "" {
		return fn.prefix
	}
	return fn.prefix + "." + fn.name
}

// inputPart represents a parsed SQLair input expression.
type inputPart struct {
	// sourceType specifies the name of a struct and one a db tag of one of its
	// fields, or map and key. The input parameter will be fetched from the
	// specified place when the query is created.
	sourceType fullName
	raw        string
}

func (p *inputPart) String() string {
	return fmt.Sprintf("Input[%+v]", p.sourceType)
}

// Marker function for queryPart.
func (p *inputPart) part() {}

// outputPart represents a parsed SQLair output expression.
type outputPart struct {
	// sourceColumns is the parsed list of columns of the output expression.
	sourceColumns []fullName

	// targetTypes is the parsed list of types that the query results will be
	// scanned into.
	targetTypes []fullName
	raw         string
}

func (p *outputPart) String() string {
	return fmt.Sprintf("Output[%+v %+v]", p.sourceColumns, p.targetTypes)
}

// Marker function for queryPart.
func (p *outputPart) part() {}

// bypassPart represents a part of the expression that is not touched by SQLair
// and is passed to the backend database verbatim.
type bypassPart struct {
	chunk string
}

func (p *bypassPart) String() string {
	return "Bypass[" + p.chunk + "]"
}

// Marker function for queryPart.
func (p *bypassPart) part() {}
