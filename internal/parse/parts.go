package parse

// A queryPart represents a section of a parsed SQL statement, which forms
// a complete query when processed together with its surrounding parts, in
// their correct order.
type queryPart interface {
	// String returns the part's representation for debugging purposes.
	String() string

	// ToSQL returns the SQL representation of the part.
	ToSQL() string
}

// FullName represents a table column or a Go type identifier.
type FullName struct {
	Prefix, Name string
}

// InputPart represents a named parameter that will be sent to the database
// while performing the query.
type InputPart struct {
	Source FullName
}

// String returns a textual representation of the InputPart meant for debugging
// purposes.
func (p *InputPart) String() string {
	return ""
}

// ToSQL returns a string with the SQL translation for the InputPart.
func (p *InputPart) ToSQL() string {
	return ""
}

// OutputPart represents an expression to be used as output in our SDL.
type OutputPart struct {
	Source FullName
	Target FullName
}

// String returns a textual representation of the OutputPart meant for debugging
// purposes.
func (p *OutputPart) String() string {
	return ""
}

// ToSQL returns a string with the SQL translation for the OutputPart.
func (p *OutputPart) ToSQL() string {
	return ""
}

// BypassPart represents a part of the SDL that we want to pass to the
// backend database verbatim.
type BypassPart struct {
	Chunk string
}

// String returns a textual representation of the BypassPart meant for debugging
// purposes.
func (p *BypassPart) String() string {
	return ""
}

// ToSQL returns a string with the SQL translation for the BypassPart.
func (p *BypassPart) ToSQL() string {
	return ""
}
