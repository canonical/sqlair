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

type FullName struct {
	Prefix, Name string
}

// InputPart represents a named parameter that will be send to the database
// while performing the query.
type InputPart struct {
	FullName
}

func (p *InputPart) String() string {
	return ""
}

func (p *InputPart) ToSQL() string {
	return ""
}

// OutputPart represents an expression to be used as output in our SDL.
type OutputPart struct {
	Source FullName
	Target FullName
}

func (p *OutputPart) String() string {
	return ""
}

func (p *OutputPart) ToSQL() string {
	return ""
}

// BypassPart represents a part of the SDL that we want to pass to the
// backend database verbatim.
type BypassPart struct {
	Chunk string
}

func (p *BypassPart) String() string {
	return ""
}

func (p *BypassPart) ToSQL() string {
	return ""
}
