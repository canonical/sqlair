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

// BypassPart represents a part of the SDL that we want to pass to the
// backend database verbatim.
type BypassPart struct {
	Chunk string
}

func (p *BypassPart) String() string {
	return "BypassPart[" + p.Chunk + "]"
}

func (p *BypassPart) ToSQL() string {
	return ""
}
