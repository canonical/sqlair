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

func (fn FullName) String() string {
	if fn.Prefix == "" {
		return fn.Name
	} else if fn.Name == "" {
		return fn.Prefix
	}
	return fn.Prefix + "." + fn.Name
}

// InputPart represents a named parameter that will be send to the database
// while performing the query.
type InputPart struct {
	FullName
}

func (p *InputPart) String() string {
	return "InputPart[" + p.FullName.String() + "]"
}

func (p *InputPart) ToSQL() string {
	return ""
}

// OutputPart represents an expression to be used as output in our SDL.
type OutputPart struct {
	Source []FullName
	Target FullName
}

func (p *OutputPart) String() string {
	var colString string
	for _, col := range p.Source {
		colString = colString + col.String() + " "
	}
	if len(colString) >= 2 {
		colString = colString[:len(colString)-1]
	}
	return "OutputPart[Source:" + colString + " Target:" + p.Target.String() + "]"
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
	return "BypassPart[" + p.Chunk + "]"
}

func (p *BypassPart) ToSQL() string {
	return ""
}
