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
	return "InputPart[" + p.Source.String() + "]"
}

func (p *InputPart) ToSQL() string {
	return ""
}

// OutputPart represents a named target output variable in the SQL expression,
// as well as the source table and column where it will be read from.
type OutputPart struct {
	Source []FullName
	Target []FullName
}

func (p *OutputPart) String() string {
	var sourceString string
	for _, col := range p.Source {
		sourceString = sourceString + col.String() + " "
	}
	if len(sourceString) >= 2 {
		sourceString = sourceString[:len(sourceString)-1]
	}
	var targetString string
	for _, obj := range p.Target {
		targetString = targetString + obj.String() + " "
	}
	if len(targetString) >= 2 {
		targetString = targetString[:len(targetString)-1]
	}
	return "OutputPart[Source:" + sourceString + " Target:" + targetString + "]"
}

func (p *OutputPart) ToSQL() string {
	return ""
}

// BypassPart represents a part of the expression that we want to pass to the
// backend database verbatim.
type BypassPart struct {
	Chunk string
}

func (p *BypassPart) String() string {
	return "BypassPart[" + p.Chunk + "]"
}

func (p *BypassPart) ToSQL() string {
	return p.Chunk
}
