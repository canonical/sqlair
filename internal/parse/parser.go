package parse

// QueryPart defines a simple interface for all the different parts that
// compose a ParsedExpr.
type QueryPart interface {
	// String returns the original string comprising this query part.
	String() string

	// ToSQL returns the executable SQL resulting from this query part.
	ToSQL() (string, error)
}

// Parser is used to parse the SQLAir DSL.
type Parser struct {
	// input is the DSL statement to be parted.
	input string

	// lastParsedPos is the character position of the last parsed part.
	lastParsedPos int

	// pos is the current character position of the parser.
	pos int
}

// NewParser returns a reference to a new parser.
func NewParser() *Parser {
	return &Parser{}
}

// init initializes the parser.
func (p *Parser) init(input string) {
	p.input = input
	p.lastParsedPos = 0
	p.pos = 0
}

func (p *Parser) Parse(input string) (*ParsedExpr, error) {
	p.init(input)
	return nil, nil
}

// ParsedExpr represents a parsed expression.
// It has a representation of the original SQL statement in terms of QueryParts
// A SQL statement like this:
//
// Select p.* as &Person.* from person where p.name = $Boss.Name
//
// would be represented as:
//
// [stringPart outputPart stringPart inputPart]
type ParsedExpr struct {
	queryParts []QueryPart
}
