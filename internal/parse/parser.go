package parse

// QueryPart defines a simple interface for all the different parts that
// compose a ParsedExpr
type QueryPart interface {
	String() string         // print for debug
	ToSql() (string, error) // print to sql format
}

// Parser defines the SDL parser
type Parser struct {
	input         string // statement to be parsed
	lastParsedPos int    // position of the last parsed chunk
	pos           int    // current position of the parser
}

// NewParser creates a new parser
func NewParser() *Parser {
	return &Parser{}
}

// init initializes the parser
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
