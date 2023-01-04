package parse_test

import (
	"github.com/canonical/sqlair/internal/parse"
	. "gopkg.in/check.v1"
)

type ParserSuite struct{}

var _ = Suite(&ParserSuite{})

func (s *ParserSuite) TestValidInput(c *C) {
	testList := []struct {
		input          string
		expectedParsed string
	}{{
		"SELECT street FROM t WHERE x = $Address.street",
		"ParsedExpr[BypassPart[SELECT street FROM t WHERE x = ] " +
			"InputPart[Address.street]]",
	}, {
		"SELECT p FROM t WHERE x = $Person.id",
		"ParsedExpr[BypassPart[SELECT p FROM t WHERE x = ] " +
			"InputPart[Person.id]]",
	}}
	for _, test := range testList {
		parser := parse.NewParser()
		parsedExpr, err := parser.Parse(test.input)
		c.Log(err)
		c.Assert(parsedExpr.String(), Equals, test.expectedParsed)
	}
}

// We return a proper error when we find an unbound string literal
func (s *ParserSuite) TestUnfinishedStringLiteral(c *C) {
	testList := []string{
		"SELECT foo FROM t WHERE x = 'dddd",
		"SELECT foo FROM t WHERE x = \"dddd",
		"SELECT foo FROM t WHERE x = \"dddd'",
	}

	for _, sql := range testList {
		parser := parse.NewParser()
		expr, err := parser.Parse(sql)
		c.Assert(err, ErrorMatches, "cannot parse expression: missing right quote in string literal")
		c.Assert(expr, IsNil)
	}
}

// Properly parsing empty string literal
func (s *ParserSuite) TestEmptyStringLiteral(c *C) {
	sql := "SELECT foo FROM t WHERE x = ''"
	parser := parse.NewParser()
	_, err := parser.Parse(sql)
	c.Assert(err, IsNil)
}

// Detect bad escaped string literal
func (s *ParserSuite) TestBadEscaped(c *C) {
	sql := "SELECT foo FROM t WHERE x = 'O'Donnell'"
	parser := parse.NewParser()
	_, err := parser.Parse(sql)
	c.Assert(err, ErrorMatches, "cannot parse expression: missing right quote in string literal")
}

func (s *ParserSuite) TestBadFormatInput(c *C) {
	testListInvalidId := []string{
		"SELECT foo FROM t WHERE x = $Address.",
		"SELECT foo FROM t WHERE x = $Address.&d",
		"SELECT foo FROM t WHERE x = $Address.-",
	}

	for _, sql := range testListInvalidId {
		parser := parse.NewParser()
		expr, err := parser.Parse(sql)
		c.Assert(err, ErrorMatches, "cannot parse expression: invalid identifier near char 37")
		c.Assert(expr, IsNil)
	}

	sql := "SELECT foo FROM t WHERE x = $Address"
	parser := parse.NewParser()
	_, err := parser.Parse(sql)
	c.Assert(err, ErrorMatches, "cannot parse expression: go object near char 36 not qualified")
}
