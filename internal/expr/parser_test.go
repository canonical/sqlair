package expr

import (
	"testing"

	. "gopkg.in/check.v1"
)

type ParserSuite struct{}

var _ = Suite(&ParserSuite{})

type Address struct {
	ID int `db:"id"`
}

type Person struct {
	ID         int    `db:"id"`
	Fullname   string `db:"name"`
	PostalCode int    `db:"address_id"`
}

type Manager struct {
	Name string `db:"manager_name"`
}

type District struct {
}

type M map[string]any

var tests = []struct {
	summary        string
	input          string
	expectedParsed string
}{{
	"star table as output",
	"SELECT p.* AS &Person.*",
	"ParsedExpr[bypassPart[SELECT p.* AS &Person.*]]",
}, {
	"quoted output expression",
	"SELECT p.* AS &Person.*, '&notAnOutputExpresion.*' AS literal FROM t",
	"ParsedExpr[bypassPart[SELECT p.* AS &Person.*, ] " +
		"bypassPart['&notAnOutputExpresion.*'] " +
		"bypassPart[ AS literal FROM t]]",
}, {
	"star as output",
	"SELECT * AS &Person.* FROM t",
	"ParsedExpr[bypassPart[SELECT * AS &Person.* FROM t]]",
}, {
	"input v1",
	"SELECT foo, bar FROM table WHERE foo = $Person.id",
	"ParsedExpr[bypassPart[SELECT foo, bar FROM table WHERE foo = ] " +
		"inputPart[Person.id]]",
}, {
	"input v2",
	"SELECT p FROM person WHERE p.name = $Person.name",
	"ParsedExpr[bypassPart[SELECT p FROM person WHERE p.name = ] inputPart[Person.name]]",
}, {
	"input v3",
	"SELECT p.*, a.district " +
		"FROM person AS p JOIN address AS a ON p.address_id = a.id " +
		"WHERE p.name = $Person.name",
	"ParsedExpr[bypassPart[SELECT p.*, a.district FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = ] " +
		"inputPart[Person.name]]",
}, {
	"output and input",
	"SELECT &Person FROM table WHERE foo = $Address.id",
	"ParsedExpr[bypassPart[SELECT &Person FROM table WHERE foo = ] " +
		"inputPart[Address.id]]",
}, {
	"star output and input",
	"SELECT &Person.* FROM table WHERE foo = $Address.id",
	"ParsedExpr[bypassPart[SELECT &Person.* FROM table WHERE foo = ] " +
		"inputPart[Address.id]]",
}, {
	"output and quote",
	"SELECT foo, bar, &Person.id FROM table WHERE foo = 'xx'",
	"ParsedExpr[bypassPart[SELECT foo, bar, &Person.id FROM table WHERE foo = ] " +
		"bypassPart['xx']]",
}, {
	"two outputs and quote",
	"SELECT foo, &Person.id, bar, baz, &Manager.name FROM table WHERE foo = 'xx'",
	"ParsedExpr[bypassPart[SELECT foo, &Person.id, bar, baz, &Manager.name FROM table WHERE foo = ] " +
		"bypassPart['xx']]",
}, {
	"star as output and quote",
	"SELECT * AS &Person.* FROM person WHERE name = 'Fred'",
	"ParsedExpr[bypassPart[SELECT * AS &Person.* FROM person WHERE name = ] " +
		"bypassPart['Fred']]",
}, {
	"star output and quote",
	"SELECT &Person.* FROM person WHERE name = 'Fred'",
	"ParsedExpr[bypassPart[SELECT &Person.* FROM person WHERE name = ] " +
		"bypassPart['Fred']]",
}, {
	"two star as outputs and quote",
	"SELECT * AS &Person.*, a.* AS &Address.* FROM person, address a WHERE name = 'Fred'",
	"ParsedExpr[bypassPart[SELECT * AS &Person.*, a.* AS &Address.* FROM person, address a WHERE name = ] " +
		"bypassPart['Fred']]",
}, {
	"multicolumn output and quote",
	"SELECT (a.district, a.street) AS &Address.* FROM address AS a WHERE p.name = 'Fred'",
	"ParsedExpr[bypassPart[SELECT (a.district, a.street) AS &Address.* FROM address AS a WHERE p.name = ] bypassPart['Fred']]",
}, {
	"quote",
	"SELECT 1 FROM person WHERE p.name = 'Fred'",
	"ParsedExpr[bypassPart[SELECT 1 FROM person WHERE p.name = ] " +
		"bypassPart['Fred']]",
}, {
	"complex query v1",
	"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.*, " +
		"(5+7), (col1 * col2) AS calculated_value FROM person AS p " +
		"JOIN address AS a ON p.address_id = a.id WHERE p.name = 'Fred'",
	"ParsedExpr[bypassPart[SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.*, (5+7), (col1 * col2) AS calculated_value FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = ] " +
		"bypassPart['Fred']]",
}, {
	"complex query v2",
	"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* " +
		"FROM person AS p JOIN address AS a ON p .address_id = a.id " +
		"WHERE p.name = 'Fred'",
	"ParsedExpr[bypassPart[SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.*" +
		" FROM person AS p JOIN address AS a ON p .address_id = a.id WHERE p.name = ] " +
		"bypassPart['Fred']]",
}, {
	"complex query v3",
	"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* " +
		"FROM person AS p JOIN address AS a ON p.address_id = a.id " +
		"WHERE p.name IN (SELECT name FROM table WHERE table.n = $Person.name)",
	"ParsedExpr[bypassPart[SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* " +
		"FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name IN (SELECT name FROM table WHERE table.n = ] " +
		"inputPart[Person.name] " +
		"bypassPart[)]]",
}, {
	"complex query v4",
	"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* " +
		"FROM person WHERE p.name IN (SELECT name FROM table " +
		"WHERE table.n = $Person.name) UNION " +
		"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* " +
		"FROM person WHERE p.name IN " +
		"(SELECT name FROM table WHERE table.n = $Person.name)",
	"ParsedExpr[bypassPart[SELECT p.* AS &Person.*, " +
		"(a.district, a.street) AS &Address.* " +
		"FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = ] " +
		"inputPart[Person.name] " +
		"bypassPart[) UNION SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* " +
		"FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = ] " +
		"inputPart[Person.name] " +
		"bypassPart[)]]",
}, {
	"complex query v5",
	"SELECT p.* AS &Person, a.District AS &District " +
		"FROM person AS p JOIN address AS a ON p.address_id = a.id " +
		"WHERE p.name = $Person.name AND p.address_id = $Person.address_id",
	"ParsedExpr[bypassPart[SELECT p.* AS &Person, a.District AS &District " +
		"FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = ] " +
		"inputPart[Person.name] " +
		"bypassPart[ AND p.address_id = ] " +
		"inputPart[Person.address_id]]",
}, {
	"complex query v6",
	"SELECT p.* AS &Person, a.District AS &District " +
		"FROM person AS p INNER JOIN address AS a " +
		"ON p.address_id = $Address.id " +
		"WHERE p.name = $Person.name AND p.address_id = $Person.address_id",
	"ParsedExpr[bypassPart[SELECT p.* AS &Person, a.District AS &District " +
		"FROM person AS p INNER JOIN address AS a ON p.address_id = ] " +
		"inputPart[Address.id] " +
		"bypassPart[ WHERE p.name = ] " +
		"inputPart[Person.name] " +
		"bypassPart[ AND p.address_id = ] " +
		"inputPart[Person.address_id]]",
}, {
	"join v1",
	"SELECT p.* AS &Person.*, m.* AS &Manager.* " +
		"FROM person AS p JOIN person AS m " +
		"ON p.manager_id = m.id WHERE p.name = 'Fred'",
	"ParsedExpr[bypassPart[SELECT p.* AS &Person.*, m.* AS &Manager.* " +
		"FROM person AS p JOIN person AS m ON p.manager_id = m.id WHERE p.name = ] " +
		"bypassPart['Fred']]",
}, {
	"join v2",
	"SELECT person.*, address.district FROM person JOIN address " +
		"ON person.address_id = address.id WHERE person.name = 'Fred'",
	"ParsedExpr[bypassPart[SELECT person.*, address.district FROM person JOIN address ON person.address_id = address.id WHERE person.name = ] " +
		"bypassPart['Fred']]",
}, {
	"insert",
	"INSERT INTO person (name) VALUES $Person.name",
	"ParsedExpr[bypassPart[INSERT INTO person (name) VALUES ] " +
		"inputPart[Person.name]]",
}, {
	"ignore dollar v1",
	"SELECT $ FROM moneytable",
	"ParsedExpr[bypassPart[SELECT $ FROM moneytable]]",
}, {
	"ignore dollar v2",
	"SELECT foo FROM data$",
	"ParsedExpr[bypassPart[SELECT foo FROM data$]]",
}, {
	"ignore dollar v3",
	"SELECT dollerrow$ FROM moneytable",
	"ParsedExpr[bypassPart[SELECT dollerrow$ FROM moneytable]]",
}, {
	"input with no space",
	"SELECT p.*, a.district " +
		"FROM person AS p WHERE p.name=$Person.name",
	"ParsedExpr[bypassPart[SELECT p.*, a.district FROM person AS p WHERE p.name=] " +
		"inputPart[Person.name]]",
}, {
	"update",
	"UPDATE person SET person.address_id = $Address.id " +
		"WHERE person.id = $Person.id",
	"ParsedExpr[bypassPart[UPDATE person SET person.address_id = ] " +
		"inputPart[Address.id] " +
		"bypassPart[ WHERE person.id = ] " +
		"inputPart[Person.id]]",
}}

func (s *ParserSuite) TestRound(c *C) {
	parser := NewParser()
	for i, test := range tests {
		var parsedExpr *ParsedExpr
		var err error
		if parsedExpr, err = parser.Parse(test.input); err != nil {
			c.Errorf("test %d failed (Parse):\nsummary: %s\ninput: %s\nexpected: %s\nerr: %s\n", i, test.summary, test.input, test.expectedParsed, err)
		} else if parsedExpr.String() != test.expectedParsed {
			c.Errorf("test %d failed (Parse):\nsummary: %s\ninput: %s\nexpected: %s\nactual:   %s\n", i, test.summary, test.input, test.expectedParsed, parsedExpr.String())
		}
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
		parser := NewParser()
		expr, err := parser.Parse(sql)
		c.Assert(err, ErrorMatches, "cannot parse expression: missing right quote in string literal")
		c.Assert(expr, IsNil)
	}
}

// Properly parsing empty string literal
func (s *ParserSuite) TestEmptyStringLiteral(c *C) {
	sql := "SELECT foo FROM t WHERE x = ''"
	parser := NewParser()
	_, err := parser.Parse(sql)
	c.Assert(err, IsNil)
}

// Detect bad escaped string literal
func (s *ParserSuite) TestBadEscaped(c *C) {
	sql := "SELECT foo FROM t WHERE x = 'O'Donnell'"
	parser := NewParser()
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
		parser := NewParser()
		expr, err := parser.Parse(sql)
		c.Assert(err, ErrorMatches, "cannot parse expression: invalid identifier near char 37")
		c.Assert(expr, IsNil)
	}

	sql := "SELECT foo FROM t WHERE x = $Address"
	parser := NewParser()
	_, err := parser.Parse(sql)
	c.Assert(err, ErrorMatches, "cannot parse expression: go object near char 36 not qualified")
}

func FuzzParser(f *testing.F) {
	// Add some values to the corpus
	for _, test := range tests {
		f.Add(test.input)
	}
	f.Fuzz(func(t *testing.T, s string) {
		// Loop forever or until it crashes
		parser := NewParser()
		parser.Parse(s)
	})
}
