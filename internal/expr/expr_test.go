package expr_test

import (
	"testing"

	"github.com/canonical/sqlair/internal/expr"
	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func TestExpr(t *testing.T) { TestingT(t) }

type ExprSuite struct{}

var _ = Suite(&ExprSuite{})

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
	"[Bypass[SELECT ] Output[[p.*] [Person.*]]]",
}, {
	"quoted output expression",
	"SELECT p.* AS &Person.*, '&notAnOutputExpresion.*' AS literal FROM t",
	"[Bypass[SELECT ] " +
		"Output[[p.*] [Person.*]] " +
		"Bypass[, ] " +
		"Bypass['&notAnOutputExpresion.*'] " +
		"Bypass[ AS literal FROM t]]",
}, {
	"star as output",
	"SELECT * AS &Person.* FROM t",
	"[Bypass[SELECT ] " +
		"Output[[*] [Person.*]] " +
		"Bypass[ FROM t]]",
}, {
	"input v1",
	"SELECT foo, bar FROM table WHERE foo = $Person.id",
	"[Bypass[SELECT foo, bar FROM table WHERE foo = ] " +
		"Input[Person.id]]",
}, {
	"input v2",
	"SELECT p FROM person WHERE p.name = $Person.name",
	"[Bypass[SELECT p FROM person WHERE p.name = ] Input[Person.name]]",
}, {
	"input v3",
	"SELECT p.*, a.district " +
		"FROM person AS p JOIN address AS a ON p.address_id = a.id " +
		"WHERE p.name = $Person.name",
	"[Bypass[SELECT p.*, a.district FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = ] " +
		"Input[Person.name]]",
}, {
	"output and input",
	"SELECT &Person.* FROM table WHERE foo = $Address.id",
	"[Bypass[SELECT ] Output[[] [Person.*]] " +
		"Bypass[ FROM table WHERE foo = ] " +
		"Input[Address.id]]",
}, {
	"star output and input",
	"SELECT &Person.* FROM table WHERE foo = $Address.id",
	"[Bypass[SELECT ] " +
		"Output[[] [Person.*]] " +
		"Bypass[ FROM table WHERE foo = ] " +
		"Input[Address.id]]",
}, {
	"output and quote",
	"SELECT foo, bar, &Person.id FROM table WHERE foo = 'xx'",
	"[Bypass[SELECT foo, bar, ] " +
		"Output[[] [Person.id]] " +
		"Bypass[ FROM table WHERE foo = ] " +
		"Bypass['xx']]",
}, {
	"two outputs and quote",
	"SELECT foo, &Person.id, bar, baz, &Manager.name FROM table WHERE foo = 'xx'",
	"[Bypass[SELECT foo, ] " +
		"Output[[] [Person.id]] " +
		"Bypass[, bar, baz, ] " +
		"Output[[] [Manager.name]] " +
		"Bypass[ FROM table WHERE foo = ] " +
		"Bypass['xx']]",
}, {
	"star as output and quote",
	"SELECT * AS &Person.* FROM person WHERE name = 'Fred'",
	"[Bypass[SELECT ] " +
		"Output[[*] [Person.*]] " +
		"Bypass[ FROM person WHERE name = ] " +
		"Bypass['Fred']]",
}, {
	"star output and quote",
	"SELECT &Person.* FROM person WHERE name = 'Fred'",
	"[Bypass[SELECT ] " +
		"Output[[] [Person.*]] " +
		"Bypass[ FROM person WHERE name = ] " +
		"Bypass['Fred']]",
}, {
	"two star as outputs and quote",
	"SELECT * AS &Person.*, a.* AS &Address.* FROM person, address a WHERE name = 'Fred'",
	"[Bypass[SELECT ] " +
		"Output[[*] [Person.*]] " +
		"Bypass[, ] " +
		"Output[[a.*] [Address.*]] " +
		"Bypass[ FROM person, address a WHERE name = ] " +
		"Bypass['Fred']]",
}, {
	"multicolumn output",
	"SELECT (a.district, a.street) AS (&Address.district, &Address.street) FROM address AS a",
	"[Bypass[SELECT ] " +
		"Output[[a.district a.street] [Address.district Address.street]] " +
		"Bypass[ FROM address AS a]]",
}, {
	"multicolumn output and output",
	"SELECT (a.district, a.street) AS (&Address.district, &Address.street), " +
		"a.id AS &Person.id FROM address AS a",
	"[Bypass[SELECT ] " +
		"Output[[a.district a.street] [Address.district Address.street]] " +
		"Bypass[, ] Output[[a.id] [Person.id]] " +
		"Bypass[ FROM address AS a]]",
}, {
	"multicolumn output and star",
	"SELECT (a.district, a.street) AS (&Address.district, &Address.street), " +
		"&Person.* FROM address AS a",
	"[Bypass[SELECT ] " +
		"Output[[a.district a.street] [Address.district Address.street]] " +
		"Bypass[, ] Output[[] [Person.*]] " +
		"Bypass[ FROM address AS a]]",
}, {
	"multicolumn output and quote",
	"SELECT (a.district, a.street) AS &Address.* FROM address AS a WHERE p.name = 'Fred'",
	"[Bypass[SELECT ] " +
		"Output[[a.district a.street] [Address.*]] " +
		"Bypass[ FROM address AS a WHERE p.name = ] Bypass['Fred']]",
}, {
	"quote",
	"SELECT 1 FROM person WHERE p.name = 'Fred'",
	"[Bypass[SELECT 1 FROM person WHERE p.name = ] " +
		"Bypass['Fred']]",
}, {
	"complex query v1",
	"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.*, " +
		"(5+7), (col1 * col2) AS calculated_value FROM person AS p " +
		"JOIN address AS a ON p.address_id = a.id WHERE p.name = 'Fred'",
	"[Bypass[SELECT ] " +
		"Output[[p.*] [Person.*]] " +
		"Bypass[, ] " +
		"Output[[a.district a.street] [Address.*]] " +
		"Bypass[, (5+7), (col1 * col2) AS calculated_value FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = ] " +
		"Bypass['Fred']]",
}, {
	"complex query v2",
	"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* " +
		"FROM person AS p JOIN address AS a ON p .address_id = a.id " +
		"WHERE p.name = 'Fred'",
	"[Bypass[SELECT ] " +
		"Output[[p.*] [Person.*]] " +
		"Bypass[, ] " +
		"Output[[a.district a.street] [Address.*]] " +
		"Bypass[ FROM person AS p JOIN address AS a ON p .address_id = a.id WHERE p.name = ] " +
		"Bypass['Fred']]",
}, {
	"complex query v3",
	"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* " +
		"FROM person AS p JOIN address AS a ON p.address_id = a.id " +
		"WHERE p.name IN (SELECT name FROM table WHERE table.n = $Person.name)",
	"[Bypass[SELECT ] " +
		"Output[[p.*] [Person.*]] " +
		"Bypass[, ] " +
		"Output[[a.district a.street] [Address.*]] " +
		"Bypass[ FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name IN (SELECT name FROM table WHERE table.n = ] " +
		"Input[Person.name] " +
		"Bypass[)]]",
}, {
	"complex query v4",
	"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* " +
		"FROM person WHERE p.name IN (SELECT name FROM table " +
		"WHERE table.n = $Person.name) UNION " +
		"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* " +
		"FROM person WHERE p.name IN " +
		"(SELECT name FROM table WHERE table.n = $Person.name)",
	"[Bypass[SELECT ] Output[[p.*] [Person.*]] " +
		"Bypass[, ] Output[[a.district a.street] [Address.*]] " +
		"Bypass[ FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = ] " +
		"Input[Person.name] " +
		"Bypass[) UNION SELECT ] " +
		"Output[[p.*] [Person.*]] " +
		"Bypass[, ] " +
		"Output[[a.district a.street] [Address.*]] " +
		"Bypass[ FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = ] " +
		"Input[Person.name] " +
		"Bypass[)]]",
}, {
	"complex query v5",
	"SELECT p.* AS &Person.*, a.district AS &District.* " +
		"FROM person AS p JOIN address AS a ON p.address_id = a.id " +
		"WHERE p.name = $Person.name AND p.address_id = $Person.address_id",
	"[Bypass[SELECT ] " +
		"Output[[p.*] [Person.*]] " +
		"Bypass[, ] " +
		"Output[[a.district] [District.*]] " +
		"Bypass[ FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = ] " +
		"Input[Person.name] " +
		"Bypass[ AND p.address_id = ] " +
		"Input[Person.address_id]]",
}, {
	"complex query v6",
	"SELECT p.* AS &Person.*, a.district AS &District.* " +
		"FROM person AS p INNER JOIN address AS a " +
		"ON p.address_id = $Address.id " +
		"WHERE p.name = $Person.name AND p.address_id = $Person.address_id",
	"[Bypass[SELECT ] " +
		"Output[[p.*] [Person.*]] " +
		"Bypass[, ] " +
		"Output[[a.district] [District.*]] " +
		"Bypass[ FROM person AS p INNER JOIN address AS a ON p.address_id = ] " +
		"Input[Address.id] " +
		"Bypass[ WHERE p.name = ] " +
		"Input[Person.name] " +
		"Bypass[ AND p.address_id = ] " +
		"Input[Person.address_id]]",
}, {
	"join v1",
	"SELECT p.* AS &Person.*, m.* AS &Manager.* " +
		"FROM person AS p JOIN person AS m " +
		"ON p.manager_id = m.id WHERE p.name = 'Fred'",
	"[Bypass[SELECT ] " +
		"Output[[p.*] [Person.*]] " +
		"Bypass[, ] " +
		"Output[[m.*] [Manager.*]] " +
		"Bypass[ FROM person AS p JOIN person AS m ON p.manager_id = m.id WHERE p.name = ] " +
		"Bypass['Fred']]",
}, {
	"join v2",
	"SELECT person.*, address.district FROM person JOIN address " +
		"ON person.address_id = address.id WHERE person.name = 'Fred'",
	"[Bypass[SELECT person.*, address.district FROM person JOIN address ON person.address_id = address.id WHERE person.name = ] " +
		"Bypass['Fred']]",
}, {
	"insert",
	"INSERT INTO person (name) VALUES $Person.name",
	"[Bypass[INSERT INTO person (name) VALUES ] " +
		"Input[Person.name]]",
}, {
	"ignore dollar v1",
	"SELECT $ FROM moneytable",
	"[Bypass[SELECT $ FROM moneytable]]",
}, {
	"ignore dollar v2",
	"SELECT foo FROM data$",
	"[Bypass[SELECT foo FROM data$]]",
}, {
	"ignore dollar v3",
	"SELECT dollerrow$ FROM moneytable",
	"[Bypass[SELECT dollerrow$ FROM moneytable]]",
}, {
	"input with no space",
	"SELECT p.*, a.district " +
		"FROM person AS p WHERE p.name=$Person.name",
	"[Bypass[SELECT p.*, a.district FROM person AS p WHERE p.name=] " +
		"Input[Person.name]]",
}, {
	"update",
	"UPDATE person SET person.address_id = $Address.id " +
		"WHERE person.id = $Person.id",
	"[Bypass[UPDATE person SET person.address_id = ] " +
		"Input[Address.id] " +
		"Bypass[ WHERE person.id = ] " +
		"Input[Person.id]]",
}}

func (s *ExprSuite) TestExpr(c *C) {
	parser := expr.NewParser()
	for i, test := range tests {
		var parsedExpr *expr.ParsedExpr
		var err error
		if parsedExpr, err = parser.Parse(test.input); err != nil {
			c.Errorf("test %d failed (Parse):\nsummary: %s\ninput: %s\nexpected: %s\nerr: %s\n", i, test.summary, test.input, test.expectedParsed, err)
		} else if parsedExpr.String() != test.expectedParsed {
			c.Errorf("test %d failed (Parse):\nsummary: %s\ninput: %s\nexpected: %s\nactual:   %s\n", i, test.summary, test.input, test.expectedParsed, parsedExpr.String())
		}
	}
}

// We return a proper error when we find an unbound string literal
func (s *ExprSuite) TestParseUnfinishedStringLiteral(c *C) {
	testList := []string{
		"SELECT foo FROM t WHERE x = 'dddd",
		"SELECT foo FROM t WHERE x = \"dddd",
		"SELECT foo FROM t WHERE x = \"dddd'",
	}

	for _, sql := range testList {
		parser := expr.NewParser()
		expr, err := parser.Parse(sql)
		c.Assert(err, ErrorMatches, "cannot parse expression: missing right quote in string literal")
		c.Assert(expr, IsNil)
	}
}

// Properly parsing empty string literal
func (s *ExprSuite) TestParseEmptyStringLiteral(c *C) {
	sql := "SELECT foo FROM t WHERE x = ''"
	parser := expr.NewParser()
	_, err := parser.Parse(sql)
	c.Assert(err, IsNil)
}

// Detect bad escaped string literal
func (s *ExprSuite) TestParseBadEscaped(c *C) {
	sql := "SELECT foo FROM t WHERE x = 'O'Donnell'"
	parser := expr.NewParser()
	_, err := parser.Parse(sql)
	c.Assert(err, ErrorMatches, "cannot parse expression: missing right quote in string literal")
}

func (s *ExprSuite) TestParseBadFormatInput(c *C) {
	testListInvalidId := []string{
		"SELECT foo FROM t WHERE x = $Address.",
		"SELECT foo FROM t WHERE x = $Address.&d",
		"SELECT foo FROM t WHERE x = $Address.-",
	}

	for _, sql := range testListInvalidId {
		parser := expr.NewParser()
		expr, err := parser.Parse(sql)
		c.Assert(err, ErrorMatches, "cannot parse expression: column 37: invalid identifier")
		c.Assert(expr, IsNil)
	}

	sql := "SELECT foo FROM t WHERE x = $Address"
	parser := expr.NewParser()
	_, err := parser.Parse(sql)
	c.Assert(err, ErrorMatches, "cannot parse expression: column 36: type not qualified")
}

func FuzzParser(f *testing.F) {
	// Add some values to the corpus
	for _, test := range tests {
		f.Add(test.input)
	}
	f.Fuzz(func(t *testing.T, s string) {
		// Loop forever or until it crashes
		parser := expr.NewParser()
		parser.Parse(s)
	})
}
