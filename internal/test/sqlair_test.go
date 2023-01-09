package test

import (
	"testing"

	"github.com/canonical/sqlair/internal/parse"
	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type TestSuite struct{}

var _ = Suite(&TestSuite{})

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
	"ParsedExpr[BypassPart[SELECT p.* AS &Person.*]]",
}, {
	"quoted output expression",
	"SELECT p.* AS &Person.*, '&notAnOutputExpresion.*' AS literal FROM t",
	"ParsedExpr[BypassPart[SELECT p.* AS &Person.*, ] BypassPart['&notAnOutputExpresion.*'] BypassPart[ AS literal FROM t]]",
}, {
	"star as output",
	"SELECT * AS &Person.* FROM t",
	"ParsedExpr[BypassPart[SELECT * AS &Person.* FROM t]]",
}, {
	"input v1",
	"SELECT foo, bar FROM table WHERE foo = $Person.id",
	"ParsedExpr[BypassPart[SELECT foo, bar FROM table WHERE foo = ] InputPart[Person.id]]",
}, {
	"input v2",
	"SELECT p FROM person WHERE p.name = $Person.name",
	"ParsedExpr[BypassPart[SELECT p FROM person WHERE p.name = ] InputPart[Person.name]]",
}, {
	"input v3",
	"SELECT p.*, a.district FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = $Person.name",
	"ParsedExpr[BypassPart[SELECT p.*, a.district FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = ] InputPart[Person.name]]",
}, {
	"output and input",
	"SELECT &Person FROM table WHERE foo = $Address.id",
	"ParsedExpr[BypassPart[SELECT &Person FROM table WHERE foo = ] InputPart[Address.id]]",
}, {
	"star output and input",
	"SELECT &Person.* FROM table WHERE foo = $Address.id",
	"ParsedExpr[BypassPart[SELECT &Person.* FROM table WHERE foo = ] InputPart[Address.id]]",
}, {
	"output and quote",
	"SELECT foo, bar, &Person.id FROM table WHERE foo = 'xx'",
	"ParsedExpr[BypassPart[SELECT foo, bar, &Person.id FROM table WHERE foo = ] BypassPart['xx']]",
}, {
	"two outputs and quote",
	"SELECT foo, &Person.id, bar, baz, &Manager.name FROM table WHERE foo = 'xx'",
	"ParsedExpr[BypassPart[SELECT foo, &Person.id, bar, baz, &Manager.name FROM table WHERE foo = ] BypassPart['xx']]",
}, {
	"star as output and quote",
	"SELECT * AS &Person.* FROM person WHERE name = 'Fred'",
	"ParsedExpr[BypassPart[SELECT * AS &Person.* FROM person WHERE name = ] BypassPart['Fred']]",
}, {
	"star output and quote",
	"SELECT &Person.* FROM person WHERE name = 'Fred'",
	"ParsedExpr[BypassPart[SELECT &Person.* FROM person WHERE name = ] BypassPart['Fred']]",
}, {
	"two star as outputs and quote",
	"SELECT * AS &Person.*, a.* AS &Address.* FROM person, address a WHERE name = 'Fred'",
	"ParsedExpr[BypassPart[SELECT * AS &Person.*, a.* AS &Address.* FROM person, address a WHERE name = ] BypassPart['Fred']]",
}, {
	"multicolumn output and quote",
	"SELECT (a.district, a.street) AS &Address.* FROM address AS a WHERE p.name = 'Fred'",
	"ParsedExpr[BypassPart[SELECT (a.district, a.street) AS &Address.* FROM address AS a WHERE p.name = ] BypassPart['Fred']]",
}, {
	"quote",
	"SELECT 1 FROM person WHERE p.name = 'Fred'",
	"ParsedExpr[BypassPart[SELECT 1 FROM person WHERE p.name = ] BypassPart['Fred']]",
}, {
	"complex query v1",
	"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.*, (5+7), (col1 * col2) AS calculated_value FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = 'Fred'",
	"ParsedExpr[BypassPart[SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.*, (5+7), (col1 * col2) AS calculated_value FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = ] BypassPart['Fred']]",
}, {
	"complex query v2",
	"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* FROM person AS p JOIN address AS a ON p .address_id = a.id WHERE p.name = 'Fred'",
	"ParsedExpr[BypassPart[SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* FROM person AS p JOIN address AS a ON p .address_id = a.id WHERE p.name = ] BypassPart['Fred']]",
}, {
	"complex query v3",
	"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name IN (SELECT name FROM table WHERE table.n = $Person.name)",
	"ParsedExpr[BypassPart[SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name IN (SELECT name FROM table WHERE table.n = ] InputPart[Person.name] BypassPart[)]]",
}, {
	"complex query v4",
	"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = $Person.name) UNION SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = $Person.name)",
	"ParsedExpr[BypassPart[SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = ] InputPart[Person.name] BypassPart[) UNION SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = ] InputPart[Person.name] BypassPart[)]]",
}, {
	"complex query v5",
	"SELECT p.* AS &Person, a.District AS &District FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = $Person.name AND p.address_id = $Person.address_id",
	"ParsedExpr[BypassPart[SELECT p.* AS &Person, a.District AS &District FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = ] InputPart[Person.name] BypassPart[ AND p.address_id = ] InputPart[Person.address_id]]",
}, {
	"complex query v6",
	"SELECT p.* AS &Person, a.District AS &District FROM person AS p INNER JOIN address AS a ON p.address_id = $Address.id WHERE p.name = $Person.name AND p.address_id = $Person.address_id",
	"ParsedExpr[BypassPart[SELECT p.* AS &Person, a.District AS &District FROM person AS p INNER JOIN address AS a ON p.address_id = ] InputPart[Address.id] BypassPart[ WHERE p.name = ] InputPart[Person.name] BypassPart[ AND p.address_id = ] InputPart[Person.address_id]]",
}, {
	"join v1",
	"SELECT p.* AS &Person.*, m.* AS &Manager.* FROM person AS p JOIN person AS m ON p.manager_id = m.id WHERE p.name = 'Fred'",
	"ParsedExpr[BypassPart[SELECT p.* AS &Person.*, m.* AS &Manager.* FROM person AS p JOIN person AS m ON p.manager_id = m.id WHERE p.name = ] BypassPart['Fred']]",
}, {
	"join v2",
	"SELECT person.*, address.district FROM person JOIN address ON person.address_id = address.id WHERE person.name = 'Fred'",
	"ParsedExpr[BypassPart[SELECT person.*, address.district FROM person JOIN address ON person.address_id = address.id WHERE person.name = ] BypassPart['Fred']]",
}, {
	"insert",
	"INSERT INTO person (name) VALUES $Person.name",
	"ParsedExpr[BypassPart[INSERT INTO person (name) VALUES ] InputPart[Person.name]]",
}, {
	"ignore dollar v1",
	"SELECT $ FROM moneytable",
	"ParsedExpr[BypassPart[SELECT $ FROM moneytable]]",
}, {
	"ignore dollar v2",
	"SELECT foo FROM data$",
	"ParsedExpr[BypassPart[SELECT foo FROM data$]]",
}, {
	"ignore dollar v3",
	"SELECT dollerrow$ FROM moneytable",
	"ParsedExpr[BypassPart[SELECT dollerrow$ FROM moneytable]]",
}, {
	"input with no space",
	"SELECT p.*, a.district FROM person AS p WHERE p.name=$Person.name",
	"ParsedExpr[BypassPart[SELECT p.*, a.district FROM person AS p WHERE p.name=] InputPart[Person.name]]",
}, {
	"update",
	"UPDATE person SET person.address_id = $Address.id WHERE person.id = $Person.id",
	"ParsedExpr[BypassPart[UPDATE person SET person.address_id = ] InputPart[Address.id] BypassPart[ WHERE person.id = ] InputPart[Person.id]]",
}}

func (s *TestSuite) TestRound(c *C) {
	parser := parse.NewParser()
	for i, test := range tests {
		var parsedExpr *parse.ParsedExpr
		var err error
		if parsedExpr, err = parser.Parse(test.input); err != nil {
			c.Errorf("test %d failed (Parse):\nsummary: %s\ninput: %s\nexpected: %s\nerr: %s\n", i, test.summary, test.input, test.expectedParsed, err)
		} else if parsedExpr.String() != test.expectedParsed {
			c.Errorf("test %d failed (Parse):\nsummary: %s\ninput: %s\nexpected: %s\nactual:   %s\n", i, test.summary, test.input, test.expectedParsed, parsedExpr.String())
		}
	}
}

// FuzzParser is in this file rather than parser_test becuase it uses the
// black box test inputs as a corpus.
func FuzzParser(f *testing.F) {
	// Add some values to the corpus
	for _, test := range tests {
		f.Add(test.input)
	}
	f.Fuzz(func(t *testing.T, s string) {
		// Loop forever or until it crashes
		parser := parse.NewParser()
		parser.Parse(s)
	})
}
