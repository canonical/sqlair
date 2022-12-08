package test

import (
	"testing"

	"github.com/canonical/sqlair/internal/parse"
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
	input          string
	expectedParsed string
}{{
	"SELECT p.* AS &Person.*",
	"ParsedExpr[BypassPart[SELECT] OutputPart[[p.*] [Person.*]]]",
}, {
	"SELECT p.* AS&Person.*",
	"ParsedExpr[BypassPart[SELECT p.* AS&Person.*]]",
}, {
	"SELECT p.* AS &Person.*, '&notAnOutputExpresion.*' AS literal FROM t",
	"ParsedExpr[BypassPart[SELECT] " +
		"OutputPart[[p.*] [Person.*]] " +
		"BypassPart[, ] " +
		"BypassPart['&notAnOutputExpresion.*'] " +
		"BypassPart[ AS literal FROM t]]",
}, {
	"SELECT * AS &Person.* FROM t",
	"ParsedExpr[BypassPart[SELECT] " +
		"OutputPart[[*] [Person.*]] " +
		"BypassPart[ FROM t]]",
}, {
	"SELECT foo, bar FROM table WHERE foo = $Person.id",
	"ParsedExpr[BypassPart[SELECT foo, bar FROM table WHERE foo = ] " +
		"InputPart[Person.id]]",
}, {
	"SELECT &Person.* FROM table WHERE foo = $Address.id",
	"ParsedExpr[BypassPart[SELECT] OutputPart[[] [Person.*]] " +
		"BypassPart[ FROM table WHERE foo = ] " +
		"InputPart[Address.id]]",
}, {
	"SELECT &Person.* FROM table WHERE foo = $Address.id",
	"ParsedExpr[BypassPart[SELECT] " +
		"OutputPart[[] [Person.*]] " +
		"BypassPart[ FROM table WHERE foo = ] " +
		"InputPart[Address.id]]",
}, {
	"SELECT foo, bar, &Person.id FROM table WHERE foo = 'xx'",
	"ParsedExpr[BypassPart[SELECT foo, bar,] " +
		"OutputPart[[] [Person.id]] " +
		"BypassPart[ FROM table WHERE foo = ] " +
		"BypassPart['xx']]",
}, {
	"SELECT foo, &Person.id, bar, baz, &Manager.name FROM table WHERE foo = 'xx'",
	"ParsedExpr[BypassPart[SELECT foo,] " +
		"OutputPart[[] [Person.id]] " +
		"BypassPart[, bar, baz,] " +
		"OutputPart[[] [Manager.name]] " +
		"BypassPart[ FROM table WHERE foo = ] " +
		"BypassPart['xx']]",
}, {
	"SELECT * AS &Person.* FROM person WHERE name = 'Fred'",
	"ParsedExpr[BypassPart[SELECT] " +
		"OutputPart[[*] [Person.*]] " +
		"BypassPart[ FROM person WHERE name = ] " +
		"BypassPart['Fred']]",
}, {
	"SELECT &Person.* FROM person WHERE name = 'Fred'",
	"ParsedExpr[BypassPart[SELECT] " +
		"OutputPart[[] [Person.*]] " +
		"BypassPart[ FROM person WHERE name = ] " +
		"BypassPart['Fred']]",
}, {
	"SELECT * AS &Person.*, a.* AS &Address.* FROM person, address a WHERE name = 'Fred'",
	"ParsedExpr[BypassPart[SELECT] " +
		"OutputPart[[*] [Person.*]] " +
		"BypassPart[,] " +
		"OutputPart[[a.*] [Address.*]] " +
		"BypassPart[ FROM person, address a WHERE name = ] " +
		"BypassPart['Fred']]",
}, {
	"SELECT (a.district, a.street) AS &(Address.district, Address.street) FROM address AS a",
	"ParsedExpr[BypassPart[SELECT] " +
		"OutputPart[[a.district a.street] [Address.district Address.street]] " +
		"BypassPart[ FROM address AS a]]",
}, {
	"SELECT (a.district, a.street) AS &(Address.district, Address.street), " +
		"a.id AS &Person.id FROM address AS a",
	"ParsedExpr[BypassPart[SELECT] " +
		"OutputPart[[a.district a.street] [Address.district Address.street]] " +
		"BypassPart[,] OutputPart[[a.id] [Person.id]] " +
		"BypassPart[ FROM address AS a]]",
}, {
	"SELECT (a.district, a.street) AS &(Address.district, Address.street), " +
		"&Person.* FROM address AS a",
	"ParsedExpr[BypassPart[SELECT] " +
		"OutputPart[[a.district a.street] [Address.district Address.street]] " +
		"BypassPart[,] OutputPart[[] [Person.*]] " +
		"BypassPart[ FROM address AS a]]",
}, {
	"SELECT (a.district, a.street) AS &Address.* FROM address AS a WHERE p.name = 'Fred'",
	"ParsedExpr[BypassPart[SELECT] " +
		"OutputPart[[a.district a.street] [Address.*]] " +
		"BypassPart[ FROM address AS a WHERE p.name = ] BypassPart['Fred']]",
}, {
	"SELECT 1 FROM person WHERE p.name = 'Fred'",
	"ParsedExpr[BypassPart[SELECT 1 FROM person WHERE p.name = ] " +
		"BypassPart['Fred']]",
}, {
	"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.*, " +
		"(5+7), (col1 * col2) AS calculated_value FROM person AS p " +
		"JOIN address AS a ON p.address_id = a.id WHERE p.name = 'Fred'",
	"ParsedExpr[BypassPart[SELECT] " +
		"OutputPart[[p.*] [Person.*]] " +
		"BypassPart[,] " +
		"OutputPart[[a.district a.street] [Address.*]] " +
		"BypassPart[, (5+7), (col1 * col2) AS calculated_value FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = ] " +
		"BypassPart['Fred']]",
}, {
	"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* " +
		"FROM person AS p JOIN address AS a ON p .address_id = a.id " +
		"WHERE p.name = 'Fred'",
	"ParsedExpr[BypassPart[SELECT] " +
		"OutputPart[[p.*] [Person.*]] " +
		"BypassPart[,] " +
		"OutputPart[[a.district a.street] [Address.*]] " +
		"BypassPart[ FROM person AS p JOIN address AS a ON p .address_id = a.id WHERE p.name = ] " +
		"BypassPart['Fred']]",
}, {
	"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* " +
		"FROM person AS p JOIN address AS a ON p.address_id = a.id " +
		"WHERE p.name IN (SELECT name FROM table WHERE table.n = $Person.name)",
	"ParsedExpr[BypassPart[SELECT] " +
		"OutputPart[[p.*] [Person.*]] " +
		"BypassPart[,] " +
		"OutputPart[[a.district a.street] [Address.*]] " +
		"BypassPart[ FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name IN (SELECT name FROM table WHERE table.n = ] " +
		"InputPart[Person.name] " +
		"BypassPart[)]]",
}, {
	"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* " +
		"FROM person WHERE p.name IN (SELECT name FROM table " +
		"WHERE table.n = $Person.name) UNION " +
		"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* " +
		"FROM person WHERE p.name IN " +
		"(SELECT name FROM table WHERE table.n = $Person.name)",
	"ParsedExpr[BypassPart[SELECT] OutputPart[[p.*] [Person.*]] " +
		"BypassPart[,] OutputPart[[a.district a.street] [Address.*]] " +
		"BypassPart[ FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = ] " +
		"InputPart[Person.name] " +
		"BypassPart[) UNION SELECT] " +
		"OutputPart[[p.*] [Person.*]] " +
		"BypassPart[,] " +
		"OutputPart[[a.district a.street] [Address.*]] " +
		"BypassPart[ FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = ] " +
		"InputPart[Person.name] " +
		"BypassPart[)]]",
}, {
	"SELECT p.* AS &Person.*, a.district AS &District.* " +
		"FROM person AS p JOIN address AS a ON p.address_id = a.id " +
		"WHERE p.name = $Person.name AND p.address_id = $Person.address_id",
	"ParsedExpr[BypassPart[SELECT] " +
		"OutputPart[[p.*] [Person.*]] " +
		"BypassPart[,] " +
		"OutputPart[[a.district] [District.*]] " +
		"BypassPart[ FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = ] " +
		"InputPart[Person.name] " +
		"BypassPart[ AND p.address_id = ] " +
		"InputPart[Person.address_id]]",
}, {
	"SELECT p.* AS &Person.*, a.district AS &District.* " +
		"FROM person AS p INNER JOIN address AS a " +
		"ON p.address_id = $Address.id " +
		"WHERE p.name = $Person.name AND p.address_id = $Person.address_id",
	"ParsedExpr[BypassPart[SELECT] " +
		"OutputPart[[p.*] [Person.*]] " +
		"BypassPart[,] " +
		"OutputPart[[a.district] [District.*]] " +
		"BypassPart[ FROM person AS p INNER JOIN address AS a ON p.address_id = ] " +
		"InputPart[Address.id] " +
		"BypassPart[ WHERE p.name = ] " +
		"InputPart[Person.name] " +
		"BypassPart[ AND p.address_id = ] " +
		"InputPart[Person.address_id]]",
}, {
	"SELECT p.* AS &Person.*, m.* AS &Manager.* " +
		"FROM person AS p JOIN person AS m " +
		"ON p.manager_id = m.id WHERE p.name = 'Fred'",
	"ParsedExpr[BypassPart[SELECT] " +
		"OutputPart[[p.*] [Person.*]] " +
		"BypassPart[,] " +
		"OutputPart[[m.*] [Manager.*]] " +
		"BypassPart[ FROM person AS p JOIN person AS m ON p.manager_id = m.id WHERE p.name = ] " +
		"BypassPart['Fred']]",
}, {
	"SELECT person.*, address.district FROM person JOIN address " +
		"ON person.address_id = address.id WHERE person.name = 'Fred'",
	"ParsedExpr[BypassPart[SELECT person.*, address.district FROM person JOIN address ON person.address_id = address.id WHERE person.name = ] " +
		"BypassPart['Fred']]",
}, {
	"SELECT p FROM person WHERE p.name = $Person.name",
	"ParsedExpr[BypassPart[SELECT p FROM person WHERE p.name = ] InputPart[Person.name]]",
}, {
	"INSERT INTO person (name) VALUES $Person.name",
	"ParsedExpr[BypassPart[INSERT INTO person (name) VALUES ] " +
		"InputPart[Person.name]]",
}, {
	"SELECT $ FROM moneytable",
	"ParsedExpr[BypassPart[SELECT $ FROM moneytable]]",
}, {
	"SELECT foo FROM data$",
	"ParsedExpr[BypassPart[SELECT foo FROM data$]]",
}, {
	"SELECT dollerrow$ FROM moneytable",
	"ParsedExpr[BypassPart[SELECT dollerrow$ FROM moneytable]]",
}, {
	"SELECT p.*, a.district " +
		"FROM person AS p WHERE p.name=$Person.name",
	"ParsedExpr[BypassPart[SELECT p.*, a.district FROM person AS p WHERE p.name=] " +
		"InputPart[Person.name]]",
}, {
	"UPDATE person SET person.address_id = $Address.id " +
		"WHERE person.id = $Person.id",
	"ParsedExpr[BypassPart[UPDATE person SET person.address_id = ] " +
		"InputPart[Address.id] " +
		"BypassPart[ WHERE person.id = ] " +
		"InputPart[Person.id]]",
}}

func (s *ParserSuite) TestRound(c *C) {
	parser := parse.NewParser()
	for i, test := range tests {
		var parsedExpr *parse.ParsedExpr
		var err error
		if parsedExpr, err = parser.Parse(test.input); err != nil {
			c.Errorf("test %d failed (Parse): input: %s\nexpected: %s\nerr: %s\n", i, test.input, test.expectedParsed, err)
		} else if parsedExpr.String() != test.expectedParsed {
			c.Errorf("test %d failed (Parse): input: %s\nexpected: %s\nactual:   %s\n", i, test.input, test.expectedParsed, parsedExpr.String())
		}
	}
}

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
