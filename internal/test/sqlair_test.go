package test

import (
	"testing"

	"github.com/canonical/sqlair/internal/assemble"
	"github.com/canonical/sqlair/internal/parse"
	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type TestSuite struct{}

var _ = Suite(&TestSuite{})

type Address struct {
	ID       int    `db:"id"`
	District string `db:"district"`
	Street   string `db:"street"`
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
	input             string
	expectedParsed    string
	assembleArgs      []any
	expectedAssembled string
}{{
	"SELECT p.* AS &Person.*",
	"ParsedExpr[BypassPart[SELECT] OutputPart[[p.*] [Person.*]]]",
	[]any{Person{}},
	"SELECT p.address_id, p.id, p.name",
}, {
	"SELECT p.* AS&Person.*",
	"ParsedExpr[BypassPart[SELECT p.* AS&Person.*]]",
	[]any{Person{}},
	"SELECT p.* AS&Person.*",
}, {
	"SELECT p.* AS &Person.*, '&notAnOutputExpresion.*' AS literal FROM t",
	"ParsedExpr[BypassPart[SELECT] " +
		"OutputPart[[p.*] [Person.*]] " +
		"BypassPart[, ] " +
		"BypassPart['&notAnOutputExpresion.*'] " +
		"BypassPart[ AS literal FROM t]]",
	[]any{Person{}},
	"SELECT p.address_id, p.id, p.name, '&notAnOutputExpresion.*' AS literal FROM t",
}, {
	"SELECT * AS &Person.* FROM t",
	"ParsedExpr[BypassPart[SELECT] " +
		"OutputPart[[*] [Person.*]] " +
		"BypassPart[ FROM t]]",
	[]any{Person{}},
	"SELECT address_id, id, name FROM t",
}, {
	"SELECT foo, bar FROM table WHERE foo = $Person.id",
	"ParsedExpr[BypassPart[SELECT foo, bar FROM table WHERE foo = ] " +
		"InputPart[Person.id]]",
	[]any{Person{}},
	"SELECT foo, bar FROM table WHERE foo = ?",
}, {
	"SELECT &Person.* FROM table WHERE foo = $Address.id",
	"ParsedExpr[BypassPart[SELECT] OutputPart[[] [Person.*]] " +
		"BypassPart[ FROM table WHERE foo = ] " +
		"InputPart[Address.id]]",
	[]any{Person{}, Address{}},
	"SELECT address_id, id, name FROM table WHERE foo = ?",
}, {
	"SELECT foo, bar, &Person.id FROM table WHERE foo = 'xx'",
	"ParsedExpr[BypassPart[SELECT foo, bar,] " +
		"OutputPart[[] [Person.id]] " +
		"BypassPart[ FROM table WHERE foo = ] " +
		"BypassPart['xx']]",
	[]any{Person{}},
	"SELECT foo, bar, id FROM table WHERE foo = 'xx'",
}, {
	"SELECT foo, &Person.id, bar, baz, &Manager.manager_name FROM table WHERE foo = 'xx'",
	"ParsedExpr[BypassPart[SELECT foo,] " +
		"OutputPart[[] [Person.id]] " +
		"BypassPart[, bar, baz,] " +
		"OutputPart[[] [Manager.manager_name]] " +
		"BypassPart[ FROM table WHERE foo = ] " +
		"BypassPart['xx']]",
	[]any{Person{}, Manager{}},
	"SELECT foo, id, bar, baz, manager_name FROM table WHERE foo = 'xx'",
}, {
	"SELECT * AS &Person.* FROM person WHERE name = 'Fred'",
	"ParsedExpr[BypassPart[SELECT] " +
		"OutputPart[[*] [Person.*]] " +
		"BypassPart[ FROM person WHERE name = ] " +
		"BypassPart['Fred']]",
	[]any{Person{}},
	"SELECT address_id, id, name FROM person WHERE name = 'Fred'",
}, {
	"SELECT &Person.* FROM person WHERE name = 'Fred'",
	"ParsedExpr[BypassPart[SELECT] " +
		"OutputPart[[] [Person.*]] " +
		"BypassPart[ FROM person WHERE name = ] " +
		"BypassPart['Fred']]",
	[]any{Person{}},
	"SELECT address_id, id, name FROM person WHERE name = 'Fred'",
}, {
	"SELECT * AS &Person.*, a.* AS &Address.* FROM person, address a WHERE name = 'Fred'",
	"ParsedExpr[BypassPart[SELECT] " +
		"OutputPart[[*] [Person.*]] " +
		"BypassPart[,] " +
		"OutputPart[[a.*] [Address.*]] " +
		"BypassPart[ FROM person, address a WHERE name = ] " +
		"BypassPart['Fred']]",
	[]any{Person{}, Address{}},
	"SELECT address_id, id, name, a.district, a.id, a.street FROM person, address a WHERE name = 'Fred'",
}, {
	"SELECT (a.district, a.street) AS &(Address.district, Address.street) FROM address AS a",
	"ParsedExpr[BypassPart[SELECT] " +
		"OutputPart[[a.district a.street] [Address.district Address.street]] " +
		"BypassPart[ FROM address AS a]]",
	[]any{Address{}, District{}},
	"SELECT a.district, a.street FROM address AS a",
}, {
	"SELECT (a.district, a.street) AS &(Address.district, Address.street), " +
		"a.id AS &Person.id FROM address AS a",
	"ParsedExpr[BypassPart[SELECT] " +
		"OutputPart[[a.district a.street] [Address.district Address.street]] " +
		"BypassPart[,] OutputPart[[a.id] [Person.id]] " +
		"BypassPart[ FROM address AS a]]",
	[]any{Person{}, Address{}},
	"SELECT a.district, a.street, a.id FROM address AS a",
}, {
	"SELECT (a.district, a.street) AS &(Address.district, Address.street), " +
		"&Person.* FROM address AS a",
	"ParsedExpr[BypassPart[SELECT] " +
		"OutputPart[[a.district a.street] [Address.district Address.street]] " +
		"BypassPart[,] OutputPart[[] [Person.*]] " +
		"BypassPart[ FROM address AS a]]",
	[]any{Person{}, Address{}},
	"SELECT a.district, a.street, address_id, id, name FROM address AS a",
}, {
	"SELECT (a.district, a.street) AS &Address.* FROM address AS a WHERE p.name = 'Fred'",
	"ParsedExpr[BypassPart[SELECT] " +
		"OutputPart[[a.district a.street] [Address.*]] " +
		"BypassPart[ FROM address AS a WHERE p.name = ] BypassPart['Fred']]",
	[]any{Address{}},
	"SELECT a.district, a.street FROM address AS a WHERE p.name = 'Fred'",
}, {
	"SELECT 1 FROM person WHERE p.name = 'Fred'",
	"ParsedExpr[BypassPart[SELECT 1 FROM person WHERE p.name = ] " +
		"BypassPart['Fred']]",
	[]any{},
	"SELECT 1 FROM person WHERE p.name = 'Fred'",
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
	[]any{Person{}, Address{}},
	"SELECT p.address_id, p.id, p.name, a.district, a.street, " +
		"(5+7), (col1 * col2) AS calculated_value FROM person AS p " +
		"JOIN address AS a ON p.address_id = a.id WHERE p.name = 'Fred'",
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
	[]any{Person{}, Address{}},
	"SELECT p.address_id, p.id, p.name, a.district, a.street " +
		"FROM person AS p JOIN address AS a ON p .address_id = a.id " +
		"WHERE p.name = 'Fred'",
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
	[]any{Person{}, Address{}},
	"SELECT p.address_id, p.id, p.name, a.district, a.street " +
		"FROM person AS p JOIN address AS a ON p.address_id = a.id " +
		"WHERE p.name IN (SELECT name FROM table WHERE table.n = ?)",
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
	[]any{Person{}, Address{}},
	"SELECT p.address_id, p.id, p.name, a.district, a.street " +
		"FROM person WHERE p.name IN (SELECT name FROM table " +
		"WHERE table.n = ?) UNION " +
		"SELECT p.address_id, p.id, p.name, a.district, a.street " +
		"FROM person WHERE p.name IN " +
		"(SELECT name FROM table WHERE table.n = ?)",
}, {
	"SELECT p.* AS &Person.*, &District.* " +
		"FROM person AS p JOIN address AS a ON p.address_id = a.id " +
		"WHERE p.name = $Person.name AND p.address_id = $Person.address_id",
	"ParsedExpr[BypassPart[SELECT] " +
		"OutputPart[[p.*] [Person.*]] " +
		"BypassPart[,] " +
		"OutputPart[[] [District.*]] " +
		"BypassPart[ FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = ] " +
		"InputPart[Person.name] " +
		"BypassPart[ AND p.address_id = ] " +
		"InputPart[Person.address_id]]",
	[]any{Person{}, District{}},
	"SELECT p.address_id, p.id, p.name,  " +
		"FROM person AS p JOIN address AS a ON p.address_id = a.id " +
		"WHERE p.name = ? AND p.address_id = ?",
}, {
	"SELECT p.* AS &Person.*, " +
		"FROM person AS p INNER JOIN address AS a " +
		"ON p.address_id = $Address.id " +
		"WHERE p.name = $Person.name AND p.address_id = $Person.address_id",
	"ParsedExpr[BypassPart[SELECT] " +
		"OutputPart[[p.*] [Person.*]] " +
		"BypassPart[, FROM person AS p INNER JOIN address AS a ON p.address_id = ] " +
		"InputPart[Address.id] " +
		"BypassPart[ WHERE p.name = ] " +
		"InputPart[Person.name] " +
		"BypassPart[ AND p.address_id = ] " +
		"InputPart[Person.address_id]]",
	[]any{Person{}, Address{}},
	"SELECT p.address_id, p.id, p.name, " +
		"FROM person AS p INNER JOIN address AS a " +
		"ON p.address_id = ? " +
		"WHERE p.name = ? AND p.address_id = ?",
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
	[]any{Person{}, Manager{}},
	"SELECT p.address_id, p.id, p.name, m.manager_name " +
		"FROM person AS p JOIN person AS m " +
		"ON p.manager_id = m.id WHERE p.name = 'Fred'",
}, {
	"SELECT person.*, address.district FROM person JOIN address " +
		"ON person.address_id = address.id WHERE person.name = 'Fred'",
	"ParsedExpr[BypassPart[SELECT person.*, address.district FROM person JOIN address ON person.address_id = address.id WHERE person.name = ] " +
		"BypassPart['Fred']]",
	[]any{},
	"SELECT person.*, address.district FROM person JOIN address " +
		"ON person.address_id = address.id WHERE person.name = 'Fred'",
}, {
	"SELECT p FROM person WHERE p.name = $Person.name",
	"ParsedExpr[BypassPart[SELECT p FROM person WHERE p.name = ] InputPart[Person.name]]",
	[]any{Person{}},
	"SELECT p FROM person WHERE p.name = ?",
}, {
	"INSERT INTO person (name) VALUES $Person.name",
	"ParsedExpr[BypassPart[INSERT INTO person (name) VALUES ] " +
		"InputPart[Person.name]]",
	[]any{Person{}},
	"INSERT INTO person (name) VALUES ?",
}, {
	"SELECT $ FROM moneytable",
	"ParsedExpr[BypassPart[SELECT $ FROM moneytable]]",
	[]any{},
	"SELECT $ FROM moneytable",
}, {
	"SELECT foo FROM data$",
	"ParsedExpr[BypassPart[SELECT foo FROM data$]]",
	[]any{},
	"SELECT foo FROM data$",
}, {
	"SELECT dollerrow$ FROM moneytable",
	"ParsedExpr[BypassPart[SELECT dollerrow$ FROM moneytable]]",
	[]any{},
	"SELECT dollerrow$ FROM moneytable",
}, {
	"SELECT p.*, a.district " +
		"FROM person AS p WHERE p.name=$Person.name",
	"ParsedExpr[BypassPart[SELECT p.*, a.district FROM person AS p WHERE p.name=] " +
		"InputPart[Person.name]]",
	[]any{Person{}},
	"SELECT p.*, a.district " +
		"FROM person AS p WHERE p.name=?",
}, {
	"UPDATE person SET person.address_id = $Address.id " +
		"WHERE person.id = $Person.id",
	"ParsedExpr[BypassPart[UPDATE person SET person.address_id = ] " +
		"InputPart[Address.id] " +
		"BypassPart[ WHERE person.id = ] " +
		"InputPart[Person.id]]",
	[]any{Person{}, Address{}},
	"UPDATE person SET person.address_id = ? " +
		"WHERE person.id = ?",
}}

func (s *TestSuite) TestRound(c *C) {
	parser := parse.NewParser()
	for i, test := range tests {
		var (
			parsedExpr    *parse.ParsedExpr
			assembledExpr *assemble.AssembledExpr
			err           error
		)
		if parsedExpr, err = parser.Parse(test.input); err != nil {
			c.Errorf("test %d failed (Parse): input: %s\nexpected: %s\nerr: %s\n", i, test.input, test.expectedParsed, err)
		} else if parsedExpr.String() != test.expectedParsed {
			c.Errorf("test %d failed (Parse): input: %s\nexpected: %s\nactual:   %s\n", i, test.input, test.expectedParsed, parsedExpr.String())
		}

		if assembledExpr, err = assemble.Assemble(parsedExpr, test.assembleArgs...); err != nil {
			c.Errorf("test %d failed (Assemble): input: %s\nexpected: %s\nerr: %s\n", i, test.input, test.expectedAssembled, err)
		} else if assembledExpr.SQL != test.expectedAssembled {
			c.Errorf("test %d failed (Assemble): input: %s\nexpected: %s\nactual:   %s\n", i, test.input, test.expectedAssembled, assembledExpr.SQL)
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
