package parse_test

import (
	"fmt"
	"testing"

	"github.com/canonical/sqlair/internal/parse"
	"github.com/stretchr/testify/assert"
)

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

func TestRound(t *testing.T) {
	var tests = []struct {
		input          string
		expectedParsed string
	}{{
		"SELECT p.* AS &Person.*",
		"ParsedExpr[BypassPart[SELECT p.* AS &Person.*]]",
	}, {
		"SELECT p.* AS&Person.*",
		"ParsedExpr[BypassPart[SELECT p.* AS&Person.*]]",
	}, {
		"SELECT p.* AS &Person.*, '&notAnOutputExpresion.*' AS literal FROM t",
		"ParsedExpr[BypassPart[SELECT p.* AS &Person.*, ] " +
			"BypassPart['&notAnOutputExpresion.*'] " +
			"BypassPart[ AS literal FROM t]]",
	}, {
		"SELECT * AS &Person.* FROM t",
		"ParsedExpr[BypassPart[SELECT * AS &Person.* FROM t]]",
	}, {
		"SELECT foo, bar FROM table WHERE foo = $Person.ID",
		"ParsedExpr[BypassPart[SELECT foo, bar FROM table WHERE foo = ] " +
			"InputPart[Person.ID]]",
	}, {
		"SELECT &Person FROM table WHERE foo = $Address.ID",
		"ParsedExpr[BypassPart[SELECT &Person FROM table WHERE foo = ] " +
			"InputPart[Address.ID]]",
	}, {
		"SELECT &Person.* FROM table WHERE foo = $Address.ID",
		"ParsedExpr[BypassPart[SELECT &Person.* FROM table WHERE foo = ] " +
			"InputPart[Address.ID]]",
	}, {
		"SELECT foo, bar, &Person.ID FROM table WHERE foo = 'xx'",
		"ParsedExpr[BypassPart[SELECT foo, bar, &Person.ID FROM table WHERE foo = ] " +
			"BypassPart['xx']]",
	}, {
		"SELECT foo, &Person.ID, bar, baz, &Manager.Name FROM table WHERE foo = 'xx'",
		"ParsedExpr[BypassPart[SELECT foo, &Person.ID, bar, baz, &Manager.Name FROM table WHERE foo = ] " +
			"BypassPart['xx']]",
	}, {
		"SELECT * AS &Person.* FROM person WHERE name = 'Fred'",
		"ParsedExpr[BypassPart[SELECT * AS &Person.* FROM person WHERE name = ] " +
			"BypassPart['Fred']]",
	}, {
		"SELECT &Person.* FROM person WHERE name = 'Fred'",
		"ParsedExpr[BypassPart[SELECT &Person.* FROM person WHERE name = ] " +
			"BypassPart['Fred']]",
	}, {
		"SELECT * AS &Person.*, a.* AS &Address.* FROM person, address a WHERE name = 'Fred'",
		"ParsedExpr[BypassPart[SELECT * AS &Person.*, a.* AS &Address.* FROM person, address a WHERE name = ] " +
			"BypassPart['Fred']]",
	}, {
		"SELECT (a.district, a.street) AS &Address.* FROM address AS a WHERE p.name = 'Fred'",
		"ParsedExpr[BypassPart[SELECT (a.district, a.street) AS &Address.* FROM address AS a WHERE p.name = ] BypassPart['Fred']]",
	}, {
		"SELECT 1 FROM person WHERE p.name = 'Fred'",
		"ParsedExpr[BypassPart[SELECT 1 FROM person WHERE p.name = ] " +
			"BypassPart['Fred']]",
	}, {
		"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.*, " +
			"(5+7), (col1 * col2) AS calculated_value FROM person AS p " +
			"JOIN address AS a ON p.address_id = a.id WHERE p.name = 'Fred'",
		"ParsedExpr[BypassPart[SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.*, (5+7), (col1 * col2) AS calculated_value FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = ] " +
			"BypassPart['Fred']]",
	}, {
		"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* " +
			"FROM person AS p JOIN address AS a ON p .address_id = a.id " +
			"WHERE p.name = 'Fred'",
		"ParsedExpr[BypassPart[SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.*" +
			" FROM person AS p JOIN address AS a ON p .address_id = a.id WHERE p.name = ] " +
			"BypassPart['Fred']]",
	}, {
		"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* " +
			"FROM person AS p JOIN address AS a ON p.address_id = a.id " +
			"WHERE p.name in (SELECT name FROM table WHERE table.n = $Person.name)",
		"ParsedExpr[BypassPart[SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* " +
			"FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name in (SELECT name FROM table WHERE table.n = ] " +
			"InputPart[Person.name] " +
			"BypassPart[)]]",
	}, {
		"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* " +
			"FROM person WHERE p.name in (SELECT name FROM table " +
			"WHERE table.n = $Person.name) UNION " +
			"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* " +
			"FROM person WHERE p.name in " +
			"(SELECT name FROM table WHERE table.n = $Person.name)",
		"ParsedExpr[BypassPart[SELECT p.* AS &Person.*, " +
			"(a.district, a.street) AS &Address.* " +
			"FROM person WHERE p.name in (SELECT name FROM table WHERE table.n = ] " +
			"InputPart[Person.name] " +
			"BypassPart[) UNION SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* " +
			"FROM person WHERE p.name in (SELECT name FROM table WHERE table.n = ] " +
			"InputPart[Person.name] " +
			"BypassPart[)]]",
	}, {
		"SELECT p.* AS &Person.*, m.* AS &Manager.* " +
			"FROM person AS p JOIN person AS m " +
			"ON p.manager_id = m.id WHERE p.name = 'Fred'",
		"ParsedExpr[BypassPart[SELECT p.* AS &Person.*, m.* AS &Manager.* " +
			"FROM person AS p JOIN person AS m ON p.manager_id = m.id WHERE p.name = ] " +
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
		"SELECT p.* AS &Person, a.District AS &District " +
			"FROM person AS p JOIN address AS a ON p.address_id = a.id " +
			"WHERE p.name = $Person.name AND p.address_id = $Person.address_id",
		"ParsedExpr[BypassPart[SELECT p.* AS &Person, a.District AS &District " +
			"FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = ] " +
			"InputPart[Person.name] " +
			"BypassPart[ AND p.address_id = ] " +
			"InputPart[Person.address_id]]",
	}, {
		"SELECT p.* AS &Person, a.District AS &District " +
			"FROM person AS p INNER JOIN address AS a " +
			"ON p.address_id = $Address.ID " +
			"WHERE p.name = $Person.name AND p.address_id = $Person.address_id",
		"ParsedExpr[BypassPart[SELECT p.* AS &Person, a.District AS &District " +
			"FROM person AS p INNER JOIN address AS a ON p.address_id = ] " +
			"InputPart[Address.ID] " +
			"BypassPart[ WHERE p.name = ] " +
			"InputPart[Person.name] " +
			"BypassPart[ AND p.address_id = ] " +
			"InputPart[Person.address_id]]",
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
		"UPDATE person SET person.address_id = $Address.ID " +
			"WHERE person.id = $Person.ID",
		"ParsedExpr[BypassPart[UPDATE person SET person.address_id = ] " +
			"InputPart[Address.ID] " +
			"BypassPart[ WHERE person.id = ] " +
			"InputPart[Person.ID]]",
	}}

	parser := parse.NewParser()
	for i, test := range tests {
		var parsedExpr *parse.ParsedExpr
		var err error
		if parsedExpr, err = parser.Parse(test.input); err != nil {
			t.Errorf("test %d failed (Parse): input: %s\nexpected: %s\nerr: %s\n", i, test.input, test.expectedParsed, err)
		} else if parsedExpr.String() != test.expectedParsed {
			t.Errorf("test %d failed (Parse): input: %s\nexpected: %s\nactual:   %s\n", i, test.input, test.expectedParsed, parsedExpr.String())
		}
	}
}

func TestUnfinishedStringLiteral(t *testing.T) {
	var testTable = []struct {
		name string
		sql  string
		err  string
	}{
		{
			"missing right single quote",
			"SELECT foo FROM t WHERE x = 'dddd",
			"cannot parse expression: missing right quote in string literal",
		}, {
			"missing right double quote",
			"SELECT foo FROM t WHERE x = \"dddd",
			"cannot parse expression: missing right quote in string literal",
		}, {
			"quote missmatch",
			"SELECT foo FROM t WHERE x = \"dddd'",
			"cannot parse expression: missing right quote in string literal",
		},
	}

	for _, test := range testTable {
		parser := parse.NewParser()
		res, err := parser.Parse(test.sql)
		t.Log(test.name)
		assert.Equal(t, test.err, err.Error())
		assert.Equal(t, (*parse.ParsedExpr)(nil), res)
	}
}

// Properly parsing empty string literal
func TestEmptyStringLiteral(t *testing.T) {
	sql := "SELECT foo FROM t WHERE x = ''"
	parser := parse.NewParser()
	_, err := parser.Parse(sql)
	assert.Equal(t, nil, err)
}

// Detect bad escaped string literal
func TestBadEscaped(t *testing.T) {
	sql := "SELECT foo FROM t WHERE x = 'O'Donnell'"
	parser := parse.NewParser()
	_, err := parser.Parse(sql)
	assert.Equal(t, fmt.Errorf("cannot parse expression: missing right quote in string literal"), err)
}

func TestBadFormatInput(t *testing.T) {
	var testTable = []struct {
		name string
		sql  string
		err  string
	}{
		{
			"missing field name",
			"SELECT foo FROM t WHERE x = $Address.",
			"cannot parse expression: invalid identifier near char 37",
		}, {
			"object not qualified",
			"SELECT foo FROM t WHERE x = $Address",
			"cannot parse expression: go object near char 36 not qualified",
		}, {
			"field name bad syntax",
			"SELECT foo FROM t WHERE x = $Address.&d",
			"cannot parse expression: invalid identifier near char 37",
		}, {
			"bad field name",
			"SELECT foo FROM t WHERE x = $Address.-",
			"cannot parse expression: invalid identifier near char 37",
		},
	}

	for _, test := range testTable {
		parser := parse.NewParser()
		res, err := parser.Parse(test.sql)
		t.Log(test.name)
		assert.Equal(t, test.err, err.Error())
		assert.Equal(t, (*parse.ParsedExpr)(nil), res)
	}
}
