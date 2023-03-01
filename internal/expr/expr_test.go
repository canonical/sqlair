package expr_test

import (
	"database/sql"
	"testing"

	"github.com/canonical/sqlair/internal/expr"
	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func TestExpr(t *testing.T) { TestingT(t) }

type ExprSuite struct{}

var _ = Suite(&ExprSuite{})

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
	summary          string
	input            string
	expectedParsed   string
	prepareArgs      []any
	expectedPrepared string
}{{
	"star table as output",
	"SELECT p.* AS &Person.*",
	"[Bypass[SELECT ] Output[[p.*] [Person.*]]]",
	[]any{Person{}},
	"SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2",
}, {
	"spaces and tabs",
	"SELECT p.* 	AS 		   &Person.*",
	"[Bypass[SELECT ] Output[[p.*] [Person.*]]]",
	[]any{Person{}},
	"SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2",
}, {
	"new lines",
	`SELECT
		p.* AS &Person.*,
		foo
	 FROM t
	 WHERE
		foo = bar
		and
		x = y`,
	`[Bypass[SELECT
		] Output[[p.*] [Person.*]] Bypass[,
		foo
	 FROM t
	 WHERE
		foo = bar
		and
		x = y]]`,
	[]any{Person{}},
	`SELECT
		p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2,
		foo
	 FROM t
	 WHERE
		foo = bar
		and
		x = y`,
}, {
	"quoted output expression",
	"SELECT p.* AS &Person.*, '&notAnOutputExpresion.*' AS literal FROM t",
	"[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[, '&notAnOutputExpresion.*' AS literal FROM t]]",
	[]any{Person{}},
	"SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2, '&notAnOutputExpresion.*' AS literal FROM t",
}, {
	"quoted input expression",
	"SELECT foo FROM t WHERE bar = '$NotAn.input'",
	"[Bypass[SELECT foo FROM t WHERE bar = '$NotAn.input']]",
	[]any{},
	`SELECT foo FROM t WHERE bar = '$NotAn.input'`,
}, {
	"star as output",
	"SELECT * AS &Person.* FROM t",
	"[Bypass[SELECT ] Output[[*] [Person.*]] Bypass[ FROM t]]",
	[]any{Person{}},
	"SELECT address_id AS _sqlair_0, id AS _sqlair_1, name AS _sqlair_2 FROM t",
}, {
	"input v1",
	"SELECT foo, bar FROM table WHERE foo = $Person.id",
	"[Bypass[SELECT foo, bar FROM table WHERE foo = ] Input[Person.id]]",
	[]any{Person{}},
	`SELECT foo, bar FROM table WHERE foo = ?`,
}, {
	"input v2",
	"SELECT p FROM person WHERE p.name = $Person.name",
	"[Bypass[SELECT p FROM person WHERE p.name = ] Input[Person.name]]",
	[]any{Person{}},
	`SELECT p FROM person WHERE p.name = ?`,
}, {
	"input v3",
	"SELECT p.*, a.district FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = $Person.name",
	"[Bypass[SELECT p.*, a.district FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = ] Input[Person.name]]",
	[]any{Person{}},
	`SELECT p.*, a.district FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = ?`,
}, {
	"output and input",
	"SELECT &Person.* FROM table WHERE foo = $Address.id",
	"[Bypass[SELECT ] Output[[] [Person.*]] Bypass[ FROM table WHERE foo = ] Input[Address.id]]",
	[]any{Person{}, Address{}},
	`SELECT address_id AS _sqlair_0, id AS _sqlair_1, name AS _sqlair_2 FROM table WHERE foo = ?`,
}, {
	"output and quote",
	"SELECT foo, bar, &Person.id FROM table WHERE foo = 'xx'",
	"[Bypass[SELECT foo, bar, ] Output[[] [Person.id]] Bypass[ FROM table WHERE foo = 'xx']]",
	[]any{Person{}},
	"SELECT foo, bar, id AS _sqlair_0 FROM table WHERE foo = 'xx'",
}, {
	"two outputs and quote",
	"SELECT foo, &Person.id, bar, baz, &Manager.manager_name FROM table WHERE foo = 'xx'",
	"[Bypass[SELECT foo, ] Output[[] [Person.id]] Bypass[, bar, baz, ] Output[[] [Manager.manager_name]] Bypass[ FROM table WHERE foo = 'xx']]",
	[]any{Person{}, Manager{}},
	"SELECT foo, id AS _sqlair_0, bar, baz, manager_name AS _sqlair_1 FROM table WHERE foo = 'xx'",
}, {
	"star as output and quote",
	"SELECT * AS &Person.* FROM person WHERE name = 'Fred'",
	"[Bypass[SELECT ] Output[[*] [Person.*]] Bypass[ FROM person WHERE name = 'Fred']]",
	[]any{Person{}},
	"SELECT address_id AS _sqlair_0, id AS _sqlair_1, name AS _sqlair_2 FROM person WHERE name = 'Fred'",
}, {
	"star output and quote",
	"SELECT &Person.* FROM person WHERE name = 'Fred'",
	"[Bypass[SELECT ] Output[[] [Person.*]] Bypass[ FROM person WHERE name = 'Fred']]",
	[]any{Person{}},
	"SELECT address_id AS _sqlair_0, id AS _sqlair_1, name AS _sqlair_2 FROM person WHERE name = 'Fred'",
}, {
	"two star as outputs and quote",
	"SELECT * AS &Person.*, a.* AS &Address.* FROM person, address a WHERE name = 'Fred'",
	"[Bypass[SELECT ] Output[[*] [Person.*]] Bypass[, ] Output[[a.*] [Address.*]] Bypass[ FROM person, address a WHERE name = 'Fred']]",
	[]any{Person{}, Address{}},
	"SELECT address_id AS _sqlair_0, id AS _sqlair_1, name AS _sqlair_2, a.district AS _sqlair_3, a.id AS _sqlair_4, a.street AS _sqlair_5 FROM person, address a WHERE name = 'Fred'",
}, {
	"multicolumn output v1",
	"SELECT (a.district, a.street) AS (&Address.district, &Address.street) FROM address AS a",
	"[Bypass[SELECT ] Output[[a.district a.street] [Address.district Address.street]] Bypass[ FROM address AS a]]",
	[]any{Address{}, District{}},
	"SELECT a.district AS _sqlair_0, a.street AS _sqlair_1 FROM address AS a",
}, {
	"multicolumn output v2",
	"SELECT (a.district, a.street) AS (&Address.district, &Address.street), a.id AS &Person.id FROM address AS a",
	"[Bypass[SELECT ] Output[[a.district a.street] [Address.district Address.street]] Bypass[, ] Output[[a.id] [Person.id]] Bypass[ FROM address AS a]]",
	[]any{Person{}, Address{}},
	"SELECT a.district AS _sqlair_0, a.street AS _sqlair_1, a.id AS _sqlair_2 FROM address AS a",
}, {
	"multicolumn output v3",
	"SELECT (a.district, a.id) AS (&Address.district, &Person.address_id) FROM address AS a",
	"[Bypass[SELECT ] Output[[a.district a.id] [Address.district Person.address_id]] Bypass[ FROM address AS a]]",
	[]any{Person{}, Address{}},
	"SELECT a.district AS _sqlair_0, a.id AS _sqlair_1 FROM address AS a",
}, {
	"multicolumn output v4",
	"SELECT (a.district, a.street) AS &Address.* FROM address AS a WHERE p.name = 'Fred'",
	"[Bypass[SELECT ] Output[[a.district a.street] [Address.*]] Bypass[ FROM address AS a WHERE p.name = 'Fred']]",
	[]any{Address{}},
	"SELECT a.district AS _sqlair_0, a.street AS _sqlair_1 FROM address AS a WHERE p.name = 'Fred'",
}, {
	"multicolumn output v5",
	"SELECT (&Address.street, &Person.id) FROM address AS a WHERE p.name = 'Fred'",
	"[Bypass[SELECT ] Output[[] [Address.street Person.id]] Bypass[ FROM address AS a WHERE p.name = 'Fred']]",
	[]any{Address{}, Person{}},
	"SELECT street AS _sqlair_0, id AS _sqlair_1 FROM address AS a WHERE p.name = 'Fred'",
}, {
	"quote",
	"SELECT 1 FROM person WHERE p.name = 'Fred'",
	"[Bypass[SELECT 1 FROM person WHERE p.name = 'Fred']]",
	[]any{},
	"SELECT 1 FROM person WHERE p.name = 'Fred'",
}, {
	"complex query v1",
	"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.*, (5+7), (col1 * col2) AS calculated_value FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = 'Fred'",
	"[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[, ] Output[[a.district a.street] [Address.*]] Bypass[, (5+7), (col1 * col2) AS calculated_value FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = 'Fred']]",
	[]any{Person{}, Address{}},
	`SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2, a.district AS _sqlair_3, a.street AS _sqlair_4, (5+7), (col1 * col2) AS calculated_value FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = 'Fred'`,
}, {
	"complex query v2",
	"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* FROM person AS p JOIN address AS a ON p .address_id = a.id WHERE p.name = 'Fred'",
	"[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[, ] Output[[a.district a.street] [Address.*]] Bypass[ FROM person AS p JOIN address AS a ON p .address_id = a.id WHERE p.name = 'Fred']]",
	[]any{Person{}, Address{}},
	"SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2, a.district AS _sqlair_3, a.street AS _sqlair_4 FROM person AS p JOIN address AS a ON p .address_id = a.id WHERE p.name = 'Fred'",
}, {
	"complex query v3",
	"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name IN (SELECT name FROM table WHERE table.n = $Person.name)",
	"[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[, ] Output[[a.district a.street] [Address.*]] Bypass[ FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name IN (SELECT name FROM table WHERE table.n = ] Input[Person.name] Bypass[)]]",
	[]any{Person{}, Address{}},
	`SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2, a.district AS _sqlair_3, a.street AS _sqlair_4 FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name IN (SELECT name FROM table WHERE table.n = ?)`,
}, {
	"complex query v4",
	"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = $Person.name) UNION SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = $Person.name)",
	"[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[, ] Output[[a.district a.street] [Address.*]] Bypass[ FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = ] Input[Person.name] Bypass[) UNION SELECT ] Output[[p.*] [Person.*]] Bypass[, ] Output[[a.district a.street] [Address.*]] Bypass[ FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = ] Input[Person.name] Bypass[)]]",
	[]any{Person{}, Address{}},
	`SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2, a.district AS _sqlair_3, a.street AS _sqlair_4 FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = ?) UNION SELECT p.address_id AS _sqlair_5, p.id AS _sqlair_6, p.name AS _sqlair_7, a.district AS _sqlair_8, a.street AS _sqlair_9 FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = ?)`,
}, {
	"complex query v5",
	"SELECT p.* AS &Person.*, &District.* FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = $Person.name AND p.address_id = $Person.address_id",
	"[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[, ] Output[[] [District.*]] Bypass[ FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = ] Input[Person.name] Bypass[ AND p.address_id = ] Input[Person.address_id]]",
	[]any{Person{}, District{}},
	`SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2,  FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = ? AND p.address_id = ?`,
}, {
	"complex query v6",
	"SELECT p.* AS &Person.*, FROM person AS p INNER JOIN address AS a ON p.address_id = $Address.id WHERE p.name = $Person.name AND p.address_id = $Person.address_id",
	"[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[, FROM person AS p INNER JOIN address AS a ON p.address_id = ] Input[Address.id] Bypass[ WHERE p.name = ] Input[Person.name] Bypass[ AND p.address_id = ] Input[Person.address_id]]",
	[]any{Person{}, Address{}},
	`SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2, FROM person AS p INNER JOIN address AS a ON p.address_id = ? WHERE p.name = ? AND p.address_id = ?`,
}, {
	"join v1",
	"SELECT p.* AS &Person.*, m.* AS &Manager.* FROM person AS p JOIN person AS m ON p.manager_id = m.id WHERE p.name = 'Fred'",
	"[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[, ] Output[[m.*] [Manager.*]] Bypass[ FROM person AS p JOIN person AS m ON p.manager_id = m.id WHERE p.name = 'Fred']]",
	[]any{Person{}, Manager{}},
	"SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2, m.manager_name AS _sqlair_3 FROM person AS p JOIN person AS m ON p.manager_id = m.id WHERE p.name = 'Fred'",
}, {
	"join v2",
	"SELECT person.*, address.district FROM person JOIN address ON person.address_id = address.id WHERE person.name = 'Fred'",
	"[Bypass[SELECT person.*, address.district FROM person JOIN address ON person.address_id = address.id WHERE person.name = 'Fred']]",
	[]any{},
	"SELECT person.*, address.district FROM person JOIN address ON person.address_id = address.id WHERE person.name = 'Fred'",
}, {
	"insert",
	"INSERT INTO person (name) VALUES $Person.name",
	"[Bypass[INSERT INTO person (name) VALUES ] Input[Person.name]]",
	[]any{Person{}},
	`INSERT INTO person (name) VALUES ?`,
}, {
	"ignore dollar v1",
	"SELECT $ FROM moneytable",
	"[Bypass[SELECT $ FROM moneytable]]",
	[]any{},
	`SELECT $ FROM moneytable`,
}, {
	"ignore dollar v2",
	"SELECT foo FROM data$",
	"[Bypass[SELECT foo FROM data$]]",
	[]any{},
	`SELECT foo FROM data$`,
}, {
	"ignore dollar v3",
	"SELECT dollerrow$ FROM moneytable",
	"[Bypass[SELECT dollerrow$ FROM moneytable]]",
	[]any{},
	`SELECT dollerrow$ FROM moneytable`,
}, {
	"input with no space",
	"SELECT p.*, a.district FROM person AS p WHERE p.name=$Person.name",
	"[Bypass[SELECT p.*, a.district FROM person AS p WHERE p.name=] Input[Person.name]]",
	[]any{Person{}},
	`SELECT p.*, a.district FROM person AS p WHERE p.name=?`,
}, {
	"escaped double quote",
	`SELECT foo FROM t WHERE t.p = "Jimmy ""Quickfingers"" Jones"`,
	`[Bypass[SELECT foo FROM t WHERE t.p = "Jimmy ""Quickfingers"" Jones"]]`,
	[]any{},
	`SELECT foo FROM t WHERE t.p = "Jimmy ""Quickfingers"" Jones"`,
}, {
	"escaped single quote",
	`SELECT foo FROM t WHERE t.p = 'Olly O''Flanagan'`,
	`[Bypass[SELECT foo FROM t WHERE t.p = 'Olly O''Flanagan']]`,
	[]any{},
	`SELECT foo FROM t WHERE t.p = 'Olly O''Flanagan'`,
}, {
	"complex escaped quotes",
	`SELECT * AS &Person.* FROM person WHERE name IN ('Lorn', 'Onos T''oolan', '', ''' ''');`,
	`[Bypass[SELECT ] Output[[*] [Person.*]] Bypass[ FROM person WHERE name IN ('Lorn', 'Onos T''oolan', '', ''' ''');]]`,
	[]any{Person{}},
	`SELECT address_id AS _sqlair_0, id AS _sqlair_1, name AS _sqlair_2 FROM person WHERE name IN ('Lorn', 'Onos T''oolan', '', ''' ''');`,
}, {
	"update",
	"UPDATE person SET person.address_id = $Address.id WHERE person.id = $Person.id",
	"[Bypass[UPDATE person SET person.address_id = ] Input[Address.id] Bypass[ WHERE person.id = ] Input[Person.id]]",
	[]any{Person{}, Address{}},
	`UPDATE person SET person.address_id = ? WHERE person.id = ?`,
}}

func (s *ExprSuite) TestExpr(c *C) {
	parser := expr.NewParser()
	for i, test := range tests {
		var (
			parsedExpr   *expr.ParsedExpr
			preparedExpr *expr.PreparedExpr
			err          error
		)
		if parsedExpr, err = parser.Parse(test.input); err != nil {
			c.Errorf("test %d failed (Parse):\nsummary: %s\ninput: %s\nexpected: %s\nerr: %s\n", i, test.summary, test.input, test.expectedParsed, err)
		} else if parsedExpr.String() != test.expectedParsed {
			c.Errorf("test %d failed (Parse):\nsummary: %s\ninput: %s\nexpected: %s\nactual:   %s\n", i, test.summary, test.input, test.expectedParsed, parsedExpr.String())
		}

		if preparedExpr, err = parsedExpr.Prepare(test.prepareArgs...); err != nil {
			c.Errorf("test %d failed (Prepare):\nsummary: %s\ninput: %s\nexpected: %s\nerr: %s\n", i, test.summary, test.input, test.expectedPrepared, err)
		} else {
			c.Check(expr.PreparedSQL(preparedExpr), Equals, test.expectedPrepared,
				Commentf("test %d failed (Prepare):\nsummary: %s\ninput: %s\nexpected: %s\nactual:   %s\n", i, test.summary, test.input, test.expectedPrepared, expr.PreparedSQL(preparedExpr)))
		}
	}
}

func (s *ExprSuite) TestValidInput(c *C) {
	testList := []struct {
		input          string
		expectedParsed string
	}{{
		"SELECT street FROM t WHERE x = $Address.street",
		"[Bypass[SELECT street FROM t WHERE x = ] Input[Address.street]]",
	}, {
		"SELECT p FROM t WHERE x = $Person.id",
		"[Bypass[SELECT p FROM t WHERE x = ] Input[Person.id]]",
	}}
	for _, test := range testList {
		parser := expr.NewParser()
		parsedExpr, err := parser.Parse(test.input)
		if err != nil {
			c.Fatal(err)
		}
		c.Assert(parsedExpr.String(), Equals, test.expectedParsed)
	}
}

// We return a proper error when we find an unbound string literal
func (s *ExprSuite) TestParseUnfinishedStringLiteral(c *C) {
	testList := []string{
		"SELECT foo FROM t WHERE x = 'dddd",
		"SELECT foo FROM t WHERE x = \"dddd",
		"SELECT foo FROM t WHERE x = \"dddd'",
		"SELECT foo FROM t WHERE x = '''",
		`SELECT foo FROM t WHERE x = '''""`,
		`SELECT foo FROM t WHERE x = """`,
		`SELECT foo FROM t WHERE x = """''`,
		`SELECT foo FROM t WHERE x = 'O'Donnell'`,
	}

	for _, sql := range testList {
		parser := expr.NewParser()
		expr, err := parser.Parse(sql)
		c.Assert(err, ErrorMatches, "cannot parse expression: column [0-9]+: missing closing quote in string literal")
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

// Detect well escaped string literals
func (s *ExprSuite) TestWellEscaped(c *C) {
	sqls := []string{
		`SELECT foo FROM t WHERE x = 'O''Donnell'`,
		`SELECT foo FROM t WHERE x = "O""Donnell"`,
		`SELECT foo FROM t WHERE x = 'O''Do''nnell'`,
		`SELECT foo FROM t WHERE x = "O""Do""nnell"`,
	}

	for _, sql := range sqls {
		parser := expr.NewParser()
		_, err := parser.Parse(sql)
		c.Assert(err, IsNil)
	}
}

func (s *ExprSuite) TestParseBadFormatInput(c *C) {
	testList := []string{
		"SELECT foo FROM t WHERE x = $Address.",
		"SELECT foo FROM t WHERE x = $Address.&d",
		"SELECT foo FROM t WHERE x = $Address.-",
	}

	for _, sql := range testList {
		parser := expr.NewParser()
		expr, err := parser.Parse(sql)
		c.Assert(err, ErrorMatches, "cannot parse expression: column [0-9]+: invalid identifier")
		c.Assert(expr, IsNil)
	}

	sql := "SELECT foo FROM t WHERE x = $Address"
	parser := expr.NewParser()
	_, err := parser.Parse(sql)
	c.Assert(err, ErrorMatches, "cannot parse expression: column [0-9]+: type not qualified")
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

func (s *ExprSuite) TestPrepareMismatchedStructName(c *C) {
	testList := []struct {
		sql      string
		structs  []any
		errorstr string
	}{{
		sql:      "SELECT street FROM t WHERE x = $Address.street",
		structs:  []any{Person{ID: 1}},
		errorstr: "cannot prepare expression: type Address unknown, have: Person",
	}, {
		sql:      "SELECT street AS &Address.street FROM t",
		structs:  []any{},
		errorstr: "cannot prepare expression: type Address unknown, have: ",
	}, {
		sql:      "SELECT street AS &Address.id FROM t",
		structs:  []any{Person{ID: 1}},
		errorstr: "cannot prepare expression: type Address unknown, have: Person",
	}}

	for i, test := range testList {
		parser := expr.NewParser()
		parsedExpr, err := parser.Parse(test.sql)
		if err != nil {
			c.Fatal(err)
		}
		_, err = parsedExpr.Prepare(test.structs...)
		c.Assert(err, ErrorMatches, test.errorstr,
			Commentf("test %d failed:\nsql: '%s'\nstructs: '%+v'", i, test.sql, test.structs))
	}

}

func (s *ExprSuite) TestPrepareMissingTag(c *C) {
	testList := []struct {
		sql      string
		structs  []any
		errorstr string
	}{{
		sql:      "SELECT street FROM t WHERE x = $Address.number",
		structs:  []any{Address{ID: 1}},
		errorstr: `cannot prepare expression: type Address has no "number" db tag`,
	}, {
		sql:      "SELECT (street, road) AS &Address.* FROM t",
		structs:  []any{Address{ID: 1}},
		errorstr: `cannot prepare expression: type Address has no "road" db tag`,
	}, {
		sql:      "SELECT &Address.road FROM t",
		structs:  []any{Address{ID: 1}},
		errorstr: `cannot prepare expression: type Address has no "road" db tag`,
	}}

	for i, test := range testList {
		parser := expr.NewParser()
		parsedExpr, err := parser.Parse(test.sql)
		if err != nil {
			c.Fatal(err)
		}
		_, err = parsedExpr.Prepare(test.structs...)
		c.Assert(err, ErrorMatches, test.errorstr,
			Commentf("test %d failed:\nsql: '%s'\nstructs:'%+v'", i, test.sql, test.structs))

	}
}

func (s *ExprSuite) TestPrepareInvalidAsteriskPlacement(c *C) {
	testList := []struct {
		sql      string
		structs  []any
		errorstr string
	}{{
		sql:      "SELECT (&Person.*, &Person.*) FROM t",
		structs:  []any{Address{}, Person{}},
		errorstr: "cannot prepare expression: invalid asterisk in output expression: Output[[] [Person.* Person.*]]",
	}, {
		sql:      "SELECT (p.*, t.*) AS &Address.* FROM t",
		structs:  []any{Address{}},
		errorstr: "cannot prepare expression: invalid asterisk in output expression: Output[[p.* t.*] [Address.*]]",
	}, {
		sql:      "SELECT p.* AS &Address.street FROM t",
		structs:  []any{Address{}},
		errorstr: "cannot prepare expression: invalid asterisk in output expression: Output[[p.*] [Address.street]]",
	}}

	for i, test := range testList {
		parser := expr.NewParser()
		parsedExpr, err := parser.Parse(test.sql)
		if err != nil {
			c.Fatal(err)
		}
		_, err = parsedExpr.Prepare(test.structs...)
		c.Assert(err.Error(), Equals, test.errorstr,
			Commentf("test %d failed:\nsql: '%s'\nstructs:'%+v'", i, test.sql, test.structs))
	}
}

func (s *ExprSuite) TestPrepareAsteriskMix(c *C) {
	testList := []struct {
		sql      string
		structs  []any
		errorstr string
	}{{
		sql:      "SELECT (&Address.*, &Address.id) FROM t",
		structs:  []any{Address{}, Person{}},
		errorstr: "cannot prepare expression: invalid asterisk in output expression: Output[[] [Address.* Address.id]]",
	}, {
		sql:      "SELECT (p.*, t.name) AS &Address.* FROM t",
		structs:  []any{Address{}},
		errorstr: "cannot prepare expression: invalid asterisk in output expression: Output[[p.* t.name] [Address.*]]",
	}, {
		sql:      "SELECT (name, p.*) AS (&Person.id, &Person.*) FROM t",
		structs:  []any{Address{}, Person{}},
		errorstr: "cannot prepare expression: invalid asterisk in output expression: Output[[name p.*] [Person.id Person.*]]",
	}}

	for i, test := range testList {
		parser := expr.NewParser()
		parsedExpr, err := parser.Parse(test.sql)
		if err != nil {
			c.Fatal(err)
		}
		_, err = parsedExpr.Prepare(test.structs...)
		c.Assert(err.Error(), Equals, test.errorstr,
			Commentf("test %d failed:\nsql: '%s'\nstructs:'%+v'", i, test.sql, test.structs))
	}
}

func (s *ExprSuite) TestPrepareMismatchedColsAndTargs(c *C) {
	testList := []struct {
		sql      string
		structs  []any
		errorstr string
	}{{
		sql:      "SELECT (p.name, t.id) AS &Address.id FROM t",
		structs:  []any{Address{}},
		errorstr: "cannot prepare expression: mismatched number of cols and targets in output expression: Output[[p.name t.id] [Address.id]]",
	}, {
		sql:      "SELECT p.name AS (&Address.district, &Address.street) FROM t",
		structs:  []any{Address{}},
		errorstr: "cannot prepare expression: mismatched number of cols and targets in output expression: Output[[p.name] [Address.district Address.street]]",
	}}

	for i, test := range testList {
		parser := expr.NewParser()
		parsedExpr, err := parser.Parse(test.sql)
		if err != nil {
			c.Fatal(err)
		}
		_, err = parsedExpr.Prepare(test.structs...)
		c.Assert(err.Error(), Equals, test.errorstr,
			Commentf("test %d failed:\nsql: '%s'\nstructs:'%+v'", i, test.sql, test.structs))
	}
}

func (s *ExprSuite) TestValidComplete(c *C) {
	testList := []struct {
		input          string
		prepareArgs    []any
		completeArgs   []any
		completeValues []any
	}{{
		"SELECT * AS &Address.* FROM t WHERE x = $Person.name",
		[]any{Address{}, Person{}},
		[]any{Person{Fullname: "Jimany Johnson"}},
		[]any{sql.Named("sqlair_0", "Jimany Johnson")},
	}, {
		"SELECT foo FROM t WHERE x = $Address.street, y = $Person.id",
		[]any{Person{}, Address{}},
		[]any{Person{ID: 666}, Address{Street: "Highway to Hell"}},
		[]any{sql.Named("sqlair_0", "Highway to Hell"), sql.Named("sqlair_1", 666)},
	}}
	for _, test := range testList {
		parser := expr.NewParser()
		parsedExpr, err := parser.Parse(test.input)
		if err != nil {
			c.Fatal(err)
		}

		preparedExpr, err := parsedExpr.Prepare(test.prepareArgs...)
		if err != nil {
			c.Fatal(err)
		}

		completedExpr, err := preparedExpr.Complete(test.completeArgs...)
		if err != nil {
			c.Fatal(err)
		}

		c.Assert(expr.CompletedArgs(completedExpr), DeepEquals, test.completeValues)
	}
}

func (s *ExprSuite) TestCompleteMissingParameter(c *C) {
	sql := "SELECT street FROM t WHERE x = $Address.street, y = $Person.name"
	parser := expr.NewParser()
	parsedExpr, err := parser.Parse(sql)
	if err != nil {
		c.Fatal(err)
	}

	preparedExpr, err := parsedExpr.Prepare(Address{}, Person{})
	if err != nil {
		c.Fatal(err)
	}
	_, err = preparedExpr.Complete(Address{Street: "Dead end road"})
	c.Assert(err, ErrorMatches, `invalid input parameter: type Person not found, have: Address`)
}

func (s *ExprSuite) TestCompleteNilType(c *C) {
	sql := "SELECT street FROM t WHERE x = $Address.street, y = $Person.name"
	parser := expr.NewParser()
	parsedExpr, err := parser.Parse(sql)
	if err != nil {
		c.Fatal(err)
	}
	preparedExpr, err := parsedExpr.Prepare(Address{}, Person{})
	if err != nil {
		c.Fatal(err)
	}
	_, err = preparedExpr.Complete(nil, Person{Fullname: "Monty Bingles"})
	c.Assert(err, ErrorMatches, "invalid input parameter: need valid struct, got nil")
}

func (s *ExprSuite) TestCompleteDifferentType(c *C) {
	sql := "SELECT street FROM t WHERE y = $Person.name"
	outerP := Person{}
	//type Person struct{}
	type Person struct {
		ID         int    `db:"id"`
		Fullname   string `db:"name"`
		PostalCode int    `db:"address_id"`
	}
	shadowedP := Person{}
	parser := expr.NewParser()
	parsedExpr, err := parser.Parse(sql)
	if err != nil {
		c.Fatal(err)
	}
	preparedExpr, err := parsedExpr.Prepare(outerP)
	if err != nil {
		c.Fatal(err)
	}
	_, err = preparedExpr.Complete(shadowedP)
	c.Assert(err, ErrorMatches, `invalid input parameter: type expr_test.Person not found, have expr_test.Person`)
}
