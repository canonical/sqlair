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

type Manager Person

type District struct {
}

type M map[string]any

var tests = []struct {
	summary          string
	query            string
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
	`SELECT foo, bar FROM table WHERE foo = @sqlair_0`,
}, {
	"input v2",
	"SELECT p FROM person WHERE p.name = $Person.name",
	"[Bypass[SELECT p FROM person WHERE p.name = ] Input[Person.name]]",
	[]any{Person{}},
	`SELECT p FROM person WHERE p.name = @sqlair_0`,
}, {
	"input v3",
	"SELECT p.*, a.district FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = $Person.name",
	"[Bypass[SELECT p.*, a.district FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = ] Input[Person.name]]",
	[]any{Person{}},
	`SELECT p.*, a.district FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = @sqlair_0`,
}, {
	"output and input",
	"SELECT &Person.* FROM table WHERE foo = $Address.id",
	"[Bypass[SELECT ] Output[[] [Person.*]] Bypass[ FROM table WHERE foo = ] Input[Address.id]]",
	[]any{Person{}, Address{}},
	`SELECT address_id AS _sqlair_0, id AS _sqlair_1, name AS _sqlair_2 FROM table WHERE foo = @sqlair_0`,
}, {
	"output and quote",
	"SELECT foo, bar, &Person.id FROM table WHERE foo = 'xx'",
	"[Bypass[SELECT foo, bar, ] Output[[] [Person.id]] Bypass[ FROM table WHERE foo = 'xx']]",
	[]any{Person{}},
	"SELECT foo, bar, id AS _sqlair_0 FROM table WHERE foo = 'xx'",
}, {
	"two outputs and quote",
	"SELECT foo, &Person.id, bar, baz, &Manager.name FROM table WHERE foo = 'xx'",
	"[Bypass[SELECT foo, ] Output[[] [Person.id]] Bypass[, bar, baz, ] Output[[] [Manager.name]] Bypass[ FROM table WHERE foo = 'xx']]",
	[]any{Person{}, Manager{}},
	"SELECT foo, id AS _sqlair_0, bar, baz, name AS _sqlair_1 FROM table WHERE foo = 'xx'",
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
	`SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2, a.district AS _sqlair_3, a.street AS _sqlair_4 FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name IN (SELECT name FROM table WHERE table.n = @sqlair_0)`,
}, {
	"complex query v4",
	"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = $Person.name) UNION SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = $Person.name)",
	"[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[, ] Output[[a.district a.street] [Address.*]] Bypass[ FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = ] Input[Person.name] Bypass[) UNION SELECT ] Output[[p.*] [Person.*]] Bypass[, ] Output[[a.district a.street] [Address.*]] Bypass[ FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = ] Input[Person.name] Bypass[)]]",
	[]any{Person{}, Address{}},
	`SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2, a.district AS _sqlair_3, a.street AS _sqlair_4 FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = @sqlair_0) UNION SELECT p.address_id AS _sqlair_5, p.id AS _sqlair_6, p.name AS _sqlair_7, a.district AS _sqlair_8, a.street AS _sqlair_9 FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = @sqlair_1)`,
}, {
	"complex query v5",
	"SELECT p.* AS &Person.*, &District.* FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = $Person.name AND p.address_id = $Person.address_id",
	"[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[, ] Output[[] [District.*]] Bypass[ FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = ] Input[Person.name] Bypass[ AND p.address_id = ] Input[Person.address_id]]",
	[]any{Person{}, District{}},
	`SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2,  FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = @sqlair_0 AND p.address_id = @sqlair_1`,
}, {
	"complex query v6",
	"SELECT p.* AS &Person.*, FROM person AS p INNER JOIN address AS a ON p.address_id = $Address.id WHERE p.name = $Person.name AND p.address_id = $Person.address_id",
	"[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[, FROM person AS p INNER JOIN address AS a ON p.address_id = ] Input[Address.id] Bypass[ WHERE p.name = ] Input[Person.name] Bypass[ AND p.address_id = ] Input[Person.address_id]]",
	[]any{Person{}, Address{}},
	`SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2, FROM person AS p INNER JOIN address AS a ON p.address_id = @sqlair_0 WHERE p.name = @sqlair_1 AND p.address_id = @sqlair_2`,
}, {
	"join v1",
	"SELECT p.* AS &Person.*, m.* AS &Manager.* FROM person AS p JOIN person AS m ON p.id = m.id WHERE p.name = 'Fred'",
	"[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[, ] Output[[m.*] [Manager.*]] Bypass[ FROM person AS p JOIN person AS m ON p.id = m.id WHERE p.name = 'Fred']]",
	[]any{Person{}, Manager{}},
	"SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2, m.address_id AS _sqlair_3, m.id AS _sqlair_4, m.name AS _sqlair_5 FROM person AS p JOIN person AS m ON p.id = m.id WHERE p.name = 'Fred'",
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
	`INSERT INTO person (name) VALUES @sqlair_0`,
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
	`SELECT p.*, a.district FROM person AS p WHERE p.name=@sqlair_0`,
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
	`UPDATE person SET person.address_id = @sqlair_0 WHERE person.id = @sqlair_1`,
}}

func (s *ExprSuite) TestExpr(c *C) {
	parser := expr.NewParser()
	for i, t := range tests {
		var (
			parsedExpr   *expr.ParsedExpr
			preparedExpr *expr.PreparedExpr
			err          error
		)
		if parsedExpr, err = parser.Parse(t.query); err != nil {
			c.Errorf("test %d failed (Parse):\nsummary: %s\nquery: %s\nexpected: %s\nerr: %s\n", i, t.summary, t.query, t.expectedParsed, err)
		} else if parsedExpr.String() != t.expectedParsed {
			c.Errorf("test %d failed (Parse):\nsummary: %s\nquery: %s\nexpected: %s\nactual:   %s\n", i, t.summary, t.query, t.expectedParsed, parsedExpr.String())
		}

		if preparedExpr, err = parsedExpr.Prepare(t.prepareArgs...); err != nil {
			c.Errorf("test %d failed (Prepare):\nsummary: %s\nquery: %s\nexpected: %s\nerr: %s\n", i, t.summary, t.query, t.expectedPrepared, err)
		} else {
			c.Check(expr.PreparedSQL(preparedExpr), Equals, t.expectedPrepared,
				Commentf("test %d failed (Prepare):\nsummary: %s\nquery: %s\nexpected: %s\nactual:   %s\n", i, t.summary, t.query, t.expectedPrepared, expr.PreparedSQL(preparedExpr)))
		}
	}
}

func (s *ExprSuite) TestParseErrors(c *C) {
	tests := []struct {
		query string
		err   string
	}{{
		query: "SELECT foo FROM t WHERE x = 'dddd",
		err:   "cannot parse expression: column 28: missing closing quote in string literal",
	}, {
		query: "SELECT foo FROM t WHERE x = \"dddd",
		err:   "cannot parse expression: column 28: missing closing quote in string literal",
	}, {
		query: "SELECT foo FROM t WHERE x = \"dddd'",
		err:   "cannot parse expression: column 28: missing closing quote in string literal",
	}, {
		query: "SELECT foo FROM t WHERE x = '''",
		err:   "cannot parse expression: column 28: missing closing quote in string literal",
	}, {
		query: `SELECT foo FROM t WHERE x = '''""`,
		err:   "cannot parse expression: column 28: missing closing quote in string literal",
	}, {
		query: `SELECT foo FROM t WHERE x = """`,
		err:   "cannot parse expression: column 28: missing closing quote in string literal",
	}, {
		query: `SELECT foo FROM t WHERE x = """''`,
		err:   "cannot parse expression: column 28: missing closing quote in string literal",
	}, {
		query: `SELECT foo FROM t WHERE x = 'O'Donnell'`,
		err:   "cannot parse expression: column 38: missing closing quote in string literal",
	}, {
		query: "SELECT foo FROM t WHERE x = $Address.",
		err:   `cannot parse expression: column 37: invalid identifier following "Address"`,
	}, {
		query: "SELECT foo FROM t WHERE x = $Address.&d",
		err:   `cannot parse expression: column 37: invalid identifier following "Address"`,
	}, {
		query: "SELECT foo FROM t WHERE x = $Address.-",
		err:   `cannot parse expression: column 37: invalid identifier following "Address"`,
	}, {
		query: "SELECT foo FROM t WHERE x = $Address",
		err:   `cannot parse expression: column 36: type "Address" not qualified. Types can be qualified with a db tag or an asterisk. e.g. &P.col1 or &P.*`,
	}}

	for _, t := range tests {
		parser := expr.NewParser()
		expr, err := parser.Parse(t.query)
		if err != nil {
			c.Assert(err.Error(), Equals, t.err)
		} else {
			c.Errorf("Expecting %q, got nil", t.err)
		}
		c.Assert(expr, IsNil)
	}
}

func FuzzParser(f *testing.F) {
	// Add some values to the corpus.
	for _, test := range tests {
		f.Add(test.query)
	}
	f.Fuzz(func(t *testing.T, s string) {
		// Loop forever or until it crashes.
		parser := expr.NewParser()
		parser.Parse(s)
	})
}

func (s *ExprSuite) TestPrepareErrors(c *C) {
	tests := []struct {
		query       string
		prepareArgs []any
		err         string
	}{{
		query:       "SELECT (p.name, t.id) AS &Address.id FROM t",
		prepareArgs: []any{Address{}},
		err:         "cannot prepare expression: mismatched number of cols and targets in output expression: (p.name, t.id) AS &Address.id",
	}, {
		query:       "SELECT p.name AS (&Address.district, &Address.street) FROM t",
		prepareArgs: []any{Address{}},
		err:         "cannot prepare expression: mismatched number of cols and targets in output expression: p.name AS (&Address.district, &Address.street)",
	}, {
		query:       "SELECT (&Address.*, &Address.id) FROM t",
		prepareArgs: []any{Address{}, Person{}},
		err:         "cannot prepare expression: invalid asterisk in output expression: (&Address.*, &Address.id)",
	}, {
		query:       "SELECT (p.*, t.name) AS &Address.* FROM t",
		prepareArgs: []any{Address{}},
		err:         "cannot prepare expression: invalid asterisk in output expression: (p.*, t.name) AS &Address.*",
	}, {
		query:       "SELECT (name, p.*) AS (&Person.id, &Person.*) FROM t",
		prepareArgs: []any{Address{}, Person{}},
		err:         "cannot prepare expression: invalid asterisk in output expression: (name, p.*) AS (&Person.id, &Person.*)",
	}, {
		query:       "SELECT (&Person.*, &Person.*) FROM t",
		prepareArgs: []any{Address{}, Person{}},
		err:         "cannot prepare expression: invalid asterisk in output expression: (&Person.*, &Person.*)",
	}, {
		query:       "SELECT (p.*, t.*) AS &Address.* FROM t",
		prepareArgs: []any{Address{}},
		err:         "cannot prepare expression: invalid asterisk in output expression: (p.*, t.*) AS &Address.*",
	}, {
		query:       "SELECT p.* AS &Address.street FROM t",
		prepareArgs: []any{Address{}},
		err:         "cannot prepare expression: invalid asterisk in output expression: p.* AS &Address.street",
	}, {
		query:       "SELECT street FROM t WHERE x = $Address.number",
		prepareArgs: []any{Address{}},
		err:         `cannot prepare expression: type "Address" has no "number" db tag`,
	}, {
		query:       "SELECT (street, road) AS &Address.* FROM t",
		prepareArgs: []any{Address{}},
		err:         `cannot prepare expression: type "Address" has no "road" db tag`,
	}, {
		query:       "SELECT &Address.road FROM t",
		prepareArgs: []any{Address{}},
		err:         `cannot prepare expression: type "Address" has no "road" db tag`,
	}, {
		query:       "SELECT street FROM t WHERE x = $Address.street",
		prepareArgs: []any{Person{}},
		err:         `cannot prepare expression: type "Address" not found, have: Person`,
	}, {
		query:       "SELECT street AS &Address.street FROM t",
		prepareArgs: []any{},
		err:         `cannot prepare expression: type "Address" not found, have: `,
	}, {
		query:       "SELECT street AS &Address.id FROM t",
		prepareArgs: []any{Person{}},
		err:         `cannot prepare expression: type "Address" not found, have: Person`,
	}, {
		query:       "SELECT * AS &Person.* FROM t",
		prepareArgs: []any{[]any{Person{}}},
		err:         `cannot prepare expression: need struct, got slice`,
	}, {
		query:       "SELECT * AS &Person.* FROM t",
		prepareArgs: []any{&Person{}},
		err:         `cannot prepare expression: need struct, got pointer. Prepare takes structs by value as they are only used for their type information`,
	}, {
		query:       "SELECT * AS &Person.* FROM t",
		prepareArgs: []any{map[string]any{}},
		err:         `cannot prepare expression: need struct, got map`,
	}}

	for i, test := range tests {
		parser := expr.NewParser()
		parsedExpr, err := parser.Parse(test.query)
		if err != nil {
			c.Fatal(err)
		}
		_, err = parsedExpr.Prepare(test.prepareArgs...)
		if err != nil {
			c.Assert(err.Error(), Equals, test.err,
				Commentf("test %d failed:\nquery: %q\nprepareArgs:'%+v'", i, test.query, test.prepareArgs))
		} else {
			c.Errorf("test %d failed:\nexpected err: %q but got nil\nquery: %q\nprepareArgs:'%+v'", i, test.err, test.query, test.prepareArgs)
		}
	}
}

func (s *ExprSuite) TestValidComplete(c *C) {
	tests := []struct {
		query          string
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
	}, {
		"SELECT foo FROM t WHERE x = $Address.street, y = $Person.id",
		[]any{Person{}, Address{}},
		[]any{&Person{ID: 666}, &Address{Street: "Highway to Hell"}},
		[]any{sql.Named("sqlair_0", "Highway to Hell"), sql.Named("sqlair_1", 666)},
	}}
	for _, t := range tests {
		parser := expr.NewParser()
		parsedExpr, err := parser.Parse(t.query)
		if err != nil {
			c.Fatal(err)
		}

		preparedExpr, err := parsedExpr.Prepare(t.prepareArgs...)
		if err != nil {
			c.Fatal(err)
		}

		completedExpr, err := preparedExpr.Complete(t.completeArgs...)
		if err != nil {
			c.Fatal(err)
		}

		c.Assert(expr.CompletedArgs(completedExpr), DeepEquals, t.completeValues)
	}
}

func (s *ExprSuite) TestCompleteError(c *C) {
	tests := []struct {
		query        string
		prepareArgs  []any
		completeArgs []any
		err          string
	}{{
		query:        "SELECT street FROM t WHERE x = $Address.street, y = $Person.name",
		prepareArgs:  []any{Address{}, Person{}},
		completeArgs: []any{Address{Street: "Dead end road"}},
		err:          `invalid input parameter: type "Person" not found, have: Address`,
	}, {
		query:        "SELECT street FROM t WHERE x = $Address.street, y = $Person.name",
		prepareArgs:  []any{Address{}, Person{}},
		completeArgs: []any{nil, Person{Fullname: "Monty Bingles"}},
		err:          "invalid input parameter: need struct, got nil",
	}, {
		query:        "SELECT street FROM t WHERE x = $Address.street",
		prepareArgs:  []any{Address{}},
		completeArgs: []any{8},
		err:          "invalid input parameter: need struct, got int",
	}, {
		query:        "SELECT street FROM t WHERE x = $Address.street",
		prepareArgs:  []any{Address{}},
		completeArgs: []any{map[string]any{}},
		err:          "invalid input parameter: need struct, got map",
	}, {
		query:        "SELECT street FROM t WHERE x = $Address.street, y = $Person.name",
		prepareArgs:  []any{Address{}, Person{}},
		completeArgs: []any{},
		err:          `invalid input parameter: type "Address" not found, no input structs were found`,
	}, {
		query:        "SELECT street FROM t WHERE x = $Person.id, y = $Person.name",
		prepareArgs:  []any{Person{}},
		completeArgs: []any{Person{}, Person{}},
		err:          `invalid input parameter: more than one instance of type "Person". To input different instances of the same struct a type alias must be used`,
	}}

	outerP := Person{}
	// Person shadows the Person struct in the tests above
	type Person struct {
		ID         int    `db:"id"`
		Fullname   string `db:"name"`
		PostalCode int    `db:"address_id"`
	}
	shadowedP := Person{}

	testsShadowed := []struct {
		query        string
		prepareArgs  []any
		completeArgs []any
		err          string
	}{{
		query:        "SELECT street FROM t WHERE y = $Person.name",
		prepareArgs:  []any{outerP},
		completeArgs: []any{shadowedP},
		err:          "invalid input parameter: type expr_test.Person not found, have expr_test.Person",
	}}

	tests = append(tests, testsShadowed...)

	for i, t := range tests {
		parser := expr.NewParser()
		parsedExpr, err := parser.Parse(t.query)
		if err != nil {
			c.Fatal(err)
		}

		preparedExpr, err := parsedExpr.Prepare(t.prepareArgs...)
		if err != nil {
			c.Fatal(err)
		}

		_, err = preparedExpr.Complete(t.completeArgs...)
		c.Assert(err, ErrorMatches, t.err,
			Commentf("test %d failed:\ninput: %s", i, t.query))

	}
}
