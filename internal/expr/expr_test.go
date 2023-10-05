package expr_test

import (
	"database/sql"
	"testing"

	"github.com/canonical/sqlair"
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

type HardMaths struct {
	X    int `db:"x"`
	Y    int `db:"y"`
	Z    int `db:"z"`
	Coef int `db:"coef"`
}

type M map[string]any

type IntMap map[string]int

type StringMap map[string]string

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
	"comments",
	`SELECT &Person.* -- The line with &Person.* on it
FROM person /* The start of a multi line comment
It keeps going here with some weird chars /-*"/
And now it stops */ WHERE "x" = /-*'' -- The "WHERE" line
AND y =/* And now we have " */ "-- /* */" /* " some comments strings */
AND z = $Person.id -- The line with $Person.id on it
`,
	`[Bypass[SELECT ] Output[[] [Person.*]] Bypass[ -- The line with &Person.* on it
FROM person /* The start of a multi line comment
It keeps going here with some weird chars /-*"/
And now it stops */ WHERE "x" = /-*'' -- The "WHERE" line
AND y =/* And now we have " */ "-- /* */" /* " some comments strings */
AND z = ] Input[Person.id] Bypass[ -- The line with $Person.id on it
]]`,
	[]any{Person{}},
	`SELECT address_id AS _sqlair_0, id AS _sqlair_1, name AS _sqlair_2 -- The line with &Person.* on it
FROM person /* The start of a multi line comment
It keeps going here with some weird chars /-*"/
And now it stops */ WHERE "x" = /-*'' -- The "WHERE" line
AND y =/* And now we have " */ "-- /* */" /* " some comments strings */
AND z = @sqlair_0 -- The line with $Person.id on it
`,
}, {
	"comments v2",
	`SELECT (*) AS (&Person.name, /* ... */ &Person.id), (*) AS (&Address.id /* ... */, &Address.street) FROM p -- End of the line`,
	`[Bypass[SELECT ] Output[[*] [Person.name Person.id]] Bypass[, ] Output[[*] [Address.id Address.street]] Bypass[ FROM p -- End of the line]]`,
	[]any{Person{}, Address{}},
	`SELECT name AS _sqlair_0, id AS _sqlair_1, id AS _sqlair_2, street AS _sqlair_3 FROM p -- End of the line`,
}, {
	"quoted io expressions",
	`SELECT "&notAnOutput.Expression" '&notAnotherOutputExpresion.*' AS literal FROM t WHERE bar = '$NotAn.Input' AND baz = "$NotAnother.Input"`,
	`[Bypass[SELECT "&notAnOutput.Expression" '&notAnotherOutputExpresion.*' AS literal FROM t WHERE bar = '$NotAn.Input' AND baz = "$NotAnother.Input"]]`,
	[]any{},
	`SELECT "&notAnOutput.Expression" '&notAnotherOutputExpresion.*' AS literal FROM t WHERE bar = '$NotAn.Input' AND baz = "$NotAnother.Input"`,
}, {
	"star as output",
	"SELECT * AS &Person.* FROM t",
	"[Bypass[SELECT ] Output[[*] [Person.*]] Bypass[ FROM t]]",
	[]any{Person{}},
	"SELECT address_id AS _sqlair_0, id AS _sqlair_1, name AS _sqlair_2 FROM t",
}, {
	"star as output multitype",
	"SELECT (*) AS (&Person.*, &Address.*) FROM t",
	"[Bypass[SELECT ] Output[[*] [Person.* Address.*]] Bypass[ FROM t]]",
	[]any{Person{}, Address{}},
	"SELECT address_id AS _sqlair_0, id AS _sqlair_1, name AS _sqlair_2, district AS _sqlair_3, id AS _sqlair_4, street AS _sqlair_5 FROM t",
}, {
	"multiple multitype",
	"SELECT (t.*) AS (&Person.*, &M.uid), (district, street, postcode) AS (&Address.district, &Address.street, &M.postcode) FROM t",
	"[Bypass[SELECT ] Output[[t.*] [Person.* M.uid]] Bypass[, ] Output[[district street postcode] [Address.district Address.street M.postcode]] Bypass[ FROM t]]",
	[]any{Person{}, Address{}, sqlair.M{}},
	"SELECT t.address_id AS _sqlair_0, t.id AS _sqlair_1, t.name AS _sqlair_2, t.uid AS _sqlair_3, district AS _sqlair_4, street AS _sqlair_5, postcode AS _sqlair_6 FROM t",
}, {
	"input",
	"SELECT p.*, a.district FROM person AS p JOIN address AS a ON p.address_id=$Address.id WHERE p.name = $Person.name",
	"[Bypass[SELECT p.*, a.district FROM person AS p JOIN address AS a ON p.address_id=] Input[Address.id] Bypass[ WHERE p.name = ] Input[Person.name]]",
	[]any{Person{}, Address{}},
	`SELECT p.*, a.district FROM person AS p JOIN address AS a ON p.address_id=@sqlair_0 WHERE p.name = @sqlair_1`,
}, {
	"output and input",
	"SELECT &Person.* FROM table WHERE foo = $Address.id",
	"[Bypass[SELECT ] Output[[] [Person.*]] Bypass[ FROM table WHERE foo = ] Input[Address.id]]",
	[]any{Person{}, Address{}},
	`SELECT address_id AS _sqlair_0, id AS _sqlair_1, name AS _sqlair_2 FROM table WHERE foo = @sqlair_0`,
}, {
	"outputs and quote",
	"SELECT foo, &Person.id, bar, baz, &Manager.name FROM table WHERE foo = 'xx'",
	"[Bypass[SELECT foo, ] Output[[] [Person.id]] Bypass[, bar, baz, ] Output[[] [Manager.name]] Bypass[ FROM table WHERE foo = 'xx']]",
	[]any{Person{}, Manager{}},
	"SELECT foo, id AS _sqlair_0, bar, baz, name AS _sqlair_1 FROM table WHERE foo = 'xx'",
}, {
	"star output and quote",
	"SELECT * AS &Person.* FROM person WHERE name = 'Fred'",
	"[Bypass[SELECT ] Output[[*] [Person.*]] Bypass[ FROM person WHERE name = 'Fred']]",
	[]any{Person{}},
	"SELECT address_id AS _sqlair_0, id AS _sqlair_1, name AS _sqlair_2 FROM person WHERE name = 'Fred'",
}, {
	"two star outputs and quote",
	"SELECT &Person.*, a.* AS &Address.* FROM person, address a WHERE name = 'Fred'",
	"[Bypass[SELECT ] Output[[] [Person.*]] Bypass[, ] Output[[a.*] [Address.*]] Bypass[ FROM person, address a WHERE name = 'Fred']]",
	[]any{Person{}, Address{}},
	"SELECT address_id AS _sqlair_0, id AS _sqlair_1, name AS _sqlair_2, a.district AS _sqlair_3, a.id AS _sqlair_4, a.street AS _sqlair_5 FROM person, address a WHERE name = 'Fred'",
}, {
	"map input and output",
	"SELECT (p.name, a.id) AS (&M.*), street AS &StringMap.*, &IntMap.id FROM person, address a WHERE name = $M.name",
	"[Bypass[SELECT ] Output[[p.name a.id] [M.*]] Bypass[, ] Output[[street] [StringMap.*]] Bypass[, ] Output[[] [IntMap.id]] Bypass[ FROM person, address a WHERE name = ] Input[M.name]]",
	[]any{sqlair.M{}, IntMap{}, StringMap{}},
	"SELECT p.name AS _sqlair_0, a.id AS _sqlair_1, street AS _sqlair_2, id AS _sqlair_3 FROM person, address a WHERE name = @sqlair_0",
}, {
	"multicolumn output v1",
	"SELECT (a.district, a.street) AS (&Address.district, &Address.street), a.id AS &Person.id FROM address AS a",
	"[Bypass[SELECT ] Output[[a.district a.street] [Address.district Address.street]] Bypass[, ] Output[[a.id] [Person.id]] Bypass[ FROM address AS a]]",
	[]any{Person{}, Address{}},
	"SELECT a.district AS _sqlair_0, a.street AS _sqlair_1, a.id AS _sqlair_2 FROM address AS a",
}, {
	"multicolumn output v2",
	"SELECT (a.district, a.id) AS (&Address.district, &Person.address_id) FROM address AS a",
	"[Bypass[SELECT ] Output[[a.district a.id] [Address.district Person.address_id]] Bypass[ FROM address AS a]]",
	[]any{Person{}, Address{}},
	"SELECT a.district AS _sqlair_0, a.id AS _sqlair_1 FROM address AS a",
}, {
	"multicolumn output v3",
	"SELECT (*) AS (&Person.address_id, &Address.*, &Manager.id) FROM address AS a",
	"[Bypass[SELECT ] Output[[*] [Person.address_id Address.* Manager.id]] Bypass[ FROM address AS a]]",
	[]any{Person{}, Address{}, Manager{}},
	"SELECT address_id AS _sqlair_0, district AS _sqlair_1, id AS _sqlair_2, street AS _sqlair_3, id AS _sqlair_4 FROM address AS a",
}, {
	"multicolumn output v4",
	"SELECT (a.district, a.street) AS (&Address.*) FROM address AS a WHERE p.name = 'Fred'",
	"[Bypass[SELECT ] Output[[a.district a.street] [Address.*]] Bypass[ FROM address AS a WHERE p.name = 'Fred']]",
	[]any{Address{}},
	"SELECT a.district AS _sqlair_0, a.street AS _sqlair_1 FROM address AS a WHERE p.name = 'Fred'",
}, {
	"multicolumn output v5",
	"SELECT (&Address.street, &Person.id) FROM address AS a WHERE p.name = 'Fred'",
	"[Bypass[SELECT (] Output[[] [Address.street]] Bypass[, ] Output[[] [Person.id]] Bypass[) FROM address AS a WHERE p.name = 'Fred']]",
	[]any{Address{}, Person{}},
	"SELECT (street AS _sqlair_0, id AS _sqlair_1) FROM address AS a WHERE p.name = 'Fred'",
}, {
	"complex query v1",
	"SELECT p.* AS &Person.*, (a.district, a.street) AS (&Address.*), (5+7), (col1 * col2) AS calculated_value FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = 'Fred'",
	"[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[, ] Output[[a.district a.street] [Address.*]] Bypass[, (5+7), (col1 * col2) AS calculated_value FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = 'Fred']]",
	[]any{Person{}, Address{}},
	`SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2, a.district AS _sqlair_3, a.street AS _sqlair_4, (5+7), (col1 * col2) AS calculated_value FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = 'Fred'`,
}, {
	"complex query v2",
	"SELECT p.* AS &Person.*, (a.district, a.street) AS (&Address.*) FROM person AS p JOIN address AS a ON p .address_id = a.id WHERE p.name = 'Fred'",
	"[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[, ] Output[[a.district a.street] [Address.*]] Bypass[ FROM person AS p JOIN address AS a ON p .address_id = a.id WHERE p.name = 'Fred']]",
	[]any{Person{}, Address{}},
	"SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2, a.district AS _sqlair_3, a.street AS _sqlair_4 FROM person AS p JOIN address AS a ON p .address_id = a.id WHERE p.name = 'Fred'",
}, {
	"complex query v3",
	"SELECT p.* AS &Person.*, (a.district, a.street) AS (&Address.*) FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name IN (SELECT name FROM table WHERE table.n = $Person.name)",
	"[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[, ] Output[[a.district a.street] [Address.*]] Bypass[ FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name IN (SELECT name FROM table WHERE table.n = ] Input[Person.name] Bypass[)]]",
	[]any{Person{}, Address{}},
	`SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2, a.district AS _sqlair_3, a.street AS _sqlair_4 FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name IN (SELECT name FROM table WHERE table.n = @sqlair_0)`,
}, {
	"complex query v4",
	"SELECT p.* AS &Person.* FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = $Person.name) UNION SELECT (a.district, a.street) AS (&Address.*) FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = $Person.name)",
	"[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[ FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = ] Input[Person.name] Bypass[) UNION SELECT ] Output[[a.district a.street] [Address.*]] Bypass[ FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = ] Input[Person.name] Bypass[)]]",
	[]any{Person{}, Address{}},
	`SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2 FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = @sqlair_0) UNION SELECT a.district AS _sqlair_3, a.street AS _sqlair_4 FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = @sqlair_1)`,
}, {
	"complex query v5",
	"SELECT p.* AS &Person.* FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = $Person.name AND p.address_id = $Person.address_id",
	"[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[ FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = ] Input[Person.name] Bypass[ AND p.address_id = ] Input[Person.address_id]]",
	[]any{Person{}},
	`SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2 FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = @sqlair_0 AND p.address_id = @sqlair_1`,
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
	"ignore dollar",
	"SELECT $, dollerrow$ FROM moneytable$",
	"[Bypass[SELECT $, dollerrow$ FROM moneytable$]]",
	[]any{},
	"SELECT $, dollerrow$ FROM moneytable$",
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
}, {
	"mathmatical operations",
	`SELECT name FROM person WHERE id =$HardMaths.x+$HardMaths.y/$HardMaths.z-
	($HardMaths.coef%$HardMaths.x)-$HardMaths.y|$HardMaths.z<$HardMaths.z<>$HardMaths.x`,
	`[Bypass[SELECT name FROM person WHERE id =] Input[HardMaths.x] Bypass[+] Input[HardMaths.y] Bypass[/] Input[HardMaths.z] Bypass[-
	(] Input[HardMaths.coef] Bypass[%] Input[HardMaths.x] Bypass[)-] Input[HardMaths.y] Bypass[|] Input[HardMaths.z] Bypass[<] Input[HardMaths.z] Bypass[<>] Input[HardMaths.x]]`,
	[]any{HardMaths{}},
	`SELECT name FROM person WHERE id =@sqlair_0+@sqlair_1/@sqlair_2-
	(@sqlair_3%@sqlair_4)-@sqlair_5|@sqlair_6<@sqlair_7<>@sqlair_8`,
}, {
	"insert array",
	"INSERT INTO arr VALUES (ARRAY[[1,2],[$HardMaths.x,4]], ARRAY[[5,6],[$HardMaths.y,8]]);",
	"[Bypass[INSERT INTO arr VALUES (ARRAY[[1,2],[] Input[HardMaths.x] Bypass[,4]], ARRAY[[5,6],[] Input[HardMaths.y] Bypass[,8]]);]]",
	[]any{HardMaths{}},
	"INSERT INTO arr VALUES (ARRAY[[1,2],[@sqlair_0,4]], ARRAY[[5,6],[@sqlair_1,8]]);",
}, {
	"functions",
	`SELECT (max(AVG(id), AVG(address_id), length("((((''""((")), IFNULL(name, "Mr &Person.id of $M.name")) AS (&M.avg, &M.name), random() AS &M.random FROM person`,
	`[Bypass[SELECT ] Output[[[Bypass[max(AVG(id), AVG(address_id), length("((((''""(("))]] [Bypass[IFNULL(name, "Mr &Person.id of $M.name")]]] [M.avg M.name]] Bypass[, ] Output[[[Bypass[random()]]] [M.random]] Bypass[ FROM person]]`,
	[]any{sqlair.M{}},
	`SELECT max(AVG(id), AVG(address_id), length("((((''""((")) AS _sqlair_0, IFNULL(name, "Mr &Person.id of $M.name") AS _sqlair_1, random() AS _sqlair_2 FROM person`,
}, {
	"function with nested input",
	`SELECT max($Person.id, 20) AS &Manager.id FROM person`,
	`[Bypass[SELECT ] Output[[[Bypass[max(] Input[Person.id] Bypass[, 20)]]] [Manager.id]] Bypass[ FROM person]]`,
	[]any{Person{}, Manager{}},
	`SELECT max(@sqlair_0, 20) AS _sqlair_0 FROM person`,
}, {
	"function with multiple nested input",
	`SELECT (manager_id, avg(30, length($Person.name), max($Person.id, 20))) AS (&Manager.id, &Person.id) FROM person`,
	`[Bypass[SELECT ] Output[[manager_id [Bypass[avg(30, length(] Input[Person.name] Bypass[), max(] Input[Person.id] Bypass[, 20))]]] [Manager.id Person.id]] Bypass[ FROM person]]`,
	[]any{Person{}, Manager{}},
	`SELECT manager_id AS _sqlair_0, avg(30, length(@sqlair_0), max(@sqlair_1, 20)) AS _sqlair_1 FROM person`,
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
			c.Check(preparedExpr.SQL(), Equals, t.expectedPrepared,
				Commentf("test %d failed (Prepare):\nsummary: %s\nquery: %s\nexpected: %s\nactual:   %s\n", i, t.summary, t.query, t.expectedPrepared, preparedExpr.SQL()))
		}
	}
}

func (s *ExprSuite) TestParseErrors(c *C) {
	tests := []struct {
		query string
		err   string
	}{{
		query: "SELECT foo FROM t WHERE x = 'dddd",
		err:   "cannot parse expression: column 29: missing closing quote in string literal",
	}, {
		query: "SELECT foo FROM t WHERE x = \"dddd",
		err:   "cannot parse expression: column 29: missing closing quote in string literal",
	}, {
		query: "SELECT foo FROM t WHERE x = \"dddd'",
		err:   "cannot parse expression: column 29: missing closing quote in string literal",
	}, {
		query: "SELECT foo FROM t WHERE x = '''",
		err:   "cannot parse expression: column 29: missing closing quote in string literal",
	}, {
		query: `SELECT foo FROM t WHERE x = '''""`,
		err:   "cannot parse expression: column 29: missing closing quote in string literal",
	}, {
		query: `SELECT foo FROM t WHERE x = """`,
		err:   "cannot parse expression: column 29: missing closing quote in string literal",
	}, {
		query: `SELECT foo FROM t WHERE x = """''`,
		err:   "cannot parse expression: column 29: missing closing quote in string literal",
	}, {
		query: `SELECT foo -- line comment
FROM t /* multiline
comment
*/
WHERE x = 'O'Donnell'`,
		err: "cannot parse expression: line 5, column 21: missing closing quote in string literal",
	}, {
		query: `SELECT foo FROM t -- line comment
WHERE x = $Address.`,
		err: `cannot parse expression: line 2, column 20: invalid identifier suffix following "Address"`,
	}, {
		query: `SELECT foo
FROM t /* multiline
comment */ WHERE x = $Address.&d`,
		err: `cannot parse expression: line 3, column 31: invalid identifier suffix following "Address"`,
	}, {
		query: "SELECT foo FROM t WHERE x = $Address.-",
		err:   `cannot parse expression: column 38: invalid identifier suffix following "Address"`,
	}, {
		query: "SELECT foo FROM t WHERE x = $Address",
		err:   `cannot parse expression: column 29: unqualified type, expected Address.* or Address.<db tag>`,
	}, {
		query: "SELECT name AS (&Person.*)",
		err:   `cannot parse expression: column 16: unexpected parentheses around types after "AS"`,
	}, {
		query: "SELECT name AS (&Person.name, &Person.id)",
		err:   `cannot parse expression: column 16: unexpected parentheses around types after "AS"`,
	}, {
		query: "SELECT (name) AS &Person.*",
		err:   `cannot parse expression: column 18: missing parentheses around types after "AS"`,
	}, {
		query: "SELECT (name, id) AS &Person.*",
		err:   `cannot parse expression: column 22: missing parentheses around types after "AS"`,
	}, {
		query: "SELECT (name, id) AS (&Person.name, Person.id)",
		err:   `cannot parse expression: column 37: invalid expression in list`,
	}, {
		query: "SELECT (name, id) AS (&Person.name, &Person.id",
		err:   `cannot parse expression: column 22: missing closing parentheses`,
	}, {
		query: "SELECT (name, id) WHERE id = $Person.*",
		err:   `cannot parse expression: column 30: asterisk not allowed in input expression "$Person.*"`,
	}, {
		query: `SELECT (name, id) AS (&Person.name, /* multiline
comment */

&Person.id`,
		err: `cannot parse expression: line 1, column 22: missing closing parentheses`,
	}, {
		query: `SELECT (name, id) WHERE name = 'multiline
string
of three lines' AND id = $Person.*`,
		err: `cannot parse expression: line 3, column 26: asterisk not allowed in input expression "$Person.*"`,
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
	type NoTags struct {
		S string
	}
	tests := []struct {
		query       string
		prepareArgs []any
		err         string
	}{{
		query:       "SELECT (p.name, t.id) AS (&Address.id) FROM t",
		prepareArgs: []any{Address{}},
		err:         "cannot prepare statement: output expression: mismatched number of columns and target types: (p.name, t.id) AS (&Address.id)",
	}, {
		query:       "SELECT (p.name) AS (&Address.district, &Address.street) FROM t",
		prepareArgs: []any{Address{}},
		err:         "cannot prepare statement: output expression: mismatched number of columns and target types: (p.name) AS (&Address.district, &Address.street)",
	}, {
		query:       "SELECT (&Address.*, &Address.id) FROM t",
		prepareArgs: []any{Address{}, Person{}},
		err:         `cannot prepare statement: member "id" of type "Address" appears more than once in output expressions`,
	}, {
		query:       "SELECT (p.*, t.name) AS (&Address.*) FROM t",
		prepareArgs: []any{Address{}},
		err:         "cannot prepare statement: output expression: invalid asterisk in columns: (p.*, t.name) AS (&Address.*)",
	}, {
		query:       "SELECT (name, p.*) AS (&Person.id, &Person.*) FROM t",
		prepareArgs: []any{Address{}, Person{}},
		err:         "cannot prepare statement: output expression: invalid asterisk in columns: (name, p.*) AS (&Person.id, &Person.*)",
	}, {
		query:       "SELECT (&Person.*, &Person.*) FROM t",
		prepareArgs: []any{Address{}, Person{}},
		err:         `cannot prepare statement: member "address_id" of type "Person" appears more than once in output expressions`,
	}, {
		query:       "SELECT (p.*, t.*) AS (&Address.*) FROM t",
		prepareArgs: []any{Address{}},
		err:         "cannot prepare statement: output expression: invalid asterisk in columns: (p.*, t.*) AS (&Address.*)",
	}, {
		query:       "SELECT (id, name) AS (&Person.id, &Address.*) FROM t",
		prepareArgs: []any{Address{}, Person{}},
		err:         "cannot prepare statement: output expression: invalid asterisk in types: (id, name) AS (&Person.id, &Address.*)",
	}, {
		query:       "SELECT (name, id) AS (&Person.*, &Address.id) FROM t",
		prepareArgs: []any{Address{}, Person{}},
		err:         "cannot prepare statement: output expression: invalid asterisk in types: (name, id) AS (&Person.*, &Address.id)",
	}, {
		query:       "SELECT (name, id) AS (&Person.*, &Address.*) FROM t",
		prepareArgs: []any{Address{}, Person{}},
		err:         "cannot prepare statement: output expression: invalid asterisk in types: (name, id) AS (&Person.*, &Address.*)",
	}, {
		query:       "SELECT street FROM t WHERE x = $Address.number",
		prepareArgs: []any{Address{}},
		err:         `cannot prepare statement: input expression: type "Address" has no "number" db tag: $Address.number`,
	}, {
		query:       "SELECT (street, road) AS (&Address.*) FROM t",
		prepareArgs: []any{Address{}},
		err:         `cannot prepare statement: output expression: type "Address" has no "road" db tag: (street, road) AS (&Address.*)`,
	}, {
		query:       "SELECT &Address.road FROM t",
		prepareArgs: []any{Address{}},
		err:         `cannot prepare statement: output expression: type "Address" has no "road" db tag: &Address.road`,
	}, {
		query:       "SELECT street FROM t WHERE x = $Address.street",
		prepareArgs: []any{Person{}, Manager{}},
		err:         `cannot prepare statement: input expression: parameter with type "Address" missing (have "Manager", "Person"): $Address.street`,
	}, {
		query:       "SELECT street AS &Address.street FROM t",
		prepareArgs: []any{},
		err:         `cannot prepare statement: output expression: parameter with type "Address" missing: street AS &Address.street`,
	}, {
		query:       "SELECT street AS &Address.id FROM t",
		prepareArgs: []any{Person{}},
		err:         `cannot prepare statement: output expression: parameter with type "Address" missing (have "Person"): street AS &Address.id`,
	}, {
		query:       "SELECT * AS &Person.* FROM t",
		prepareArgs: []any{[]any{Person{}}},
		err:         `cannot prepare statement: need struct or map, got slice`,
	}, {
		query:       "SELECT * AS &Person.* FROM t",
		prepareArgs: []any{&Person{}},
		err:         `cannot prepare statement: need struct or map, got pointer to struct`,
	}, {
		query:       "SELECT * AS &Person.* FROM t",
		prepareArgs: []any{(*Person)(nil)},
		err:         `cannot prepare statement: need struct or map, got pointer to struct`,
	}, {
		query:       "SELECT * AS &Person.* FROM t",
		prepareArgs: []any{map[string]any{}},
		err:         `cannot prepare statement: cannot use anonymous map`,
	}, {
		query:       "SELECT * AS &Person.* FROM t",
		prepareArgs: []any{nil},
		err:         `cannot prepare statement: need struct or map, got nil`,
	}, {
		query:       "SELECT * AS &.* FROM t",
		prepareArgs: []any{struct{ f int }{f: 1}},
		err:         `cannot prepare statement: cannot use anonymous struct`,
	}, {
		query:       "SELECT avg(num) AS &Person.* FROM t",
		prepareArgs: []any{Person{}},
		err:         `cannot prepare statement: output expression: cannot use function with star type: avg(num) AS &Person.*`,
	}, {
		query:       "SELECT avg(id) AS &M.* FROM t",
		prepareArgs: []any{sqlair.M{}},
		err:         `cannot prepare statement: output expression: cannot use function with star type: avg(id) AS &M.*`,
	}, {
		query:       "SELECT &NoTags.* FROM t",
		prepareArgs: []any{NoTags{}},
		err:         `cannot prepare statement: output expression: no "db" tags found in struct "NoTags": &NoTags.*`,
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

func (s *ExprSuite) TestPrepareMapError(c *C) {
	type InvalidMap map[int]any
	type CustomMap map[string]int
	type M struct {
		F string `db:"id"`
	}
	tests := []struct {
		summary string
		input   string
		args    []any
		expect  string
	}{{
		"all output into map star",
		"SELECT &M.* FROM person WHERE name = 'Fred'",
		[]any{sqlair.M{}},
		"cannot prepare statement: output expression: columns must be specified for map with star: &M.*",
	}, {
		"all output into map star from table star",
		"SELECT p.* AS &M.* FROM person WHERE name = 'Fred'",
		[]any{sqlair.M{}},
		"cannot prepare statement: output expression: columns must be specified for map with star: p.* AS &M.*",
	}, {
		"all output into map star from lone star",
		"SELECT * AS &CustomMap.* FROM person WHERE name = 'Fred'",
		[]any{CustomMap{}},
		"cannot prepare statement: output expression: columns must be specified for map with star: * AS &CustomMap.*",
	}, {
		"invalid map",
		"SELECT * AS &InvalidMap.* FROM person WHERE name = 'Fred'",
		[]any{InvalidMap{}},
		"cannot prepare statement: map type InvalidMap must have key type string, found type int",
	}, {
		"clashing map and struct names",
		"SELECT * AS &M.* FROM person WHERE name = $M.id",
		[]any{M{}, sqlair.M{}},
		`cannot prepare statement: two types found with name "M": "expr_test.M" and "sqlair.M"`,
	},
	}
	for _, test := range tests {
		parser := expr.NewParser()
		parsedExpr, err := parser.Parse(test.input)
		if err != nil {
			c.Fatal(err)
		}
		_, err = parsedExpr.Prepare(test.args...)
		c.Assert(err.Error(), Equals, test.expect)
	}
}

func (s *ExprSuite) TestValidQuery(c *C) {
	tests := []struct {
		query       string
		prepareArgs []any
		queryArgs   []any
		queryValues []any
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
	}, {
		"SELECT * AS &Address.* FROM t WHERE x = $M.fullname",
		[]any{Address{}, sqlair.M{}},
		[]any{sqlair.M{"fullname": "Jimany Johnson"}},
		[]any{sql.Named("sqlair_0", "Jimany Johnson")},
	}, {
		"SELECT foo FROM t WHERE x = $M.street, y = $Person.id",
		[]any{Person{}, sqlair.M{}},
		[]any{Person{ID: 666}, sqlair.M{"street": "Highway to Hell"}},
		[]any{sql.Named("sqlair_0", "Highway to Hell"), sql.Named("sqlair_1", 666)},
	}, {
		"SELECT * AS &Address.* FROM t WHERE x = $StringMap.fullname",
		[]any{Address{}, StringMap{}},
		[]any{StringMap{"fullname": "Jimany Johnson"}},
		[]any{sql.Named("sqlair_0", "Jimany Johnson")},
	}, {
		"SELECT foo FROM t WHERE x = $StringMap.street, y = $Person.id",
		[]any{Person{}, StringMap{}},
		[]any{Person{ID: 666}, StringMap{"street": "Highway to Hell"}},
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

		query, err := preparedExpr.Query(t.queryArgs...)
		if err != nil {
			c.Fatal(err)
		}

		c.Assert(query.QueryArgs(), DeepEquals, t.queryValues)
	}
}

func (s *ExprSuite) TestQueryError(c *C) {
	tests := []struct {
		query       string
		prepareArgs []any
		queryArgs   []any
		err         string
	}{{
		query:       "SELECT street FROM t WHERE x = $Address.street, y = $Person.name",
		prepareArgs: []any{Address{}, Person{}},
		queryArgs:   []any{Address{Street: "Dead end road"}},
		err:         `invalid input parameter: parameter with type "Person" missing (have "Address")`,
	}, {
		query:       "SELECT street FROM t WHERE x = $Address.street, y = $Person.name",
		prepareArgs: []any{Address{}, Person{}},
		queryArgs:   []any{nil, Person{Fullname: "Monty Bingles"}},
		err:         "invalid input parameter: need struct or map, got nil",
	}, {
		query:       "SELECT street FROM t WHERE x = $Address.street, y = $Person.name",
		prepareArgs: []any{Address{}, Person{}},
		queryArgs:   []any{(*Person)(nil)},
		err:         "invalid input parameter: need struct or map, got nil",
	}, {
		query:       "SELECT street FROM t WHERE x = $Address.street",
		prepareArgs: []any{Address{}},
		queryArgs:   []any{8},
		err:         "invalid input parameter: need struct or map, got int",
	}, {
		query:       "SELECT street FROM t WHERE x = $Address.street",
		prepareArgs: []any{Address{}},
		queryArgs:   []any{[]any{}},
		err:         "invalid input parameter: need struct or map, got slice",
	}, {
		query:       "SELECT street FROM t WHERE x = $Address.street",
		prepareArgs: []any{Address{}},
		queryArgs:   []any{Address{}, Person{}},
		err:         "invalid input parameter: Person not referenced in query",
	}, {
		query:       "SELECT * AS &Address.* FROM t WHERE x = $M.Fullname",
		prepareArgs: []any{Address{}, sqlair.M{}},
		queryArgs:   []any{sqlair.M{"fullname": "Jimany Johnson"}},
		err:         `invalid input parameter: map "M" does not contain key "Fullname"`,
	}, {
		query:       "SELECT foo FROM t WHERE x = $M.street, y = $Person.id",
		prepareArgs: []any{Person{}, sqlair.M{}},
		queryArgs:   []any{Person{ID: 666}, sqlair.M{"Street": "Highway to Hell"}},
		err:         `invalid input parameter: map "M" does not contain key "street"`,
	}, {
		query:       "SELECT street FROM t WHERE x = $Address.street, y = $Person.name",
		prepareArgs: []any{Address{}, Person{}},
		queryArgs:   []any{},
		err:         `invalid input parameter: parameter with type "Address" missing`,
	}, {
		query:       "SELECT street FROM t WHERE x = $Person.id, y = $Person.name",
		prepareArgs: []any{Person{}},
		queryArgs:   []any{Person{}, Person{}},
		err:         `invalid input parameter: type "Person" provided more than once`,
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
		query       string
		prepareArgs []any
		queryArgs   []any
		err         string
	}{{
		query:       "SELECT street FROM t WHERE y = $Person.name",
		prepareArgs: []any{outerP},
		queryArgs:   []any{shadowedP},
		err:         `invalid input parameter: parameter with type "expr_test.Person" missing, have type with same name: "expr_test.Person"`,
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

		_, err = preparedExpr.Query(t.queryArgs...)
		if err != nil {
			c.Assert(err.Error(), Equals, t.err,
				Commentf("test %d failed:\nquery: %s", i, t.query))
		} else {
			c.Errorf("test %d failed:\nexpected err: %q but got nil\nquery: %q", i, t.err, t.query)
		}
	}
}
