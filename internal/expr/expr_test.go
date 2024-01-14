// Copyright 2023 Canonical Ltd.
// Licensed under Apache 2.0, see LICENCE file for details.

package expr_test

import (
	"database/sql"
	"strconv"
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
	summary        string
	query          string
	expectedParsed string
	typeSamples    []any
	inputArgs      []any
	expectedParams []any
	expectedSQL    string
}{{
	summary:        "star table as output",
	query:          "SELECT p.* AS &Person.*",
	expectedParsed: "[Bypass[SELECT ] Output[[p.*] [Person.*]]]",
	typeSamples:    []any{Person{}},
	expectedSQL:    "SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2",
}, {
	summary:        "spaces and tabs",
	query:          "SELECT p.* 	AS 		   &Person.*",
	expectedParsed: "[Bypass[SELECT ] Output[[p.*] [Person.*]]]",
	typeSamples:    []any{Person{}},
	expectedSQL:    "SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2",
}, {
	summary: "new lines",
	query: `SELECT
		p.* AS &Person.*,
		foo
	 FROM t
	 WHERE
		foo = bar
		and
		x = y`,
	expectedParsed: `[Bypass[SELECT
		] Output[[p.*] [Person.*]] Bypass[,
		foo
	 FROM t
	 WHERE
		foo = bar
		and
		x = y]]`,
	typeSamples: []any{Person{}},
	expectedSQL: `SELECT
		p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2,
		foo
	 FROM t
	 WHERE
		foo = bar
		and
		x = y`,
}, {
	summary: "comments",
	query: `SELECT &Person.* -- The line with &Person.* on it
FROM person /* The start of a multi line comment
It keeps going here with some weird chars /-*"/
And now it stops */ WHERE "x" = /-*'' -- The "WHERE" line
AND y =/* And now we have " */ "-- /* */" /* " some comments strings */
AND z = $Person.id -- The line with $Person.id on it
`,
	expectedParsed: `[Bypass[SELECT ] Output[[] [Person.*]] Bypass[ -- The line with &Person.* on it
FROM person /* The start of a multi line comment
It keeps going here with some weird chars /-*"/
And now it stops */ WHERE "x" = /-*'' -- The "WHERE" line
AND y =/* And now we have " */ "-- /* */" /* " some comments strings */
AND z = ] Input[Person.id] Bypass[ -- The line with $Person.id on it
]]`,
	typeSamples:    []any{Person{}},
	inputArgs:      []any{Person{ID: 1}},
	expectedParams: []any{1},
	expectedSQL: `SELECT address_id AS _sqlair_0, id AS _sqlair_1, name AS _sqlair_2 -- The line with &Person.* on it
FROM person /* The start of a multi line comment
It keeps going here with some weird chars /-*"/
And now it stops */ WHERE "x" = /-*'' -- The "WHERE" line
AND y =/* And now we have " */ "-- /* */" /* " some comments strings */
AND z = @sqlair_0 -- The line with $Person.id on it
`,
}, {
	summary:        "comments v2",
	query:          `SELECT (*) AS (&Person.name, /* ... */ &Person.id), (*) AS (&Address.id /* ... */, &Address.street) FROM p -- End of the line`,
	expectedParsed: `[Bypass[SELECT ] Output[[*] [Person.name Person.id]] Bypass[, ] Output[[*] [Address.id Address.street]] Bypass[ FROM p -- End of the line]]`,
	typeSamples:    []any{Person{}, Address{}},
	expectedSQL:    `SELECT name AS _sqlair_0, id AS _sqlair_1, id AS _sqlair_2, street AS _sqlair_3 FROM p -- End of the line`,
}, {
	summary:        "quoted io expressions",
	query:          `SELECT "&notAnOutput.Expression" '&notAnotherOutputExpresion.*' AS literal FROM t WHERE bar = '$NotAn.Input' AND baz = "$NotAnother.Input"`,
	expectedParsed: `[Bypass[SELECT "&notAnOutput.Expression" '&notAnotherOutputExpresion.*' AS literal FROM t WHERE bar = '$NotAn.Input' AND baz = "$NotAnother.Input"]]`,
	typeSamples:    []any{},
	expectedSQL:    `SELECT "&notAnOutput.Expression" '&notAnotherOutputExpresion.*' AS literal FROM t WHERE bar = '$NotAn.Input' AND baz = "$NotAnother.Input"`,
}, {
	summary:        "star as output",
	query:          "SELECT * AS &Person.* FROM t",
	expectedParsed: "[Bypass[SELECT ] Output[[*] [Person.*]] Bypass[ FROM t]]",
	typeSamples:    []any{Person{}},
	expectedSQL:    "SELECT address_id AS _sqlair_0, id AS _sqlair_1, name AS _sqlair_2 FROM t",
}, {
	summary:        "star as output multitype",
	query:          "SELECT (*) AS (&Person.*, &Address.*) FROM t",
	expectedParsed: "[Bypass[SELECT ] Output[[*] [Person.* Address.*]] Bypass[ FROM t]]",
	typeSamples:    []any{Person{}, Address{}},
	expectedSQL:    "SELECT address_id AS _sqlair_0, id AS _sqlair_1, name AS _sqlair_2, district AS _sqlair_3, id AS _sqlair_4, street AS _sqlair_5 FROM t",
}, {
	summary:        "multiple multitype",
	query:          "SELECT (t.*) AS (&Person.*, &M.uid), (district, street, postcode) AS (&Address.district, &Address.street, &M.postcode) FROM t",
	expectedParsed: "[Bypass[SELECT ] Output[[t.*] [Person.* M.uid]] Bypass[, ] Output[[district street postcode] [Address.district Address.street M.postcode]] Bypass[ FROM t]]",
	typeSamples:    []any{Person{}, Address{}, sqlair.M{}},
	expectedSQL:    "SELECT t.address_id AS _sqlair_0, t.id AS _sqlair_1, t.name AS _sqlair_2, t.uid AS _sqlair_3, district AS _sqlair_4, street AS _sqlair_5, postcode AS _sqlair_6 FROM t",
}, {
	summary:        "input",
	query:          "SELECT p.*, a.district FROM person AS p JOIN address AS a ON p.address_id=$Address.id WHERE p.name = $Person.name",
	expectedParsed: "[Bypass[SELECT p.*, a.district FROM person AS p JOIN address AS a ON p.address_id=] Input[Address.id] Bypass[ WHERE p.name = ] Input[Person.name]]",
	typeSamples:    []any{Person{}, Address{}},
	inputArgs:      []any{Person{Fullname: "Foo"}, Address{ID: 1}},
	expectedParams: []any{1, "Foo"},
	expectedSQL:    `SELECT p.*, a.district FROM person AS p JOIN address AS a ON p.address_id=@sqlair_0 WHERE p.name = @sqlair_1`,
}, {
	summary:        "output and input",
	query:          "SELECT &Person.* FROM table WHERE foo = $Address.id",
	expectedParsed: "[Bypass[SELECT ] Output[[] [Person.*]] Bypass[ FROM table WHERE foo = ] Input[Address.id]]",
	typeSamples:    []any{Person{}, Address{}},
	inputArgs:      []any{Address{ID: 1}},
	expectedParams: []any{1},
	expectedSQL:    `SELECT address_id AS _sqlair_0, id AS _sqlair_1, name AS _sqlair_2 FROM table WHERE foo = @sqlair_0`,
}, {
	summary:        "outputs and quote",
	query:          "SELECT foo, &Person.id, bar, baz, &Manager.name FROM table WHERE foo = 'xx'",
	expectedParsed: "[Bypass[SELECT foo, ] Output[[] [Person.id]] Bypass[, bar, baz, ] Output[[] [Manager.name]] Bypass[ FROM table WHERE foo = 'xx']]",
	typeSamples:    []any{Person{}, Manager{}},
	expectedSQL:    "SELECT foo, id AS _sqlair_0, bar, baz, name AS _sqlair_1 FROM table WHERE foo = 'xx'",
}, {
	summary:        "star output and quote",
	query:          "SELECT * AS &Person.* FROM person WHERE name = 'Fred'",
	expectedParsed: "[Bypass[SELECT ] Output[[*] [Person.*]] Bypass[ FROM person WHERE name = 'Fred']]",
	typeSamples:    []any{Person{}},
	expectedSQL:    "SELECT address_id AS _sqlair_0, id AS _sqlair_1, name AS _sqlair_2 FROM person WHERE name = 'Fred'",
}, {
	summary:        "two star outputs and quote",
	query:          "SELECT &Person.*, a.* AS &Address.* FROM person, address a WHERE name = 'Fred'",
	expectedParsed: "[Bypass[SELECT ] Output[[] [Person.*]] Bypass[, ] Output[[a.*] [Address.*]] Bypass[ FROM person, address a WHERE name = 'Fred']]",
	typeSamples:    []any{Person{}, Address{}},
	expectedSQL:    "SELECT address_id AS _sqlair_0, id AS _sqlair_1, name AS _sqlair_2, a.district AS _sqlair_3, a.id AS _sqlair_4, a.street AS _sqlair_5 FROM person, address a WHERE name = 'Fred'",
}, {
	summary:        "map input and output",
	query:          "SELECT (p.name, a.id) AS (&M.*), street AS &StringMap.*, &IntMap.id FROM person, address a WHERE name = $M.name",
	expectedParsed: "[Bypass[SELECT ] Output[[p.name a.id] [M.*]] Bypass[, ] Output[[street] [StringMap.*]] Bypass[, ] Output[[] [IntMap.id]] Bypass[ FROM person, address a WHERE name = ] Input[M.name]]",
	typeSamples:    []any{sqlair.M{}, IntMap{}, StringMap{}},
	inputArgs:      []any{sqlair.M{"name": "Foo"}},
	expectedParams: []any{"Foo"},
	expectedSQL:    "SELECT p.name AS _sqlair_0, a.id AS _sqlair_1, street AS _sqlair_2, id AS _sqlair_3 FROM person, address a WHERE name = @sqlair_0",
}, {
	summary:        "multicolumn output v1",
	query:          "SELECT (a.district, a.street) AS (&Address.district, &Address.street), a.id AS &Person.id FROM address AS a",
	expectedParsed: "[Bypass[SELECT ] Output[[a.district a.street] [Address.district Address.street]] Bypass[, ] Output[[a.id] [Person.id]] Bypass[ FROM address AS a]]",
	typeSamples:    []any{Person{}, Address{}},
	expectedSQL:    "SELECT a.district AS _sqlair_0, a.street AS _sqlair_1, a.id AS _sqlair_2 FROM address AS a",
}, {
	summary:        "multicolumn output v2",
	query:          "SELECT (a.district, a.id) AS (&Address.district, &Person.address_id) FROM address AS a",
	expectedParsed: "[Bypass[SELECT ] Output[[a.district a.id] [Address.district Person.address_id]] Bypass[ FROM address AS a]]",
	typeSamples:    []any{Person{}, Address{}},
	expectedSQL:    "SELECT a.district AS _sqlair_0, a.id AS _sqlair_1 FROM address AS a",
}, {
	summary:        "multicolumn output v3",
	query:          "SELECT (*) AS (&Person.address_id, &Address.*, &Manager.id) FROM address AS a",
	expectedParsed: "[Bypass[SELECT ] Output[[*] [Person.address_id Address.* Manager.id]] Bypass[ FROM address AS a]]",
	typeSamples:    []any{Person{}, Address{}, Manager{}},
	expectedSQL:    "SELECT address_id AS _sqlair_0, district AS _sqlair_1, id AS _sqlair_2, street AS _sqlair_3, id AS _sqlair_4 FROM address AS a",
}, {
	summary:        "multicolumn output v4",
	query:          "SELECT (a.district, a.street) AS (&Address.*) FROM address AS a WHERE p.name = 'Fred'",
	expectedParsed: "[Bypass[SELECT ] Output[[a.district a.street] [Address.*]] Bypass[ FROM address AS a WHERE p.name = 'Fred']]",
	typeSamples:    []any{Address{}},
	expectedSQL:    "SELECT a.district AS _sqlair_0, a.street AS _sqlair_1 FROM address AS a WHERE p.name = 'Fred'",
}, {
	summary:        "multicolumn output v5",
	query:          "SELECT (&Address.street, &Person.id) FROM address AS a WHERE p.name = 'Fred'",
	expectedParsed: "[Bypass[SELECT (] Output[[] [Address.street]] Bypass[, ] Output[[] [Person.id]] Bypass[) FROM address AS a WHERE p.name = 'Fred']]",
	typeSamples:    []any{Address{}, Person{}},
	expectedSQL:    "SELECT (street AS _sqlair_0, id AS _sqlair_1) FROM address AS a WHERE p.name = 'Fred'",
}, {
	summary:        "complex query v1",
	query:          "SELECT p.* AS &Person.*, (a.district, a.street) AS (&Address.*), (5+7), (col1 * col2) AS calculated_value FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = 'Fred'",
	expectedParsed: "[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[, ] Output[[a.district a.street] [Address.*]] Bypass[, (5+7), (col1 * col2) AS calculated_value FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = 'Fred']]",
	typeSamples:    []any{Person{}, Address{}},
	expectedSQL:    `SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2, a.district AS _sqlair_3, a.street AS _sqlair_4, (5+7), (col1 * col2) AS calculated_value FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = 'Fred'`,
}, {
	summary:        "complex query v2",
	query:          "SELECT p.* AS &Person.*, (a.district, a.street) AS (&Address.*) FROM person AS p JOIN address AS a ON p .address_id = a.id WHERE p.name = 'Fred'",
	expectedParsed: "[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[, ] Output[[a.district a.street] [Address.*]] Bypass[ FROM person AS p JOIN address AS a ON p .address_id = a.id WHERE p.name = 'Fred']]",
	typeSamples:    []any{Person{}, Address{}},
	expectedSQL:    "SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2, a.district AS _sqlair_3, a.street AS _sqlair_4 FROM person AS p JOIN address AS a ON p .address_id = a.id WHERE p.name = 'Fred'",
}, {
	summary:        "complex query v3",
	query:          "SELECT p.* AS &Person.*, (a.district, a.street) AS (&Address.*) FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name IN (SELECT name FROM table WHERE table.n = $Person.name)",
	expectedParsed: "[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[, ] Output[[a.district a.street] [Address.*]] Bypass[ FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name IN (SELECT name FROM table WHERE table.n = ] Input[Person.name] Bypass[)]]",
	typeSamples:    []any{Person{}, Address{}},
	inputArgs:      []any{Person{Fullname: "Foo"}},
	expectedParams: []any{"Foo"},
	expectedSQL:    `SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2, a.district AS _sqlair_3, a.street AS _sqlair_4 FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name IN (SELECT name FROM table WHERE table.n = @sqlair_0)`,
}, {
	summary:        "complex query v4",
	query:          "SELECT p.* AS &Person.* FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = $Person.name) UNION SELECT (a.district, a.street) AS (&Address.*) FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = $Person.name)",
	expectedParsed: "[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[ FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = ] Input[Person.name] Bypass[) UNION SELECT ] Output[[a.district a.street] [Address.*]] Bypass[ FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = ] Input[Person.name] Bypass[)]]",
	typeSamples:    []any{Person{}, Address{}},
	inputArgs:      []any{Person{Fullname: "Foo"}},
	expectedParams: []any{"Foo", "Foo"},
	expectedSQL:    `SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2 FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = @sqlair_0) UNION SELECT a.district AS _sqlair_3, a.street AS _sqlair_4 FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = @sqlair_1)`,
}, {
	summary:        "complex query v5",
	query:          "SELECT p.* AS &Person.* FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = $Person.name AND p.address_id = $Person.address_id",
	expectedParsed: "[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[ FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = ] Input[Person.name] Bypass[ AND p.address_id = ] Input[Person.address_id]]",
	typeSamples:    []any{Person{}},
	inputArgs:      []any{Person{Fullname: "Foo", PostalCode: 1}},
	expectedParams: []any{"Foo", 1},
	expectedSQL:    `SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2 FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = @sqlair_0 AND p.address_id = @sqlair_1`,
}, {
	summary:        "complex query v6",
	query:          "SELECT p.* AS &Person.*, FROM person AS p INNER JOIN address AS a ON p.address_id = $Address.id WHERE p.name = $Person.name AND p.address_id = $Person.address_id",
	expectedParsed: "[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[, FROM person AS p INNER JOIN address AS a ON p.address_id = ] Input[Address.id] Bypass[ WHERE p.name = ] Input[Person.name] Bypass[ AND p.address_id = ] Input[Person.address_id]]",
	typeSamples:    []any{Person{}, Address{}},
	inputArgs:      []any{Person{Fullname: "Foo", PostalCode: 1}, Address{ID: 2}},
	expectedParams: []any{2, "Foo", 1},
	expectedSQL:    `SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2, FROM person AS p INNER JOIN address AS a ON p.address_id = @sqlair_0 WHERE p.name = @sqlair_1 AND p.address_id = @sqlair_2`,
}, {
	summary:        "join v1",
	query:          "SELECT p.* AS &Person.*, m.* AS &Manager.* FROM person AS p JOIN person AS m ON p.id = m.id WHERE p.name = 'Fred'",
	expectedParsed: "[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[, ] Output[[m.*] [Manager.*]] Bypass[ FROM person AS p JOIN person AS m ON p.id = m.id WHERE p.name = 'Fred']]",
	typeSamples:    []any{Person{}, Manager{}},
	expectedSQL:    "SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2, m.address_id AS _sqlair_3, m.id AS _sqlair_4, m.name AS _sqlair_5 FROM person AS p JOIN person AS m ON p.id = m.id WHERE p.name = 'Fred'",
}, {
	summary:        "join v2",
	query:          "SELECT person.*, address.district FROM person JOIN address ON person.address_id = address.id WHERE person.name = 'Fred'",
	expectedParsed: "[Bypass[SELECT person.*, address.district FROM person JOIN address ON person.address_id = address.id WHERE person.name = 'Fred']]",
	typeSamples:    []any{},
	expectedSQL:    "SELECT person.*, address.district FROM person JOIN address ON person.address_id = address.id WHERE person.name = 'Fred'",
}, {
	summary:        "insert",
	query:          "INSERT INTO person (name) VALUES $Person.name",
	expectedParsed: "[Bypass[INSERT INTO person (name) VALUES ] Input[Person.name]]",
	typeSamples:    []any{Person{}},
	inputArgs:      []any{Person{Fullname: "Foo"}},
	expectedParams: []any{"Foo"},
	expectedSQL:    `INSERT INTO person (name) VALUES @sqlair_0`,
}, {
	summary:        "ignore dollar",
	query:          "SELECT $, dollerrow$ FROM moneytable$",
	expectedParsed: "[Bypass[SELECT $, dollerrow$ FROM moneytable$]]",
	typeSamples:    []any{},
	expectedSQL:    "SELECT $, dollerrow$ FROM moneytable$",
}, {
	summary:        "escaped double quote",
	query:          `SELECT foo FROM t WHERE t.p = "Jimmy ""Quickfingers"" Jones"`,
	expectedParsed: `[Bypass[SELECT foo FROM t WHERE t.p = "Jimmy ""Quickfingers"" Jones"]]`,
	typeSamples:    []any{},
	expectedSQL:    `SELECT foo FROM t WHERE t.p = "Jimmy ""Quickfingers"" Jones"`,
}, {
	summary:        "escaped single quote",
	query:          `SELECT foo FROM t WHERE t.p = 'Olly O''Flanagan'`,
	expectedParsed: `[Bypass[SELECT foo FROM t WHERE t.p = 'Olly O''Flanagan']]`,
	typeSamples:    []any{},
	expectedSQL:    `SELECT foo FROM t WHERE t.p = 'Olly O''Flanagan'`,
}, {
	summary:        "complex escaped quotes",
	query:          `SELECT * AS &Person.* FROM person WHERE name IN ('Lorn', 'Onos T''oolan', '', ''' ''');`,
	expectedParsed: `[Bypass[SELECT ] Output[[*] [Person.*]] Bypass[ FROM person WHERE name IN ('Lorn', 'Onos T''oolan', '', ''' ''');]]`,
	typeSamples:    []any{Person{}},
	expectedSQL:    `SELECT address_id AS _sqlair_0, id AS _sqlair_1, name AS _sqlair_2 FROM person WHERE name IN ('Lorn', 'Onos T''oolan', '', ''' ''');`,
}, {
	summary:        "update",
	query:          "UPDATE person SET person.address_id = $Address.id WHERE person.id = $Person.id",
	expectedParsed: "[Bypass[UPDATE person SET person.address_id = ] Input[Address.id] Bypass[ WHERE person.id = ] Input[Person.id]]",
	typeSamples:    []any{Person{}, Address{}},
	inputArgs:      []any{Person{ID: 1}, Address{ID: 2}},
	expectedParams: []any{2, 1},
	expectedSQL:    `UPDATE person SET person.address_id = @sqlair_0 WHERE person.id = @sqlair_1`,
}, {
	summary: "mathmatical operations",
	query: `SELECT name FROM person WHERE id =$HardMaths.x+$HardMaths.y/$HardMaths.z-
	($HardMaths.coef%$HardMaths.x)-$HardMaths.y|$HardMaths.z<$HardMaths.z<>$HardMaths.x`,
	expectedParsed: `[Bypass[SELECT name FROM person WHERE id =] Input[HardMaths.x] Bypass[+] Input[HardMaths.y] Bypass[/] Input[HardMaths.z] Bypass[-
	(] Input[HardMaths.coef] Bypass[%] Input[HardMaths.x] Bypass[)-] Input[HardMaths.y] Bypass[|] Input[HardMaths.z] Bypass[<] Input[HardMaths.z] Bypass[<>] Input[HardMaths.x]]`,
	typeSamples:    []any{HardMaths{}},
	inputArgs:      []any{HardMaths{X: 1, Y: 2, Z: 3, Coef: 4}},
	expectedParams: []any{1, 2, 3, 4, 1, 2, 3, 3, 1},
	expectedSQL: `SELECT name FROM person WHERE id =@sqlair_0+@sqlair_1/@sqlair_2-
	(@sqlair_3%@sqlair_4)-@sqlair_5|@sqlair_6<@sqlair_7<>@sqlair_8`,
}, {
	summary:        "insert array",
	query:          "INSERT INTO arr VALUES (ARRAY[[1,2],[$HardMaths.x,4]], ARRAY[[5,6],[$HardMaths.y,8]]);",
	expectedParsed: "[Bypass[INSERT INTO arr VALUES (ARRAY[[1,2],[] Input[HardMaths.x] Bypass[,4]], ARRAY[[5,6],[] Input[HardMaths.y] Bypass[,8]]);]]",
	typeSamples:    []any{HardMaths{}},
	inputArgs:      []any{HardMaths{X: 1, Y: 2}},
	expectedParams: []any{1, 2},
	expectedSQL:    "INSERT INTO arr VALUES (ARRAY[[1,2],[@sqlair_0,4]], ARRAY[[5,6],[@sqlair_1,8]]);",
}}

func (s *ExprSuite) TestExprPkg(c *C) {
	parser := expr.NewParser()
	for i, t := range tests {
		var (
			parsedExpr  *expr.ParsedExpr
			typedExpr   *expr.TypeBoundExpr
			primedQuery *expr.PrimedQuery
			err         error
		)
		if parsedExpr, err = parser.Parse(t.query); err != nil {
			c.Errorf("test %d failed (Parse):\nsummary: %s\nquery: %s\nexpected: %s\nerr: %s\n", i, t.summary, t.query, t.expectedParsed, err)
		} else if parsedExpr.String() != t.expectedParsed {
			c.Errorf("test %d failed (Parse):\nsummary: %s\nquery: %s\nexpected: %s\nactual:   %s\n", i, t.summary, t.query, t.expectedParsed, parsedExpr.String())
		}

		if typedExpr, err = parsedExpr.BindTypes(t.typeSamples...); err != nil {
			c.Errorf("test %d failed (BindTypes):\nsummary: %s\nquery: %s\nexpected: %s\nerr: %s\n", i, t.summary, t.query, t.expectedSQL, err)
		}

		if primedQuery, err = typedExpr.BindInputs(t.inputArgs...); err != nil {
			c.Errorf("test %d failed (Query):\nsummary: %s\nquery: %s\nexpected: %s\nerr: %s\n", i, t.summary, t.query, t.expectedSQL, err)
		} else {
			c.Assert(primedQuery.SQL(), Equals, t.expectedSQL,
				Commentf("test %d failed (Query):\nsummary: %s\nquery: %s\n", i, t.summary, t.query, t.expectedSQL, primedQuery.SQL()))
			if t.inputArgs != nil {
				params := primedQuery.Params()
				c.Assert(params, HasLen, len(t.expectedParams),
					Commentf("test %d failed (Query Args):\nsummary: %s\nquery: %s\n", i, t.summary, t.query))
				for paramIndex, param := range params {
					param := param.(sql.NamedArg)
					c.Assert(param.Name, Equals, "sqlair_"+strconv.Itoa(paramIndex),
						Commentf("test %d failed (Query Args):\nsummary: %s\nquery: %s\n", i, t.summary, t.query))
					c.Assert(param.Value, Equals, t.expectedParams[paramIndex],
						Commentf("test %d failed (Query Args):\nsummary: %s\nquery: %s\n", i, t.summary, t.query))
				}
			}
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
		err:   `cannot parse expression: column 29: unqualified type, expected Address.* or Address.<db tag> or Address[:]`,
	}, {
		query: "SELECT foo FROM t WHERE x = $Address [:]",
		err:   `cannot parse expression: column 29: unqualified type, expected Address.* or Address.<db tag> or Address[:]`,
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
	}, {
		query: "SELECT &S[:] FROM t",
		err:   `cannot parse expression: column 8: cannot use slice syntax "S[:]" in output expression`,
	}, {
		query: "SELECT &S[0] FROM t",
		err:   `cannot parse expression: column 8: cannot use slice syntax in output expression`,
	}, {
		query: "SELECT &S[1:5] FROM t",
		err:   `cannot parse expression: column 8: cannot use slice syntax in output expression`,
	}, {
		query: "SELECT col1 AS &S[1:5] FROM t",
		err:   `cannot parse expression: column 16: cannot use slice syntax in output expression`,
	}, {
		query: "SELECT col1 AS &S[] FROM t",
		err:   `cannot parse expression: column 16: cannot use slice syntax in output expression`,
	}, {
		query: "SELECT * FROM t WHERE id IN $ids[:-1]",
		err:   `cannot parse expression: column 30: invalid slice: expected 'ids[:]'`,
	}, {
		query: "SELECT * FROM t WHERE id IN $ids[3:1]",
		err:   `cannot parse expression: column 30: invalid slice: expected 'ids[:]'`,
	}, {
		query: "SELECT * FROM t WHERE id IN $ids[1:1]",
		err:   `cannot parse expression: column 30: invalid slice: expected 'ids[:]'`,
	}, {
		query: "SELECT * FROM t WHERE id IN $ids[a:]",
		err:   `cannot parse expression: column 30: invalid slice: expected 'ids[:]'`,
	}, {
		query: "SELECT * FROM t WHERE id IN $ids[:b]",
		err:   `cannot parse expression: column 30: invalid slice: expected 'ids[:]'`,
	}, {
		query: "SELECT * FROM t WHERE id = $ids[]",
		err:   `cannot parse expression: column 29: invalid slice: expected 'ids[:]'`,
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

func (s *ExprSuite) TestBindTypesErrors(c *C) {
	type NoTags struct {
		S string
	}
	tests := []struct {
		query       string
		typeSamples []any
		err         string
	}{{
		query:       "SELECT (p.name, t.id) AS (&Address.id) FROM t",
		typeSamples: []any{Address{}},
		err:         "cannot prepare statement: output expression: mismatched number of columns and target types: (p.name, t.id) AS (&Address.id)",
	}, {
		query:       "SELECT (p.name) AS (&Address.district, &Address.street) FROM t",
		typeSamples: []any{Address{}},
		err:         "cannot prepare statement: output expression: mismatched number of columns and target types: (p.name) AS (&Address.district, &Address.street)",
	}, {
		query:       "SELECT (&Address.*, &Address.id) FROM t",
		typeSamples: []any{Address{}, Person{}},
		err:         `cannot prepare statement: tag "id" of struct "Address" appears more than once in output expressions`,
	}, {
		query:       "SELECT (&M.id, &M.id) FROM t",
		typeSamples: []any{sqlair.M{}},
		err:         `cannot prepare statement: key "id" of map "M" appears more than once in output expressions`,
	}, {
		query:       "SELECT (p.*, t.name) AS (&Address.*) FROM t",
		typeSamples: []any{Address{}},
		err:         "cannot prepare statement: output expression: invalid asterisk in columns: (p.*, t.name) AS (&Address.*)",
	}, {
		query:       "SELECT (name, p.*) AS (&Person.id, &Person.*) FROM t",
		typeSamples: []any{Address{}, Person{}},
		err:         "cannot prepare statement: output expression: invalid asterisk in columns: (name, p.*) AS (&Person.id, &Person.*)",
	}, {
		query:       "SELECT (&Person.*, &Person.*) FROM t",
		typeSamples: []any{Address{}, Person{}},
		err:         `cannot prepare statement: tag "address_id" of struct "Person" appears more than once in output expressions`,
	}, {
		query:       "SELECT (p.*, t.*) AS (&Address.*) FROM t",
		typeSamples: []any{Address{}},
		err:         "cannot prepare statement: output expression: invalid asterisk in columns: (p.*, t.*) AS (&Address.*)",
	}, {
		query:       "SELECT (id, name) AS (&Person.id, &Address.*) FROM t",
		typeSamples: []any{Address{}, Person{}},
		err:         "cannot prepare statement: output expression: invalid asterisk in types: (id, name) AS (&Person.id, &Address.*)",
	}, {
		query:       "SELECT (name, id) AS (&Person.*, &Address.id) FROM t",
		typeSamples: []any{Address{}, Person{}},
		err:         "cannot prepare statement: output expression: invalid asterisk in types: (name, id) AS (&Person.*, &Address.id)",
	}, {
		query:       "SELECT (name, id) AS (&Person.*, &Address.*) FROM t",
		typeSamples: []any{Address{}, Person{}},
		err:         "cannot prepare statement: output expression: invalid asterisk in types: (name, id) AS (&Person.*, &Address.*)",
	}, {
		query:       "SELECT street FROM t WHERE x = $Address.number",
		typeSamples: []any{Address{}},
		err:         `cannot prepare statement: input expression: type "Address" has no "number" db tag: $Address.number`,
	}, {
		query:       "SELECT (street, road) AS (&Address.*) FROM t",
		typeSamples: []any{Address{}},
		err:         `cannot prepare statement: output expression: type "Address" has no "road" db tag: (street, road) AS (&Address.*)`,
	}, {
		query:       "SELECT &Address.road FROM t",
		typeSamples: []any{Address{}},
		err:         `cannot prepare statement: output expression: type "Address" has no "road" db tag: &Address.road`,
	}, {
		query:       "SELECT street FROM t WHERE x = $Address.street",
		typeSamples: []any{Person{}, Manager{}},
		err:         `cannot prepare statement: input expression: parameter with type "Address" missing (have "Manager", "Person"): $Address.street`,
	}, {
		query:       "SELECT street AS &Address.street FROM t",
		typeSamples: []any{},
		err:         `cannot prepare statement: output expression: parameter with type "Address" missing: street AS &Address.street`,
	}, {
		query:       "SELECT street AS &Address.id FROM t",
		typeSamples: []any{Person{}},
		err:         `cannot prepare statement: output expression: parameter with type "Address" missing (have "Person"): street AS &Address.id`,
	}, {
		query:       "SELECT * AS &Person.* FROM t",
		typeSamples: []any{[]any{Person{}}},
		err:         `cannot prepare statement: need struct or map, got slice`,
	}, {
		query:       "SELECT * AS &Person.* FROM t",
		typeSamples: []any{&Person{}},
		err:         `cannot prepare statement: need struct or map, got pointer to struct`,
	}, {
		query:       "SELECT * AS &Person.* FROM t",
		typeSamples: []any{(*Person)(nil)},
		err:         `cannot prepare statement: need struct or map, got pointer to struct`,
	}, {
		query:       "SELECT * AS &Person.* FROM t",
		typeSamples: []any{map[string]any{}},
		err:         `cannot prepare statement: cannot use anonymous map`,
	}, {
		query:       "SELECT * AS &Person.* FROM t",
		typeSamples: []any{nil},
		err:         `cannot prepare statement: need struct or map, got nil`,
	}, {
		query:       "SELECT * AS &.* FROM t",
		typeSamples: []any{struct{ f int }{f: 1}},
		err:         `cannot prepare statement: cannot use anonymous struct`,
	}, {
		query:       "SELECT &NoTags.* FROM t",
		typeSamples: []any{NoTags{}},
		err:         `cannot prepare statement: output expression: no "db" tags found in struct "NoTags": &NoTags.*`,
	}}

	for i, test := range tests {
		parser := expr.NewParser()
		parsedExpr, err := parser.Parse(test.query)
		if err != nil {
			c.Fatal(err)
		}
		_, err = parsedExpr.BindTypes(test.typeSamples...)
		if err != nil {
			c.Assert(err.Error(), Equals, test.err,
				Commentf("test %d failed:\nquery: %q\ntypeSamples:'%+v'", i, test.query, test.typeSamples))
		} else {
			c.Errorf("test %d failed:\nexpected err: %q but got nil\nquery: %q\ntypeSamples:'%+v'", i, test.err, test.query, test.typeSamples)
		}
	}
}

func (s *ExprSuite) TestMapError(c *C) {
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
		"cannot prepare statement: output expression: columns must be specified for non-struct type: &M.*",
	}, {
		"all output into map star from table star",
		"SELECT p.* AS &M.* FROM person WHERE name = 'Fred'",
		[]any{sqlair.M{}},
		"cannot prepare statement: output expression: columns must be specified for non-struct type: p.* AS &M.*",
	}, {
		"all output into map star from lone star",
		"SELECT * AS &CustomMap.* FROM person WHERE name = 'Fred'",
		[]any{CustomMap{}},
		"cannot prepare statement: output expression: columns must be specified for non-struct type: * AS &CustomMap.*",
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
		_, err = parsedExpr.BindTypes(test.args...)
		c.Assert(err.Error(), Equals, test.expect)
	}
}

func (s *ExprSuite) TestBindInputsError(c *C) {
	tests := []struct {
		query       string
		typeSamples []any
		inputArgs   []any
		err         string
	}{{
		query:       "SELECT street FROM t WHERE x = $Address.street, y = $Person.name",
		typeSamples: []any{Address{}, Person{}},
		inputArgs:   []any{Address{Street: "Dead end road"}},
		err:         `invalid input parameter: parameter with type "Person" missing (have "Address")`,
	}, {
		query:       "SELECT street FROM t WHERE x = $Address.street, y = $Person.name",
		typeSamples: []any{Address{}, Person{}},
		inputArgs:   []any{nil, Person{Fullname: "Monty Bingles"}},
		err:         "invalid input parameter: need struct or map, got nil",
	}, {
		query:       "SELECT street FROM t WHERE x = $Address.street, y = $Person.name",
		typeSamples: []any{Address{}, Person{}},
		inputArgs:   []any{(*Person)(nil)},
		err:         "invalid input parameter: need struct or map, got nil",
	}, {
		query:       "SELECT street FROM t WHERE x = $Address.street",
		typeSamples: []any{Address{}},
		inputArgs:   []any{8},
		err:         "invalid input parameter: need struct or map, got int",
	}, {
		query:       "SELECT street FROM t WHERE x = $Address.street",
		typeSamples: []any{Address{}},
		inputArgs:   []any{[]any{}},
		err:         "invalid input parameter: need struct or map, got slice",
	}, {
		query:       "SELECT street FROM t WHERE x = $Address.street",
		typeSamples: []any{Address{}},
		inputArgs:   []any{Address{}, Person{}},
		err:         "invalid input parameter: Person not referenced in query",
	}, {
		query:       "SELECT * AS &Address.* FROM t WHERE x = $M.Fullname",
		typeSamples: []any{Address{}, sqlair.M{}},
		inputArgs:   []any{sqlair.M{"fullname": "Jimany Johnson"}},
		err:         `invalid input parameter: map "M" does not contain key "Fullname"`,
	}, {
		query:       "SELECT foo FROM t WHERE x = $M.street, y = $Person.id",
		typeSamples: []any{Person{}, sqlair.M{}},
		inputArgs:   []any{Person{ID: 666}, sqlair.M{"Street": "Highway to Hell"}},
		err:         `invalid input parameter: map "M" does not contain key "street"`,
	}, {
		query:       "SELECT street FROM t WHERE x = $Address.street, y = $Person.name",
		typeSamples: []any{Address{}, Person{}},
		inputArgs:   []any{},
		err:         `invalid input parameter: parameter with type "Address" missing`,
	}, {
		query:       "SELECT street FROM t WHERE x = $Person.id, y = $Person.name",
		typeSamples: []any{Person{}},
		inputArgs:   []any{Person{}, Person{}},
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
		typeSamples []any
		inputArgs   []any
		err         string
	}{{
		query:       "SELECT street FROM t WHERE y = $Person.name",
		typeSamples: []any{outerP},
		inputArgs:   []any{shadowedP},
		err:         `invalid input parameter: parameter with type "expr_test.Person" missing, have type with same name: "expr_test.Person"`,
	}}

	tests = append(tests, testsShadowed...)

	for i, t := range tests {
		parser := expr.NewParser()
		parsedExpr, err := parser.Parse(t.query)
		if err != nil {
			c.Fatal(err)
		}

		typedExpr, err := parsedExpr.BindTypes(t.typeSamples...)
		if err != nil {
			c.Fatal(err)
		}

		_, err = typedExpr.BindInputs(t.inputArgs...)
		if err != nil {
			c.Assert(err.Error(), Equals, t.err,
				Commentf("test %d failed:\nquery: %s", i, t.query))
		} else {
			c.Errorf("test %d failed:\nexpected err: %q but got nil\nquery: %q", i, t.err, t.query)
		}
	}
}

func (s *ExprSuite) TestSliceSyntax(c *C) {
	tests := []struct {
		summery string
		query   string
		parts   string
	}{{
		"single slice",
		"SELECT name FROM person WHERE id IN ($S[:])",
		"[Bypass[SELECT name FROM person WHERE id IN (] Input[S[:]] Bypass[)]]",
	}, {
		"many slice ranges",
		"SELECT * AS &Person.* FROM person WHERE id IN ($Person.id, $S[:], $Manager.id, $IntSlice[:], $StringSlice[:])",
		"[Bypass[SELECT ] Output[[*] [Person.*]] Bypass[ FROM person WHERE id IN (] Input[Person.id] Bypass[, ] Input[S[:]] Bypass[, ] Input[Manager.id] Bypass[, ] Input[IntSlice[:]] Bypass[, ] Input[StringSlice[:]] Bypass[)]]",
	}, {
		"slices and other expressions in IN statement",
		`SELECT name FROM person WHERE id IN ($S[:], func(1,2), "one", $IntSlice[:])`,
		`[Bypass[SELECT name FROM person WHERE id IN (] Input[S[:]] Bypass[, func(1,2), "one", ] Input[IntSlice[:]] Bypass[)]]`,
	}}
	for _, t := range tests {
		parser := expr.NewParser()
		parsedExpr, err := parser.Parse(t.query)
		c.Assert(err, IsNil)
		c.Assert(parsedExpr.String(), Equals, t.parts)
	}
}
