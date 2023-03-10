package expr_test

import (
	"database/sql"

	"github.com/canonical/sqlair/internal/expr"
	_ "github.com/mattn/go-sqlite3"

	. "gopkg.in/check.v1"
)

type DBSuite struct{}

var _ = Suite(&DBSuite{})

func setupDB() (*sql.DB, error) {
	return sql.Open("sqlite3", ":memory:")
}

func createExampleDB(createTables string, inserts []string) (*sql.DB, error) {
	db, err := setupDB()
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(createTables)
	if err != nil {
		return nil, err
	}
	for _, insert := range inserts {
		_, err := db.Exec(insert)
		if err != nil {
			return nil, err
		}
	}

	return db, nil
}

func personAndAddressDB() (string, *sql.DB, error) {
	createTables := `
CREATE TABLE person (
	name text,
	id integer,
	address_id integer,
	email text
);
CREATE TABLE address (
	id integer,
	district text,
	street text
);
`
	dropTables := `
DROP TABLE person;
DROP TABLE address;
`

	inserts := []string{
		"INSERT INTO person VALUES ('Fred', 30, 1000, 'fred@email.com');",
		"INSERT INTO person VALUES ('Mark', 20, 1500, 'mark@email.com');",
		"INSERT INTO person VALUES ('Mary', 40, 3500, 'mary@email.com');",
		"INSERT INTO person VALUES ('James', 35, 4500, 'james@email.com');",
		"INSERT INTO address VALUES (1000, 'Happy Land', 'Main Street');",
		"INSERT INTO address VALUES (1500, 'Sad World', 'Church Road');",
		"INSERT INTO address VALUES (3500, 'Ambivalent Commons', 'Station Lane');",
	}

	db, err := createExampleDB(createTables, inserts)
	if err != nil {
		return "", nil, err
	}
	return dropTables, db, nil
}

func (s *DBSuite) TestValidDecode(c *C) {
	var tests = []struct {
		summary  string
		query    string
		types    []any
		inputs   []any
		outputs  [][]any
		expected [][]any
	}{{
		summary:  "double select with name clash (first 4 rows)",
		query:    "SELECT p.id AS &Person.*, a.id AS &Address.* FROM person AS p, address AS a",
		types:    []any{Person{}, Address{}},
		inputs:   []any{},
		outputs:  [][]any{{&Person{}, &Address{}}, {&Person{}, &Address{}}, {&Person{}, &Address{}}, {&Person{}, &Address{}}},
		expected: [][]any{{&Person{ID: 30}, &Address{ID: 1000}}, {&Person{ID: 30}, &Address{ID: 1500}}, {&Person{ID: 30}, &Address{ID: 3500}}, {&Person{ID: 20}, &Address{ID: 1000}}},
	}, {
		summary:  "simple select person",
		query:    "SELECT * AS &Person.* FROM person",
		types:    []any{Person{}},
		inputs:   []any{},
		outputs:  [][]any{{&Person{}}, {&Person{}}, {&Person{}}, {&Person{}}},
		expected: [][]any{{&Person{30, "Fred", 1000}}, {&Person{20, "Mark", 1500}}, {&Person{40, "Mary", 3500}}, {&Person{35, "James", 4500}}},
	}, {
		summary:  "select multiple with extras",
		query:    "SELECT name, * AS &Person.*, address_id AS &Address.id, * AS &Manager.*, id FROM person WHERE id = $Address.id",
		types:    []any{Person{}, Address{}, Manager{}},
		inputs:   []any{Address{ID: 30}},
		outputs:  [][]any{{&Person{}, &Address{}, &Manager{}}},
		expected: [][]any{{&Person{30, "Fred", 1000}, &Address{ID: 1000}, &Manager{30, "Fred", 1000}}},
	}, {
		summary:  "select with renaming",
		query:    "SELECT (name, address_id) AS (&Address.street, &Address.id) FROM person WHERE id = $Manager.id",
		types:    []any{Address{}, Manager{}},
		inputs:   []any{Manager{ID: 30}},
		outputs:  [][]any{{&Address{}}},
		expected: [][]any{{&Address{Street: "Fred", ID: 1000}}},
	}, {
		summary:  "select into star struct",
		query:    "SELECT (name, address_id) AS &Person.* FROM person WHERE address_id IN ( $Manager.address_id, $Address.district )",
		types:    []any{Person{}, Address{}, Manager{}},
		inputs:   []any{Manager{PostalCode: 1000}, Address{ID: 2000}},
		outputs:  [][]any{{&Person{}}},
		expected: [][]any{{&Person{Fullname: "Fred", PostalCode: 1000}}},
	}}

	// A Person struct that shadows the one in tests above and has different int types.
	type Person struct {
		ID         int32  `db:"id"`
		Fullname   string `db:"name"`
		PostalCode int32  `db:"address_id"`
	}

	var testsWithShadowPerson = []struct {
		summary  string
		query    string
		types    []any
		inputs   []any
		outputs  [][]any
		expected [][]any
	}{{
		summary:  "alternative type shadow person",
		query:    "SELECT * AS &Person.* FROM person",
		types:    []any{Person{}},
		inputs:   []any{},
		outputs:  [][]any{{&Person{}}, {&Person{}}, {&Person{}}, {&Person{}}},
		expected: [][]any{{&Person{30, "Fred", 1000}}, {&Person{20, "Mark", 1500}}, {&Person{40, "Mary", 3500}}, {&Person{35, "James", 4500}}},
	}}

	tests = append(tests, testsWithShadowPerson...)

	dropTables, db, err := personAndAddressDB()
	if err != nil {
		c.Fatal(err)
	}

	sqlairDB := expr.NewTestDB(db)

	parser := expr.NewParser()

	for _, t := range tests {
		parsedExpr, err := parser.Parse(t.query)
		if err != nil {
			c.Errorf("test %q failed (Parse):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}
		preparedExpr, err := parsedExpr.Prepare(t.types...)
		if err != nil {
			c.Errorf("test %q failed (Prepare):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}
		completedExpr, err := preparedExpr.Complete(t.inputs...)
		if err != nil {
			c.Errorf("test %q failed (Complete):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}
		resultExpr, err := sqlairDB.Query(completedExpr)
		if err != nil {
			c.Errorf("test %q failed (Exec):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}

		// Get as many rows as we have lists of structs to output into.
		for i, outs := range t.outputs {
			if ok, err := resultExpr.Next(); err != nil {
				c.Fatalf("test %q failed (Next):\ninput: %s\nerr: %s", t.summary, t.query, err)
			} else if !ok {
				c.Fatalf("test %q failed (Next):\ninput: %s\nerr: no more rows in query results", t.summary, t.query)
			}
			err := resultExpr.Decode(outs...)
			if err != nil {
				c.Fatalf("test %q failed (Decode):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			}
			c.Assert(outs, HasLen, len(t.expected[i]),
				Commentf("test %q failed:\ninput: %s", t.summary, t.query))
			for j, out := range outs {
				c.Assert(out, DeepEquals, t.expected[i][j],
					Commentf("test %q failed (Decode):\ninput: %s\noutput row: %d\n", t.summary, t.query, j))
			}
		}
		resultExpr.Close()
	}

	_, err = db.Exec(dropTables)
	if err != nil {
		c.Fatal(err)
	}
}

func (s *DBSuite) TestDecodeErrors(c *C) {
	var tests = []struct {
		summary string
		query   string
		types   []any
		inputs  []any
		outputs [][]any
		err     string
	}{{
		summary: "nil parameter",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		outputs: [][]any{{nil}},
		err:     "cannot decode expression: need valid struct, got nil",
	}, {
		summary: "non pointer parameter",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		outputs: [][]any{{Person{}}},
		err:     "cannot decode expression: need pointer to struct, got non-pointer",
	}, {
		summary: "wrong struct",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		outputs: [][]any{{&Address{}}},
		err:     "cannot decode expression: no output expression of type Address",
	}, {
		summary: "not a struct",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		outputs: [][]any{{&map[string]any{}}},
		err:     "cannot decode expression: need struct, got map",
	}}

	dropTables, db, err := personAndAddressDB()
	if err != nil {
		c.Fatal(err)
	}

	sqlairDB := expr.NewTestDB(db)

	parser := expr.NewParser()

	for _, t := range tests {
		parsedExpr, err := parser.Parse(t.query)
		if err != nil {
			c.Errorf("test %q failed (Parse):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}
		preparedExpr, err := parsedExpr.Prepare(t.types...)
		if err != nil {
			c.Errorf("test %q failed (Prepare):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}
		completedExpr, err := preparedExpr.Complete(t.inputs...)
		if err != nil {
			c.Errorf("test %q failed (Complete):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}
		resultExpr, err := sqlairDB.Query(completedExpr)
		if err != nil {
			c.Errorf("test %q failed (Exec):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}

		// Get len(t.outputs) rows.
		for _, outs := range t.outputs {
			if ok, err := resultExpr.Next(); err != nil {
				c.Fatalf("test %q failed (Next) :\ninput: %s\nerr: %s", t.summary, t.query, err)
			} else if !ok {
				c.Fatalf("test %q failed (Next) :\ninput: %s\nerr: no more rows in query results", t.summary, t.query)
			}
			err := resultExpr.Decode(outs...)
			c.Assert(err, ErrorMatches, t.err,
				Commentf("test %q failed:\ninput: %s\noutputs: %s", t.summary, t.query, t.outputs))
		}
		resultExpr.Close()
	}

	_, err = db.Exec(dropTables)
	if err != nil {
		c.Fatal(err)
	}
}

func (s *DBSuite) TestValidAll(c *C) {
	var tests = []struct {
		summary  string
		query    string
		types    []any
		inputs   []any
		expected [][]any
	}{{
		summary:  "double select with name clash",
		query:    "SELECT p.id AS &Person.*, a.id AS &Address.* FROM person AS p, address AS a",
		types:    []any{Person{}, Address{}},
		inputs:   []any{},
		expected: [][]any{{Person{ID: 30}, Address{ID: 1000}}, {Person{ID: 30}, Address{ID: 1500}}, {Person{ID: 30}, Address{ID: 3500}}, {Person{ID: 20}, Address{ID: 1000}}, {Person{ID: 20}, Address{ID: 1500}}, {Person{ID: 20}, Address{ID: 3500}}, {Person{ID: 40}, Address{ID: 1000}}, {Person{ID: 40}, Address{ID: 1500}}, {Person{ID: 40}, Address{ID: 3500}}, {Person{ID: 35}, Address{ID: 1000}}, {Person{ID: 35}, Address{ID: 1500}}, {Person{ID: 35}, Address{ID: 3500}}},
	}, {
		summary:  "select all columns into person",
		query:    "SELECT * AS &Person.* FROM person",
		types:    []any{Person{}},
		inputs:   []any{},
		expected: [][]any{{Person{30, "Fred", 1000}}, {Person{20, "Mark", 1500}}, {Person{40, "Mary", 3500}}, {Person{35, "James", 4500}}},
	}, {
		summary:  "single line of query with inputs",
		query:    "SELECT p.* AS &Person.*, a.* AS &Address.*, p.* AS &Manager.* FROM person AS p, address AS a WHERE p.id = $Person.id AND a.id = $Address.id ",
		types:    []any{Person{}, Address{}, Manager{}},
		inputs:   []any{Address{ID: 1000}, Person{ID: 30}},
		expected: [][]any{{Person{30, "Fred", 1000}, Address{1000, "Happy Land", "Main Street"}, Manager{30, "Fred", 1000}}},
	}, {
		summary:  "nothing returned",
		query:    "SELECT &Person.* FROM person WHERE id = $Person.id",
		types:    []any{Person{}},
		inputs:   []any{Person{ID: 1243321}},
		expected: [][]any{},
	}}

	dropTables, db, err := personAndAddressDB()
	if err != nil {
		c.Fatal(err)
	}

	sqlairDB := expr.NewTestDB(db)

	parser := expr.NewParser()

	for _, t := range tests {
		parsedExpr, err := parser.Parse(t.query)
		if err != nil {
			c.Errorf("test %q failed (Parse):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}
		preparedExpr, err := parsedExpr.Prepare(t.types...)
		if err != nil {
			c.Errorf("test %q failed (Prepare):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}
		completedExpr, err := preparedExpr.Complete(t.inputs...)
		if err != nil {
			c.Errorf("test %q failed (Complete):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}
		resultExpr, err := sqlairDB.Query(completedExpr)
		if err != nil {
			c.Errorf("test %q failed (Exec):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}
		res, err := resultExpr.All()
		if err != nil {
			c.Errorf("test %q failed (All):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}
		c.Assert(res, HasLen, len(t.expected),
			Commentf("test %q failed:\ninput: %s", t.summary, t.query))
		for i, es := range t.expected {
			for j, e := range es {
				c.Assert(res[i][j], DeepEquals, e,
					Commentf("test %q failed:\ninput: %s", t.summary, t.query))
			}
		}
	}

	_, err = db.Exec(dropTables)
	if err != nil {
		c.Fatal(err)
	}
}

func (s *DBSuite) TestValidOne(c *C) {
	var tests = []struct {
		summary  string
		query    string
		types    []any
		inputs   []any
		outputs  []any
		expected []any
	}{{
		summary:  "double select with name clash",
		query:    "SELECT p.id AS &Person.*, a.id AS &Address.* FROM person AS p, address AS a",
		types:    []any{Person{}, Address{}},
		inputs:   []any{},
		outputs:  []any{&Person{}, &Address{}},
		expected: []any{&Person{ID: 30}, &Address{ID: 1000}},
	}, {
		summary:  "select into multiple structs, with input conditions",
		query:    "SELECT p.* AS &Person.*, a.* AS &Address.*, p.* AS &Manager.* FROM person AS p, address AS a WHERE p.id = $Person.id AND a.id = $Address.id ",
		types:    []any{Person{}, Address{}, Manager{}},
		inputs:   []any{Address{ID: 1000}, Person{ID: 30}},
		outputs:  []any{&Person{}, &Address{}, &Manager{}},
		expected: []any{&Person{30, "Fred", 1000}, &Address{1000, "Happy Land", "Main Street"}, &Manager{30, "Fred", 1000}},
	}}

	dropTables, db, err := personAndAddressDB()
	if err != nil {
		c.Fatal(err)
	}

	sqlairDB := expr.NewTestDB(db)

	parser := expr.NewParser()

	for _, t := range tests {
		parsedExpr, err := parser.Parse(t.query)
		if err != nil {
			c.Errorf("test %q failed (Parse):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}
		preparedExpr, err := parsedExpr.Prepare(t.types...)
		if err != nil {
			c.Errorf("test %q failed (Prepare):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}
		completedExpr, err := preparedExpr.Complete(t.inputs...)
		if err != nil {
			c.Errorf("test %q failed (Complete):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}
		resultExpr, err := sqlairDB.Query(completedExpr)
		if err != nil {
			c.Errorf("test %q failed (Exec):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}
		err = resultExpr.One(t.outputs...)
		if err != nil {
			c.Errorf("test %q failed (One):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}
		for i, e := range t.expected {
			c.Assert(t.outputs[i], DeepEquals, e,
				Commentf("test %q failed:\ninput: %s", t.summary, t.query))
		}
	}

	_, err = db.Exec(dropTables)
	if err != nil {
		c.Fatal(err)
	}
}

func (s *DBSuite) TestOneErrors(c *C) {
	var tests = []struct {
		summary string
		query   string
		types   []any
		inputs  []any
		outputs []any
		err     string
	}{{
		summary: "no rows",
		query:   "SELECT * AS &Person.* FROM person WHERE id=12312",
		types:   []any{Person{}},
		inputs:  []any{},
		outputs: []any{&Person{}},
		err:     "cannot return one row: no results",
	}}

	dropTables, db, err := personAndAddressDB()
	if err != nil {
		c.Fatal(err)
	}

	sqlairDB := expr.NewTestDB(db)

	parser := expr.NewParser()

	for _, t := range tests {
		parsedExpr, err := parser.Parse(t.query)
		if err != nil {
			c.Errorf("test %q failed (Parse):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}
		preparedExpr, err := parsedExpr.Prepare(t.types...)
		if err != nil {
			c.Errorf("test %q failed (Prepare):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}
		completedExpr, err := preparedExpr.Complete(t.inputs...)
		if err != nil {
			c.Errorf("test %q failed (Complete):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}
		resultExpr, err := sqlairDB.Query(completedExpr)
		if err != nil {
			c.Errorf("test %q failed (Exec):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}

		err = resultExpr.One(t.outputs...)
		c.Assert(err, ErrorMatches, t.err,
			Commentf("test %q failed:\ninput: %s\noutputs: %s", t.summary, t.query, t.outputs))
	}

	_, err = db.Exec(dropTables)
	if err != nil {
		c.Fatal(err)
	}
}
