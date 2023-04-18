package sqlair_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	. "gopkg.in/check.v1"

	"github.com/canonical/sqlair"
)

// Hook up gocheck into the "go test" runner.
func TestExpr(t *testing.T) { TestingT(t) }

type PackageSuite struct{}

var _ = Suite(&PackageSuite{})

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

type District struct{}

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

func (s *PackageSuite) TestValidIter(c *C) {
	var tests = []struct {
		summary  string
		query    string
		types    []any
		inputs   []any
		outputs  [][]any
		expected [][]any
	}{{
		summary:  "double select with name clash",
		query:    "SELECT p.id AS &Person.*, a.id AS &Address.* FROM person AS p, address AS a",
		types:    []any{Person{}, Address{}},
		inputs:   []any{},
		outputs:  [][]any{{&Person{}, &Address{}}, {&Person{}, &Address{}}, {&Person{}, &Address{}}, {&Person{}, &Address{}}, {&Person{}, &Address{}}, {&Person{}, &Address{}}, {&Person{}, &Address{}}, {&Person{}, &Address{}}, {&Person{}, &Address{}}, {&Person{}, &Address{}}, {&Person{}, &Address{}}, {&Person{}, &Address{}}},
		expected: [][]any{{&Person{ID: 30}, &Address{ID: 1000}}, {&Person{ID: 30}, &Address{ID: 1500}}, {&Person{ID: 30}, &Address{ID: 3500}}, {&Person{ID: 20}, &Address{ID: 1000}}, {&Person{ID: 20}, &Address{ID: 1500}}, {&Person{ID: 20}, &Address{ID: 3500}}, {&Person{ID: 40}, &Address{ID: 1000}}, {&Person{ID: 40}, &Address{ID: 1500}}, {&Person{ID: 40}, &Address{ID: 3500}}, {&Person{ID: 35}, &Address{ID: 1000}}, {&Person{ID: 35}, &Address{ID: 1500}}, {&Person{ID: 35}, &Address{ID: 3500}}},
	}, {
		summary:  "simple select person",
		query:    "SELECT * AS &Person.* FROM person",
		types:    []any{Person{}},
		inputs:   []any{},
		outputs:  [][]any{{&Person{}}, {&Person{}}, {&Person{}}, {&Person{}}},
		expected: [][]any{{&Person{30, "Fred", 1000}}, {&Person{20, "Mark", 1500}}, {&Person{40, "Mary", 3500}}, {&Person{35, "James", 4500}}},
	}, {
		summary:  "select multiple with extras",
		query:    "SELECT email, * AS &Person.*, address_id AS &Address.id, * AS &Manager.*, id FROM person WHERE id = $Address.id",
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

	dropTables, sqldb, err := personAndAddressDB()
	c.Assert(err, IsNil)

	db := sqlair.NewDB(sqldb)

	for _, t := range tests {
		stmt, err := sqlair.Prepare(t.query, t.types...)
		if err != nil {
			c.Errorf("\ntest %q failed (Prepare):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}

		iter := db.Query(nil, stmt, t.inputs...).Iter()
		defer iter.Close()
		i := 0
		for iter.Next() {
			if i >= len(t.outputs) {
				c.Errorf("\ntest %q failed (Next):\ninput: %s\nerr: more rows that expected (%d >= %d)\n", t.summary, t.query, i, len(t.outputs))
				break
			}
			if err := iter.Decode(t.outputs[i]...); err != nil {
				c.Errorf("\ntest %q failed (Decode):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			}
			i++
		}

		err = iter.Close()
		if err != nil {
			c.Errorf("\ntest %q failed (Close):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
		}
		for i, row := range t.expected {
			for j, col := range row {
				c.Assert(t.outputs[i][j], DeepEquals, col,
					Commentf("\ntest %q failed:\ninput: %s\nrow: %d\n", t.summary, t.query, i))
			}
		}

	}

	err = db.Query(nil, sqlair.MustPrepare(dropTables)).Run()
	c.Assert(err, IsNil)
}

func (s *PackageSuite) TestIterErrors(c *C) {
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
		err:     "cannot decode result: need pointer to struct, got nil",
	}, {
		summary: "nil pointer parameter",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		outputs: [][]any{{(*Person)(nil)}},
		err:     "cannot decode result: got nil pointer",
	}, {
		summary: "non pointer parameter",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		outputs: [][]any{{Person{}}},
		err:     "cannot decode result: need pointer to struct, got struct",
	}, {
		summary: "wrong struct",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		outputs: [][]any{{&Address{}}},
		err:     `cannot decode result: type "Address" does not appear in query, have: Person`,
	}, {
		summary: "not a struct",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		outputs: [][]any{{&map[string]any{}}},
		err:     "cannot decode result: need pointer to struct, got pointer to map",
	}, {
		summary: "missing decode value",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		outputs: [][]any{{}},
		err:     `cannot decode result: type "Person" found in query but not passed to decode`,
	}, {
		summary: "multiple of the same type",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		outputs: [][]any{{&Person{}, &Person{}}},
		err:     `cannot decode result: type "Person" provided more than once`,
	}, {
		summary: "output expr in a with clause",
		query: `WITH averageID(avgid) AS
  (SELECT &Person.id
   FROM person)
  SELECT id
  FROM person, averageID
  WHERE id > averageID.avgid`,
		types:   []any{Person{}},
		inputs:  []any{},
		outputs: [][]any{{&Person{}}},
		err:     "cannot decode result: column for Person.id not found in results",
	}}

	dropTables, sqldb, err := personAndAddressDB()
	c.Assert(err, IsNil)

	db := sqlair.NewDB(sqldb)

	for _, t := range tests {
		stmt, err := sqlair.Prepare(t.query, t.types...)
		if err != nil {
			c.Errorf("\ntest %q failed (Prepare):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}

		iter := db.Query(nil, stmt, t.inputs...).Iter()
		defer iter.Close()
		i := 0
		for iter.Next() {
			if i >= len(t.outputs) {
				c.Errorf("\ntest %q failed (Next):\ninput: %s\nerr: more rows that expected\n", t.summary, t.query)
				break
			}
			if err := iter.Decode(t.outputs[i]...); err != nil {
				c.Assert(err, ErrorMatches, t.err,
					Commentf("\ntest %q failed:\ninput: %s\noutputs: %s", t.summary, t.query, t.outputs))
				iter.Close()
				break
			}
			i++
		}
		err = iter.Close()
		if err != nil {
			c.Errorf("\ntest %q failed (Close):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
		}
	}

	err = db.Query(nil, sqlair.MustPrepare(dropTables)).Run()
	c.Assert(err, IsNil)
}

func (s *PackageSuite) TestValidOne(c *C) {
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

	dropTables, sqldb, err := personAndAddressDB()
	c.Assert(err, IsNil)

	db := sqlair.NewDB(sqldb)

	for _, t := range tests {
		stmt, err := sqlair.Prepare(t.query, t.types...)
		if err != nil {
			c.Errorf("\ntest %q failed (Prepare):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}

		q := db.Query(nil, stmt, t.inputs...)
		err = q.One(t.outputs...)
		if err != nil {
			c.Errorf("\ntest %q failed (One):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}
		for i, s := range t.expected {
			c.Assert(t.outputs[i], DeepEquals, s,
				Commentf("\ntest %q failed:\ninput: %s", t.summary, t.query))
		}
	}

	err = db.Query(nil, sqlair.MustPrepare(dropTables)).Run()
	c.Assert(err, IsNil)
}

func (s *PackageSuite) TestOneErrors(c *C) {
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
		err:     "sql: no rows in result set",
	}, {
		summary: "missing parameter",
		query:   "SELECT * AS &Person.* FROM person WHERE id = $Person.id",
		types:   []any{Person{}},
		inputs:  []any{},
		outputs: []any{&Person{}},
		err:     `invalid input parameter: type "Person" not passed as a parameter`,
	}}

	dropTables, sqldb, err := personAndAddressDB()
	c.Assert(err, IsNil)

	db := sqlair.NewDB(sqldb)

	for _, t := range tests {
		stmt, err := sqlair.Prepare(t.query, t.types...)
		if err != nil {
			c.Errorf("\ntest %q failed (Prepare):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}

		err = db.Query(nil, stmt, t.inputs...).One(t.outputs...)
		c.Assert(err, ErrorMatches, t.err,
			Commentf("\ntest %q failed:\ninput: %s\noutputs: %s", t.summary, t.query, t.outputs))
	}

	err = db.Query(nil, sqlair.MustPrepare(dropTables)).Run()
	c.Assert(err, IsNil)
}

func (s *PackageSuite) TestErrNoRows(c *C) {
	dropTables, sqldb, err := personAndAddressDB()
	c.Assert(err, IsNil)

	db := sqlair.NewDB(sqldb)
	stmt := sqlair.MustPrepare("SELECT * AS &Person.* FROM person WHERE id=12312", Person{})
	err = db.Query(nil, stmt).One(&Person{})
	if !errors.Is(err, sqlair.ErrNoRows) {
		c.Errorf("expected %q, got %q", sqlair.ErrNoRows, err)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		c.Errorf("expected %q, got %q", sql.ErrNoRows, err)
	}

	err = db.Query(nil, sqlair.MustPrepare(dropTables)).Run()
	c.Assert(err, IsNil)
}

func (s *PackageSuite) TestValidAll(c *C) {
	var tests = []struct {
		summary  string
		query    string
		types    []any
		inputs   []any
		slices   []any
		expected []any
	}{{
		summary:  "double select with name clash",
		query:    "SELECT p.id AS &Person.*, a.id AS &Address.* FROM person AS p, address AS a",
		types:    []any{Person{}, Address{}},
		inputs:   []any{},
		slices:   []any{&[]*Person{}, &[]*Address{}},
		expected: []any{&[]*Person{&Person{ID: 30}, &Person{ID: 30}, &Person{ID: 30}, &Person{ID: 20}, &Person{ID: 20}, &Person{ID: 20}, &Person{ID: 40}, &Person{ID: 40}, &Person{ID: 40}, &Person{ID: 35}, &Person{ID: 35}, &Person{ID: 35}}, &[]*Address{&Address{ID: 1000}, &Address{ID: 1500}, &Address{ID: 3500}, &Address{ID: 1000}, &Address{ID: 1500}, &Address{ID: 3500}, &Address{ID: 1000}, &Address{ID: 1500}, &Address{ID: 3500}, &Address{ID: 1000}, &Address{ID: 1500}, &Address{ID: 3500}}},
	}, {
		summary:  "select all columns into person",
		query:    "SELECT * AS &Person.* FROM person",
		types:    []any{Person{}},
		inputs:   []any{},
		slices:   []any{&[]*Person{}},
		expected: []any{&[]*Person{&Person{30, "Fred", 1000}, &Person{20, "Mark", 1500}, &Person{40, "Mary", 3500}, &Person{35, "James", 4500}}},
	}, {
		summary:  "select all columns into person with no pointers",
		query:    "SELECT * AS &Person.* FROM person",
		types:    []any{Person{}},
		inputs:   []any{},
		slices:   []any{&[]Person{}},
		expected: []any{&[]Person{Person{30, "Fred", 1000}, Person{20, "Mark", 1500}, Person{40, "Mary", 3500}, Person{35, "James", 4500}}},
	}, {
		summary:  "single line of query with inputs",
		query:    "SELECT p.* AS &Person.*, a.* AS &Address.*, p.* AS &Manager.* FROM person AS p, address AS a WHERE p.id = $Person.id AND a.id = $Address.id ",
		types:    []any{Person{}, Address{}, Manager{}},
		inputs:   []any{Address{ID: 1000}, Person{ID: 30}},
		slices:   []any{&[]*Manager{}, &[]*Person{}, &[]*Address{}},
		expected: []any{&[]*Manager{{30, "Fred", 1000}}, &[]*Person{{30, "Fred", 1000}}, &[]*Address{{1000, "Happy Land", "Main Street"}}},
	}, {
		summary:  "nothing returned",
		query:    "SELECT &Person.* FROM person WHERE id = $Person.id",
		types:    []any{Person{}},
		inputs:   []any{Person{ID: 1243321}},
		slices:   []any{&[]*Person{}},
		expected: []any{},
	}}

	dropTables, sqldb, err := personAndAddressDB()
	c.Assert(err, IsNil)

	db := sqlair.NewDB(sqldb)

	for _, t := range tests {
		stmt, err := sqlair.Prepare(t.query, t.types...)
		if err != nil {
			c.Errorf("\ntest %q failed (Prepare):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}

		q := db.Query(nil, stmt, t.inputs...)
		err = q.All(t.slices...)
		if err != nil {
			c.Errorf("\ntest %q failed (All):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}
		for i, column := range t.expected {
			c.Assert(t.slices[i], DeepEquals, column,
				Commentf("\ntest %q failed:\ninput: %s", t.summary, t.query))
		}
	}

	err = db.Query(nil, sqlair.MustPrepare(dropTables)).Run()
	c.Assert(err, IsNil)
}

func (s *PackageSuite) TestAllErrors(c *C) {
	var tests = []struct {
		summary string
		query   string
		types   []any
		inputs  []any
		slices  []any
		err     string
	}{{
		summary: "nil argument",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		slices:  []any{nil},
		err:     "cannot populate slice: need pointer to slice, got invalid",
	}, {
		summary: "nil pointer argument",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		slices:  []any{(*[]Person)(nil)},
		err:     "cannot populate slice: need pointer to slice, got nil",
	}, {
		summary: "none slice argument",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		slices:  []any{Person{}},
		err:     "cannot populate slice: need pointer to slice, got struct",
	}, {
		summary: "none slice pointer argument",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		slices:  []any{&Person{}},
		err:     "cannot populate slice: need pointer to slice, got pointer to struct",
	}, {
		summary: "wrong struct argument",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		slices:  []any{&[]*Address{}},
		err:     `cannot populate slice: cannot decode result: type "Address" does not appear in query, have: Person`,
	}, {
		summary: "wrong struct argument",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		slices:  []any{&[]int{}},
		err:     `cannot populate slice: need slice of struct, got slice of int`,
	}}

	dropTables, sqldb, err := personAndAddressDB()
	c.Assert(err, IsNil)

	db := sqlair.NewDB(sqldb)

	for _, t := range tests {
		stmt, err := sqlair.Prepare(t.query, t.types...)
		if err != nil {
			c.Errorf("\ntest %q failed (Prepare):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}

		err = db.Query(nil, stmt, t.inputs...).All(t.slices...)
		c.Assert(err, ErrorMatches, t.err,
			Commentf("\ntest %q failed:\ninput: %s\nslices: %s", t.summary, t.query, t.slices))
	}

	err = db.Query(nil, sqlair.MustPrepare(dropTables)).Run()
	c.Assert(err, IsNil)
}

func (s *PackageSuite) TestIterMethodOrder(c *C) {
	dropTables, sqldb, err := personAndAddressDB()
	c.Assert(err, IsNil)

	db := sqlair.NewDB(sqldb)

	var p = Person{}
	stmt := sqlair.MustPrepare("SELECT &Person.* FROM person", Person{})

	// Check immidiate Decode.
	iter := db.Query(nil, stmt).Iter()
	if iter.Decode(&p) {
		c.Fatal("expected false, got true")
	}
	err = iter.Close()
	c.Assert(err, ErrorMatches, "cannot decode result: sql: Scan called without calling Next")

	// Check Next after closing.
	iter = db.Query(nil, stmt).Iter()
	err = iter.Close()
	c.Assert(err, IsNil)
	if iter.Next() {
		c.Fatal("expected false, got true")
	}
	err = iter.Close()
	c.Assert(err, IsNil)

	// Check Decode after closing.
	iter = db.Query(nil, stmt).Iter()
	err = iter.Close()
	c.Assert(err, IsNil)
	if iter.Decode(&p) {
		c.Fatal("expected false, got true")
	}
	err = iter.Close()
	c.Assert(err, ErrorMatches, "cannot decode result: iteration ended or not started")

	// Check multiple closes.
	iter = db.Query(nil, stmt).Iter()
	err = iter.Close()
	c.Assert(err, IsNil)
	err = iter.Close()
	c.Assert(err, IsNil)

	// Check SQL Scan error (scanning string into an int).
	badTypesStmt := sqlair.MustPrepare("SELECT name AS &Person.id FROM person", Person{})
	iter = db.Query(nil, badTypesStmt).Iter()
	if !iter.Next() {
		c.Fatal("expected true, got false")
	}
	// SQL scan error, try to
	if iter.Decode(&p) {
		c.Fatal("expected false, got true")
	}
	err = iter.Close()
	c.Assert(err, ErrorMatches, `cannot decode result: sql: Scan error on column index 0, name "_sqlair_0": converting driver.Value type string \("Fred"\) to a int: invalid syntax`)

	// Check rows close properly if we get a decode error.
	// If they do not close properly we will not be able to
	// drop the table as the connection will not be closed.

	// SQLair error in decode
	iter = db.Query(nil, stmt).Iter()
	if !iter.Next() {
		c.Fatal("expected true, got false")
	}
	// Decode is missing output struct (SQLair throws an error).
	if iter.Decode() {
		c.Fatal("expected false, got true")
	}

	// SQL error in decode
	iter = db.Query(nil, badTypesStmt).Iter()
	if !iter.Next() {
		c.Fatal("expected true, got false")
	}
	// SQL scan error, try to scan string into an int.
	if iter.Decode(&p) {
		c.Fatal("expected false, got true")
	}

	_, err = db.PlainDB().Exec(dropTables)
	c.Assert(err, IsNil)
}

func (s *PackageSuite) TestRun(c *C) {
	dropTables, sqldb, err := personAndAddressDB()
	c.Assert(err, IsNil)

	var jim = Person{
		ID:         70,
		Fullname:   "Jim",
		PostalCode: 500,
	}

	db := sqlair.NewDB(sqldb)

	insertStmt := sqlair.MustPrepare("INSERT INTO person VALUES ( $Person.name, $Person.id, $Person.address_id, 'jimmy@email.com');", Person{})
	err = db.Query(nil, insertStmt, &jim).Run()
	c.Assert(err, IsNil)

	selectStmt := sqlair.MustPrepare("SELECT &Person.* FROM person WHERE id = $Person.id", Person{})
	var jimCheck = Person{}
	err = db.Query(nil, selectStmt, &jim).One(&jimCheck)
	c.Assert(err, IsNil)
	c.Assert(jimCheck, Equals, jim)

	err = db.Query(nil, sqlair.MustPrepare(dropTables)).Run()
	c.Assert(err, IsNil)
}

func (s *PackageSuite) TestQueryMultipleRuns(c *C) {
	allOutput := &[]*Person{}
	allExpected := &[]*Person{&Person{30, "Fred", 1000}, &Person{20, "Mark", 1500}, &Person{40, "Mary", 3500}, &Person{35, "James", 4500}}

	iterOutputs := []any{&Person{}, &Person{}, &Person{}, &Person{}}
	iterExpected := []any{&Person{30, "Fred", 1000}, &Person{20, "Mark", 1500}, &Person{40, "Mary", 3500}, &Person{35, "James", 4500}}

	oneOutput := &Person{}
	oneExpected := &Person{30, "Fred", 1000}

	dropTables, sqldb, err := personAndAddressDB()
	c.Assert(err, IsNil)

	db := sqlair.NewDB(sqldb)
	stmt := sqlair.MustPrepare("SELECT &Person.* FROM person", Person{})

	// Run different Query methods.
	q := db.Query(nil, stmt)
	err = q.One(oneOutput)
	c.Assert(err, IsNil)
	c.Assert(oneExpected, DeepEquals, oneOutput)

	err = q.All(allOutput)
	c.Assert(err, IsNil)
	c.Assert(allOutput, DeepEquals, allExpected)

	err = q.Run()
	c.Assert(err, IsNil)

	iter := q.Iter()
	defer iter.Close()
	i := 0
	for iter.Next() {
		if i >= len(iterOutputs) {
			c.Fatalf("expected %d rows, got more", len(iterOutputs))
		}
		if err := iter.Decode(iterOutputs[i]); err != nil {
			c.Fatal(err)
		}
		i++
	}
	err = iter.Close()
	c.Assert(err, IsNil)
	c.Assert(iterOutputs, DeepEquals, iterExpected)

	// Run them all again for good measure.
	allOutput = &[]*Person{}
	iterOutputs = []any{&Person{}, &Person{}, &Person{}, &Person{}}
	oneOutput = &Person{}

	err = q.All(allOutput)
	c.Assert(err, IsNil)
	c.Assert(allOutput, DeepEquals, allExpected)

	iter = q.Iter()
	defer iter.Close()
	i = 0
	for iter.Next() {
		if i >= len(iterOutputs) {
			c.Fatalf("expected %d rows, got more", len(iterOutputs))
		}
		if err := iter.Decode(iterOutputs[i]); err != nil {
			c.Fatal(err)
		}
		i++
	}
	err = iter.Close()
	c.Assert(err, IsNil)
	c.Assert(iterOutputs, DeepEquals, iterExpected)

	q = db.Query(nil, stmt)
	err = q.One(oneOutput)
	c.Assert(err, IsNil)
	c.Assert(oneExpected, DeepEquals, oneOutput)

	err = q.Run()
	c.Assert(err, IsNil)

	err = db.Query(nil, sqlair.MustPrepare(dropTables)).Run()
	c.Assert(err, IsNil)
}

func (s *PackageSuite) TestTransactions(c *C) {
	dropTables, sqldb, err := personAndAddressDB()
	c.Assert(err, IsNil)

	selectStmt := sqlair.MustPrepare("SELECT &Person.* FROM person WHERE address_id = $Person.address_id", Person{})
	insertStmt := sqlair.MustPrepare("INSERT INTO person VALUES ( $Person.name, $Person.id, $Person.address_id, 'fred@email.com');", Person{})
	var derek = Person{ID: 85, Fullname: "Derek", PostalCode: 8000}
	ctx := context.Background()

	db := sqlair.NewDB(sqldb)
	tx, err := db.Begin(ctx, nil)
	c.Assert(err, IsNil)

	// Insert derek then rollback.
	err = tx.Query(ctx, insertStmt, &derek).Run()
	c.Assert(err, IsNil)
	err = tx.Rollback()
	c.Assert(err, IsNil)

	// Check derek isnt in db; insert derek; commit.
	tx, err = db.Begin(ctx, nil)
	c.Assert(err, IsNil)
	var derekCheck = Person{}
	err = tx.Query(ctx, selectStmt, &derek).One(&derekCheck)
	if !errors.Is(err, sqlair.ErrNoRows) {
		c.Fatalf("got err %s, expected %s", err, sqlair.ErrNoRows)
	}
	err = tx.Query(ctx, insertStmt, &derek).Run()
	c.Assert(err, IsNil)

	err = tx.Commit()
	c.Assert(err, IsNil)

	// Check derek is now in the db.
	tx, err = db.Begin(ctx, nil)
	c.Assert(err, IsNil)

	err = tx.Query(ctx, selectStmt, &derek).One(&derekCheck)
	c.Assert(err, IsNil)
	c.Assert(derek, Equals, derekCheck)
	err = tx.Commit()
	c.Assert(err, IsNil)

	err = db.Query(ctx, sqlair.MustPrepare(dropTables)).Run()
	c.Assert(err, IsNil)
}

func (s *PackageSuite) TestTransactionErrors(c *C) {
	dropTables, sqldb, err := personAndAddressDB()
	c.Assert(err, IsNil)

	insertStmt := sqlair.MustPrepare("INSERT INTO person VALUES ( $Person.name, $Person.id, $Person.address_id, 'fred@email.com');", Person{})
	var derek = Person{ID: 85, Fullname: "Derek", PostalCode: 8000}
	ctx := context.Background()

	// Test running query after commit.
	db := sqlair.NewDB(sqldb)
	tx, err := db.Begin(ctx, nil)
	c.Assert(err, IsNil)

	q := tx.Query(ctx, insertStmt, &derek)
	err = tx.Commit()
	c.Assert(err, IsNil)
	err = q.Run()
	c.Assert(err, ErrorMatches, "sql: transaction has already been committed or rolled back")

	// Test running query after rollback.
	tx, err = db.Begin(ctx, nil)
	c.Assert(err, IsNil)

	q = tx.Query(ctx, insertStmt, &derek)
	err = tx.Rollback()
	c.Assert(err, IsNil)
	err = q.Run()
	c.Assert(err, ErrorMatches, "sql: transaction has already been committed or rolled back")

	err = db.Query(ctx, sqlair.MustPrepare(dropTables)).Run()
	c.Assert(err, IsNil)
}

type JujuLeaseKey struct {
	Namespace string `db:"type"`
	ModelUUID string `db:"model_uuid"`
	Lease     string `db:"name"`
}

type JujuLeaseInfo struct {
	Holder string `db:"holder"`
	Expiry int    `db:"expiry"`
}

func JujuStoreLeaseDB() (string, *sql.DB, error) {
	createTables := `
CREATE TABLE lease (
	model_uuid text,
	name text,
	holder text,
	expiry integer,
	lease_type_id text
);
CREATE TABLE lease_type (
	id text,
	type text
);

`
	dropTables := `
DROP TABLE lease;
DROP TABLE lease_type;
`

	inserts := []string{
		"INSERT INTO lease VALUES ('uuid1', 'name1', 'holder1', 1, 'type_id1');",
		"INSERT INTO lease VALUES ('uuid2', 'name2', 'holder2', 4, 'type_id1');",
		"INSERT INTO lease VALUES ('uuid3', 'name3', 'holder3', 7, 'type_id2');",
		"INSERT INTO lease_type VALUES ('type_id1', 'type1');",
		"INSERT INTO lease_type VALUES ('type_id2', 'type2');",
	}

	db, err := createExampleDB(createTables, inserts)
	if err != nil {
		return "", nil, err
	}
	return dropTables, db, nil

}

func (s *PackageSuite) TestIterMethodOrder(c *C) {
	dropTables, sqldb, err := personAndAddressDB()
	if err != nil {
		c.Fatal(err)
	}

	db := sqlair.NewDB(sqldb)

	var p = Person{}
	stmt := sqlair.MustPrepare("SELECT &Person.* FROM person", Person{})

	// Check immidiate Decode.
	iter := db.Query(nil, stmt).Iter()
	err = iter.Decode(&p)
	c.Assert(err, ErrorMatches, "cannot decode result: sql: Scan called without calling Next")
	err = iter.Close()
	c.Assert(err, IsNil)

	// Check Next after closing.
	iter = db.Query(nil, stmt).Iter()
	err = iter.Close()
	c.Assert(err, IsNil)
	if iter.Next() {
		c.Fatal("expected false, got true")
	}
	err = iter.Close()
	c.Assert(err, IsNil)

	// Check Decode after closing.
	iter = db.Query(nil, stmt).Iter()
	err = iter.Close()
	c.Assert(err, IsNil)
	err = iter.Decode(&p)
	c.Assert(err, ErrorMatches, "cannot decode result: iteration ended or not started")
	err = iter.Close()
	c.Assert(err, IsNil)

	// Check multiple closes.
	iter = db.Query(nil, stmt).Iter()
	err = iter.Close()
	c.Assert(err, IsNil)
	err = iter.Close()
	c.Assert(err, IsNil)

	// Check SQL Scan error (scanning string into an int).
	badTypesStmt := sqlair.MustPrepare("SELECT name AS &Person.id FROM person", Person{})
	iter = db.Query(nil, badTypesStmt).Iter()
	if !iter.Next() {
		c.Fatal("expected true, got false")
	}
	err = iter.Decode(&p)
	c.Assert(err, ErrorMatches, `cannot decode result: sql: Scan error on column index 0, name "_sqlair_0": converting driver.Value type string \("Fred"\) to a int: invalid syntax`)
	err = iter.Close()
	c.Assert(err, IsNil)

	_, err = db.PlainDB().Exec(dropTables)
	c.Assert(err, IsNil)
}

func (s *PackageSuite) TestJujuStore(c *C) {
	var tests = []struct {
		summary  string
		query    string
		types    []any
		inputs   []any
		outputs  [][]any
		expected [][]any
	}{{
		summary: "juju store lease group query",
		query: `
SELECT (t.type, l.model_uuid, l.name) AS &JujuLeaseKey.*, (l.holder, l.expiry) AS &JujuLeaseInfo.*
FROM   lease l JOIN lease_type t ON l.lease_type_id = t.id
WHERE  t.type = $JujuLeaseKey.type
AND    l.model_uuid = $JujuLeaseKey.model_uuid`,
		types:    []any{JujuLeaseKey{}, JujuLeaseInfo{}},
		inputs:   []any{JujuLeaseKey{Namespace: "type1", ModelUUID: "uuid1"}},
		outputs:  [][]any{{&JujuLeaseKey{}, &JujuLeaseInfo{}}},
		expected: [][]any{{&JujuLeaseKey{Namespace: "type1", ModelUUID: "uuid1", Lease: "name1"}, &JujuLeaseInfo{Holder: "holder1", Expiry: 1}}},
	}}

	dropTables, sqldb, err := JujuStoreLeaseDB()
	c.Assert(err, IsNil)

	db := sqlair.NewDB(sqldb)

	for _, t := range tests {

		stmt, err := sqlair.Prepare(t.query, t.types...)
		if err != nil {
			c.Errorf("\ntest %q failed (Prepare):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}

		iter := db.Query(nil, stmt, t.inputs...).Iter()
		defer iter.Close()
		i := 0
		for iter.Next() {
			if i >= len(t.outputs) {
				c.Errorf("\ntest %q failed (Next):\ninput: %s\nerr: more rows that expected (%d > %d)\n", t.summary, t.query, i+1, len(t.outputs))
				break
			}
			if err := iter.Decode(t.outputs[i]...); err != nil {
				c.Errorf("\ntest %q failed (Decode):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			}
			i++
		}

		err = iter.Close()
		if err != nil {
			c.Errorf("\ntest %q failed (Close):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
		}
	}

	err = db.Query(nil, sqlair.MustPrepare(dropTables)).Run()
	c.Assert(err, IsNil)
}
