package sqlair_test

import (
	"context"
	"database/sql"
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

func (s *PackageSuite) TestValidIterGet(c *C) {
	type CustomMap map[string]any
	type StringMap map[string]string
	type lowerCaseMap map[string]any
	type M struct {
		F string `db:"id"`
	}
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
	}, {
		summary:  "select into map",
		query:    "SELECT &M.name FROM person WHERE address_id = $M.p1 OR address_id = $M.p2",
		types:    []any{sqlair.M{}},
		inputs:   []any{sqlair.M{"p1": 1000, "p2": 1500}},
		outputs:  [][]any{{sqlair.M{}}, {sqlair.M{}}},
		expected: [][]any{{sqlair.M{"name": "Fred"}}, {sqlair.M{"name": "Mark"}}},
	}, {
		summary:  "select into star map",
		query:    "SELECT (name, address_id) AS &M.* FROM person WHERE address_id = $M.p1",
		types:    []any{sqlair.M{}},
		inputs:   []any{sqlair.M{"p1": 1000}},
		outputs:  [][]any{{&sqlair.M{"address_id": 0}}},
		expected: [][]any{{&sqlair.M{"name": "Fred", "address_id": int64(1000)}}},
	}, {
		summary:  "select into custom map",
		query:    "SELECT (name, address_id) AS &CustomMap.* FROM person WHERE address_id IN ( $CustomMap.address_id, $CustomMap.district)",
		types:    []any{CustomMap{}},
		inputs:   []any{CustomMap{"address_id": 1000, "district": 2000}},
		outputs:  [][]any{{&CustomMap{"address_id": 0}}},
		expected: [][]any{{&CustomMap{"name": "Fred", "address_id": int64(1000)}}},
	}, {
		summary:  "multiple maps",
		query:    "SELECT name AS &StringMap.*, id AS &CustomMap.* FROM person WHERE address_id = $M.address_id AND id = $StringMap.id",
		types:    []any{StringMap{}, sqlair.M{}, CustomMap{}},
		inputs:   []any{sqlair.M{"address_id": "1000"}, &StringMap{"id": "30"}},
		outputs:  [][]any{{&StringMap{}, CustomMap{}}},
		expected: [][]any{{&StringMap{"name": "Fred"}, CustomMap{"id": int64(30)}}},
	}, {
		summary:  "lower case map",
		query:    "SELECT name AS &lowerCaseMap.*, id AS &lowerCaseMap.* FROM person WHERE address_id = $lowerCaseMap.address_id",
		types:    []any{lowerCaseMap{}},
		inputs:   []any{lowerCaseMap{"address_id": "1000"}},
		outputs:  [][]any{{&lowerCaseMap{}}},
		expected: [][]any{{&lowerCaseMap{"name": "Fred", "id": int64(30)}}},
	}, {
		summary:  "insert",
		query:    "INSERT INTO address VALUES ($Address.id, $Address.district, $Address.street);",
		types:    []any{Address{}},
		inputs:   []any{Address{8000, "Crazy Town", "Willow Wong"}},
		outputs:  [][]any{},
		expected: [][]any{},
	}, {
		summary:  "update",
		query:    "UPDATE address SET id=$Address.id WHERE id=8000",
		types:    []any{Address{}},
		inputs:   []any{Address{ID: 1000}},
		outputs:  [][]any{},
		expected: [][]any{},
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
		c.Assert(err, IsNil,
			Commentf("\ntest %q failed (Prepare):\ninput: %s\n", t.summary, t.query))

		iter := db.Query(nil, stmt, t.inputs...).Iter()
		defer iter.Close()
		i := 0
		for iter.Next() {
			if i >= len(t.outputs) {
				c.Errorf("\ntest %q failed (Next):\ninput: %s\nerr: more rows that expected (%d > %d)\n", t.summary, t.query, i+1, len(t.outputs))
				break
			}
			if err := iter.Get(t.outputs[i]...); err != nil {
				c.Errorf("\ntest %q failed (Get):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			}
			i++
		}

		c.Assert(iter.Close(), IsNil,
			Commentf("\ntest %q failed (Close):\ninput: %s\n", t.summary, t.query))
		for i, row := range t.expected {
			for j, col := range row {
				c.Assert(t.outputs[i][j], DeepEquals, col,
					Commentf("\ntest %q failed:\ninput: %s\nrow: %d\n", t.summary, t.query, i))
			}
		}
	}
	c.Assert(db.Query(nil, sqlair.MustPrepare(dropTables)).Run(), IsNil)
}

func (s *PackageSuite) TestIterGetErrors(c *C) {
	type SliceMap map[string][]string
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
		err:     "cannot get result: need map or pointer to struct, got nil",
	}, {
		summary: "nil pointer parameter",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		outputs: [][]any{{(*Person)(nil)}},
		err:     "cannot get result: got nil pointer",
	}, {
		summary: "non pointer parameter",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		outputs: [][]any{{Person{}}},
		err:     "cannot get result: need map or pointer to struct, got struct",
	}, {
		summary: "wrong struct",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		outputs: [][]any{{&Address{}}},
		err:     `cannot get result: type "Address" does not appear in query, have: Person`,
	}, {
		summary: "not a struct",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		outputs: [][]any{{&[]any{}}},
		err:     "cannot get result: need map or pointer to struct, got pointer to slice",
	}, {
		summary: "missing get value",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		outputs: [][]any{{}},
		err:     `cannot get result: type "Person" found in query but not passed to get`,
	}, {
		summary: "multiple of the same type",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		outputs: [][]any{{&Person{}, &Person{}}},
		err:     `cannot get result: type "Person" provided more than once`,
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
		err:     "cannot get result: column for Person.id not found in results",
	}, {
		summary: "multiple of the same type",
		query:   "SELECT name AS &M.* FROM person",
		types:   []any{sqlair.M{}},
		inputs:  []any{},
		outputs: [][]any{{&sqlair.M{}, sqlair.M{}}},
		err:     `cannot get result: type "M" provided more than once`,
	}}

	dropTables, sqldb, err := personAndAddressDB()
	c.Assert(err, IsNil)

	db := sqlair.NewDB(sqldb)

	for _, t := range tests {
		stmt, err := sqlair.Prepare(t.query, t.types...)
		c.Assert(err, IsNil,
			Commentf("\ntest %q failed (Prepare):\ninput: %s\n", t.summary, t.query))

		iter := db.Query(nil, stmt, t.inputs...).Iter()
		defer iter.Close()
		i := 0
		for iter.Next() {
			if i >= len(t.outputs) {
				c.Errorf("\ntest %q failed (Next):\ninput: %s\nerr: more rows that expected (%d > %d)\n", t.summary, t.query, i+1, len(t.outputs))
				break
			}
			if err := iter.Get(t.outputs[i]...); err != nil {
				c.Assert(err, ErrorMatches, t.err,
					Commentf("\ntest %q failed:\ninput: %s\noutputs: %s", t.summary, t.query, t.outputs))
				iter.Close()
				break
			}
			i++
		}
		c.Assert(iter.Close(), IsNil,
			Commentf("\ntest %q failed (Close):\ninput: %s\n", t.summary, t.query))
	}
	c.Assert(db.Query(nil, sqlair.MustPrepare(dropTables)).Run(), IsNil)
}

func (s *PackageSuite) TestValidGet(c *C) {
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
		c.Assert(err, IsNil,
			Commentf("\ntest %q failed (Prepare):\ninput: %s\n", t.summary, t.query))

		q := db.Query(nil, stmt, t.inputs...)
		c.Assert(q.Get(t.outputs...), IsNil, Commentf("\ntest %q failed (Get):\ninput: %s\n", t.summary, t.query))
		for i, s := range t.expected {
			c.Assert(t.outputs[i], DeepEquals, s,
				Commentf("\ntest %q failed:\ninput: %s", t.summary, t.query))
		}
	}

	c.Assert(db.Query(nil, sqlair.MustPrepare(dropTables)).Run(), IsNil)
}

func (s *PackageSuite) TestGetErrors(c *C) {
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
		err:     `invalid input parameter: type "Person" not found`,
	}, {
		summary: "no outputs",
		query:   "UPDATE person SET id=300 WHERE id=30",
		types:   []any{Person{}},
		inputs:  []any{},
		outputs: []any{&Person{}},
		err:     "cannot get results: output variables provided but not referenced in query",
	}}

	dropTables, sqldb, err := personAndAddressDB()
	c.Assert(err, IsNil)

	db := sqlair.NewDB(sqldb)

	for _, t := range tests {
		stmt, err := sqlair.Prepare(t.query, t.types...)
		c.Assert(err, IsNil,
			Commentf("\ntest %q failed (Prepare):\ninput: %s\n", t.summary, t.query))
		c.Assert(db.Query(nil, stmt, t.inputs...).Get(t.outputs...), ErrorMatches, t.err,
			Commentf("\ntest %q failed:\ninput: %s\noutputs: %s", t.summary, t.query, t.outputs))
	}

	c.Assert(db.Query(nil, sqlair.MustPrepare(dropTables)).Run(), IsNil)
}

func (s *PackageSuite) TestErrNoRows(c *C) {
	dropTables, sqldb, err := personAndAddressDB()
	c.Assert(err, IsNil)

	db := sqlair.NewDB(sqldb)
	stmt := sqlair.MustPrepare("SELECT * AS &Person.* FROM person WHERE id=12312", Person{})
	err = db.Query(nil, stmt).Get(&Person{})
	c.Assert(err, Equals, sqlair.ErrNoRows)
	c.Assert(err, Equals, sql.ErrNoRows)
	c.Assert(db.Query(nil, sqlair.MustPrepare(dropTables)).Run(), IsNil)
}

func (s *PackageSuite) TestValidGetAll(c *C) {
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
		expected: []any{&[]*Person{{ID: 30}, {ID: 30}, {ID: 30}, {ID: 20}, {ID: 20}, {ID: 20}, {ID: 40}, {ID: 40}, {ID: 40}, {ID: 35}, {ID: 35}, {ID: 35}}, &[]*Address{{ID: 1000}, {ID: 1500}, {ID: 3500}, {ID: 1000}, {ID: 1500}, {ID: 3500}, {ID: 1000}, {ID: 1500}, {ID: 3500}, {ID: 1000}, {ID: 1500}, {ID: 3500}}},
	}, {
		summary:  "select all columns into person",
		query:    "SELECT * AS &Person.* FROM person",
		types:    []any{Person{}},
		inputs:   []any{},
		slices:   []any{&[]*Person{}},
		expected: []any{&[]*Person{{30, "Fred", 1000}, {20, "Mark", 1500}, {40, "Mary", 3500}, {35, "James", 4500}}},
	}, {
		summary:  "select all columns into person with no pointers",
		query:    "SELECT * AS &Person.* FROM person",
		types:    []any{Person{}},
		inputs:   []any{},
		slices:   []any{&[]Person{}},
		expected: []any{&[]Person{{30, "Fred", 1000}, {20, "Mark", 1500}, {40, "Mary", 3500}, {35, "James", 4500}}},
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
		c.Assert(err, IsNil, Commentf("\ntest %q failed (Prepare):\ninput: %s\n", t.summary, t.query))

		q := db.Query(nil, stmt, t.inputs...)
		c.Assert(q.GetAll(t.slices...), IsNil,
			Commentf("\ntest %q failed (All):\ninput: %s\n", t.summary, t.query))
		for i, column := range t.expected {
			c.Assert(t.slices[i], DeepEquals, column,
				Commentf("\ntest %q failed:\ninput: %s", t.summary, t.query))
		}
	}

	c.Assert(db.Query(nil, sqlair.MustPrepare(dropTables)).Run(), IsNil)
}

func (s *PackageSuite) TestGetAllErrors(c *C) {
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
		err:     "cannot get all results: need pointer to slice, got invalid",
	}, {
		summary: "nil pointer argument",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		slices:  []any{(*[]Person)(nil)},
		err:     "cannot get all results: need pointer to slice, got nil",
	}, {
		summary: "none slice argument",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		slices:  []any{Person{}},
		err:     "cannot get all results: need pointer to slice, got struct",
	}, {
		summary: "none slice pointer argument",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		slices:  []any{&Person{}},
		err:     "cannot get all results: need pointer to slice, got pointer to struct",
	}, {
		summary: "wrong struct argument",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		slices:  []any{&[]*Address{}},
		err:     `cannot get all results: cannot get result: type "Address" does not appear in query, have: Person`,
	}, {
		summary: "wrong struct argument",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		slices:  []any{&[]int{}},
		err:     `cannot get all results: need slice of struct, got slice of int`,
	}, {
		summary: "wrong struct argument",
		query:   "SELECT name FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		slices:  []any{&[]Person{}},
		err:     `cannot get all results: output slices provided but not referenced in query`,
	}}

	dropTables, sqldb, err := personAndAddressDB()
	c.Assert(err, IsNil)

	db := sqlair.NewDB(sqldb)

	for _, t := range tests {
		stmt, err := sqlair.Prepare(t.query, t.types...)
		c.Assert(err, IsNil, Commentf("\ntest %q failed (Prepare):\ninput: %s\n", t.summary, t.query))
		c.Assert(db.Query(nil, stmt, t.inputs...).GetAll(t.slices...), ErrorMatches, t.err,
			Commentf("\ntest %q failed:\ninput: %s\nslices: %s", t.summary, t.query, t.slices))
	}

	c.Assert(db.Query(nil, sqlair.MustPrepare(dropTables)).Run(), IsNil)
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

	// Insert Jim.
	insertStmt := sqlair.MustPrepare("INSERT INTO person VALUES ($Person.name, $Person.id, $Person.address_id, 'jimmy@email.com');", Person{})
	c.Assert(db.Query(nil, insertStmt, &jim).Run(), IsNil)

	// Check Jim is in the db.
	selectStmt := sqlair.MustPrepare("SELECT &Person.* FROM person WHERE id = $Person.id", Person{})
	var jimCheck = Person{}
	c.Assert(db.Query(nil, selectStmt, &jim).Get(&jimCheck), IsNil)
	c.Assert(jimCheck, Equals, jim)

	c.Assert(db.Query(nil, sqlair.MustPrepare(dropTables)).Run(), IsNil)
}

func (s *PackageSuite) TestOutcome(c *C) {
	dropTables, sqldb, err := personAndAddressDB()
	c.Assert(err, IsNil)

	var jim = Person{
		ID:         70,
		Fullname:   "Jim",
		PostalCode: 500,
	}

	db := sqlair.NewDB(sqldb)

	var outcome = sqlair.Outcome{}

	insertStmt := sqlair.MustPrepare("INSERT INTO person VALUES ($Person.name, $Person.id, $Person.address_id, 'jimmy@email.com');", Person{})
	q1 := db.Query(nil, insertStmt, &jim)
	// Test INSERT with Get
	c.Assert(q1.Get(&outcome), IsNil)
	if outcome.Result() == nil {
		c.Errorf("result in outcome is nil")
	}
	rowsAffected, err := outcome.Result().RowsAffected()
	c.Assert(err, IsNil)
	if rowsAffected != 1 {
		c.Errorf("got %d for rowsAffected, expected 1", rowsAffected)
	}
	// Test SELECT with Get
	selectStmt := sqlair.MustPrepare("SELECT &Person.* FROM person", Person{})
	q2 := db.Query(nil, selectStmt)
	c.Assert(q2.Get(&outcome, &jim), IsNil)
	c.Assert(outcome.Result(), IsNil)
	// Test INSERT with Iter
	iter := q1.Iter()
	c.Assert(iter.Get(&outcome), IsNil)
	if outcome.Result() == nil {
		c.Errorf("result in outcome is nil")
	}
	rowsAffected, err = outcome.Result().RowsAffected()
	c.Assert(err, IsNil)
	if rowsAffected != 1 {
		c.Errorf("got %d for rowsAffected, expected 1", rowsAffected)
	}
	c.Assert(iter.Next(), Equals, false)
	// Test SELECT with Iter.Get
	iter = q2.Iter()
	c.Assert(iter.Get(&outcome), IsNil)
	c.Assert(outcome.Result(), IsNil)
	c.Assert(iter.Next(), Equals, true)
	c.Assert(iter.Get(&jim), IsNil)
	c.Assert(iter.Close(), IsNil)
	// Test SELECT with GetAll
	var jims = []Person{}
	err = q2.GetAll(&outcome, &jims)
	c.Assert(err, IsNil)
	c.Assert(outcome.Result(), IsNil)

	err = db.Query(nil, sqlair.MustPrepare(dropTables)).Run()
	c.Assert(err, IsNil)
}

func (s *PackageSuite) TestQueryMultipleRuns(c *C) {
	// Note: Query structs are not designed to be reused (hence why they store a context as a struct field).
	//       It is, however, possible.
	allOutput := &[]*Person{}
	allExpected := &[]*Person{{30, "Fred", 1000}, {20, "Mark", 1500}, {40, "Mary", 3500}, {35, "James", 4500}}

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
	c.Assert(q.Get(oneOutput), IsNil)
	c.Assert(oneExpected, DeepEquals, oneOutput)

	c.Assert(q.GetAll(allOutput), IsNil)
	c.Assert(allOutput, DeepEquals, allExpected)

	iter := q.Iter()
	defer iter.Close()
	i := 0
	for iter.Next() {
		if i >= len(iterOutputs) {
			c.Fatalf("expected %d rows, got more", len(iterOutputs))
		}
		c.Assert(iter.Get(iterOutputs[i]), IsNil)
		i++
	}
	c.Assert(iter.Close(), IsNil)
	c.Assert(iterOutputs, DeepEquals, iterExpected)

	c.Assert(db.Query(nil, sqlair.MustPrepare(dropTables)).Run(), IsNil)
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
	c.Assert(tx.Query(ctx, insertStmt, &derek).Run(), IsNil)
	c.Assert(tx.Rollback(), IsNil)

	// Check derek isnt in db; insert derek; commit.
	tx, err = db.Begin(ctx, nil)
	c.Assert(err, IsNil)
	var derekCheck = Person{}
	c.Assert(tx.Query(ctx, selectStmt, &derek).Get(&derekCheck), Equals, sqlair.ErrNoRows)
	c.Assert(tx.Query(ctx, insertStmt, &derek).Run(), IsNil)
	c.Assert(tx.Commit(), IsNil)

	// Check derek is now in the db.
	tx, err = db.Begin(ctx, nil)
	c.Assert(err, IsNil)

	c.Assert(tx.Query(ctx, selectStmt, &derek).Get(&derekCheck), IsNil)
	c.Assert(derek, Equals, derekCheck)
	c.Assert(tx.Commit(), IsNil)

	c.Assert(db.Query(ctx, sqlair.MustPrepare(dropTables)).Run(), IsNil)
}

func (s *PackageSuite) TestTransactionErrors(c *C) {
	dropTables, sqldb, err := personAndAddressDB()
	c.Assert(err, IsNil)

	insertStmt := sqlair.MustPrepare("INSERT INTO person VALUES ($Person.name, $Person.id, $Person.address_id, 'fred@email.com');", Person{})
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

func (s *PackageSuite) TestIterMethodOrder(c *C) {
	dropTables, sqldb, err := personAndAddressDB()
	c.Assert(err, IsNil)

	db := sqlair.NewDB(sqldb)

	var p = Person{}
	stmt := sqlair.MustPrepare("SELECT &Person.* FROM person", Person{})

	// Check immidiate Get.
	iter := db.Query(nil, stmt).Iter()
	c.Assert(iter.Get(&p), ErrorMatches, "cannot get result: cannot call Get before Next unless getting outcome")
	c.Assert(iter.Close(), IsNil)

	// Check Next after closing.
	iter = db.Query(nil, stmt).Iter()
	c.Assert(iter.Close(), IsNil)
	c.Assert(iter.Next(), Equals, false)
	c.Assert(iter.Close(), IsNil)

	// Check Get after closing.
	iter = db.Query(nil, stmt).Iter()
	c.Assert(iter.Close(), IsNil)
	c.Assert(iter.Get(&p), ErrorMatches, "cannot get result: iteration ended")
	c.Assert(iter.Close(), IsNil)

	// Check multiple closes.
	iter = db.Query(nil, stmt).Iter()
	c.Assert(iter.Close(), IsNil)
	c.Assert(iter.Close(), IsNil)

	// Check SQL Scan error (scanning string into an int).
	badTypesStmt := sqlair.MustPrepare("SELECT name AS &Person.id FROM person", Person{})
	iter = db.Query(nil, badTypesStmt).Iter()
	c.Assert(iter.Next(), Equals, true)
	c.Assert(iter.Get(&p), ErrorMatches, `cannot get result: sql: Scan error on column index 0, name "_sqlair_0": converting driver.Value type string \("Fred"\) to a int: invalid syntax`)
	c.Assert(iter.Close(), IsNil)

	_, err = db.PlainDB().Exec(dropTables)
	c.Assert(err, IsNil)
}
