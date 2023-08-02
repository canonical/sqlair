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

func testDB() (*sqlair.DB, error) {
	sqldb, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, err
	}
	db := sqlair.NewDB(sqldb)
	return db, nil
}

type Address struct {
	ID       int    `db:"id"`
	District string `db:"district"`
	Street   string `db:"street"`
}

type Person struct {
	ID       int    `db:"id"`
	Name     string `db:"name"`
	Postcode int    `db:"address_id"`
}

type Manager Person

type District struct{}

type CustomMap map[string]any

var fred = Person{Name: "Fred", ID: 30, Postcode: 1000}
var mark = Person{Name: "Mark", ID: 20, Postcode: 1500}
var mary = Person{Name: "Mary", ID: 40, Postcode: 3500}
var dave = Person{Name: "Dave", ID: 35, Postcode: 4500}
var allPeople = []Person{fred, mark, mary, dave}

var mainStreet = Address{Street: "Main Street", District: "Happy Land", ID: 1000}
var churchRoad = Address{Street: "Church Road", District: "Sad World", ID: 1500}
var stationLane = Address{Street: "Station Lane", District: "Ambivalent Commons", ID: 3500}
var allAddresses = []Address{mainStreet, churchRoad, stationLane}

func personAndAddressDB() (string, *sqlair.DB, error) {
	db, err := testDB()
	if err != nil {
		return "", nil, err
	}

	createPerson, err := sqlair.Prepare(`
		CREATE TABLE person (
			name text,
			id integer,
			address_id integer,
			email text
		);
	`)
	if err != nil {
		return "", nil, err
	}
	createAddress, err := sqlair.Prepare(`
		CREATE TABLE address (
			id integer,
			district text,
			street text
		);
	`)
	if err != nil {
		return "", nil, err
	}

	err = db.Query(nil, createPerson).Run()
	if err != nil {
		return "", nil, err
	}
	err = db.Query(nil, createAddress).Run()
	if err != nil {
		return "", nil, err
	}

	insertPerson, err := sqlair.Prepare("INSERT INTO person (*) VALUES ($Person.*)", Person{})
	for _, person := range allPeople {
		err := db.Query(nil, insertPerson, person).Run()
		if err != nil {
			return "", nil, err
		}
	}

	insertAddress, err := sqlair.Prepare("INSERT INTO address (*) VALUES ($Address.*)", Address{})
	for _, address := range allAddresses {
		err := db.Query(nil, insertAddress, address).Run()
		if err != nil {
			return "", nil, err
		}
	}

	dropTables := `
		DROP TABLE person;
		DROP TABLE address;
	`
	return dropTables, db, nil
}

func (s *PackageSuite) TestValidIterGet(c *C) {
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
		expected: [][]any{{&Person{30, "Fred", 1000}}, {&Person{20, "Mark", 1500}}, {&Person{40, "Mary", 3500}}, {&Person{35, "Dave", 4500}}},
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
		query:    "SELECT (name, address_id) AS (&Person.*) FROM person WHERE address_id IN ( $Manager.address_id, $Address.district )",
		types:    []any{Person{}, Address{}, Manager{}},
		inputs:   []any{Manager{Postcode: 1000}, Address{ID: 2000}},
		outputs:  [][]any{{&Person{}}},
		expected: [][]any{{&Person{Name: "Fred", Postcode: 1000}}},
	}, {
		summary:  "select into map",
		query:    "SELECT &M.name FROM person WHERE address_id = $M.p1 OR address_id = $M.p2",
		types:    []any{sqlair.M{}},
		inputs:   []any{sqlair.M{"p1": 1000, "p2": 1500}},
		outputs:  [][]any{{sqlair.M{}}, {sqlair.M{}}},
		expected: [][]any{{sqlair.M{"name": "Fred"}}, {sqlair.M{"name": "Mark"}}},
	}, {
		summary:  "select into star map",
		query:    "SELECT (name, address_id) AS (&M.*) FROM person WHERE address_id = $M.p1",
		types:    []any{sqlair.M{}},
		inputs:   []any{sqlair.M{"p1": 1000}},
		outputs:  [][]any{{&sqlair.M{"address_id": 0}}},
		expected: [][]any{{&sqlair.M{"name": "Fred", "address_id": int64(1000)}}},
	}, {
		summary:  "select into custom map",
		query:    "SELECT (name, address_id) AS (&CustomMap.*) FROM person WHERE address_id IN ( $CustomMap.address_id, $CustomMap.district)",
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
		ID       int32  `db:"id"`
		Name     string `db:"name"`
		Postcode int32  `db:"address_id"`
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
		expected: [][]any{{&Person{30, "Fred", 1000}}, {&Person{20, "Mark", 1500}}, {&Person{40, "Mary", 3500}}, {&Person{35, "Dave", 4500}}},
	}}

	tests = append(tests, testsWithShadowPerson...)

	dropTables, db, err := personAndAddressDB()
	if err != nil {
		c.Fatal(err)
	}

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
			if err := iter.Get(t.outputs[i]...); err != nil {
				c.Errorf("\ntest %q failed (Get):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
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
	if err != nil {
		c.Fatal(err)
	}
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
		err:     `cannot get result: type "Person" provided more than once, rename one of them`,
	}, {
		summary: "multiple of the same type",
		query:   "SELECT name AS &M.* FROM person",
		types:   []any{sqlair.M{}},
		inputs:  []any{},
		outputs: [][]any{{&sqlair.M{}, sqlair.M{}}},
		err:     `cannot get result: type "M" provided more than once, rename one of them`,
	}}

	dropTables, db, err := personAndAddressDB()
	if err != nil {
		c.Fatal(err)
	}

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
			if err := iter.Get(t.outputs[i]...); err != nil {
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
	if err != nil {
		c.Fatal(err)
	}
}

type ScannerInt struct {
	SI int
}

func (si *ScannerInt) Scan(v any) error {
	if _, ok := v.(int); ok {
		si.SI = 42
	} else {
		si.SI = 666
	}
	return nil
}

type ScannerString struct {
	SS string
}

func (ss *ScannerString) Scan(v any) error {
	if _, ok := v.(string); ok {
		ss.SS = "ScannerString scanned well!"
	} else {
		ss.SS = "ScannerString found a NULL"
	}
	return nil
}

func (s *PackageSuite) TestNulls(c *C) {
	type I int
	type J = int
	type S = string
	type PersonWithStrangeTypes struct {
		ID       I `db:"id"`
		Name     S `db:"name"`
		Postcode J `db:"address_id"`
	}
	type NullGuy struct {
		ID       sql.NullInt64  `db:"id"`
		Name     sql.NullString `db:"name"`
		Postcode sql.NullInt64  `db:"address_id"`
	}
	type ScannerDude struct {
		ID       ScannerInt    `db:"id"`
		Name     ScannerString `db:"name"`
		Postcode ScannerInt    `db:"address_id"`
	}

	var tests = []struct {
		summary  string
		query    string
		types    []any
		inputs   []any
		outputs  []any
		expected []any
	}{{
		summary:  "reading nulls",
		query:    `SELECT &Person.* FROM person WHERE name = "Nully"`,
		types:    []any{Person{}},
		inputs:   []any{},
		outputs:  []any{&Person{ID: 5, Postcode: 10}},
		expected: []any{&Person{Name: "Nully", ID: 0, Postcode: 0}},
	}, {
		summary:  "reading nulls with custom types",
		query:    `SELECT &PersonWithStrangeTypes.* FROM person WHERE name = "Nully"`,
		types:    []any{PersonWithStrangeTypes{}},
		inputs:   []any{},
		outputs:  []any{&PersonWithStrangeTypes{ID: 5, Postcode: 10}},
		expected: []any{&PersonWithStrangeTypes{Name: "Nully", ID: 0, Postcode: 0}},
	}, {
		summary:  "regular nulls",
		query:    `SELECT &NullGuy.* FROM person WHERE name = "Nully"`,
		types:    []any{NullGuy{}},
		inputs:   []any{},
		outputs:  []any{&NullGuy{}},
		expected: []any{&NullGuy{Name: sql.NullString{Valid: true, String: "Nully"}, ID: sql.NullInt64{Valid: false}, Postcode: sql.NullInt64{Valid: false}}},
	}, {
		summary:  "nulls with custom scan type",
		query:    `SELECT &ScannerDude.* FROM person WHERE name = "Nully"`,
		types:    []any{ScannerDude{}},
		inputs:   []any{},
		outputs:  []any{&ScannerDude{}},
		expected: []any{&ScannerDude{Name: ScannerString{SS: "ScannerString scanned well!"}, ID: ScannerInt{SI: 666}, Postcode: ScannerInt{SI: 666}}},
	}}

	dropTables, db, err := personAndAddressDB()
	if err != nil {
		c.Fatal(err)
	}

	insertNullPerson, err := sqlair.Prepare("INSERT INTO person VALUES ('Nully', NULL, NULL, NULL);")
	c.Assert(err, IsNil)
	c.Assert(db.Query(nil, insertNullPerson).Run(), IsNil)

	for _, t := range tests {
		stmt, err := sqlair.Prepare(t.query, t.types...)
		if err != nil {
			c.Errorf("\ntest %q failed (prepare):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}

		q := db.Query(nil, stmt, t.inputs...)
		err = q.Get(t.outputs...)
		if err != nil {
			c.Errorf("\ntest %q failed (Get):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}
		for i, s := range t.expected {
			c.Assert(t.outputs[i], DeepEquals, s,
				Commentf("\ntest %q failed:\ninput: %s", t.summary, t.query))
		}
	}

	err = db.Query(nil, sqlair.MustPrepare(dropTables)).Run()
	if err != nil {
		c.Fatal(err)
	}
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
	}, {
		summary:  "select into map",
		query:    "SELECT &M.name FROM person WHERE address_id = $M.p1",
		types:    []any{sqlair.M{}},
		inputs:   []any{sqlair.M{"p1": 1000}},
		outputs:  []any{sqlair.M{}},
		expected: []any{sqlair.M{"name": "Fred"}},
	}}

	dropTables, db, err := personAndAddressDB()
	if err != nil {
		c.Fatal(err)
	}

	for _, t := range tests {
		stmt, err := sqlair.Prepare(t.query, t.types...)
		if err != nil {
			c.Errorf("\ntest %q failed (Prepare):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}

		q := db.Query(nil, stmt, t.inputs...)
		err = q.Get(t.outputs...)
		if err != nil {
			c.Errorf("\ntest %q failed (Get):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}
		for i, s := range t.expected {
			c.Assert(t.outputs[i], DeepEquals, s,
				Commentf("\ntest %q failed:\ninput: %s", t.summary, t.query))
		}
	}

	err = db.Query(nil, sqlair.MustPrepare(dropTables)).Run()
	if err != nil {
		c.Fatal(err)
	}
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
		summary: "no outputs",
		query:   "UPDATE person SET id=300 WHERE id=30",
		types:   []any{Person{}},
		inputs:  []any{},
		outputs: []any{&Person{}},
		err:     "cannot get results: output variables provided but not referenced in query",
	}, {
		summary: "key not in map",
		query:   "SELECT &M.name FROM person WHERE address_id = $M.p1",
		types:   []any{sqlair.M{}},
		inputs:  []any{sqlair.M{}},
		outputs: []any{sqlair.M{}},
		err:     `invalid input parameter: map "M" does not contain key "p1"`,
	}}

	dropTables, db, err := personAndAddressDB()
	if err != nil {
		c.Fatal(err)
	}

	for _, t := range tests {
		stmt, err := sqlair.Prepare(t.query, t.types...)
		if err != nil {
			c.Errorf("\ntest %q failed (Prepare):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}

		err = db.Query(nil, stmt, t.inputs...).Get(t.outputs...)
		c.Assert(err, ErrorMatches, t.err,
			Commentf("\ntest %q failed:\ninput: %s\noutputs: %s", t.summary, t.query, t.outputs))
	}

	err = db.Query(nil, sqlair.MustPrepare(dropTables)).Run()
	if err != nil {
		c.Fatal(err)
	}
}

func (s *PackageSuite) TestErrNoRows(c *C) {
	dropTables, db, err := personAndAddressDB()
	if err != nil {
		c.Fatal(err)
	}

	stmt := sqlair.MustPrepare("SELECT * AS &Person.* FROM person WHERE id=12312", Person{})
	err = db.Query(nil, stmt).Get(&Person{})
	if !errors.Is(err, sqlair.ErrNoRows) {
		c.Errorf("expected %q, got %q", sqlair.ErrNoRows, err)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		c.Errorf("expected %q, got %q", sql.ErrNoRows, err)
	}

	err = db.Query(nil, sqlair.MustPrepare(dropTables)).Run()
	if err != nil {
		c.Fatal(err)
	}
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
		expected: []any{&[]*Person{&Person{ID: 30}, &Person{ID: 30}, &Person{ID: 30}, &Person{ID: 20}, &Person{ID: 20}, &Person{ID: 20}, &Person{ID: 40}, &Person{ID: 40}, &Person{ID: 40}, &Person{ID: 35}, &Person{ID: 35}, &Person{ID: 35}}, &[]*Address{&Address{ID: 1000}, &Address{ID: 1500}, &Address{ID: 3500}, &Address{ID: 1000}, &Address{ID: 1500}, &Address{ID: 3500}, &Address{ID: 1000}, &Address{ID: 1500}, &Address{ID: 3500}, &Address{ID: 1000}, &Address{ID: 1500}, &Address{ID: 3500}}},
	}, {
		summary:  "select all columns into person",
		query:    "SELECT * AS &Person.* FROM person",
		types:    []any{Person{}},
		inputs:   []any{},
		slices:   []any{&[]*Person{}},
		expected: []any{&[]*Person{&Person{30, "Fred", 1000}, &Person{20, "Mark", 1500}, &Person{40, "Mary", 3500}, &Person{35, "Dave", 4500}}},
	}, {
		summary:  "select all columns into person with no pointers",
		query:    "SELECT * AS &Person.* FROM person",
		types:    []any{Person{}},
		inputs:   []any{},
		slices:   []any{&[]Person{}},
		expected: []any{&[]Person{Person{30, "Fred", 1000}, Person{20, "Mark", 1500}, Person{40, "Mary", 3500}, Person{35, "Dave", 4500}}},
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
		expected: []any{&[]*Person{}},
	}, {
		summary:  "select into maps",
		query:    "SELECT &M.name, &CustomMap.id FROM person WHERE name = 'Mark'",
		types:    []any{sqlair.M{}, CustomMap{}},
		inputs:   []any{},
		slices:   []any{&[]sqlair.M{}, &[]CustomMap{}},
		expected: []any{&[]sqlair.M{{"name": "Mark"}}, &[]CustomMap{{"id": int64(20)}}},
	}}

	dropTables, db, err := personAndAddressDB()
	if err != nil {
		c.Fatal(err)
	}

	for _, t := range tests {
		stmt, err := sqlair.Prepare(t.query, t.types...)
		if err != nil {
			c.Errorf("\ntest %q failed (Prepare):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}

		q := db.Query(nil, stmt, t.inputs...)
		err = q.GetAll(t.slices...)
		if err != nil {
			c.Errorf("\ntest %q failed (GetAll):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}
		for i, column := range t.expected {
			c.Assert(t.slices[i], DeepEquals, column,
				Commentf("\ntest %q failed:\ninput: %s", t.summary, t.query))
		}
	}

	err = db.Query(nil, sqlair.MustPrepare(dropTables)).Run()
	if err != nil {
		c.Fatal(err)
	}
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
		summary: "wrong slice type (struct)",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		slices:  []any{&[]*Address{}},
		err:     `cannot populate slice: cannot get result: type "Address" does not appear in query, have: Person`,
	}, {
		summary: "wrong slice type (int)",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		slices:  []any{&[]int{}},
		err:     `cannot populate slice: need slice of structs/maps, got slice of int`,
	}, {
		summary: "wrong slice type (pointer to int)",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		slices:  []any{&[]*int{}},
		err:     `cannot populate slice: need slice of structs/maps, got slice of pointer to int`,
	}, {
		summary: "wrong slice type (pointer to map)",
		query:   "SELECT &M.name FROM person",
		types:   []any{sqlair.M{}},
		inputs:  []any{},
		slices:  []any{&[]*sqlair.M{}},
		err:     `cannot populate slice: need slice of structs/maps, got slice of pointer to map`,
	}, {
		summary: "output not referenced in query",
		query:   "SELECT name FROM person",
		types:   []any{},
		inputs:  []any{},
		slices:  []any{&[]Person{}},
		err:     `cannot populate slice: output variables provided but not referenced in query`,
	}}

	dropTables, db, err := personAndAddressDB()
	if err != nil {
		c.Fatal(err)
	}

	for _, t := range tests {
		stmt, err := sqlair.Prepare(t.query, t.types...)
		if err != nil {
			c.Errorf("\ntest %q failed (Prepare):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}

		err = db.Query(nil, stmt, t.inputs...).GetAll(t.slices...)
		c.Assert(err, ErrorMatches, t.err,
			Commentf("\ntest %q failed:\ninput: %s\nslices: %s", t.summary, t.query, t.slices))
	}

	err = db.Query(nil, sqlair.MustPrepare(dropTables)).Run()
	if err != nil {
		c.Fatal(err)
	}
}

func (s *PackageSuite) TestRun(c *C) {
	dropTables, db, err := personAndAddressDB()
	c.Assert(err, IsNil)

	var jim = Person{
		ID:       70,
		Name:     "Jim",
		Postcode: 500,
	}

	// Insert Jim.
	insertStmt := sqlair.MustPrepare("INSERT INTO person VALUES ($Person.name, $Person.id, $Person.address_id, 'jimmy@email.com');", Person{})
	err = db.Query(nil, insertStmt, &jim).Run()
	c.Assert(err, IsNil)

	// Check Jim is in the db.
	selectStmt := sqlair.MustPrepare("SELECT &Person.* FROM person WHERE id = $Person.id", Person{})
	var jimCheck = Person{}
	err = db.Query(nil, selectStmt, &jim).Get(&jimCheck)
	c.Assert(err, IsNil)
	c.Assert(jimCheck, Equals, jim)

	err = db.Query(nil, sqlair.MustPrepare(dropTables)).Run()
	c.Assert(err, IsNil)
}

func (s *PackageSuite) TestOutcome(c *C) {
	dropTables, db, err := personAndAddressDB()
	c.Assert(err, IsNil)

	var jim = Person{
		ID:       70,
		Name:     "Jim",
		Postcode: 500,
	}

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
	allOutput := &[]*Person{}
	allExpected := &[]*Person{&Person{30, "Fred", 1000}, &Person{20, "Mark", 1500}, &Person{40, "Mary", 3500}, &Person{35, "Dave", 4500}}

	iterOutputs := []any{&Person{}, &Person{}, &Person{}, &Person{}}
	iterExpected := []any{&Person{30, "Fred", 1000}, &Person{20, "Mark", 1500}, &Person{40, "Mary", 3500}, &Person{35, "Dave", 4500}}

	oneOutput := &Person{}
	oneExpected := &Person{30, "Fred", 1000}

	dropTables, db, err := personAndAddressDB()
	c.Assert(err, IsNil)

	stmt := sqlair.MustPrepare("SELECT &Person.* FROM person", Person{})

	// Run different Query methods.
	q := db.Query(nil, stmt)
	err = q.Get(oneOutput)
	c.Assert(err, IsNil)
	c.Assert(oneExpected, DeepEquals, oneOutput)

	err = q.GetAll(allOutput)
	c.Assert(err, IsNil)
	c.Assert(allOutput, DeepEquals, allExpected)

	iter := q.Iter()
	defer iter.Close()
	i := 0
	for iter.Next() {
		if i >= len(iterOutputs) {
			c.Fatalf("expected %d rows, got more", len(iterOutputs))
		}
		if err := iter.Get(iterOutputs[i]); err != nil {
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

	err = q.GetAll(allOutput)
	c.Assert(err, IsNil)
	c.Assert(allOutput, DeepEquals, allExpected)

	iter = q.Iter()
	defer iter.Close()
	i = 0
	for iter.Next() {
		if i >= len(iterOutputs) {
			c.Fatalf("expected %d rows, got more", len(iterOutputs))
		}
		if err := iter.Get(iterOutputs[i]); err != nil {
			c.Fatal(err)
		}
		i++
	}
	err = iter.Close()
	c.Assert(err, IsNil)
	c.Assert(iterOutputs, DeepEquals, iterExpected)

	q = db.Query(nil, stmt)
	err = q.Get(oneOutput)
	c.Assert(err, IsNil)
	c.Assert(oneExpected, DeepEquals, oneOutput)

	err = db.Query(nil, sqlair.MustPrepare(dropTables)).Run()
	c.Assert(err, IsNil)
}

func (s *PackageSuite) TestTransactions(c *C) {
	dropTables, db, err := personAndAddressDB()
	c.Assert(err, IsNil)

	selectStmt := sqlair.MustPrepare("SELECT &Person.* FROM person WHERE address_id = $Person.address_id", Person{})
	insertStmt := sqlair.MustPrepare("INSERT INTO person VALUES ( $Person.name, $Person.id, $Person.address_id, 'fred@email.com');", Person{})
	var derek = Person{ID: 85, Name: "Derek", Postcode: 8000}
	ctx := context.Background()

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
	err = tx.Query(ctx, selectStmt, &derek).Get(&derekCheck)
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

	err = tx.Query(ctx, selectStmt, &derek).Get(&derekCheck)
	c.Assert(err, IsNil)
	c.Assert(derek, Equals, derekCheck)
	err = tx.Commit()
	c.Assert(err, IsNil)

	err = db.Query(ctx, sqlair.MustPrepare(dropTables)).Run()
	c.Assert(err, IsNil)
}

func (s *PackageSuite) TestTransactionErrors(c *C) {
	dropTables, db, err := personAndAddressDB()
	c.Assert(err, IsNil)

	insertStmt := sqlair.MustPrepare("INSERT INTO person VALUES ($Person.name, $Person.id, $Person.address_id, 'fred@email.com');", Person{})
	var derek = Person{ID: 85, Name: "Derek", Postcode: 8000}
	ctx := context.Background()

	// Test running query after commit.
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

func (s *PackageSuite) TestInsert(c *C) {
	dropTables, db, err := personAndAddressDB()
	c.Assert(err, IsNil)

	// The INSERT statements are prepared along with SELECT statements for
	// checking that the insert has worked correctly, and DELETE statements to
	// remove the inserted row.
	insertPersonStmt, err := sqlair.Prepare("INSERT INTO person (*) VALUES ($Person.*)", Person{})
	c.Assert(err, IsNil)

	insertNameIDStmt, err := sqlair.Prepare("INSERT INTO person (name, id) VALUES ($Person.*)", Person{})
	c.Assert(err, IsNil)

	insertAddressIDStmt, err := sqlair.Prepare("INSERT INTO address (id) VALUES ($Person.address_id)", Person{})
	c.Assert(err, IsNil)

	insertAddressStmt, err := sqlair.Prepare("INSERT INTO address (*) VALUES ($Address.id, $Address.street, $Address.district)", Person{}, Address{})
	c.Assert(err, IsNil)

	// RETURNING clauses are supported by SQLite with syntax taken from
	// postgresql. The inserted values are returned as query results.
	returningStmt, err := sqlair.Prepare("INSERT INTO address(*) VALUES($Address.*) RETURNING &Address.*", Person{}, Address{})
	c.Assert(err, IsNil)

	selectPerson, err := sqlair.Prepare("SELECT &Person.* FROM person WHERE id = $Person.id", Person{})
	c.Assert(err, IsNil)

	selectAddress, err := sqlair.Prepare("SELECT &Address.* FROM address WHERE id = $Address.id", Address{})
	c.Assert(err, IsNil)

	deletePersonStmt, err := sqlair.Prepare("DELETE FROM person WHERE id = $Person.id", Person{})
	c.Assert(err, IsNil)

	deleteAddressStmt, err := sqlair.Prepare("DELETE FROM address WHERE id = $Address.id", Address{})
	c.Assert(err, IsNil)

	deleteAddressStmtReturning, err := sqlair.Prepare("DELETE FROM address WHERE id = $Address.id RETURNING &Address.*", Address{})
	c.Assert(err, IsNil)

	dropTables, db, err = personAndAddressDB()
	c.Assert(err, IsNil)

	var p = Person{}
	var a = Address{}
	var outcome = sqlair.Outcome{}
	var eric = Person{Name: "Eric", ID: 60, Postcode: 7000}
	var millLane = Address{Street: "Mill Lane", District: "Crazy County", ID: 7000}

	// Each block follows the sequence:
	// - Insert value
	// - Select value from DB
	// - Check the selected value matches the inserted one
	// - Delete the value from the database
	// - Check that one row was deleted

	c.Assert(db.Query(nil, insertPersonStmt, eric).Run(), IsNil)
	c.Assert(db.Query(nil, selectPerson, eric).Get(&p), IsNil)
	c.Assert(p, Equals, eric)
	c.Assert(db.Query(nil, deletePersonStmt, eric).Get(&outcome), IsNil)
	i, err := outcome.Result().RowsAffected()
	c.Assert(err, IsNil)
	c.Assert(i, Equals, int64(1))

	c.Assert(db.Query(nil, insertNameIDStmt, eric).Run(), IsNil)
	c.Assert(db.Query(nil, selectPerson, eric).Get(&p), IsNil)
	c.Assert(p.ID, Equals, eric.ID)
	c.Assert(db.Query(nil, deletePersonStmt, eric).Get(&outcome), IsNil)
	i, err = outcome.Result().RowsAffected()
	c.Assert(err, IsNil)
	c.Assert(i, Equals, int64(1))

	c.Assert(db.Query(nil, insertAddressIDStmt, eric).Run(), IsNil)
	c.Assert(db.Query(nil, selectAddress, millLane).Get(&a), IsNil)
	c.Assert(eric.Postcode, Equals, a.ID)
	c.Assert(db.Query(nil, deleteAddressStmt, millLane).Get(&outcome), IsNil)
	i, err = outcome.Result().RowsAffected()
	c.Assert(err, IsNil)
	c.Assert(i, Equals, int64(1))

	c.Assert(db.Query(nil, insertAddressStmt, millLane).Run(), IsNil)
	c.Assert(db.Query(nil, selectAddress, millLane).Get(&a), IsNil)
	c.Assert(millLane, Equals, a)
	c.Assert(db.Query(nil, deleteAddressStmt, millLane).Get(&outcome), IsNil)
	i, err = outcome.Result().RowsAffected()
	c.Assert(err, IsNil)
	c.Assert(i, Equals, int64(1))

	// The RETURNING clause in this statement returns the inserted data.
	c.Assert(db.Query(nil, returningStmt, millLane).Get(&a), IsNil)
	c.Assert(a.Street, Equals, millLane.Street)
	c.Assert(a.District, Equals, millLane.District)
	var a0 = Address{}
	c.Assert(db.Query(nil, deleteAddressStmtReturning, a).Get(&a0), IsNil)
	c.Assert(a, Equals, a0)

	c.Assert(db.Query(nil, sqlair.MustPrepare(dropTables)).Run(), IsNil)
}

func (s *PackageSuite) TestIterMethodOrder(c *C) {
	dropTables, db, err := personAndAddressDB()
	if err != nil {
		c.Fatal(err)
	}

	var p = Person{}
	stmt := sqlair.MustPrepare("SELECT &Person.* FROM person", Person{})

	// Check immidiate Get.
	iter := db.Query(nil, stmt).Iter()
	err = iter.Get(&p)
	c.Assert(err, ErrorMatches, "cannot get result: cannot call Get before Next unless getting outcome")
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

	// Check Get after closing.
	iter = db.Query(nil, stmt).Iter()
	err = iter.Close()
	c.Assert(err, IsNil)
	err = iter.Get(&p)
	c.Assert(err, ErrorMatches, "cannot get result: iteration ended")
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
	err = iter.Get(&p)
	c.Assert(err, ErrorMatches, `cannot get result: sql: Scan error on column index 0, name "_sqlair_0": converting driver.Value type string \("Fred"\) to a int: invalid syntax`)
	err = iter.Close()
	c.Assert(err, IsNil)

	_, err = db.PlainDB().Exec(dropTables)
	c.Assert(err, IsNil)
}
