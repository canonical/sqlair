// Copyright 2023 Canonical Ltd.
// Licensed under Apache 2.0, see LICENCE file for details.

package sqlair_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"

	_ "github.com/mattn/go-sqlite3"
	. "gopkg.in/check.v1"

	"github.com/canonical/sqlair"
)

type PackageSuite struct {
	db *sql.DB
}

var _ = Suite(&PackageSuite{})

func (s *PackageSuite) SetUpTest(c *C) {
	c.Assert(s.db, IsNil)
	db, err := sql.Open("sqlite3", "file:test.db?cache=shared&mode=memory")
	c.Assert(err, IsNil)
	s.db = db
}

func (s *PackageSuite) TearDownTest(c *C) {
	// Close and forget DB.
	err := s.db.Close()
	c.Assert(err, IsNil)
	s.db = nil
}

func dropTables(c *C, db *sqlair.DB, tables ...string) error {
	for _, table := range tables {
		stmt, err := sqlair.Prepare(fmt.Sprintf("DROP TABLE %s;", table))
		c.Assert(err, IsNil)
		err = db.Query(nil, stmt).Run()
		c.Assert(err, IsNil)
	}
	return nil
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

type unexportedStruct struct {
	X int `db:"id"`
}

type ScannerValuerInt struct {
	F int
}

func (sv *ScannerValuerInt) Scan(v any) error {
	if i, ok := v.(int64); ok {
		sv.F = int(i)
	} else {
		sv.F = 666
	}
	return nil
}

func (sv *ScannerValuerInt) Value() (driver.Value, error) {
	return int64(sv.F), nil
}

type ScannerValuerString struct {
	S string
}

func (svs *ScannerValuerString) Scan(v any) error {
	if _, ok := v.(string); ok {
		svs.S = "ScannerString scanned well!"
	} else {
		svs.S = "ScannerString found a NULL"
	}
	return nil
}

func (svs *ScannerValuerString) Value() (driver.Value, error) {
	return svs.S, nil
}

var fred = Person{Name: "Fred", ID: 30, Postcode: 1000}
var mark = Person{Name: "Mark", ID: 20, Postcode: 1500}
var mary = Person{Name: "Mary", ID: 40, Postcode: 3500}
var dave = Person{Name: "Dave", ID: 35, Postcode: 4500}
var allPeople = []Person{fred, mark, mary, dave}

var mainStreet = Address{Street: "Main Street", District: "Happy Land", ID: 1000}
var churchRoad = Address{Street: "Church Road", District: "Sad World", ID: 1500}
var stationLane = Address{Street: "Station Lane", District: "Ambivalent Commons", ID: 3500}
var allAddresses = []Address{mainStreet, churchRoad, stationLane}

func (s *PackageSuite) personAndAddressDB(c *C) (db *sqlair.DB, tables []string) {
	db = sqlair.NewDB(s.db)

	createPerson, err := sqlair.Prepare(`
		CREATE TABLE person (
			name text,
			id integer,
			address_id integer,
			email text
		);
	`)
	c.Assert(err, IsNil)
	createAddress, err := sqlair.Prepare(`
		CREATE TABLE address (
			id integer,
			district text,
			street text
		);
	`)
	c.Assert(err, IsNil)

	err = db.Query(nil, createPerson).Run()
	c.Assert(err, IsNil)
	err = db.Query(nil, createAddress).Run()
	c.Assert(err, IsNil)

	insertPerson, err := sqlair.Prepare("INSERT INTO person (*) VALUES ($Person.*)", Person{})
	for _, person := range allPeople {
		err := db.Query(nil, insertPerson, person).Run()
		c.Assert(err, IsNil)
	}

	insertAddress, err := sqlair.Prepare("INSERT INTO address (*) VALUES ($Address.*)", Address{})
	for _, address := range allAddresses {
		err := db.Query(nil, insertAddress, address).Run()
		c.Assert(err, IsNil)
	}

	return db, []string{"person", "address"}
}

func (s *PackageSuite) TestValidIterGet(c *C) {
	type StringMap map[string]string
	type unexportedMap map[string]any
	type M struct {
		F string `db:"id"`
	}
	type IntSlice []int
	type StringSlice []string
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
		expected: [][]any{{&Person{ID: fred.ID}, &Address{ID: mainStreet.ID}}, {&Person{ID: fred.ID}, &Address{ID: churchRoad.ID}}, {&Person{ID: fred.ID}, &Address{ID: stationLane.ID}}, {&Person{ID: mark.ID}, &Address{ID: mainStreet.ID}}, {&Person{ID: mark.ID}, &Address{ID: churchRoad.ID}}, {&Person{ID: mark.ID}, &Address{ID: stationLane.ID}}, {&Person{ID: mary.ID}, &Address{ID: mainStreet.ID}}, {&Person{ID: mary.ID}, &Address{ID: churchRoad.ID}}, {&Person{ID: mary.ID}, &Address{ID: stationLane.ID}}, {&Person{ID: dave.ID}, &Address{ID: mainStreet.ID}}, {&Person{ID: dave.ID}, &Address{ID: churchRoad.ID}}, {&Person{ID: dave.ID}, &Address{ID: stationLane.ID}}},
	}, {
		summary:  "simple select person",
		query:    "SELECT * AS &Person.* FROM person",
		types:    []any{Person{}},
		inputs:   []any{},
		outputs:  [][]any{{&Person{}}, {&Person{}}, {&Person{}}, {&Person{}}},
		expected: [][]any{{&fred}, {&mark}, {&mary}, {&dave}},
	}, {
		summary:  "select multiple with extras",
		query:    "SELECT email, * AS &Person.*, address_id AS &Address.id, * AS &Manager.*, id FROM person WHERE id = $Address.id",
		types:    []any{Person{}, Address{}, Manager{}},
		inputs:   []any{Address{ID: fred.ID}},
		outputs:  [][]any{{&Person{}, &Address{}, &Manager{}}},
		expected: [][]any{{&fred, &Address{ID: mainStreet.ID}, &Manager{fred.ID, fred.Name, fred.Postcode}}},
	}, {
		summary:  "select with renaming",
		query:    "SELECT (name, address_id) AS (&Address.street, &Address.id) FROM person WHERE id = $Manager.id",
		types:    []any{Address{}, Manager{}},
		inputs:   []any{Manager{ID: fred.ID}},
		outputs:  [][]any{{&Address{}}},
		expected: [][]any{{&Address{Street: fred.Name, ID: fred.Postcode}}},
	}, {
		summary:  "select into star struct",
		query:    "SELECT (name, address_id) AS (&Person.*) FROM person WHERE address_id IN ( $Manager.address_id, $Address.district )",
		types:    []any{Person{}, Address{}, Manager{}},
		inputs:   []any{Manager{Postcode: fred.Postcode}, Address{}},
		outputs:  [][]any{{&Person{}}},
		expected: [][]any{{&Person{Name: fred.Name, Postcode: fred.Postcode}}},
	}, {
		summary:  "select into map",
		query:    "SELECT &M.name FROM person WHERE address_id = $M.p1 OR address_id = $M.p2",
		types:    []any{sqlair.M{}},
		inputs:   []any{sqlair.M{"p1": fred.Postcode, "p2": mark.Postcode}},
		outputs:  [][]any{{sqlair.M{}}, {sqlair.M{}}},
		expected: [][]any{{sqlair.M{"name": fred.Name}}, {sqlair.M{"name": mark.Name}}},
	}, {
		summary:  "select into star map",
		query:    "SELECT (name, address_id) AS (&M.*) FROM person WHERE address_id = $M.p1",
		types:    []any{sqlair.M{}},
		inputs:   []any{sqlair.M{"p1": fred.Postcode}},
		outputs:  [][]any{{&sqlair.M{"address_id": 0}}},
		expected: [][]any{{&sqlair.M{"name": fred.Name, "address_id": int64(fred.Postcode)}}},
	}, {
		summary:  "select into custom map",
		query:    "SELECT (name, address_id) AS (&CustomMap.*) FROM person WHERE address_id IN ( $CustomMap.address_id, $CustomMap.district)",
		types:    []any{CustomMap{}},
		inputs:   []any{CustomMap{"address_id": fred.Postcode, "district": "Lala land"}},
		outputs:  [][]any{{&CustomMap{"address_id": 0}}},
		expected: [][]any{{&CustomMap{"name": fred.Name, "address_id": int64(fred.Postcode)}}},
	}, {
		summary:  "multiple maps",
		query:    "SELECT name AS &StringMap.*, id AS &CustomMap.* FROM person WHERE address_id = $M.address_id AND id = $StringMap.id",
		types:    []any{StringMap{}, sqlair.M{}, CustomMap{}},
		inputs:   []any{sqlair.M{"address_id": "1000"}, &StringMap{"id": "30"}},
		outputs:  [][]any{{&StringMap{}, CustomMap{}}},
		expected: [][]any{{&StringMap{"name": fred.Name}, CustomMap{"id": int64(30)}}},
	}, {
		summary:  "lower case map",
		query:    "SELECT name AS &unexportedMap.*, id AS &unexportedMap.* FROM person WHERE address_id = $unexportedMap.address_id",
		types:    []any{unexportedMap{}},
		inputs:   []any{unexportedMap{"address_id": "1000"}},
		outputs:  [][]any{{&unexportedMap{}}},
		expected: [][]any{{&unexportedMap{"name": fred.Name, "id": int64(fred.ID)}}},
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
	}, {
		summary:  "simple in",
		query:    "SELECT * AS &Person.* FROM person WHERE id IN ($S[:])",
		types:    []any{Person{}, sqlair.S{}},
		inputs:   []any{sqlair.S{30, 35, 36, 37, 38, 39, 40, fred.ID, mary.ID, dave.ID}},
		outputs:  [][]any{{&Person{}}, {&Person{}}, {&Person{}}},
		expected: [][]any{{&fred}, {&mary}, {&dave}},
	}, {
		summary:  "complex in",
		query:    "SELECT * AS &Person.* FROM person WHERE id IN ($Person.id, $S[:], $Manager.id, $IntSlice[:], $StringSlice[:])",
		types:    []any{Person{}, sqlair.S{}, Manager{}, IntSlice{}, StringSlice{}},
		inputs:   []any{mark, sqlair.S{21, 23, 24, 25, 26, 27, 28, 29}, IntSlice{31, 32, 33, 34, dave.ID}, &Manager{ID: fred.ID}, StringSlice{"36", "37", "38", "39", strconv.Itoa(mary.ID)}},
		outputs:  [][]any{{&Person{}}, {&Person{}}, {&Person{}}, {&Person{}}},
		expected: [][]any{{&fred}, {&mark}, {&mary}, {&dave}},
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

	db, tables := s.personAndAddressDB(c)
	defer dropTables(c, db, tables...)

	for _, t := range tests {
		stmt, err := sqlair.Prepare(t.query, t.types...)
		c.Assert(err, IsNil,
			Commentf("\ntest %q failed (Prepare):\ninput: %s\n", t.summary, t.query))

		iter := db.Query(nil, stmt, t.inputs...).Iter()
		defer iter.Close()
		i := 0
		for iter.Next() {
			if i >= len(t.outputs) {
				c.Fatalf("\ntest %q failed (Next):\ninput: %s\nerr: more rows that expected (%d > %d)\n", t.summary, t.query, i+1, len(t.outputs))
			}

			err = iter.Get(t.outputs[i]...)
			c.Assert(err, IsNil,
				Commentf("\ntest %q failed (Get):\ninput: %s\n", t.summary, t.query))

			i++
		}

		err = iter.Close()
		c.Assert(err, IsNil,
			Commentf("\ntest %q failed (Close):\ninput: %s\n", t.summary, t.query))
		for i, row := range t.expected {
			for j, col := range row {
				c.Assert(t.outputs[i][j], DeepEquals, col,
					Commentf("\ntest %q failed:\ninput: %s\nrow: %d\n", t.summary, t.query, i))
			}
		}
	}
}

func (s *PackageSuite) TestIterGetErrors(c *C) {
	type SliceMap map[string][]string
	var tests = []struct {
		summary string
		query   string
		types   []any
		inputs  []any
		outputs []any
		err     string
	}{{
		summary: "nil parameter",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		outputs: []any{nil},
		err:     "cannot get result: got nil argument",
	}, {
		summary: "nil pointer parameter",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		outputs: []any{(*Person)(nil)},
		err:     "cannot get result: got nil pointer to Person",
	}, {
		summary: "non pointer parameter",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		outputs: []any{Person{}},
		err:     "cannot get result: need map or pointer to struct, got struct",
	}, {
		summary: "wrong struct",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		outputs: []any{&Address{}},
		err:     `cannot get result: parameter with type "Person" missing \(have "Address"\)`,
	}, {
		summary: "not a struct",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		outputs: []any{&[]any{}},
		err:     "cannot get result: need map or pointer to struct, got pointer to slice",
	}, {
		summary: "missing get value",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		outputs: []any{},
		err:     `cannot get result: parameter with type "Person" missing`,
	}, {
		summary: "multiple of the same type",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		outputs: []any{&Person{}, &Person{}},
		err:     `cannot get result: type "Person" provided more than once`,
	}, {
		summary: "multiple of the same type",
		query:   "SELECT name AS &M.* FROM person",
		types:   []any{sqlair.M{}},
		inputs:  []any{},
		outputs: []any{&sqlair.M{}, sqlair.M{}},
		err:     `cannot get result: type "M" provided more than once`,
	}, {
		summary: "nil map output",
		query:   "SELECT name AS &M.* FROM person",
		types:   []any{sqlair.M{}},
		inputs:  []any{},
		outputs: []any{(sqlair.M)(nil)},
		err:     `cannot get result: got nil M`,
	}, {
		summary: "type not in query",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		outputs: []any{&Person{}, &Address{}},
		err:     `cannot get result: "Address" not referenced in query`,
	}, {
		summary: "output expr in a with clause",
		query: `WITH averageID(avgid) AS (SELECT &Person.id FROM person)
		        SELECT id FROM person, averageID WHERE id > averageID.avgid LIMIT 1`,
		types:   []any{Person{}},
		inputs:  []any{},
		outputs: []any{&Person{}},
		err:     `cannot get result: query uses "&Person" outside of result context`,
	}}

	db, tables := s.personAndAddressDB(c)
	defer dropTables(c, db, tables...)

	for _, t := range tests {
		stmt, err := sqlair.Prepare(t.query, t.types...)
		c.Assert(err, IsNil,
			Commentf("\ntest %q failed (Prepare):\ninput: %s\n", t.summary, t.query))

		iter := db.Query(nil, stmt, t.inputs...).Iter()
		defer iter.Close()
		if !iter.Next() {
			c.Fatalf("\ntest %q failed (Get):\ninput: %s\nerr: no rows returned\n", t.summary, t.query)
		}
		err = iter.Get(t.outputs...)
		c.Assert(err, ErrorMatches, t.err,
			Commentf("\ntest %q failed:\ninput: %s\noutputs: %s\n", t.summary, t.query, t.outputs))
		err = iter.Close()
		c.Assert(err, IsNil,
			Commentf("\ntest %q failed (Close):\ninput: %s\n", t.summary, t.query))
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
		expected: []any{&Person{ID: fred.ID}, &Address{ID: mainStreet.ID}},
	}, {
		summary:  "select into multiple structs, with input conditions",
		query:    "SELECT p.* AS &Person.*, a.* AS &Address.*, p.* AS &Manager.* FROM person AS p, address AS a WHERE p.id = $Person.id AND a.id = $Address.id ",
		types:    []any{Person{}, Address{}, Manager{}},
		inputs:   []any{mainStreet, fred},
		outputs:  []any{&Person{}, &Address{}, &Manager{}},
		expected: []any{&fred, &mainStreet, &Manager{fred.ID, fred.Name, fred.Postcode}},
	}, {
		summary:  "select into map",
		query:    "SELECT &M.name FROM person WHERE address_id = $M.p1",
		types:    []any{sqlair.M{}},
		inputs:   []any{sqlair.M{"p1": fred.Postcode}},
		outputs:  []any{sqlair.M{}},
		expected: []any{sqlair.M{"name": fred.Name}},
	}, {
		summary:  "lower case struct",
		query:    "SELECT &unexportedStruct.* FROM person",
		types:    []any{unexportedStruct{}},
		inputs:   []any{},
		outputs:  []any{&unexportedStruct{}},
		expected: []any{&unexportedStruct{X: 30}},
	}, {
		summary:  "sql functions",
		query:    `SELECT (max(AVG(id), AVG(address_id), length("((((''""((")), IFNULL(name, "Mr &Person.id of $M.name")) AS (&M.avg, &M.name), round(24.5234) AS other_col FROM person`,
		types:    []any{sqlair.M{}},
		inputs:   []any{},
		outputs:  []any{sqlair.M{}},
		expected: []any{sqlair.M{"avg": float64(2625), "name": "Fred"}},
	}}

	db, tables := s.personAndAddressDB(c)
	defer dropTables(c, db, tables...)

	for _, t := range tests {
		stmt, err := sqlair.Prepare(t.query, t.types...)
		if err != nil {
			c.Errorf("\ntest %q failed (Prepare):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}

		err = db.Query(nil, stmt, t.inputs...).Get(t.outputs...)
		c.Assert(err, IsNil,
			Commentf("\ntest %q failed (Get):\ninput: %s\n", t.summary, t.query))

		for i, s := range t.expected {
			c.Assert(t.outputs[i], DeepEquals, s,
				Commentf("\ntest %q failed:\ninput: %s", t.summary, t.query))
		}
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
		types:   []any{},
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

	db, tables := s.personAndAddressDB(c)
	defer dropTables(c, db, tables...)

	for _, t := range tests {
		stmt, err := sqlair.Prepare(t.query, t.types...)
		c.Assert(err, IsNil,
			Commentf("\ntest %q failed (Prepare):\ninput: %s\n", t.summary, t.query))

		err = db.Query(nil, stmt, t.inputs...).Get(t.outputs...)
		c.Assert(err, ErrorMatches, t.err,
			Commentf("\ntest %q failed:\ninput: %s\noutputs: %s", t.summary, t.query, t.outputs))
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
		expected: []any{&[]*Person{{ID: fred.ID}, {ID: fred.ID}, {ID: fred.ID}, {ID: mark.ID}, {ID: mark.ID}, {ID: mark.ID}, {ID: mary.ID}, {ID: mary.ID}, {ID: mary.ID}, {ID: dave.ID}, {ID: dave.ID}, {ID: dave.ID}}, &[]*Address{{ID: mainStreet.ID}, {ID: churchRoad.ID}, {ID: stationLane.ID}, {ID: mainStreet.ID}, {ID: churchRoad.ID}, {ID: stationLane.ID}, {ID: mainStreet.ID}, {ID: churchRoad.ID}, {ID: stationLane.ID}, {ID: mainStreet.ID}, {ID: churchRoad.ID}, {ID: stationLane.ID}}},
	}, {
		summary:  "select all columns into person",
		query:    "SELECT * AS &Person.* FROM person",
		types:    []any{Person{}},
		inputs:   []any{},
		slices:   []any{&[]*Person{}},
		expected: []any{&[]*Person{&fred, &mark, &mary, &dave}},
	}, {
		summary:  "select all columns into person with no pointers",
		query:    "SELECT * AS &Person.* FROM person",
		types:    []any{Person{}},
		inputs:   []any{},
		slices:   []any{&[]Person{}},
		expected: []any{&[]Person{fred, mark, mary, dave}},
	}, {
		summary:  "single line of query with inputs",
		query:    "SELECT p.* AS &Person.*, a.* AS &Address.*, p.* AS &Manager.* FROM person AS p, address AS a WHERE p.id = $Person.id AND a.id = $Address.id ",
		types:    []any{Person{}, Address{}, Manager{}},
		inputs:   []any{Address{ID: mainStreet.ID}, Person{ID: fred.ID}},
		slices:   []any{&[]*Manager{}, &[]*Person{}, &[]*Address{}},
		expected: []any{&[]*Manager{{fred.ID, fred.Name, fred.Postcode}}, &[]*Person{&fred}, &[]*Address{&mainStreet}},
	}, {
		summary:  "select into maps",
		query:    "SELECT &M.name, &CustomMap.id FROM person WHERE name = $Person.name",
		types:    []any{sqlair.M{}, CustomMap{}, Person{}},
		inputs:   []any{mark},
		slices:   []any{&[]sqlair.M{}, &[]CustomMap{}},
		expected: []any{&[]sqlair.M{{"name": mark.Name}}, &[]CustomMap{{"id": int64(mark.ID)}}},
	}, {
		summary:  "GetAll returns no error when there are no outputs",
		query:    `INSERT INTO person (name) VALUES ($M.name)`,
		types:    []any{sqlair.M{}},
		inputs:   []any{sqlair.M{"name": "Joe"}},
		slices:   []any{},
		expected: []any{},
	}}

	db, tables := s.personAndAddressDB(c)
	defer dropTables(c, db, tables...)

	for _, t := range tests {
		stmt, err := sqlair.Prepare(t.query, t.types...)
		c.Assert(err, IsNil,
			Commentf("\ntest %q failed (Prepare):\ninput: %s\n", t.summary, t.query))

		err = db.Query(nil, stmt, t.inputs...).GetAll(t.slices...)
		c.Assert(err, IsNil,
			Commentf("\ntest %q failed (All):\ninput: %s\n", t.summary, t.query))

		for i, column := range t.expected {
			c.Assert(t.slices[i], DeepEquals, column,
				Commentf("\ntest %q failed:\ninput: %s", t.summary, t.query))
		}
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
		err:     "need pointer to slice, got invalid",
	}, {
		summary: "nil pointer argument",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		slices:  []any{(*[]Person)(nil)},
		err:     "need pointer to slice, got nil",
	}, {
		summary: "none slice argument",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		slices:  []any{Person{}},
		err:     "need pointer to slice, got struct",
	}, {
		summary: "none slice pointer argument",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		slices:  []any{&Person{}},
		err:     "need pointer to slice, got pointer to struct",
	}, {
		summary: "wrong slice type (struct)",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		slices:  []any{&[]*Address{}},
		err:     `cannot get result: parameter with type "Person" missing \(have "Address"\)`,
	}, {
		summary: "wrong slice type (int)",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		slices:  []any{&[]int{}},
		err:     `need slice of structs/maps, got slice of int`,
	}, {
		summary: "wrong slice type (pointer to int)",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		slices:  []any{&[]*int{}},
		err:     `need slice of structs/maps, got slice of pointer to int`,
	}, {
		summary: "wrong slice type (pointer to map)",
		query:   "SELECT &M.name FROM person",
		types:   []any{sqlair.M{}},
		inputs:  []any{},
		slices:  []any{&[]*sqlair.M{}},
		err:     `need slice of structs/maps, got slice of pointer to map`,
	}, {
		summary: "output not referenced in query",
		query:   "SELECT name FROM person",
		types:   []any{},
		inputs:  []any{},
		slices:  []any{&[]Person{}},
		err:     `output variables provided but not referenced in query`,
	}, {
		summary: "nothing returned",
		query:   "SELECT &Person.* FROM person WHERE id = $Person.id",
		types:   []any{Person{}},
		inputs:  []any{Person{ID: 1243321}},
		slices:  []any{&[]*Person{}},
		err:     "sql: no rows in result set",
	}}

	db, tables := s.personAndAddressDB(c)
	defer dropTables(c, db, tables...)

	for _, t := range tests {
		stmt, err := sqlair.Prepare(t.query, t.types...)
		c.Assert(err, IsNil,
			Commentf("\ntest %q failed (Prepare):\ninput: %s\n", t.summary, t.query))

		err = db.Query(nil, stmt, t.inputs...).GetAll(t.slices...)
		c.Assert(err, ErrorMatches, t.err,
			Commentf("\ntest %q failed:\ninput: %s\nslices: %s", t.summary, t.query, t.slices))
	}
}

func (s *PackageSuite) TestRun(c *C) {
	db, tables := s.personAndAddressDB(c)
	defer dropTables(c, db, tables...)

	var jim = Person{
		ID:       70,
		Name:     "Jim",
		Postcode: 500,
	}

	// Insert Jim.
	insertStmt := sqlair.MustPrepare("INSERT INTO person (*) VALUES ($Person.*);", Person{})
	err := db.Query(nil, insertStmt, &jim).Run()
	c.Assert(err, IsNil)

	// Check Jim is in the db.
	selectStmt := sqlair.MustPrepare("SELECT &Person.* FROM person WHERE id = $Person.id", Person{})
	var jimCheck = Person{}
	err = db.Query(nil, selectStmt, &jim).Get(&jimCheck)
	c.Assert(err, IsNil)
	c.Check(jimCheck, Equals, jim)

	joe := Person{
		ID:       34,
		Name:     "Joe",
		Postcode: 55555,
	}
	// Insert Joe.
	err = db.Query(nil, insertStmt, &joe).Run()
	c.Assert(err, IsNil)

	// Check Joe is in the db.
	selectStmt = sqlair.MustPrepare("SELECT &Person.* FROM person WHERE id = $Person.id", Person{})
	var joeCheck = Person{}
	err = db.Query(nil, selectStmt, &joe).Get(&joeCheck)
	c.Assert(err, IsNil)
	c.Check(joeCheck, Equals, joe)
}

func (s *PackageSuite) TestRunBulkInsert(c *C) {
	db := sqlair.NewDB(s.db)
	createPerson, err := sqlair.Prepare(`
		CREATE TABLE person (
			name text,
			id integer,
			address_id integer,
			email text
		);
	`)
	c.Assert(err, IsNil)
	err = db.Query(nil, createPerson).Run()
	c.Assert(err, IsNil)
	createAddress, err := sqlair.Prepare(`
		CREATE TABLE address (
			id integer,
			district text,
			street text
		);
	`)
	c.Assert(err, IsNil)

	err = db.Query(nil, createAddress).Run()
	c.Assert(err, IsNil)
	defer dropTables(c, db, "person", "address")

	// Insert all people in a bulk insert.
	insertPeopleStmt, err := sqlair.Prepare("INSERT INTO person (*) VALUES ($Person.*);", Person{})
	c.Assert(err, IsNil)
	err = db.Query(nil, insertPeopleStmt, allPeople).Run()
	c.Assert(err, IsNil)

	// Check all people are in the db.
	selectPeopleStmt, err := sqlair.Prepare("SELECT &Person.* FROM person", Person{})
	c.Assert(err, IsNil)
	var checkPeople []Person
	err = db.Query(nil, selectPeopleStmt).GetAll(&checkPeople)
	c.Assert(err, IsNil)
	c.Check(checkPeople, DeepEquals, allPeople)

	// Insert all address in db and check outcome to confirm 3 rows inserted.
	insertAddressStmt, err := sqlair.Prepare(
		`INSERT INTO address (id, street, district) VALUES ($Person.address_id, $Address.street, "const district")`,
		Person{},
		Address{},
	)
	c.Assert(err, IsNil)
	outcome := sqlair.Outcome{}
	err = db.Query(nil, insertAddressStmt, []Person{fred, mark, mary}, []Address{mainStreet, churchRoad, stationLane}).Get(&outcome)
	c.Assert(err, IsNil)
	rowsAffected, err := outcome.Result().RowsAffected()
	c.Assert(err, IsNil)
	c.Assert(rowsAffected, Equals, int64(3))

	// Check all added addresses are in the db.
	selectAddressStmt, err := sqlair.Prepare("SELECT &Address.* FROM address", Address{})
	c.Assert(err, IsNil)
	checkAddresses := []Address{
		{ID: 1000, Street: "Main Street", District: "const district"},
		{ID: 1500, Street: "Church Road", District: "const district"},
		{ID: 3500, Street: "Station Lane", District: "const district"},
	}
	var addresses []Address
	err = db.Query(nil, selectAddressStmt).GetAll(&addresses)
	c.Assert(err, IsNil)
	c.Check(checkAddresses, DeepEquals, addresses)
}

func (s *PackageSuite) TestOutcome(c *C) {
	db, tables := s.personAndAddressDB(c)
	defer dropTables(c, db, tables...)

	var jim = Person{
		ID:       70,
		Name:     "Jim",
		Postcode: 500,
	}

	var outcome = sqlair.Outcome{}

	// Test INSERT with Get
	insertStmt := sqlair.MustPrepare(`
		INSERT INTO person 
		VALUES ($Person.name, $Person.id, $Person.address_id, 'jimmy@email.com');
	`, Person{})
	err := db.Query(nil, insertStmt, &jim).Get(&outcome)
	c.Assert(err, IsNil)

	res := outcome.Result()
	c.Assert(res, Not(IsNil))
	rowsAffected, err := outcome.Result().RowsAffected()
	c.Assert(err, IsNil)
	c.Assert(rowsAffected, Equals, int64(1))

	// Test SELECT with Get
	selectStmt := sqlair.MustPrepare(`
		SELECT &Person.*
		FROM person
	`, Person{})
	err = db.Query(nil, selectStmt).Get(&outcome, &jim)
	c.Assert(err, IsNil)

	c.Assert(outcome.Result(), IsNil)

	// Test INSERT with Iter
	iter := db.Query(nil, insertStmt, &jim).Iter()
	err = iter.Get(&outcome)
	c.Assert(err, IsNil)

	res = outcome.Result()
	c.Assert(res, Not(IsNil))
	rowsAffected, err = outcome.Result().RowsAffected()
	c.Assert(err, IsNil)
	c.Assert(rowsAffected, Equals, int64(1))

	c.Assert(iter.Next(), Equals, false)
	c.Assert(iter.Close(), IsNil)

	// Test SELECT with Iter.Get
	iter = db.Query(nil, selectStmt).Iter()
	c.Assert(iter.Get(&outcome), IsNil)

	c.Assert(outcome.Result(), IsNil)

	c.Assert(iter.Next(), Equals, true)
	c.Assert(iter.Get(&jim), IsNil)
	c.Assert(iter.Close(), IsNil)

	// Test SELECT with GetAll
	var jims = []Person{}
	err = db.Query(nil, selectStmt).GetAll(&outcome, &jims)
	c.Assert(err, IsNil)

	c.Assert(outcome.Result(), IsNil)
	// Test Iter.Get with zero args and without Outcome
	selectStmt = sqlair.MustPrepare(`SELECT 'hello'`)
	iter = db.Query(nil, selectStmt).Iter()
	err = iter.Get()
	c.Assert(err, ErrorMatches, "cannot get result: cannot call Get before Next unless getting outcome")
}

func (s *PackageSuite) TestErrNoRows(c *C) {
	db, tables := s.personAndAddressDB(c)
	defer dropTables(c, db, tables...)

	stmt := sqlair.MustPrepare("SELECT * AS &Person.* FROM person WHERE id=12312", Person{})
	err := db.Query(nil, stmt).Get(&Person{})
	c.Check(err, ErrorMatches, "sql: no rows in result set")
	c.Check(errors.Is(err, sqlair.ErrNoRows), Equals, true)
	c.Check(errors.Is(err, sql.ErrNoRows), Equals, true)

	err = db.Query(nil, stmt).GetAll(&[]Person{})
	c.Check(err, ErrorMatches, "sql: no rows in result set")
	c.Check(errors.Is(err, sqlair.ErrNoRows), Equals, true)
	c.Check(errors.Is(err, sql.ErrNoRows), Equals, true)
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
		ID       ScannerValuerInt    `db:"id"`
		Name     ScannerValuerString `db:"name"`
		Postcode ScannerValuerInt    `db:"address_id"`
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
		expected: []any{&ScannerDude{Name: ScannerValuerString{S: "ScannerString scanned well!"}, ID: ScannerValuerInt{F: 666}, Postcode: ScannerValuerInt{F: 666}}},
	}}

	db, tables := s.personAndAddressDB(c)
	defer dropTables(c, db, tables...)

	insertNullPerson, err := sqlair.Prepare("INSERT INTO person VALUES ('Nully', NULL, NULL, NULL);")
	c.Assert(err, IsNil)
	c.Assert(db.Query(nil, insertNullPerson).Run(), IsNil)

	for _, t := range tests {
		stmt, err := sqlair.Prepare(t.query, t.types...)
		c.Assert(err, IsNil,
			Commentf("\ntest %q failed (Prepare):\ninput: %s\n", t.summary, t.query))

		err = db.Query(nil, stmt, t.inputs...).Get(t.outputs...)
		c.Assert(err, IsNil,
			Commentf("\ntest %q failed (Get):\ninput: %s\n", t.summary, t.query))

		for i, s := range t.expected {
			c.Assert(t.outputs[i], DeepEquals, s,
				Commentf("\ntest %q failed:\ninput: %s", t.summary, t.query))
		}
	}
}

func (s *PackageSuite) TestQueryMultipleRuns(c *C) {
	// Note: Query structs are not designed to be reused (hence why they store a context as a struct field).
	//       It is, however, possible.
	allOutput := &[]*Person{}
	allExpected := &[]*Person{&fred, &mark, &mary, &dave}

	iterOutputs := []any{&Person{}, &Person{}, &Person{}, &Person{}}
	iterExpected := []any{&fred, &mark, &mary, &dave}

	oneOutput := &Person{}
	oneExpected := &fred

	db, tables := s.personAndAddressDB(c)
	defer dropTables(c, db, tables...)

	stmt := sqlair.MustPrepare("SELECT &Person.* FROM person", Person{})

	// Run different Query methods.
	q := db.Query(nil, stmt)
	err := q.Get(oneOutput)
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
		err = iter.Get(iterOutputs[i])
		c.Assert(err, IsNil)
		i++
	}

	err = iter.Close()
	c.Assert(err, IsNil)
	c.Assert(iterOutputs, DeepEquals, iterExpected)
}

func (s *PackageSuite) TestTransactions(c *C) {
	db, tables := s.personAndAddressDB(c)
	defer dropTables(c, db, tables...)

	selectStmt := sqlair.MustPrepare("SELECT &Person.* FROM person WHERE address_id = $Person.address_id", Person{})
	insertStmt := sqlair.MustPrepare("INSERT INTO person VALUES ( $Person.name, $Person.id, $Person.address_id, 'fred@email.com');", Person{})
	var derek = Person{ID: 85, Name: "Derek", Postcode: 8000}
	ctx := context.Background()

	tx, err := db.Begin(ctx, nil)
	c.Assert(err, IsNil)

	// Insert Derek then rollback.
	err = tx.Query(ctx, insertStmt, &derek).Run()
	c.Assert(err, IsNil)
	err = tx.Rollback()
	c.Assert(err, IsNil)

	// Check Derek isn't in db.
	tx, err = db.Begin(ctx, nil)
	c.Assert(err, IsNil)
	var derekCheck = Person{}
	err = tx.Query(ctx, selectStmt, &derek).Get(&derekCheck)
	if !errors.Is(err, sqlair.ErrNoRows) {
		c.Fatalf("got err %s, expected %s", err, sqlair.ErrNoRows)
	}

	// Insert Derek.
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
}

func (s *PackageSuite) TestTransactionErrors(c *C) {
	db, tables := s.personAndAddressDB(c)
	defer dropTables(c, db, tables...)

	insertStmt := sqlair.MustPrepare("INSERT INTO person VALUES ($Person.name, $Person.id, $Person.address_id, 'fred@email.com');", Person{})
	var derek = Person{ID: 85, Name: "Derek", Postcode: 8000}
	ctx := context.Background()

	// Test running query after commit.
	tx, err := db.Begin(ctx, nil)
	c.Assert(err, IsNil)
	// Create Query.
	q := tx.Query(ctx, insertStmt, &derek)
	// Commit.
	err = tx.Commit()
	c.Assert(err, IsNil)
	// Test Query created before commit.
	err = q.Run()
	c.Assert(err, ErrorMatches, "sql: transaction has already been committed or rolled back")
	// Test Query created after commit.
	err = tx.Query(ctx, insertStmt, &derek).Run()
	c.Assert(err, ErrorMatches, "sql: transaction has already been committed or rolled back")

	// Test error when running query after rollback against the public error variable.
	tx, err = db.Begin(ctx, nil)
	c.Assert(err, IsNil)
	// Create Query.
	q = tx.Query(ctx, insertStmt, &derek)
	// Rollback.
	err = tx.Rollback()
	c.Assert(err, IsNil)
	err = tx.Query(ctx, insertStmt, &derek).Run()
	// Check against sqlair package error.
	if !errors.Is(err, sqlair.ErrTXDone) {
		c.Errorf("expected %q, got %q", sqlair.ErrTXDone, err)
	}
	err = q.Run()
	// Check against sql package error.
	if !errors.Is(err, sql.ErrTxDone) {
		c.Errorf("expected %q, got %q", sql.ErrTxDone, err)
	}
}

func (s *PackageSuite) TestTransactionWithOneConn(c *C) {
	db, tables := s.personAndAddressDB(c)
	defer dropTables(c, db, tables...)
	db.PlainDB().SetMaxOpenConns(1)
	ctx := context.Background()

	// This test sets the maximum number of connections to the DB to one. The
	// database/sql library makes use of a pool of connections to communicate
	// with the DB. Certain operations require a dedicated connection to run,
	// such as transactions.
	// This test ensures that we do not enter a deadlock when doing a behind
	// the scenes prepare for a transaction.
	selectStmt := sqlair.MustPrepare("SELECT &Person.* FROM person WHERE name = 'Mark'", Person{})
	mark := Person{20, "Mark", 1500}

	tx, err := db.Begin(ctx, nil)
	c.Assert(err, IsNil)

	q := tx.Query(ctx, selectStmt)
	defer func() {
		c.Assert(tx.Commit(), IsNil)
	}()
	iter := q.Iter()
	c.Assert(iter.Next(), Equals, true)
	p := Person{}
	c.Assert(iter.Get(&p), IsNil)
	c.Assert(mark, Equals, p)
	c.Assert(iter.Next(), Equals, false)
	c.Assert(iter.Close(), IsNil)
}

func (s *PackageSuite) TestIterMethodOrder(c *C) {
	db, tables := s.personAndAddressDB(c)
	defer dropTables(c, db, tables...)

	var p = Person{}
	stmt := sqlair.MustPrepare("SELECT &Person.* FROM person", Person{})

	// Check immediate Get.
	iter := db.Query(nil, stmt).Iter()
	err := iter.Get(&p)
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
}

func (s *PackageSuite) TestOmitOnEmpty(c *C) {
	db := sqlair.NewDB(s.db)
	createTables, err := sqlair.Prepare(`
CREATE TABLE person (
	name text,
	id integer PRIMARY KEY AUTOINCREMENT,
	address_id integer,
	email text
);
`)
	c.Assert(err, IsNil)
	err = db.Query(nil, createTables).Run()
	c.Assert(err, IsNil)
	defer dropTables(c, db, "person")

	type Person struct {
		ID       int    `db:"id, omitempty"`
		Name     string `db:"name"`
		Postcode int    `db:"address_id"`
	}
	var jim = Person{
		Name:     "Jim",
		Postcode: 500,
	}
	// Insert Jim.
	insertStmt := sqlair.MustPrepare("INSERT INTO person (*) VALUES ($Person.*);", Person{})
	err = db.Query(nil, insertStmt, &jim).Run()
	c.Assert(err, IsNil)

	// Check Jim is in the db.
	selectStmt := sqlair.MustPrepare("SELECT &Person.* FROM person WHERE name = $Person.name", Person{})
	var jimCheck = Person{}
	err = db.Query(nil, selectStmt, &jim).Get(&jimCheck)
	c.Assert(err, IsNil)
	c.Assert(jimCheck, Equals, Person{Name: "Jim", Postcode: 500, ID: 1})
}

func (s *PackageSuite) TestInsert(c *C) {
	insertPersonStmt, err := sqlair.Prepare("INSERT INTO person (*) VALUES ($Person.*)", Person{})
	c.Assert(err, IsNil)

	insertNameIDStmt, err := sqlair.Prepare("INSERT INTO person (name, id) VALUES ($Person.*)", Person{})
	c.Assert(err, IsNil)

	insertAddressIDStmt, err := sqlair.Prepare("INSERT INTO address (id) VALUES ($Person.address_id)", Person{})
	c.Assert(err, IsNil)

	insertAddressStmt, err := sqlair.Prepare("INSERT INTO address (*) VALUES ($Address.id, $Address.street, $Address.district)", Address{})
	c.Assert(err, IsNil)

	// RETURNING clauses are supported by SQLite with syntax taken from
	// postgresql. The inserted values are returned as query results.
	returningStmt, err := sqlair.Prepare("INSERT INTO address(*) VALUES($Address.*) RETURNING &Address.*", Address{})
	c.Assert(err, IsNil)

	// SELECT statements to check the inserts have worked correctly.
	selectPerson, err := sqlair.Prepare("SELECT &Person.* FROM person WHERE id = $Person.id", Person{})
	c.Assert(err, IsNil)

	selectAddress, err := sqlair.Prepare("SELECT &Address.* FROM address WHERE id = $Address.id", Address{})
	c.Assert(err, IsNil)

	// DELETE statements to remove the inserted rows.
	deletePersonStmt, err := sqlair.Prepare("DELETE FROM person WHERE id = $Person.id", Person{})
	c.Assert(err, IsNil)

	deleteAddressStmt, err := sqlair.Prepare("DELETE FROM address WHERE id = $Address.id", Address{})
	c.Assert(err, IsNil)

	deleteAddressStmtReturning, err := sqlair.Prepare("DELETE FROM address WHERE id = $Address.id RETURNING &Address.*", Address{})
	c.Assert(err, IsNil)

	db, tables := s.personAndAddressDB(c)
	defer dropTables(c, db, tables...)

	p := Person{}
	a := Address{}
	outcome := sqlair.Outcome{}
	eric := Person{Name: "Eric", ID: 60, Postcode: 7000}
	millLane := Address{Street: "Mill Lane", District: "Crazy County", ID: 7000}

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
}

func (s *PackageSuite) TestScannerValuerInterfaces(c *C) {
	type ScannerValuerStruct struct {
		ScannerValuerInt *ScannerValuerInt `db:"id"`
	}

	db, tables := s.personAndAddressDB(c)
	defer dropTables(c, db, tables...)

	stmt, err := sqlair.Prepare("SELECT address_id AS &ScannerValuerStruct.id FROM person WHERE id = $ScannerValuerStruct.id", ScannerValuerStruct{})
	c.Assert(err, IsNil)

	svs := ScannerValuerStruct{ScannerValuerInt: &ScannerValuerInt{F: 30}}
	err = db.Query(nil, stmt, svs).Get(&svs)
	c.Assert(err, IsNil)
	c.Check(svs, DeepEquals, ScannerValuerStruct{ScannerValuerInt: &ScannerValuerInt{F: 1000}})
}
