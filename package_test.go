package sqlair_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	. "gopkg.in/check.v1"

	"github.com/canonical/sqlair"
)

// Hook up gocheck into the "go test" runner.
func TestPackage(t *testing.T) { TestingT(t) }

type PackageSuite struct{}

var _ = Suite(&PackageSuite{})

func (s *PackageSuite) TearDownTest(c *C) {
	// Try to run finalizers by calling GC several times.
	for i := 0; i <= 10; i++ {
		runtime.GC()
		time.Sleep(0)
	}

	stmtRegistryMutex.Lock()
	defer stmtRegistryMutex.Unlock()

	// Asssert that all the open statements were closed.
	for sPtr, query := range openedStmts[c.TestName()] {
		c.Check(closedStmts[c.TestName()][sPtr], Equals, true,
			Commentf("%s: failed to close statement: %s", c.TestName(), query))
	}
}

func (s *PackageSuite) TearDownSuite(c *C) {
	stmtRegistryMutex.Lock()
	defer stmtRegistryMutex.Unlock()

	// Reset state.
	closedStmts = map[string]map[uintptr]bool{}
	openedStmts = map[string]map[uintptr]string{}
}

func setupDB(testName string) (*sql.DB, error) {
	return sql.Open("sqlite3_stmtChecked", "file:test.db?cache=shared&mode=memory&testName="+testName)
}

func createExampleDB(c *C, createTables string, inserts []string) (*sql.DB, error) {
	db, err := setupDB(c.TestName())
	c.Assert(err, IsNil)

	_, err = db.Exec(createTables)
	c.Assert(err, IsNil)
	for _, insert := range inserts {
		_, err := db.Exec(insert)
		c.Assert(err, IsNil)
	}
	return db, nil
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
	ID         int    `db:"id"`
	Fullname   string `db:"name"`
	PostalCode int    `db:"address_id"`
}

type Manager Person

type District struct{}

type CustomMap map[string]any

func personAndAddressDB(c *C) ([]string, *sql.DB, error) {

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
	inserts := []string{
		"INSERT INTO person VALUES ('Fred', 30, 1000, 'fred@email.com');",
		"INSERT INTO person VALUES ('Mark', 20, 1500, 'mark@email.com');",
		"INSERT INTO person VALUES ('Mary', 40, 3500, 'mary@email.com');",
		"INSERT INTO person VALUES ('James', 35, 4500, 'james@email.com');",
		"INSERT INTO address VALUES (1000, 'Happy Land', 'Main Street');",
		"INSERT INTO address VALUES (1500, 'Sad World', 'Church Road');",
		"INSERT INTO address VALUES (3500, 'Ambivalent Commons', 'Station Lane');",
	}

	db, err := createExampleDB(c, createTables, inserts)
	c.Assert(err, IsNil)
	return []string{"person", "address"}, db, nil
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
		query:    "SELECT (name, address_id) AS (&Person.*) FROM person WHERE address_id IN ( $Manager.address_id, $Address.district )",
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

	tables, sqldb, err := personAndAddressDB(c)
	if err != nil {
		c.Fatal(err)
	}

	db := sqlair.NewDB(sqldb)
	defer dropTables(c, db, tables...)

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
		err:     "cannot get result: need map or pointer to struct, got nil",
	}, {
		summary: "nil pointer parameter",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		outputs: []any{(*Person)(nil)},
		err:     "cannot get result: got nil pointer",
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
		err:     `cannot get result: type "Address" does not appear in query, have: Person`,
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
		err:     `cannot get result: type "Person" found in query but not passed to get`,
	}, {
		summary: "multiple of the same type",
		query:   "SELECT * AS &Person.* FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		outputs: []any{&Person{}, &Person{}},
		err:     `cannot get result: type "Person" provided more than once, rename one of them`,
	}, {
		summary: "multiple of the same type",
		query:   "SELECT name AS &M.* FROM person",
		types:   []any{sqlair.M{}},
		inputs:  []any{},
		outputs: []any{&sqlair.M{}, sqlair.M{}},
		err:     `cannot get result: type "M" provided more than once, rename one of them`,
	}, {
		summary: "output expr in a with clause",
		query: `WITH averageID(avgid) AS (SELECT &Person.id FROM person)
		        SELECT id FROM person, averageID WHERE id > averageID.avgid LIMIT 1`,
		types:   []any{Person{}},
		inputs:  []any{},
		outputs: []any{&Person{}},
		err:     `cannot get result: query uses "&Person" outside of result context`,
	}}

	tables, sqldb, err := personAndAddressDB(c)
	if err != nil {
		c.Fatal(err)
	}

	db := sqlair.NewDB(sqldb)
	defer dropTables(c, db, tables...)

	for _, t := range tests {
		stmt, err := sqlair.Prepare(t.query, t.types...)
		if err != nil {
			c.Errorf("\ntest %q failed (Prepare):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
			continue
		}

		iter := db.Query(nil, stmt, t.inputs...).Iter()
		defer iter.Close()
		if !iter.Next() {
			c.Fatalf("\ntest %q failed (Get):\ninput: %s\nerr: no rows returned\n", t.summary, t.query)
		}
		err = iter.Get(t.outputs...)
		c.Assert(err, ErrorMatches, t.err,
			Commentf("\ntest %q failed:\ninput: %s\noutputs: %s\n", t.summary, t.query, t.outputs))
		err = iter.Close()
		if err != nil {
			c.Errorf("\ntest %q failed (Close):\ninput: %s\nerr: %s\n", t.summary, t.query, err)
		}
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
		ID         I `db:"id"`
		Fullname   S `db:"name"`
		PostalCode J `db:"address_id"`
	}
	type NullGuy struct {
		ID         sql.NullInt64  `db:"id"`
		Fullname   sql.NullString `db:"name"`
		PostalCode sql.NullInt64  `db:"address_id"`
	}
	type ScannerDude struct {
		ID         ScannerInt    `db:"id"`
		Fullname   ScannerString `db:"name"`
		PostalCode ScannerInt    `db:"address_id"`
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
		outputs:  []any{&Person{ID: 5, PostalCode: 10}},
		expected: []any{&Person{Fullname: "Nully", ID: 0, PostalCode: 0}},
	}, {
		summary:  "reading nulls with custom types",
		query:    `SELECT &PersonWithStrangeTypes.* FROM person WHERE name = "Nully"`,
		types:    []any{PersonWithStrangeTypes{}},
		inputs:   []any{},
		outputs:  []any{&PersonWithStrangeTypes{ID: 5, PostalCode: 10}},
		expected: []any{&PersonWithStrangeTypes{Fullname: "Nully", ID: 0, PostalCode: 0}},
	}, {
		summary:  "regular nulls",
		query:    `SELECT &NullGuy.* FROM person WHERE name = "Nully"`,
		types:    []any{NullGuy{}},
		inputs:   []any{},
		outputs:  []any{&NullGuy{}},
		expected: []any{&NullGuy{Fullname: sql.NullString{Valid: true, String: "Nully"}, ID: sql.NullInt64{Valid: false}, PostalCode: sql.NullInt64{Valid: false}}},
	}, {
		summary:  "nulls with custom scan type",
		query:    `SELECT &ScannerDude.* FROM person WHERE name = "Nully"`,
		types:    []any{ScannerDude{}},
		inputs:   []any{},
		outputs:  []any{&ScannerDude{}},
		expected: []any{&ScannerDude{Fullname: ScannerString{SS: "ScannerString scanned well!"}, ID: ScannerInt{SI: 666}, PostalCode: ScannerInt{SI: 666}}},
	}}

	tables, sqldb, err := personAndAddressDB(c)
	if err != nil {
		c.Fatal(err)
	}

	db := sqlair.NewDB(sqldb)
	defer dropTables(c, db, tables...)

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

	tables, sqldb, err := personAndAddressDB(c)
	if err != nil {
		c.Fatal(err)
	}

	db := sqlair.NewDB(sqldb)
	defer dropTables(c, db, tables...)

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

	tables, sqldb, err := personAndAddressDB(c)
	if err != nil {
		c.Fatal(err)
	}

	db := sqlair.NewDB(sqldb)
	defer dropTables(c, db, tables...)

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
}

func (s *PackageSuite) TestErrNoRows(c *C) {
	tables, sqldb, err := personAndAddressDB(c)
	if err != nil {
		c.Fatal(err)
	}

	db := sqlair.NewDB(sqldb)
	defer dropTables(c, db, tables...)

	stmt := sqlair.MustPrepare("SELECT * AS &Person.* FROM person WHERE id=12312", Person{})
	err = db.Query(nil, stmt).Get(&Person{})
	if !errors.Is(err, sqlair.ErrNoRows) {
		c.Errorf("expected %q, got %q", sqlair.ErrNoRows, err)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		c.Errorf("expected %q, got %q", sql.ErrNoRows, err)
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
		expected: []any{&[]*Person{}},
	}, {
		summary:  "select into maps",
		query:    "SELECT &M.name, &CustomMap.id FROM person WHERE name = 'Mark'",
		types:    []any{sqlair.M{}, CustomMap{}},
		inputs:   []any{},
		slices:   []any{&[]sqlair.M{}, &[]CustomMap{}},
		expected: []any{&[]sqlair.M{{"name": "Mark"}}, &[]CustomMap{{"id": int64(20)}}},
	}}

	tables, sqldb, err := personAndAddressDB(c)
	if err != nil {
		c.Fatal(err)
	}

	db := sqlair.NewDB(sqldb)
	defer dropTables(c, db, tables...)

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

	tables, sqldb, err := personAndAddressDB(c)
	if err != nil {
		c.Fatal(err)
	}

	db := sqlair.NewDB(sqldb)
	defer dropTables(c, db, tables...)

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
}

func (s *PackageSuite) TestRun(c *C) {
	tables, sqldb, err := personAndAddressDB(c)
	c.Assert(err, IsNil)

	var jim = Person{
		ID:         70,
		Fullname:   "Jim",
		PostalCode: 500,
	}

	db := sqlair.NewDB(sqldb)
	defer dropTables(c, db, tables...)

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
}

func (s *PackageSuite) TestOutcome(c *C) {
	tables, sqldb, err := personAndAddressDB(c)
	c.Assert(err, IsNil)

	var jim = Person{
		ID:         70,
		Fullname:   "Jim",
		PostalCode: 500,
	}

	db := sqlair.NewDB(sqldb)
	defer dropTables(c, db, tables...)

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
}

func (s *PackageSuite) TestQueryMultipleRuns(c *C) {
	allOutput := &[]*Person{}
	allExpected := &[]*Person{&Person{30, "Fred", 1000}, &Person{20, "Mark", 1500}, &Person{40, "Mary", 3500}, &Person{35, "James", 4500}}

	iterOutputs := []any{&Person{}, &Person{}, &Person{}, &Person{}}
	iterExpected := []any{&Person{30, "Fred", 1000}, &Person{20, "Mark", 1500}, &Person{40, "Mary", 3500}, &Person{35, "James", 4500}}

	oneOutput := &Person{}
	oneExpected := &Person{30, "Fred", 1000}

	tables, sqldb, err := personAndAddressDB(c)
	c.Assert(err, IsNil)

	db := sqlair.NewDB(sqldb)
	defer dropTables(c, db, tables...)

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
}

func (s *PackageSuite) TestTransactions(c *C) {
	tables, sqldb, err := personAndAddressDB(c)
	c.Assert(err, IsNil)

	selectStmt := sqlair.MustPrepare("SELECT &Person.* FROM person WHERE address_id = $Person.address_id", Person{})
	insertStmt := sqlair.MustPrepare("INSERT INTO person VALUES ( $Person.name, $Person.id, $Person.address_id, 'fred@email.com');", Person{})
	var derek = Person{ID: 85, Fullname: "Derek", PostalCode: 8000}
	ctx := context.Background()

	db := sqlair.NewDB(sqldb)
	defer dropTables(c, db, tables...)

	tx, err := db.Begin(ctx, nil)
	c.Assert(err, IsNil)

	// Insert Derek then rollback.
	err = tx.Query(ctx, insertStmt, &derek).Run()
	c.Assert(err, IsNil)
	err = tx.Rollback()
	c.Assert(err, IsNil)

	// Check Derek isnt in db.
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

// Test that when preparing a statement inside a transaction it can still be prepared on the db directly and that it is
// not closed along with the transaction.
func (s *PackageSuite) TestStatementTXReuse(c *C) {
	sqldb, err := setupDB(c.TestName())
	c.Assert(err, IsNil)

	db := sqlair.NewDB(sqldb)

	// Create a statement and run it on a transaction.
	selectStmt := sqlair.MustPrepare(`SELECT 'hello'`)
	tx, err := db.Begin(nil, nil)
	c.Assert(err, IsNil)
	q := tx.Query(nil, selectStmt)
	c.Assert(q.Run(), IsNil)
	c.Assert(tx.Commit(), IsNil)

	// Run the same existing statement outside the transaction.
	q = db.Query(nil, selectStmt)
	c.Assert(q.Run(), IsNil)
}

func (s *PackageSuite) TestTransactionErrors(c *C) {
	tables, sqldb, err := personAndAddressDB(c)
	c.Assert(err, IsNil)

	db := sqlair.NewDB(sqldb)
	defer dropTables(c, db, tables...)

	insertStmt := sqlair.MustPrepare("INSERT INTO person VALUES ($Person.name, $Person.id, $Person.address_id, 'fred@email.com');", Person{})
	var derek = Person{ID: 85, Fullname: "Derek", PostalCode: 8000}
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

// TestPreparedStmtCaching checks that the cache of statements prepared on databases behaves
// as expected.
func (s *PackageSuite) TestPreparedStmtCaching(c *C) {
	// Get cache variables.
	stmtDBCache, dbStmtCache, cacheMutex := sqlair.Cache()

	// checkStmtCache is a helper function to check if a prepared statement is
	// cached or not.
	checkStmtCache := func(dbID int64, sID int64, inCache bool) {
		cacheMutex.RLock()
		defer cacheMutex.RUnlock()
		dbCache, ok1 := stmtDBCache[sID]
		var ok2 bool
		if ok1 {
			_, ok2 = dbCache[dbID]
		}
		_, ok3 := dbStmtCache[dbID][sID]
		c.Assert(ok2, Equals, inCache)
		c.Assert(ok3, Equals, inCache)
	}

	// checkDBNotInCache is a helper function to check a db is not mentioned in
	// the cache.
	checkDBNotInCache := func(dbID int64) {
		cacheMutex.RLock()
		defer cacheMutex.RUnlock()
		for _, dbCache := range stmtDBCache {
			_, ok := dbCache[dbID]
			c.Assert(ok, Equals, false)
		}
		_, ok := dbStmtCache[dbID]
		c.Assert(ok, Equals, false)
	}

	// checkCacheEmpty asserts both the sides of the cache are empty.
	checkCacheEmpty := func() {
		cacheMutex.RLock()
		defer cacheMutex.RUnlock()
		c.Assert(dbStmtCache, HasLen, 0)
		c.Assert(stmtDBCache, HasLen, 0)
	}

	// For a Statement or DB to be removed from the cache it needs to go out of
	// scope and be garbage collected. Because of this, the tests below make
	// extensive use of functions to "forget" statements and databases.

	q1 := `SELECT &Person.*	FROM person WHERE name = "Fred"`
	q2 := `SELECT &Person.* FROM person WHERE name = "Mark"`
	p := Person{}

	// createAndCacheStmt takes a db and prepares a statement on it.
	createAndCacheStmt := func(db *sqlair.DB) (stmtID int64) {
		// Create stmt.
		stmt, err := sqlair.Prepare(q1, Person{})
		c.Assert(err, IsNil)

		// Start a query with stmt on db. This will prepare the stmt on the db.
		c.Assert(db.Query(nil, stmt).Get(&p), IsNil)
		// Check that stmt is now in the cache.
		checkStmtCache(db.CacheID(), stmt.CacheID(), true)
		return stmt.CacheID()
	}

	// testStmtsOnDB prepares a given statement on the db then creates a second
	// statement inside another function and checks it has been cleared from
	// the cache on garbage collection.
	testStmtsOnDB := func(db *sqlair.DB, stmt *sqlair.Statement) {
		// Start a query with stmt on db. This will prepare stmt on db.
		c.Assert(db.Query(nil, stmt).Get(&p), IsNil)
		// Check the stmt now is in the cache.
		checkStmtCache(db.CacheID(), stmt.CacheID(), true)

		// Run createAndCacheStmt and check that once the function has finished
		// the stmt it created is not in the cache.
		stmt2ID := createAndCacheStmt(db)
		// Run the garbage collector and wait one millisecond for the finalizer to finish.
		runtime.GC()
		time.Sleep(1 * time.Millisecond)
		checkStmtCache(db.CacheID(), stmt2ID, false)
	}

	// createDBAndTestStmt opens a new database and runs testStmtsOnDB on it.
	createDBAndTestStmt := func(stmt *sqlair.Statement) (dbID int64) {
		// Create db.
		tables, sqldb, err := personAndAddressDB(c)
		c.Assert(err, IsNil)
		db := sqlair.NewDB(sqldb)
		defer dropTables(c, db, tables...)
		// Test stmt.
		testStmtsOnDB(db, stmt)
		return db.CacheID()
	}

	// createStmtAndTestOnDBs creates a statement then runs createDBAndTestStmt
	// twice. It then checks that the stmts prepared on the databases have been
	// cleared from the cache once createDBAndTestStmt is finished.
	createStmtAndTestOnDBs := func() {
		// Create stmt.
		stmt, err := sqlair.Prepare(q2, Person{})
		c.Assert(err, IsNil)
		db1ID := createDBAndTestStmt(stmt)
		db2ID := createDBAndTestStmt(stmt)
		// Run the garbage collector and wait one millisecond for the finalizer to finish.
		runtime.GC()
		time.Sleep(1 * time.Millisecond)
		checkDBNotInCache(db1ID)
		checkDBNotInCache(db2ID)
	}

	// Run the functions above.
	createStmtAndTestOnDBs()
	// Run the garbage collector and wait one millisecond for the finalizer to finish.
	runtime.GC()
	time.Sleep(1 * time.Millisecond)
	checkCacheEmpty()
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

func JujuStoreLeaseDB(c *C) ([]string, *sql.DB, error) {
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
	tables := []string{`lease`, `lease_type`}

	inserts := []string{
		"INSERT INTO lease VALUES ('uuid1', 'name1', 'holder1', 1, 'type_id1');",
		"INSERT INTO lease VALUES ('uuid2', 'name2', 'holder2', 4, 'type_id1');",
		"INSERT INTO lease VALUES ('uuid3', 'name3', 'holder3', 7, 'type_id2');",
		"INSERT INTO lease_type VALUES ('type_id1', 'type1');",
		"INSERT INTO lease_type VALUES ('type_id2', 'type2');",
	}

	db, err := createExampleDB(c, createTables, inserts)
	c.Assert(err, IsNil)
	return tables, db, nil

}

func (s *PackageSuite) TestIterMethodOrder(c *C) {
	tables, sqldb, err := personAndAddressDB(c)
	if err != nil {
		c.Fatal(err)
	}

	db := sqlair.NewDB(sqldb)
	defer dropTables(c, db, tables...)

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
SELECT (t.type, l.model_uuid, l.name) AS (&JujuLeaseKey.*), (l.holder, l.expiry) AS (&JujuLeaseInfo.*)
FROM   lease l JOIN lease_type t ON l.lease_type_id = t.id
WHERE  t.type = $JujuLeaseKey.type
AND    l.model_uuid = $JujuLeaseKey.model_uuid`,
		types:    []any{JujuLeaseKey{}, JujuLeaseInfo{}},
		inputs:   []any{JujuLeaseKey{Namespace: "type1", ModelUUID: "uuid1"}},
		outputs:  [][]any{{&JujuLeaseKey{}, &JujuLeaseInfo{}}},
		expected: [][]any{{&JujuLeaseKey{Namespace: "type1", ModelUUID: "uuid1", Lease: "name1"}, &JujuLeaseInfo{Holder: "holder1", Expiry: 1}}},
	}}

	tables, sqldb, err := JujuStoreLeaseDB(c)
	if err != nil {
		c.Fatal(err)
	}

	db := sqlair.NewDB(sqldb)
	defer dropTables(c, db, tables...)

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
	}
}

// Because the Query struct did not contain references to either Statement or
// DB, if either of those would go out of scope the underlying sql.Stmt would
// be closed.
func (s *PackageSuite) TestRaceConditionFinalizer(c *C) {
	var q *sqlair.Query
	// Drop all the values except the query itself.
	func() {
		sqldb, err := setupDB(c.TestName())
		c.Assert(err, IsNil)

		db := sqlair.NewDB(sqldb)

		selectStmt := sqlair.MustPrepare(`SELECT 'hello'`)
		q = db.Query(nil, selectStmt)
	}()

	// Try to run their finalizers by calling GC several times.
	for i := 0; i <= 10; i++ {
		runtime.GC()
		time.Sleep(0)
	}

	// Assert that sql.Stmt was not closed early.
	c.Assert(q.Run(), IsNil)
}
func (s *PackageSuite) TestRaceConditionFinalizerTX(c *C) {
	var q *sqlair.Query
	// Drop all the values except the query itself.
	func() {
		sqldb, err := setupDB(c.TestName())
		c.Assert(err, IsNil)

		db := sqlair.NewDB(sqldb)

		selectStmt := sqlair.MustPrepare(`SELECT 'hello'`)
		tx, err := db.Begin(nil, nil)
		c.Assert(err, IsNil)
		q = tx.Query(nil, selectStmt)
	}()

	// Try to run their finalizers by calling GC several times.
	for i := 0; i <= 10; i++ {
		runtime.GC()
		time.Sleep(0)
	}

	// Assert that sql.Stmt was not closed early.
	c.Assert(q.Run(), IsNil)
}
