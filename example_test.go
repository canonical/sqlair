package sqlair_test

import (
	"database/sql"
	"fmt"

	"github.com/canonical/sqlair"

	_ "github.com/mattn/go-sqlite3"
)

type Employee struct {
	ID   int    `db:"id"`
	Team string `db:"team_name"`
	Name string `db:"name"`
}

func Example() {
	sqldb, err := sql.Open("sqlite3", "file:example.db?cache=shared&mode=memory")
	if err != nil {
		panic(err)
	}

	db := sqlair.NewDB(sqldb)

	createEmployees := sqlair.MustPrepare(`CREATE TABLE employees (id integer, team_name string, name text);`)
	err = db.Query(nil, createEmployees).Run()
	if err != nil {
		panic(err)
	}

	e1 := Employee{ID: 1, Team: "Engineering", Name: "Alastair"}
	e2 := Employee{ID: 2, Team: "Engineering", Name: "Ed"}
	e3 := Employee{ID: 3, Team: "Management", Name: "Marco"}
	e4 := Employee{ID: 4, Team: "Management", Name: "Pedro"}
	e5 := Employee{ID: 5, Team: "HR", Name: "Igor"}
	employees := []Employee{e1, e2, e3, e4, e5}

	// Statement to populate the employees table.
	insertEmployee, err := sqlair.Prepare(
		`INSERT INTO employees (id, name, team_name) VALUES ($Employee.id, $Employee.name, $Employee.team_name);`,
		Employee{},
	)
	if err != nil {
		panic(err)
	}
	for _, e := range employees {
		err := db.Query(nil, insertEmployee, e).Run()
		if err != nil {
			panic(err)
		}
	}

	// Select an employee from the database.
	selectEmployee, err := sqlair.Prepare(`SELECT &Employee.* FROM employees`, Employee{})
	if err != nil {
		panic(err)
	}
	// Get fetches a single result from the database and scans it into the
	// arguement. To fetch all results from the database use GetAll, and to
	// iterate through the results row by row, use Iter.
	employee := Employee{}
	err = db.Query(nil, selectEmployee).Get(&employee)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Employee %s has ID %d.\n", employee.Name, employee.ID)

	// Select the team name of an employee with a given ID.
	selectTeam, err := sqlair.Prepare(
		`SELECT &Employee.team_name FROM employees WHERE id = $Employee.id`,
		Employee{},
	)
	if err != nil {
		panic(err)
	}
	// Get the team of employee with ID 4
	employee4 := Employee{ID: 4}
	err = db.Query(nil, selectTeam, employee4).Get(&employee4)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Employee %s is in the %s team.\n", employee.Name, employee.Team)

	dropEmployees := sqlair.MustPrepare(`DROP TABLE employees`)
	err = db.Query(nil, dropEmployees).Run()
	if err != nil {
		panic(err)
	}

	// Output:
	// Employee Alastair has ID 1.
	// Employee Alastair is in the Engineering team.
}

func ExampleOutcome_get() {
	sqldb, err := sql.Open("sqlite3", "file:exampleoutcomeget.db?cache=shared&mode=memory")
	if err != nil {
		panic(err)
	}
	db := sqlair.NewDB(sqldb)
	stmt := sqlair.MustPrepare(`CREATE TABLE people (name text, id integer);`)

	outcome := sqlair.Outcome{}

	err = db.Query(nil, stmt).Get(&outcome)

	res := outcome.Result()
	s, err := res.RowsAffected()
	if err != nil {
		panic(err)
	}
	fmt.Println(s)

	dropPeople := sqlair.MustPrepare(`DROP TABLE people`)
	err = db.Query(nil, dropPeople).Run()
	if err != nil {
		panic(err)
	}
	// Output:
	// 0
}

func ExampleOutcome_iter() {
	sqldb, err := sql.Open("sqlite3", "file:exampleoutcomeiter.db?cache=shared&mode=memory")
	if err != nil {
		panic(err)
	}
	db := sqlair.NewDB(sqldb)
	stmt := sqlair.MustPrepare(`CREATE TABLE people (name text, id integer);`)

	outcome := sqlair.Outcome{}

	// If Iter is used on a statement with no output arguments, then Outcome
	// can be passed to Iter.Get before Iter.Next is called.
	iter := db.Query(nil, stmt).Iter()
	err = iter.Get(&outcome)
	if err != nil {
		panic(err)
	}
	err = iter.Close()
	if err != nil {
		panic(err)
	}

	res := outcome.Result()
	s, err := res.RowsAffected()
	if err != nil {
		panic(err)
	}
	fmt.Println(s)

	dropPeople := sqlair.MustPrepare(`DROP TABLE people`)
	err = db.Query(nil, dropPeople).Run()
	if err != nil {
		panic(err)
	}
	// Output:
	// 0
}

func ExampleM() {
	sqldb, err := sql.Open("sqlite3", "file:examplem.db?cache=shared&mode=memory")
	if err != nil {
		panic(err)
	}
	db := sqlair.NewDB(sqldb)

	stmt := sqlair.MustPrepare(`CREATE TABLE people (name text, id integer);`)
	err = db.Query(nil, stmt).Run()
	if err != nil {
		panic(err)
	}

	// sqlair.M has type map[string]any.
	m := sqlair.M{}
	m["name"] = "Fred"
	m["id"] = 30

	stmt, err = sqlair.Prepare(
		`INSERT INTO people (name, id) VALUES ($M.name, $M.id)`,
		sqlair.M{},
	)
	if err != nil {
		panic(err)
	}
	// This will insert Fred with id 30 into the database.
	err = db.Query(nil, stmt, m).Run()
	if err != nil {
		panic(err)
	}

	// Maps can be used in queries, the only requisite is that they have a
	// name, and a key type of string.
	type MyIntMap map[string]int

	stmt, err = sqlair.Prepare(
		`SELECT &MyIntMap.id FROM people WHERE name = $M.name`,
		sqlair.M{}, MyIntMap{},
	)
	// Select the id of Fred into mm["id"].
	// Maps do not have to be passed as a pointer.
	mm := MyIntMap{}
	err = db.Query(nil, stmt, m).Get(mm)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Fred's id is %d", mm["id"])

	dropPeople := sqlair.MustPrepare(`DROP TABLE people`)
	err = db.Query(nil, dropPeople).Run()
	if err != nil {
		panic(err)
	}
	// Output:
	// Fred's id is 30
}
