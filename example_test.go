package sqlair_test

import (
	"database/sql"
	"fmt"

	"github.com/canonical/sqlair"

	_ "github.com/mattn/go-sqlite3"
)

type Location struct {
	ID   int    `db:"room_id"`
	Name string `db:"room_name"`
}

type Employee struct {
	ID     int    `db:"id"`
	TeamID int    `db:"team_id"`
	Name   string `db:"name"`
}

type Team struct {
	ID     int    `db:"id"`
	RoomID int    `db:"room_id"`
	Name   string `db:"team_name"`
}

func Example() {
	sqldb, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		panic(err)
	}

	db := sqlair.NewDB(sqldb)
	create := sqlair.MustPrepare(`
	CREATE TABLE locations (
		room_id integer,
		room_name text
	);
	CREATE TABLE employees (
		id integer,
		team_id integer,
		name text
	);
	CREATE TABLE teams (
		id integer,
		room_id integer,
		team_name text
	)`)
	err = db.Query(nil, create).Run()
	if err != nil {
		panic(err)
	}

	// Statement to populate the locations table.
	insertLocation := sqlair.MustPrepare(`
		INSERT INTO locations (room_name, room_id) 
		VALUES ($Location.room_name, $Location.room_id)`,
		Location{},
	)

	var l1 = Location{ID: 1, Name: "The Basement"}
	var l2 = Location{ID: 2, Name: "Court"}
	var l3 = Location{ID: 3, Name: "The Market"}
	var l4 = Location{ID: 4, Name: "The Bar"}
	var l5 = Location{ID: 5, Name: "The Penthouse"}
	var locations = []Location{l1, l2, l3, l4, l5}
	for _, l := range locations {
		err := db.Query(nil, insertLocation, l).Run()
		if err != nil {
			panic(err)
		}
	}

	// Statement to populate the employees table.
	insertEmployee := sqlair.MustPrepare(`
		INSERT INTO employees (id, name, team_id)
		VALUES ($Employee.id, $Employee.name, $Employee.team_id);`,
		Employee{},
	)

	var e1 = Employee{ID: 1, TeamID: 1, Name: "Alastair"}
	var e2 = Employee{ID: 2, TeamID: 1, Name: "Ed"}
	var e3 = Employee{ID: 3, TeamID: 1, Name: "Marco"}
	var e4 = Employee{ID: 4, TeamID: 2, Name: "Pedro"}
	var e5 = Employee{ID: 5, TeamID: 3, Name: "Serdar"}
	var e6 = Employee{ID: 6, TeamID: 3, Name: "Lina"}
	var e7 = Employee{ID: 7, TeamID: 4, Name: "Joe"}
	var e8 = Employee{ID: 8, TeamID: 5, Name: "Ben"}
	var e9 = Employee{ID: 9, TeamID: 5, Name: "Jenny"}
	var e10 = Employee{ID: 10, TeamID: 6, Name: "Sam"}
	var e11 = Employee{ID: 11, TeamID: 7, Name: "Melody"}
	var e12 = Employee{ID: 12, TeamID: 8, Name: "Mark"}
	var e13 = Employee{ID: 13, TeamID: 8, Name: "Gustavo"}
	var employees = []Employee{e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13}
	for _, e := range employees {
		err := db.Query(nil, insertEmployee, e).Run()
		if err != nil {
			panic(err)
		}
	}

	// Statement to populate the teams table.
	insertTeam := sqlair.MustPrepare(`
		INSERT INTO teams (id, team_name, room_id)
		VALUES ($Team.id, $Team.team_name, $Team.room_id);`,
		Team{},
	)

	var t1 = Team{ID: 1, RoomID: 1, Name: "Engineering"}
	var t2 = Team{ID: 2, RoomID: 1, Name: "Management"}
	var t3 = Team{ID: 3, RoomID: 1, Name: "Presentation Engineering"}
	var t4 = Team{ID: 4, RoomID: 2, Name: "Marketing"}
	var t5 = Team{ID: 5, RoomID: 3, Name: "Legal"}
	var t6 = Team{ID: 6, RoomID: 3, Name: "HR"}
	var t7 = Team{ID: 7, RoomID: 4, Name: "Sales"}
	var t8 = Team{ID: 8, RoomID: 5, Name: "Leadership"}
	var teams = []Team{t1, t2, t3, t4, t5, t6, t7, t8}
	for _, t := range teams {
		err := db.Query(nil, insertTeam, t).Run()
		if err != nil {
			panic(err)
		}
	}

	// Example 1
	// Find the team the employee 1 works in.

	selectSomeoneInTeam := sqlair.MustPrepare(`
		SELECT &Team.*
		FROM teams
		WHERE id = $Employee.team_id`,
		Employee{}, Team{},
	)

	// Get returns a single result.
	var team = Team{}
	err = db.Query(nil, selectSomeoneInTeam, e1).Get(&team)
	if err != nil {
		panic(err)
	}

	fmt.Printf("%s is on the %s team\n", e1.Name, team.Name)

	// Example 2
	// Find out who is in location l1 and what team they work for.
	selectPeopleInRoom := sqlair.MustPrepare(`
		SELECT e.* AS &Employee.*, t.* AS &Team.*
		FROM employees AS e, teams AS t
		WHERE t.room_id = $Location.room_id AND t.id = e.team_id`,
		Employee{}, Team{}, Location{},
	)

	// GetAll returns all the results.
	var roomDwellers = []Employee{}
	var dwellersTeams = []Team{}
	err = db.Query(nil, selectPeopleInRoom, l1).GetAll(&roomDwellers, &dwellersTeams)
	if err != nil {
		panic(err)
	}

	for i := range roomDwellers {
		fmt.Printf("%s (%s), ", roomDwellers[i].Name, dwellersTeams[i].Name)
	}
	fmt.Printf("are in %s\n", l1.Name)

	// Example 3
	// Cycle through employees until we find one in the Penthouse.

	// A map with a key type of string is used to
	// pass arguments that are not fields of structs.
	// sqlair.M is of type map[string]any but if
	// the map has a key type of string it can be used.
	selectPeopleAndRoom := sqlair.MustPrepare(`
		SELECT (e.name, t.team_name, l.room_name) AS (&M.employee_name, &M.team, &M.location)
		FROM locations AS l
		JOIN teams AS t
		ON t.room_id = l.room_id
		JOIN employees AS e
		ON e.team_id = t.id`,
		sqlair.M{},
	)

	// Results can be iterated through with an Iterable.
	// iter.Next prepares the next result.
	// iter.Get reads it into structs.
	// iter.Close closes the query returning any errors. It must be called after iteration is finished.
	iter := db.Query(nil, selectPeopleAndRoom).Iter()
	defer iter.Close()
	for iter.Next() {
		var m = sqlair.M{}
		err := iter.Get(&m)
		if err != nil {
			panic(err)
		}
		if m["location"] == "The Penthouse" {
			fmt.Printf("%s from team %s is in %s\n", m["employee_name"], m["team"], m["location"])
			break
		}
	}
	err = iter.Close()
	if err != nil {
		panic(err)
	}

	drop := sqlair.MustPrepare(`
		DROP TABLE employees;
		DROP TABLE teams;
		DROP TABLE locations;`,
	)
	err = db.Query(nil, drop).Run()
	if err != nil {
		panic(err)
	}

	// Output:
	//Alastair is on the Engineering team
	//Alastair (Engineering), Ed (Engineering), Marco (Engineering), Pedro (Management), Serdar (Presentation Engineering), Lina (Presentation Engineering), are in The Basement
	//Gustavo from team Leadership is in The Penthouse
}

func ExampleOutcome_get() {
	sqldb, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		panic(err)
	}
	db := sqlair.NewDB(sqldb)
	stmt := sqlair.MustPrepare(`
	CREATE TABLE people (
		name text,
		id integer
	);
	`)

	var outcome = sqlair.Outcome{}

	err = db.Query(nil, stmt).Get(&outcome)

	res := outcome.Result()
	s, _ := res.RowsAffected()
	fmt.Println(s)

	// Output:
	//0
}

func ExampleOutcome_iter() {
	sqldb, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		panic(err)
	}
	db := sqlair.NewDB(sqldb)
	stmt := sqlair.MustPrepare(`
	CREATE TABLE people (
		name text,
		id integer
	);
	`)

	var outcome = sqlair.Outcome{}

	// If Iter is used on a statement with no output arguments, then Outcome
	// can be passed to Iter.Get before Iter.Next is called.
	iter := db.Query(nil, stmt).Iter()
	err = iter.Get(&outcome)
	iter.Close()

	res := outcome.Result()
	s, _ := res.RowsAffected()
	fmt.Println(s)

	// Output:
	//0
}
