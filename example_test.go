package sqlair_test

import (
	"database/sql"
	"fmt"

	"github.com/canonical/sqlair"

	_ "github.com/mattn/go-sqlite3"
)

type Location struct {
	ID   int    `db:"room_id"`
	Name string `db:"name"`
	Team string `db:"team"`
}

type Employee struct {
	Name string `db:"name"`
	ID   int    `db:"id"`
	Team string `db:"team"`
}

func Example() {
	sqldb, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		panic(err)
	}

	db := sqlair.NewDB(sqldb)
	create := sqlair.MustPrepare(`
	CREATE TABLE person (
		name text,
		id integer,
		team text
	);
	CREATE TABLE location (
		room_id integer,
		name text,
		team text
	)`)
	err = db.Query(nil, create).Run()
	if err != nil {
		panic(err)
	}

	// Query to populate the person table.
	insertEmployee := sqlair.MustPrepare(`
		INSERT INTO person (name, id, team)
		VALUES ($Employee.name, $Employee.id, $Employee.team);`,
		Employee{},
	)

	var al = Employee{"Alastair", 1, "engineering"}
	var ed = Employee{"Ed", 2, "engineering"}
	var marco = Employee{"Marco", 3, "engineering"}
	var pedro = Employee{"Pedro", 4, "management"}
	var serdar = Employee{"Serdar", 5, "presentation engineering"}
	var joe = Employee{"Joe", 6, "marketing"}
	var ben = Employee{"Ben", 7, "legal"}
	var sam = Employee{"Sam", 8, "hr"}
	var paul = Employee{"Paul", 9, "sales"}
	var mark = Employee{"Mark", 10, "leadership"}
	var people = []Employee{ed, al, marco, pedro, serdar, joe, ben, sam, paul, mark}
	for _, p := range people {
		err := db.Query(nil, insertEmployee, p).Run()
		if err != nil {
			panic(err)
		}
	}

	// Query to populate the location table.
	insertLocation := sqlair.MustPrepare(`
		INSERT INTO location (name, room_id, team) 
		VALUES ($Location.name, $Location.room_id, $Location.team)`,
		Location{},
	)

	l1 := Location{1, "The Basement", "engineering"}
	l2 := Location{8, "Floor 2", "presentation engineering"}
	l3 := Location{10, "Floor 3", "management"}
	l4 := Location{19, "Floors 4 to 89", "hr"}
	l5 := Location{23, "Court", "legal"}
	l6 := Location{26, "The Market", "marketing"}
	l7 := Location{46, "The Bar", "Sales"}
	l8 := Location{73, "The Penthouse", "leadership"}
	var locations = []Location{l1, l2, l3, l4, l5, l6, l7, l8}
	for _, l := range locations {
		err := db.Query(nil, insertLocation, l).Run()
		if err != nil {
			panic(err)
		}
	}

	// Example 1
	// Find someone on the engineering team.

	// A map with a key type of string is used to
	// pass arguments that are not fields of structs.
	// sqlair.M is of type map[string]any but if
	// the map has a key type of string it can be used.
	selectSomeoneInTeam := sqlair.MustPrepare(`
		SELECT &Employee.*
		FROM person
		WHERE team = $M.team`,
		Employee{}, sqlair.M{},
	)

	// Get returns a single result.
	var pal = Employee{}
	team := "engineering"
	err = db.Query(nil, selectSomeoneInTeam, sqlair.M{"team": team}).Get(&pal)
	if err != nil {
		panic(err)
	}

	fmt.Printf("%s is on the %s team\n", pal.Name, team)

	// Example 2
	// Find out who is in location l1.
	selectPeopleInRoom := sqlair.MustPrepare(`
		SELECT &Employee.*
		FROM person
		WHERE team = $Location.team`,
		Location{}, Employee{},
	)

	// GetAll returns all the results.
	var roomDwellers = []Employee{}
	err = db.Query(nil, selectPeopleInRoom, l1).GetAll(&roomDwellers)
	if err != nil {
		panic(err)
	}

	for _, p := range roomDwellers {
		fmt.Printf("%s, ", p.Name)
	}
	fmt.Printf("are in %s\n", l1.Name)

	// Example 3
	// Print out who is in which room.
	selectPeopleAndRoom := sqlair.MustPrepare(`
		SELECT l.* AS &Location.*, (p.name, p.team) AS &Employee.*
		FROM location AS l
		JOIN person AS p
		ON p.team = l.team`,
		Location{}, Employee{},
	)

	// Results can be iterated through with an Iterable.
	// iter.Next prepares the next result.
	// iter.Get reads it into structs.
	// iter.Close closes the query returning any errors. It must be called after iteration is finished.
	iter := db.Query(nil, selectPeopleAndRoom).Iter()
	for iter.Next() {
		var l = Location{}
		var p = Employee{}

		err := iter.Get(&l, &p)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%s is in %s\n", p.Name, l.Name)
	}
	err = iter.Close()
	if err != nil {
		panic(err)
	}

	drop := sqlair.MustPrepare(`
		DROP TABLE person;
		DROP TABLE location;`,
	)
	err = db.Query(nil, drop).Run()
	if err != nil {
		panic(err)
	}

	// Output:
	// Ed is on the engineering team
	// Ed, Alastair, Marco, are in The Basement
	// Alastair is in The Basement
	// Ed is in The Basement
	// Marco is in The Basement
	// Serdar is in Floor 2
	// Pedro is in Floor 3
	// Sam is in Floors 4 to 89
	// Ben is in Court
	// Joe is in The Market
	// Mark is in The Penthouse
}
