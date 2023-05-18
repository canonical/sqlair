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
	panic("this does run")
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

	insertEmployee := sqlair.MustPrepare("INSERT INTO person (name, id, team) VALUES ($Employee.name, $Employee.id, $Employee.team);", Employee{})

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
	var gus = Employee{"Gustavo", 11, "leadership"}
	var people = []Employee{ed, al, marco, pedro, serdar, joe, ben, sam, paul, mark, gus}
	for _, p := range people {
		err := db.Query(nil, insertEmployee, p).Run()
		if err != nil {
			panic(err)
		}
	}

	insertLocation := sqlair.MustPrepare("INSERT INTO location (name, room_id, team) VALUES ($Location.name, $Location.room_id, $Location.team)", Location{})

	l1 := Location{1, "Basement", "engineering"}
	l2 := Location{34, "Floor 2", "presentation engineering"}
	l3 := Location{19, "Floor 3", "management"}
	l4 := Location{66, "The Market", "marketing"}
	l5 := Location{7, "Court", "legal"}
	l6 := Location{9, "Floors 4 to 89", "hr"}
	l7 := Location{73, "Bar", "Sales"}
	l8 := Location{32, "Penthouse", "leadership"}
	var locations = []Location{l1, l2, l3, l4, l5, l6, l7, l8}
	for _, l := range locations {
		err := db.Query(nil, insertLocation, l).Run()
		if err != nil {
			panic(err)
		}
	}

	// Find someone on the engineering team.
	selectEngineer := sqlair.MustPrepare(`
		SELECT &Employee.*
		FROM person
		WHERE team = "engineering"`,
		Employee{})

	q := db.Query(nil, selectEngineer)

	// Get returns a single result.
	var pal = Employee{}
	err = q.Get(&pal)
	if err != nil {
		panic(err)
	}

	fmt.Printf("%s is on the engineering team.\n", pal.Name)

	// Find out who is in location l1.
	selectPeopleInRoom := sqlair.MustPrepare(`
		SELECT &Employee.*
		FROM person
		WHERE team = $Location.team`,
		Location{}, Employee{})

	q = db.Query(nil, selectPeopleInRoom, l1)

	// GetAll returns all the results.
	var roomDwellers = []Employee{}
	err = q.GetAll(&roomDwellers)
	if err != nil {
		panic(err)
	}

	for _, p := range roomDwellers {
		fmt.Printf("%s, ", p.Name)
	}
	fmt.Println("are in room l1.")

	// Print out who is in which room.
	selectPeopleAndRoom := sqlair.MustPrepare(`
		SELECT l.* AS &Location.*, (p.name, p.team) AS &Employee.*
		FROM location AS l
			JOIN person AS p
			ON p.team = l.team`,
		Location{}, Employee{},
	)

	q = db.Query(nil, selectPeopleAndRoom)

	// Results can be iterated through with an Iterable.
	// iter.Next prepares the next result.
	// iter.Get reads it into structs.
	// iter.Close closes the query returning any errors
	// 	it must be called after iteration is finished.
	iter := q.Iter()
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

	drop := sqlair.MustPrepare("DROP TABLE person; DROP TABLE location;")
	err = db.Query(nil, drop).Run()
	if err != nil {
		panic(err)
	}
}
