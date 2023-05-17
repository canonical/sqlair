package example

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

type Person struct {
	Name string `db:"name"`
	ID   int    `db:"id"`
	Team string `db:"team"`
}

func example() {
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

	insertPerson := sqlair.MustPrepare("INSERT INTO person (name, id, team) VALUES ($Person.name, $Person.id, $Person.team);", Person{})

	var al = Person{"Alastair", 1, "engineering"}
	var ed = Person{"Ed", 2, "engineering"}
	var marco = Person{"Marco", 3, "engineering"}
	var pedro = Person{"Pedro", 4, "management"}
	var serdar = Person{"Serdar", 5, "presentation engineering"}
	var joe = Person{"Joe", 6, "marketing"}
	var ben = Person{"Ben", 7, "legal"}
	var sam = Person{"Sam", 8, "hr"}
	var paul = Person{"Paul", 9, "sales"}
	var mark = Person{"Mark", 10, "leadership"}
	var gus = Person{"Gustavo", 11, "leadership"}
	var people = []Person{ed, al, marco, pedro, serdar, joe, ben, sam, paul, mark, gus}
	for _, p := range people {
		err := db.Query(nil, insertPerson, p).Run()
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
		SELECT &Person.*
		FROM person
		WHERE team = "engineering"`,
		Person{})

	q := db.Query(nil, selectEngineer)

	var pal = Person{}
	// Get returns a single result.
	err = q.Get(&pal)
	if err != nil {
		panic(err)
	}

	fmt.Printf("%s is on the engineering team.\n", pal.Name)

	// Find out who is in location l1.
	selectPeopleInRoom := sqlair.MustPrepare(`
		SELECT &Person.*
		FROM person
		WHERE team = $Location.team`,
		Location{}, Person{})

	q = db.Query(nil, selectPeopleInRoom, l1)

	var roomDwellers = []Person{}
	// GetAll returns all the results
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
		SELECT l.* AS &Location.*, (p.name, p.team) AS &Person.*
		FROM location AS l
			JOIN person AS p
			ON p.team = l.team`,
		Location{}, Person{},
	)

	q = db.Query(nil, selectPeopleAndRoom)

	// Results can be iterated through with Iter()
	iter := q.Iter()
	for iter.Next() {
		var l = Location{}
		var p = Person{}

		err := iter.Get(&l, &p)
		if err != nil {
			panic(err)
		}

		fmt.Printf("%s is in %s\n", p.Name, l.Name)
	}

	drop := sqlair.MustPrepare("DROP TABLE person; DROP TABLE location;")
	err = db.Query(nil, drop).Run()
	if err != nil {
		panic(err)
	}
}
