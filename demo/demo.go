package demo

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"

	"github.com/canonical/sqlair"
)

type Person struct {
	Name     string `db:"name"`
	Height   int    `db:"height_cm"`
	HomeTown string `db:"home_town"`
}

type Place struct {
	Name       string `db:"town_name"`
	Population int    `db:"population"`
}

func example() error {
	sqldb, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return err
	}
	db := sqlair.NewDB(sqldb)
	createPerson := sqlair.MustPrepare(`
		CREATE TABLE people (
			name text,
			height_cm integer,
			home_town text
		);`,
	)
	createPlace := sqlair.MustPrepare(`
		CREATE TABLE location (
			town_name text,
			population integer
		);`,
	)
	insertPerson := sqlair.MustPrepare(`
		INSERT INTO people (name, height_cm, home_town)
		VALUES ( $Person.name, $Person.height_cm, $Person.home_town);`,
		Person{},
	)
	insertPlace := sqlair.MustPrepare(`
		INSERT INTO location (town_name, population)
		VALUES ( $Place.town_name, $Place.population);`,
		Place{},
	)
	tallerThan := sqlair.MustPrepare(`
		SELECT &Person.*
		FROM people AS p
		WHERE height_cm > $Person.height_cm`,
		Person{},
	)
	tallerCity := sqlair.MustPrepare(`
		SELECT p.* AS &Person.*, l.* AS &Place.*
		FROM people AS p, location AS l
		WHERE p.home_town = l.town_name
		AND p.height_cm > $Person.height_cm;`,
		Person{},
		Place{},
	)
	var people = []Person{{"Jim", 150, "Kabul"}, {"Saba", 162, "Berlin"}, {"Dave", 169, "Brasília"}, {"Sophie", 174, "Berlin"}, {"Kiri", 168, "Cape Town"}}
	var places = []Place{{"Kabul", 13000000}, {"Berlin", 3677472}, {"Brasília", 3039444}, {"Cape Town", 4710000}}

	// Create the tables
	err = db.Query(context.Background(), createPerson).Run()
	if err != nil {
		return err
	}
	err = db.Query(context.Background(), createPlace).Run()
	if err != nil {
		return err
	}

	// Insert the people and places
	for _, person := range people {
		err := db.Query(context.Background(), insertPerson, person).Run()
		if err != nil {
			return err
		}
	}

	for _, place := range places {
		err := db.Query(context.Background(), insertPlace, place).Run()
		if err != nil {
			return err
		}
	}

	// Find people taller than Jim
	jim := people[0]
	q := db.Query(context.Background(), tallerThan, jim)
	iter := q.Iter()
	for iter.Next() {
		p := Person{}
		if !iter.Decode(&p) {
			break
		}
		fmt.Printf("%s is taller than %s.\n", p.Name, jim.Name)
	}
	err = iter.Close()
	if err != nil {
		return err
	}

	// Find cities with people taller than Jim
	tallCities := []Place{}
	tallPeople := []Person{}
	err = db.Query(context.Background(), tallerCity, jim).All(&tallCities, &tallPeople)
	if err != nil {
		return err
	}
	fmt.Printf("This is a list of cities with people taller than Jim: %v\n", tallCities)
	fmt.Printf("This is a list of people taller than Jim: %v\n", tallPeople)
	return nil
}

func main() {
	err := example()
	if err != nil {
		panic(err)
	}
}
