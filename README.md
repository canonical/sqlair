# SQLair

SQLair is a package for Go that acts as a compatibility layer between Go and SQL databases. It allows for easy mapping between Go objects and query arguments and results.

SQLair allows you to write SQL with SQLair input/output expressions included where mapping is needed. These expressions indicate the Go objects to map the database arguments/results into. SQLair is not an ORM and will leave all parts of the query outside of the expressions untouched.

# Usage
The first step when using SQLair is to tag your structs. The "`db`" tag is used to map between the database column names and struct fields.

For example:
```Go
type Person struct {
	Name	string	`db:"name_col"`
	ID 	int64 	`db:"id_col"`
	Gender	string  `db:"gender_col"`
}
```
It is important to note that SQLair __needs__ the fields to be public in order read from them and write to them.

To run a query with SQLair you need to create your `sqlair.DB` object. This is done by wrapping a `sql.DB` with:
```Go
db := sqlair.NewDB(sqldb)
```

Now to get a `Person` from the database.

```Go
stmt := sqlair.MustPrepare(
    "SELECT &Person.* FROM people",
    Person{},
)

var person := Person{}
err := db.Query(stmt).One(&person)
```

Or maybe we want all the people from the database

```Go
var people := []Person{}
err := db.Query(stmt).All(&people)
```

Or maybe some more precise control is needed

```Go
iter := db.Query(stmt).Iter()
for iter.Next() {
    var p := Person{}
    if !iter.Decode(&p) {
        break
    }
    doSomethingWithPerson(p)
}
err := iter.Close()
```

## Input and Output Expressions
### Input Expressions
To specify SQLair inputs and outputs, the characters `$` and `&` are used.

For now, input expressions are limited to `$Type.col_name`. In the case of the `Person` struct above we could write:
```SQL
SELECT name_col FROM person WHERE id_col = $Person.id_col
```
When we run `DB.Query(ctx, stmt, &person) the value in the `ID` field will be used as the query argument.
 
### Output Expressions
With output expressions we can do much more. 

|Output expressions| Result |
| --- | --- |
| &Person.name\_col | The Name field of Person is set to the result from the name column |
| &Person.\* | All columns mentioned in the field tags of Person are set to the result of their tagged column |
| t.\* AS &Person.\* | All columns mentioned in the field tags of Person are set to the results of the tagged column from table `t` |
| (client\_name, client\_id) AS (&Person.name\_col, &Person.id\_col) | The Name and ID fields of Person will be set with the results from client\_name and client\_id |
| (gender\_col, name\_col) AS &Person.\* | The Gender and Name fields of Person will be set with the results from gender\_col and name\_col |

# Example
```Go
package main

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

	err = db.Query(context.Background(), createPerson).Run()
	if err != nil {
		return err
	}
	err = db.Query(context.Background(), createPlace).Run()
	if err != nil {
		return err
	}

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
```

# FAQ


## Contributing

See our [code and contribution guidelines](CONTRIBUTING.md)

