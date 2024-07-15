(tutorial)=
# SQLair tutorial
SQLair is a package for Go that provides type mapping between structs/maps and SQL databases by allowing references to the Go types within the SQL query itself.

This tutorial will take you through the basics of writing queries with SQLair and give you a taste of what is possible.

As a simple example, given Go structs `Employee` and `Team`, instead of the SQL query:

```
SELECT id, team_id, name
FROM employee
WHERE team_id = ?
```

With SQLair you could write:

```
SELECT &Employee.*
FROM employee
WHERE team_id = $Location.team_id
```

SQLair adds special *expressions* to your SQL statements. These expressions allow you to use Go types directly in the query to reference the source of the parameters and the destination of the results. The purpose of a query in the context of your program can be worked out from a single glance.

SQLair uses the [`database/sql`](https://pkg.go.dev/database/sql) package to interact with the raw database and provides a convenience layer on top. This convenience layer provides a very different API to `database/sql`.

In this introduction we will use the example of a company's database containing employees and the teams they belong to. A full example code listing is given at the end of this post.

## Getting Started
### Tagging structs

To use a struct type with SQLair the first step is to tag the fields your structs with column names. The `db` tag is used to indicate which database column a struct field corresponds to.

For example:

```go
type Employee struct {
	Name   string `db:"name"`
	ID     int    `db:"id"`
	TeamID int    `db:"team_id"`
}
```

All struct fields that correspond to a column and are used with SQLair should be tagged with the `db` tag. If a field of the struct does not correspond to a database column it can be left untagged, and it will be ignored by SQLair.

It is important to note that SQLair *needs* the struct fields to be public in to order read from them and write to them.

### Writing the SQL
#### SQLair Prepare

A SQLair statement is built by preparing a query string. The `sqlair.Prepare` function turns a query with SQLair expressions into a `sqlair.Statement`. These statements are not tied to any database and can be created at the start of the program for use later on.

*Note:* `sqlair.Prepare` does not prepare the statement on the database. This is done automatically behind the scenes when the query is built on a `DB`.

```go
stmt, err := sqlair.Prepare(`
		SELECT &Employee.*
		FROM person
		WHERE team = $Manager.team_id`,
		Employee{}, Manager{},
)
```

The `Prepare` function needs a sample of every struct mentioned in the query. It uses reflection information from the struct to generate the columns and check that the expressions make sense. These type samples are only used for reflection information, their content is disregarded.

There is also a function `sqlair.MustPrepare` that does not return an error and panics if it encounters one.

#### Input and output expressions

In the SQLair expressions, the characters `$` and `&` are used to specify input and outputs respectively. The dollar `$` specifies a struct to fetch an argument from and the ampersand `&` specifies a struct to read query results into.

##### Input expressions

SQLair Input expressions replace the question mark placeholders in the SQL statement. An input expression is made up of the struct type name and a column name taken from the `db` tag of a struct field. For example:

```
UPDATE employee 
SET name = $Empolyee.name
WHERE id = $Employee.id
```

The expression `$Employee.name` tells SQLair that when we create the query we will give it an `Employee` struct and that the `Name` field is passed as a query argument.

*Note:* we use the column name from the `db` tag, *not* the struct field name after the type.

See {ref}`input-expressions` for how to use all the input expressions, including slices.

##### Output expressions

Output expressions replace the columns in a SQL query string. Because the struct has been tagged, you can use an asterisk (`*`) to tell SQLair that you want to fetch and fill *all* of the tagged the columns in that struct. If not every columns is needed then there are other forms of output expression that can be used.

In the code below, SQLair will use the reflection information of the sample `Employee` struct and substitute in all the columns mentioned in its `db` tags:

```go
stmt, err := sqlair.Prepare(`
		SELECT &Employee.*
		FROM employee`,
		Employee{},
)
```

There are other forms of output expressions as well. You can specify exactly which columns you want, and which table to get them from.

In the statement below there are two output expressions. The first instructs SQLair to fetch and fill all tagged fields in `Employee`, prefixed with the table `e`, and the second tells SQLair that the columns `t.team_name` and `t.id` should be substituted into the query and scanned into the `Team` struct when the results are fetched:

```go
stmt, err := sqlair.Prepare(`
		SELECT e.* AS &Employee.*, (t.team_name, t.id) AS (&Team.*)
		FROM employees AS e, teams AS t
		WHERE t.room_id = $Location.room_id AND t.id = e.team_id`,
	Location{}, Employee{},
)
```

If the columns on a particular table don't match the tags on the structs, you can also rename the columns. This query tells SQLair to put the columns `manager_name` and `manager_team` in the fields of `Employee` tagged with `name` and `team` once the results are fetched:

```go
stmt, err := sqlair.Prepare(`
	SELECT (manager_name, manager_team) AS (&Employee.name, &Employee.team)
	FROM managers`,
	Employee{},
)
```

As with input expressions, it is important to note that we always use the column names found in the `db` tags of the struct in the output expression rather than the field name.

See {ref}`output-expressions` for more.

##### Insert statements

Input expressions can also be used inside insert statements with syntax similar to output expressions. Below is a simple example of using an insert statement to insert a struct into a database:
```go
type Person struct {
	ID       int    `db:"id"`
	Name     string `db:"name"`
	Postcode int    `db:"postal_code"`
}

stmt, err := sqlair.Prepare(
	"INSERT INTO person (*) VALUES ($Person.*)",
	Person{},
)
fred := Person{ID: 1, Name: "Fred", Postcode: 1000}
err := db.Query(ctx, stmt, fred).Run()
```
The `Person` struct is tagged with the columns that the fields correspond to. These are used to generate the SQL that is sent to the database when the SQLair query is prepared. 
The query is created on the database with `db.Query` and then executed with `Run`. The variable `fred` which is of the type `Person` is passed as a parameter to the query. When the query is run it will insert `1` into column the `id`, `"Fred"` into the column name, and `1000` into the column `postal_code`.  

See more in {ref}`input-expressions`.


### Wrapping the database

SQLair does not handle configuring and opening a connection to the database. For this, you need to use `database/sql`. Once you have created a database object of type `*sql.DB` this can be wrapped with `sqlair.NewDB`.

If you want to quickly try out SQLair the Go `sqlite3` driver makes it very easy to set up an in memory database:

```go
import (
	"database/sql"
	"github.com/canonical/sqlair"
	_ "github.com/mattn/go-sqlite3"
)

sqldb, err := sql.Open("sqlite3", "file:test.db?cache=shared&mode=memory")
if err != nil {
	panic(err)
}

db := sqlair.NewDB(sqldb)
```

### Querying the database

Now you have your database its time to run a query. This is done with `DB.Query`. The arguments to `DB.Query` are:
- The context  (`context.Context`).  This can be `nil` if no context is needed for the query.
- The statement (`sqlair.Statement`) to be run on the database.
- Any structs mentioned in input expressions that contain query arguments (`DB.Query` is variadic).

The `Query` object returned by `DB.Query` does not actually execute the query on the database. That is done when we get the results. `Query.Get` fetches a single row from the database and populates the output structs of the query.

In the example below, the `Employee` struct, `res`, will be filled with the first employee in the managers team:


```go
//  stmt:
//	    SELECT &Employee.*
//	    FROM employees
//	    WHERE team_id = $Manager.team_id

arg := Manager{Name: "Pedro", TeamID: 1}
res := Employee{}

err := db.Query(ctx, stmt, arg).Get(&res)
```

#### Getting the results

The `Get` method is one of several options for fetching results from the database. The others are `Run`, `GetAll` and `Iter`. 

`GetAll` fetches all the result rows from the database. It takes a slice of structs for each argument. This query (the same query as the previous example) will fetch all employees in team 1 and append them to `res`:

```go
arg := Manager{Name: "Pedro", TeamID: 1}
res := []Employee{}
err := db.Query(ctx, stmt, arg).GetAll(&es)
// res == []Employee{
//          Employee{ID: 1, TeamID: 1, Name: "Alastair"},
//          Employee{ID: 2, TeamID: 1, Name: "Ed"},
//          ...
// }
```


`Iter` returns an `Iterator` that fetches one row at a time. `Iter.Next` returns true if there is a next row, the row can be read with `iter.Get`, and once the iteration has finished `Iter.Close` must be called. `Iter.Close` will also return any errors that happened during iteration. For example:

```go
arg := Manager{Name: "Pedro", TeamID: 1}
iter := db.Query(ctx, stmt, arg).Iter()
for iter.Next() {
	res := Employee{}
	err := iter.Get(&e)
	// res == Employee{Name: "Alastair", ID: 1, TeamID: 1} 
	if err != nil {
		// Handle error.
	}
	// Do something with res.
}
err := iter.Close()
```


`Run` runs a query on the DB. It is the same as `Get` with no arguments. This query will insert the employee named "Alastair" into the database:

```go
//  stmt:
//	    INSERT INTO person (name, id, team_id)
//	    VALUES ($Employee.name, $Employee.id, $Employee.team_id);`,

var arg = Employee{Name: "Alastair", ID: 1, TeamID: 1} 
err := db.Query(ctx, stmt, arg).Run()
```

*Note:* If the query has no output expressions then it is executed on the database meaning that no rows are returned. This can be confusing for new users because SQL statements such as `SELECT name FROM person` will not work if executed with SQLair; an output expression is needed: `SELECT &Person.name FROM person`.

### Maps

SQLair supports maps as well as structs. So far in this introduction, all examples have used structs to keep it simple, but in nearly all cases maps can be used as well.

This query will fetch the columns `name` and `team_id` from the database and put the results into the map `m` at key `"name"` and `"team_id"`:

```go
stmt := sqlair.MustPrepare(`
	SELECT (name, team_id) AS &M.*
	FROM employee
	WHERE id = $M.pid
`, sqlair.M{})

var m = sqlair.M{}
err := db.Query(ctx, stmt, sqlair.M{"pid": 1}).Get(m)
// m == sqlair.M{"name": "Alastair", "team_id": 1}
```

The type `sqlair.M` seen here is simply the type `map[string]any`. To reference it in the query however, it needs a name which is why SQLair provides this builtin. The `sqlair.M` type is not special in any way; any named map with a key type of `string` can be used with a SQLair query.

When using a map in an output expression, the query results are stored in the map with the column name as the key. In input expressions, the argument is specified by the map key.

It is not permitted to use a map with an asterisk and no columns specified e.g. `SELECT &M.* FROM employee`. The columns always have to be specified. 

### Transactions

Transactions are also supported by SQLair. A transaction can be started with:
```go
tx, err := db.Begin(ctx, txOpts)
if err != nil {
	// Handle error.
}
```

The second argument to `Begin` contains the optional settings for the transaction. It is a pointer to a `sqlair.TXOptions` which can be created with the desired settings:

```go
tx, err := db.Begin(ctx, &sqlair.TXOptions{Isolation: 0, ReadOnly: false})
```

To use the default settings set `nil` can be passed as the second parameter.

Queries are run on a transaction just like they are run on the database. The transaction is finished with a `tx.Commit` or a `tx.Rollback`.

```go
err = tx.Query(ctx, stmt, e).Run()
if err != nil {
	// Handle error.
}
err = tx.Commit()
if err != nil {
	// Handle error.
}
```

Remember to always commit or rollback a transaction to finish it and apply it to the database.

### Wrapping up

If you have any more in depth questions or issues please use see our [GitHub](https://github.com/canonical/sqlair). Contributions are always welcome! Hopefully this has been of some use, enjoy using the library.
