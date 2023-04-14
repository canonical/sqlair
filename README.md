# SQLair

SQLair is a package for Go that acts as a compatibility layer between Go and SQL databases. It allows for easy mapping between Go objects and query arguments and results.

SQLair allows you to write SQL with SQLair input/output expressions included where mapping is needed. These expressions indicate the Go objects to map the database arguments/results into. SQLair is not an ORM and will leave all parts of the query outside of the expressions untouched.

The API can be found at [pkg.go.dev](https://pkg.go.dev/github.com/canonical/sqlair).
A demo can be found at [demo/demo.go](demo/demo.go).

There will also soon be a full tutorial but that is currently a work in progress.
### Motivation
When writing an SQL query with `database/sql` in Go there are multiple points of redundancy/failure:

- The order of the columns in the query must match the order of columns in `Scan`
- The columns from the query must be manually matched to their destinations
- If the columns needed changeing, all queries must be changed

For example, when selecting a particular `Person` from a database, instead of the query: 
```SQL
SELECT name_col, id_col, gender_col FROM person WHERE manager_col = ?
```
In SQLair you could write:
```SQL
SELECT &Person.* FROM person WHERE manager_col = $Manager.name
```
These results from this second query could then be directly decoded in the the `Person` struct.

**Note:** Using `*` in a SQLair expression does **not** insert a `*` into the query, it will fetch at most the columns mentioned in the struct. Using `SELECT *` in a regular SQL query is bad practice.


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
It is important to note that SQLair __needs__ the fields to be public in to order read from them and write to them.

#### Mini exmaple
An example with a simple `SELECT` query with SQLair:
```Go
// Wrap the *sql.DB.
db := sqlair.NewDB(sqldb)

// Prepare the query for use with SQLair.
// A sample of each Go type mentioned in the query is provided.
stmt := sqlair.MustPrepare(
    "SELECT &Person.* FROM people WHERE id = $Address.id",
    Person{}, Address{},
)

// Pass the context, prepared statement, and Address struct containing the "id" argument.
q := db.Query(ctx, stmt, address)
// Iterate over the returned rows, decoding each one into a Person struct.
iter := db.Query(stmt).Iter()
for iter.Next() {
    var p := Person{}
    if !iter.Decode(&p) {
        break
    }
    doSomethingWithPerson(p)
}
// Make sure to close the Iterator to avoid a hanging DB connection.
err := iter.Close()
```
## Writing the SQL
In SQLair expressions, the chatacters `$` and `&` are used to specify input and outputs respectivly. These expressions specify the Go objects to fetch arguements from or read results into. 
### Input Expressions
Input expressions are limited to the form `$Type.col_name`. In the case of the `Person` struct above, we could write:
```SQL
SELECT name_col FROM person WHERE id_col = $Person.id_col
```
When we run `DB.Query(ctx, stmt, &person)` the value in the `ID` field will be used as the query argument.
 
_There are future plans to allow more intergrated use of Go objects in `INSERT` statements._ 
### Output Expressions
With output expressions we can do much more. Below is a full table of the different forms of output expression.

|Output expressions| Result |
| --- | --- |
| `&Person.name_col` | The `Name` field of `Person` is set to the result from the name column |
| `&Person.*` | All columns mentioned in the field tags of `Person` are set to the result of their tagged column |
| `t.* AS &Person.*` | All columns mentioned in the field tags of `Person` are set to the results of the tagged column from table `t` |
| `(client_name, client_id) AS (&Person.name_col, &Person.id_col)` | The `Name` and `ID` fields of `Person` will be set with the results from `client_name` and `client_id` |
| `(gender_col, name_col) AS &Person.*` | The `Gender` and `Name` fields of `Person` will be set with the results from `gender_col` and `name_col` |

Take, for example, this SQLair query:
```Go
stmt, err := sqlair.Prepare(`
SELECT p.* AS &Person.*, a.* AS &Address.*
FROM person AS p, address AS a`,
Person{}, Address{})
```
This query will select columns from table `p` that are mentioned in the tags of the `Person` struct and columns from table `a` that are mentioned in the tags of the `Address` struct.

To retrive the first row of results of this query, you would do:
```Go
var p1 = Person{}
var a1 = Address{}
err := db.Query(ctx, stmt).One(&p1, &a1)
```
## A SQLair DB
To start off you will need to wrap your database. SQLair does not handle the creation and configuration of the database, for that you use `database/sql`.

Once you have a `*sql.DB` then this can be transformed into a SQLair db with `NewDB`.

For example:
```Go
sqldb, err := sql.Open("sqlite3", ":memory:")
if err != nil {...}

db := sqlair.NewDB(sqldb)
```
It is still possible to access the underlying `sqldb` with `db.PlainDB()` for any further configuration needed. 

## Prepare
The SQLair function `Prepare` has the signiture:
```Go
sqlair.Prepare(query string, typeSamples ...any) (*Statement, error)
```

**Note:** This function does **not** prepare the query on the database. _In the future we intend to support preparing on the database as a backend optimisation once a query is created._

The `typeSamples` are samples of all the structs that are mentioned in the query. For example, in the query:
```Go
q := "SELECT &Person.*, &Address.id FROM person WHERE name = $Manager.name"
```
We mention three structs: `Person` and `Address` as outputs, and `Manager` as an input. Therefore we need to pass an instansiation of each of these structs to `Prepare`.

```Go
stmt, err := sqlair.Prepare(q, Person{}, Address{}, Manager{})
```

These structs are only used for their type information. Nothing else. It is an unfortate limiation of generics in Go that we cannot pass a variadic list of types and therefore require instances of the objects themsevles.

Prepare uses the `reflect` library to gather information about the types and their struct tags. This information is used to fill in the correct columns to fetch in place of `&Person.*`, to check that there is a field in `Address` with a corrosponding tag `db:"id"` and a field in `Manager` with a corrosponding tag `db:"name"`. 

There is also a function `sqlair.MustPrepare` which is the same in all respects but will panic on error.
## Query
A SQLair `Query` captures all the opterations assosiated with running SQL on a database.

A new `Query` object can be created with:
```Go
DB.Query(ctx Context, stmt *Statement, inputArgs ...any) *Query
```
If `ctx` is `nil`, `context.Background()` will be used.
The `stmt` is the prepared statement from before, and the `inputArgs` must contain all the Go objects mentioned in the SQLair input expressions of the `Statement`. The  query arguments will be extracted from these objects and passed the the database. 

The creation of the query does not actually trigger its exection. This is only done once one of the following methods on `Query` is called. 
### Run
`Query.Run()` will execute the query on the database without returning any results. This is useful for statements such as `INSERT`, `UPDATE`, `CREATE` or `DROP`.
### Iter
`Query.Iter()` returns an `Iterator` that cycles through the rows returned from the query. `iter.Next()` is called to prepare the next row (and returns false if there is no next row or an error has occoured). Then the row can be scanned into the structs mentioned in the output expressions with `iter.Decode()`.

An underlying database connection is created upon the inital call of `Iter()`. This is only closed up the execution of `Iter.Close()` which will also return any errors that have occured during the execution of the query.

Make sure that `iter.Close()` is called when iteration of the results is finished otherwise a connection to the database will be left hanging. You should **always** defer `iter.Close()`. Calling it twice is harmless and always a better option than leaving the connection open.

If the `Iterator` is not closed the underlying database connection is busy and cannot be used for any other queries. 
```Go
iter := q.Iter()
defer iter.Close()
for iter.Next() {
    var p := Person{}
    var a := Address{}
    if err := iter.Decode(&p, &a) {
        return err
    }
    doSomethingWithPAndA(p, a)
}
err := iter.Close()
```

Remeber, `Iter.Next` can return false either becuase the results have finished iterating **or because an error has occured**.
### One
`One` will decode the first result from the query. If there are no results then it will return `ErrNoRows`.
```Go
var person := Person{}
var address := Address{}
err := q.One(&person, &address)
```
### All
`A#l` will decode all the rows returned by the query into slices of each of the objects mentioned in the output expressions.
```Go
var people := []Person{}
var addresses := []Address{}
err := q.All(&people, &addresses)
```
It is only advised that you use this if you __know__ that the results set is small and can fit comfortably in memory.

# FAQ


## Contributing

See our [code and contribution guidelines](CONTRIBUTING.md)
