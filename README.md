# SQLair
[![Go Reference](https://pkg.go.dev/badge/github.com/canonical/sqlair)](https://pkg.go.dev/github.com/canonical/sqlair) [![Go Report Card](https://goreportcard.com/badge/github.com/canonical/sqlair)](https://goreportcard.com/report/github.com/canonical/sqlair) [![CI](https://github.com/canonical/sqlair/actions/workflows/go-test.yml/badge.svg)](https://github.com/canonical/sqlair/actions/workflows/go-test.yml)

SQLair is a Go package which streamlines interaction with SQL databases by embedding Go structs into SQL queries.

Things SQLair does:
 - Maps database rows directly into Go structs
 - Allows you to write queries in SQL
 - Provides a user friendly query API

Things SQLair does *not* do:
 - Acts as an ORM 
 - Optimise queries

API docs can be found at [pkg.go.dev](https://pkg.go.dev/github.com/canonical/sqlair) and a complete demo can be found at [demo/demo.go](demo/demo.go).

### Why?
There are [many solutions](https://github.com/d-tsuji/awesome-go-orms) for using a database with Go. Most of these are ORMs that require learning a complex API which abstracts pure SQL. Others provide a light weight convenience layer on top of the default library[`database/sql`](https://pkg.go.dev/database/sql), however, these may be coupled to the `database/sql` query API and none of them are as full featured as SQLair.

With `database/sql` reading rows into Go structs requires a lot of repetitive and redundant code:

- The order of the columns in the query must match the order of columns in `Rows.Scan`
- The columns from the query must be manually matched to their destinations
- If the columns needed changing, all queries must be changed

SQLair expands the SQL syntax with input and output expressions which indicate parts of the query that correspond to Go structs. It allows the user to specify the Go structs they want in the SQL query itself whilst allowing the full power of SQL to be utilised. 

SQLair also provides an alternative API for reading the rows from the database. It does not copy `database/sql`, it instead improves upon it and removes inconsistencies.

## Mini example
An example with a simple `SELECT` query with SQLair:
```Go
// Tag struct
type Person struct {
	Name	string	`db:"name_col"`
	ID 	int64 	`db:"id_col"`
	Gender	string  `db:"gender_col"`
}

// Wrap the *sql.DB.
db := sqlair.NewDB(sqldb)

// Prepare the query for use with SQLair.
stmt := sqlair.MustPrepare(
    "SELECT &Person.* FROM people WHERE id = $Address.id",
    Person{}, Address{},
)

// Build the query
q := db.Query(ctx, stmt, address)

// Get the first row
var p := Person{}
err := q.Get(&p)
```
# Usage
#### Contents
- [Tagging structs](#tagging-structs)
- [Writing the SQL](#writing-the-sql)
  - [Input Expressions](#input-expressions)
  - [Output Expressions](#output-expressions)
- [A SQLair DB](#a-sqlair-db)
    - [Transactions](#transactions)
- [Prepare](#prepare)
- [Query](#query)
  - [Get](#get)
  - [Run](#run)
  - [Iter](#iter)
  - [GetAll](#getall)
- [Outcome](#outcome)

## Tagging structs
The first step when using SQLair is to tag your structs. The `db` tag is used to map between the database column names and struct fields.

For example:
```Go
type Person struct {
	Name	string	`db:"name_col"`
	ID 	int64 	`db:"id_col"`
	Gender	string  `db:"gender_col"`
}
```
It is important to note that SQLair __needs__ the fields to be public in to order read from them and write to them.
## Writing the SQL
In SQLair expressions, the characters `$` and `&` are used to specify input and outputs respectively. These expressions specify the Go structs to fetch arguments from or read results into. 

For example, when selecting a particular `Person` from a database, instead of the query: 
```SQL
SELECT name_col, id_col, gender_col FROM person WHERE manager_col = ?
```
In SQLair you would write:
```SQL
SELECT &Person.* FROM person WHERE manager_col = $Manager.name
```
This tells SQLair to substitute `&Person.*` for all columns mentioned in the `db` tags of the struct `Person` and pass the `Name` field of the struct `Manager` as an argument.

### Input Expressions
Input expressions are limited to the form `$Type.col_name`. In the case of the `Person` struct above, we could write:
```Go
stmt, err := sqlair.Prepare(`
SELECT name_col FROM person WHERE id_col = $Person.id_col`,
Person{})
```
When we run:
```Go
var person = Person{ID: 42}
q := db.Query(ctx, stmt, &person)
```
then the value in the `ID` field will be used as the query argument.

 
_There are future plans to allow more integrated use of Go structs in `INSERT` statements._ 
### Output Expressions
Output expressions have multiple different formats. An asterisk `*` can be used to indicate that we want to read into _all_ the tagged fields in the struct.

Below is a full table of the different forms of output expression:

|Output expressions| Result |
| --- | --- |
| `&Person.name_col` | The `Name` field of `Person` is set to the result from the name column |
| `&Person.*` | All columns mentioned in the field tags of `Person` are set to the result of their tagged column |
| `t.* AS &Person.*` | All columns mentioned in the field tags of `Person` are set to the results of the tagged column from table `t` |
| `(client_name, client_id) AS (&Person.name_col, &Person.id_col)` | The `Name` and `ID` fields of `Person` will be set with the results from `client_name` and `client_id` |
| `(gender_col, name_col) AS &Person.*` | The `Gender` and `Name` fields of `Person` will be set with the results from `gender_col` and `name_col` |

The output expression should be places in the query where the columns to be returned would usually be found. Behind the scenes SQLair will replace the output expression with a list of comma separated aliased columns.

Multiple output expressions can be placed in the same query. 
For example:
```Go
stmt, err := sqlair.Prepare(`
SELECT p.* AS &Person.*, a.* AS &Address.*
FROM person AS p, address AS a`,
Person{}, Address{})
```
This query will select columns from table `p` that are mentioned in the tags of the `Person` struct and columns from table `a` that are mentioned in the tags of the `Address` struct.

To retrieve the first row of results of this query, you would do:
```Go
var p1 = Person{}
var a1 = Address{}
err := db.Query(ctx, stmt).One(&p1, &a1)
```

**Note:** Using `*` in a SQLair expression does **not** insert a `*` into the query, it will fetch at most the columns mentioned in the struct. Using `SELECT *` in a regular SQL query is bad practice.

## A SQLair DB
To run your SQLair queries you will need to wrap your database. SQLair does not handle the creation and configuration of the database, for that you use `database/sql`.

Once you have a `*sql.DB` then this can be transformed into a SQLair db with `NewDB`.

For example:
```Go
sqldb, err := sql.Open("sqlite3", ":memory:")
if err != nil {...}

db := sqlair.NewDB(sqldb)
```
It is still possible to access the underlying `sqldb` with `db.PlainDB()` for any further configuration needed. 

#### Transactions
SQLair databases support transactions. A `Query` can be created on a transaction in the same way it can on a `DB`.

Options can be passed to a transaction with `TXOptions`. `opts` can be `nil` if no options are needed. If `ctx` is `nil` `context.Background()` is used.

An existing transaction can be wrapped with `NewTX` mirroring the `NewDB` method for the database.

Transactions on the database can be created in SQLair with `tx, err := db.Begin(ctx, opts)` and `Query`, `Commit` or `Rollback` can be executed on the transaction.

For example: 

```Go
opts := sqlair.TXOptions{Isolation: 0, ReadOnly: false}

tx, err := db.Begin(ctx, opts)
if err != nil {...}
err = tx.Query(ctx, stmt).Run()
if err != nil {...}
err = tx.Rollback()
if err != nil {...}

tx, err = db.Begin(ctx, nil)
if err != nil {...}
err = tx.Query(ctx, stmt).Run()
if err != nil {...}
err = tx.Commit()
```

For more details, see the [API](https://pkg.go.dev/github.com/canonical/sqlair).
## Prepare
`Prepare` parses and prepares the SQLair query for passing to the database. It checks the SQLair expressions in the query for correctness, saves information about the query, and generates the pure SQL.

**Note:** This function does **not** prepare the query on the database. _In the future we intend to support preparing on the database as a backend optimisation once a query is created._

The SQLair function `Prepare` has the signature:
```Go
sqlair.Prepare(query string, typeSamples ...any) (*Statement, error)
```

The `typeSamples` are samples of all the structs that are mentioned in the query.
For example, in the query:
```Go
q := "SELECT &Person.*, &Address.id FROM person WHERE name = $Manager.name"
```
We mention three structs: `Person` and `Address` as outputs, and `Manager` as an input. Therefore we need to pass a sample of each of these structs to `Prepare`.

```Go
stmt, err := sqlair.Prepare(q, Person{}, Address{}, Manager{})
```

These structs are only used for their type information. Nothing else. It is an unfortunate limitation of generics in Go that we cannot pass a variadic list of types and, therefore, require instances of the struct itself.

Prepare uses the `reflect` library to gather information about the types and their struct tags. This information is used to fill in the correct columns to fetch in place of `&Person.*`, to check that there is a field in `Address` with a corresponding tag `db:"id"` and a field in `Manager` with a corresponding tag `db:"name"`. 

There is also a function `sqlair.MustPrepare` which is the same in all respects but will panic on error.
## Query
A SQLair `Query` object captures all the operations associated with running SQL on a database/transaction. It should be created when the query is ready to be run on the database/transaction. 

A new `Query` object can be created on a database or transaction with:
```Go
Query(ctx Context, stmt *Statement, inputArgs ...any) *Query
```
If `ctx` is `nil`, `context.Background()` will be used.
The `stmt` is the prepared statement from before, and the `inputArgs` must contain all the Go objects mentioned in the SQLair input expressions of the `Statement`. The  query arguments will be extracted from these objects and passed the the database. 

The creation of the query does not actually trigger its execution. This is only done once one of the following methods on `Query` is called. 
### Get
`Get` will execute the query. There are two cases for what will happen:
- **The query contains SQLair output expressions** - The first result from the query will be scanned into the arguments of `Get`. If there are no results then it will return `ErrNoRows`. In this case the query is executed on the database or transaction with [`sql.QueryContext`](https://pkg.go.dev/database/sql#DB.QueryContext).
- **The query does not contain SQLair output expressions** - No results will be returned. In this case the query is executed with [`sql.ExecContext`](https://pkg.go.dev/database/sql#DB.ExecContext).

To get metadata about the execution of a query an `sqlair.Outcome` struct can be passed as the first argument. See the [Outcome](#outcome) section for more detail.

```Go
var person := Person{}
var address := Address{}
err := q.Get(&person, &address)
```
### Run
`Query.Run` is the same as `Query.Get` with no arguments. This is useful for data manipulation statements such as `INSERT`, `UPDATE`, `CREATE` or `DROP`.
### Iter
`Query.Iter()` returns an `Iterator` that cycles through the rows returned from the query.An underlying database connection is created upon the inital call of `Iter()`.
    
- `iter.Next()`
  Prepares the next row for `iter.Get()`. It returns `true` if this has been successful and `false` if there is no next row or an error has occurred (the error can be checked with `iter.Close()`).
- `iter.Get(outputArgs...)`
  Scans the results from the query into the provided `outputArgs`. If it is called before the first call to `iter.Next()` with an `Outcome` it will populate the `Outcome` with metadata about the query (see [Outcome](#outcome) for more detail). 
It uses [`sql.Rows.Scan`](https://pkg.go.dev/database/sql#Rows) under the hood. This provides useful implicit conversions from the default database types to the types of the struct fields. 
- `Iter.Close()`
  Closes the underlying database connection and also return any errors that have occurred during iteration.

Make sure that `iter.Close()` is called when iteration of the results is finished otherwise a connection to the database will be left hanging. You should **defer** `iter.Close()` **every time** unless there is a good reason not to. Calling it twice is harmless and always a better option than leaving the connection open.

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

Remember, `Iter.Next` can return false either because the results have finished iterating **or because an error has occured**.
### GetAll
`GetAll` will scan all the rows returned by the query into slices of each of the objects mentioned in the output expressions. New structs will be created in the slices containing each row.

It is possible to pass a slice of structs, or a slice of pointers to the structs.

For example:

```Go
var people := []Person{}
var addresses := []*Address{}
err := q.All(&people, &addresses)
```
It is only advised that you use this if you __know__ that the results set is small and can fit comfortably in memory.

## Outcome
The `sqlair.Outcome` struct can be passed by reference to `Query.Get`, `Query.GetAll` or `Iter.Get` (but only before the first call of `Iter.Next()`). The struct that has been passed will be filled with metadata about the execution of the query.

Currently this struct only has the method `Result`. This returns a [`sql.Result`](https://pkg.go.dev/database/sql#Result) but only when the query contains no output expressions (i.e. it is most likely an `INSERT` or `UPDATE`). If the query contains output expressions then `Outcome.Result()` will return `nil`.

For example
```Go
var outcome = sqlair.Outcome{}

stmt, err := sqlair.Prepare(`INSERT INTO person VALUES ($Person.name)`)
q := db.Query(ctx, stmt, &p)

// An outcome with Query.Get
err := q.Get(&outcome)
if err != nil {...}
res := outcome.Result()

// An outcome with Iter
iter := q.Iter()
defer iter.Close()
err := iter.Get(&outcome)
if err != nil {...}
err := iter.Close()
if err != nil {...}
res := outcome.Result()
```

*There are plans to add more data to `Outcome` to make it useful with `SELECT` statements as well.*
# FAQ


# Contributing

See our [code and contribution guidelines](CONTRIBUTING.md)
