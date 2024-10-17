(query)=
# Query the database

## SQLair-proof your types
The first step in building your query is to decide what you would like to put
into/get out of the database. Identify the types in your Go program that hold
this information or create them if they don't already exist.

Make sure that the types are in the right format to work with SQLair. For example:
```go
type Employee struct {
    ID         string `db:"id"`
    Name       string `db:"name"`
    LocationID string `db:"location_id"`
}

type Location struct {
    ID   string `db:"id"`
    City string `db:"city"`
}
```

> See more: {ref}`types` 

## Write your query string using the types

SQLair queries are regular SQL queries with special input and output expressions
that enable you to reference the Go types directly from SQL.

Write your SQL query with SQLair input expressions instead of query parameters
and SQLair output expressions instead of columns to select.

For example:
```
query := `
    SELECT &Employee.*
    FROM employee
    WHERE location_id = $Location.id
`
```

> See more {ref}`input-expression-syntax`, {ref}`output-expression-syntax`.

## Prepare your query string to get a statement

Now you have your query string, you need to pass it to `sqlair.Prepare` along
with samples of all the types mentioned in the input and output expressions.

SQLair uses the type information from the type samples to generate the SQL and
verify the input/output expressions. If there is an issue with the actual SQL in
the query, this will not catch it. The returned `Statement` holds the parsed and
verified query.


```{note}
SQLair also provides the `sqlair.MustPrepare` method which panics on error
instead of returning.
```

For example:
```go
stmt, err := sqlair.Prepare(ctx, query, Employee{}, Location{})
if err != nil {
    return err
}
```

> See more:
[`sqlair.Prepare`](https://pkg.go.dev/github.com/canonical/sqlair#Prepare),
[`sqlair.MustPrepare`](https://pkg.go.dev/github.com/canonical/sqlair#MustPrepare)

## Execute the statement on the database

To execute the statement on a SQLair wrapped `DB` or a `TX`, use the `Query`
method, passing as parameters the `Statement` and all the input arguments
specified in the input expressions. This returns a `Query` object that can then
be run with one of four methods below.

```{note}
The `Query` object returned from `DB.Query`/`TX.Query` is not designed to be
reused. One of the methods on `Query` should immediately be called. It should
not be saved as variable.
```

> See more:
[`DB.Query`](https://pkg.go.dev/github.com/canonical/sqlair#DB.Query),
[`TX.Query`](https://pkg.go.dev/github.com/canonical/sqlair#TX.Query),
[`sqlair.Query`](https://pkg.go.dev/github.com/canonical/sqlair#Query)

### Get one row
To get only the first row returned from the database use `Query.Get`, passing
pointers to all the output variables mentioned in the query.

For example:
```go
location := Location{City: "Edinburgh"}
var employee Employee

err := db.Query(ctx, stmt, location).Get(&employee)
if err != nil {
    return err
}

// employee now contains the first employee returned from the database.
```

> See more: [`Query.Get`](https://pkg.go.dev/github.com/canonical/sqlair#Query.Get)

### Get all the rows
To get all the rows returned from the database use `Query.GetAll`, passing
pointers to slices of all the output variables in the query.

For example:
```go
location := Location{City: "Edinburgh"}
var employees []Employee

err := db.Query(ctx, stmt, location).GetAll(&employees)
if err != nil {
    return err
}

// employees now contains all the employees returned from the database.
```

> See more:
[`Query.GetAll`](https://pkg.go.dev/github.com/canonical/sqlair#Query.GetAll)

### Iterate over the rows
To iterate over the rows returned from the query, get an `Iterator` with
`Query.Iter`.

`Iterator.Next` prepares the next row for `Iterator.Get`. It will return false
if there are no more rows or there is an error. `Iterator.Get` works the same as
`Query.Get` above, except it gets the current row.

Make sure to run `Iterator.Close` once you are finished iterating. The
`Iterator.Close` operation should generally be deferred when the `Iterator` is
created. Any errors encountered during iteration will be returned with
`Iterator.Close`.

For example:
```go
location := Location{City: "Edinburgh"}

iter := db.Query(ctx, stmt, location).Iter()

// Defer closing of the iterator and set its error to the error returned from 
// the function (if the function error is not nil).
defer func(){
    closeErr := iter.Close()
    if err == nil {
        err = closeErr
    }
}()

for iter.Next() {
    var employee Employee
    err := iter.Get(&employee)
    if err != nil {
        return err
    }  
}
```

> See more:
[`Query.Iter`](https://pkg.go.dev/github.com/canonical/sqlair#Query.Iter),
[`sqlair.Iterator`](https://pkg.go.dev/github.com/canonical/sqlair#Iterator),
[`Iterator.Next`](https://pkg.go.dev/github.com/canonical/sqlair#Iterator.Next),
[`Iterator.Get`](https://pkg.go.dev/github.com/canonical/sqlair#Iterator.Get),
[`Iterator.Close`](https://pkg.go.dev/github.com/canonical/sqlair#Iterator.Close)
### Just run 
To run a query that does not return any rows, use `Query.Run`. This is useful
when doing operations that are not expected to return anything.

For example:
```go
stmt, err := sqlair.Prepare("INSERT INTO employee (*) VALUES ($Employee.*)", Employee{})
if err != nil {
    return err
}

employee := Employee{
    ID:         1 
    Name:       "Joe"
    LocationID: 17
}
err := tx.Query(ctx, stmt, employee).Run()
if err != nil {
    return err
}
// employee has been inserted into the database.
```

> See more:
[`Query.Run`](https://pkg.go.dev/github.com/canonical/sqlair#Query.Run)


## (Optional) Get the query outcome

To get the query outcome, use any of the `Get` methods, providing as a first
argument a pointer to a `sqlair.Outcome` object. This will fill the
`sqlair.Outcome` with information about the outcome of the query.

```{note}
The query outcome contains metadata about the execution of a query. Currently,
it only contains a `sql.Result` object which contains information returned from
the driver such as the number of rows affected. It may contain more in the
future.
```

For example:
```go
stmt, err := sqlair.Prepare("DELETE FROM employee WHERE name = $Employee.name", Employee{})
if err != nil {
    return err
}

var outcome sqlair.Outcome
err := tx.Query(ctx, stmt, Employee{Name: "Joe"}).Get(&outcome)
if err != nil {
    return err
}

result := outcome.Result()
rowsAffected, err := reslut.RowsAffected()
if err != nil {
    return err
}
```

> See more:
[`sqlair.Outcome`](https://pkg.go.dev/github.com/canonical/sqlair#Outcome),
[`sql.Result`](https://pkg.go.dev/database/sql#Result)