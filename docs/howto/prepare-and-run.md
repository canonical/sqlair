(prepare-and-run)=
# How to prepare and run a SQLair query

To create and run a SQLair query:
1. Ensure the types you want to map to the query inputs and outputs are in the
   right form for SQLair.
2. Write your query with SQLair syntax.
3. Prepare your query with `sqlair.Prepare`.
4. Run your query on a database or transaction.

## Prepare your Go types

See {ref}`types` for how to prepare the types you would like to map the query
inputs and outputs to.

## Write your query

SQLair queries are regular SQL queries with special input and output expressions
inserted which allow you to reference the Go types directly from the SQL. You
can have any number of input and output expressions in your query.

Write your SQL query and add in the input and output expressions to specify the mappings to Go types 

> See more {ref}`input-expressions`, {ref}`output-expressions`.

## Prepare your query

Now you have your query string, you need to pass it to `sqlair.Prepare` along
with all the types mentioned in the input and output expression.

> See more [`sqlair.Preapre`](https://pkg.go.dev/github.com/canonical/sqlair#Prepare)

This is needed so that SQLair can gather reflection information about the types
and verify the input/output expressions. If there is an issue with the actual
SQL in the query this will not catch it.

Note that this will not prepare the query on the database. In fact, the
`sqlair.Statement` you get back here can be used on any database or transaction.
The query is automatically prepared against the database the first time it is
run. As a user, you do not need to worry about managing the driver prepared
statements on the database. SQLair manages this for you.


## Run your query

To run your query you need a SQLair database
[`DB`](https://pkg.go.dev/github.com/canonical/sqlair#DB) or a
SQLair transaction on a SQLair database
[`TX`](https://pkg.go.dev/github.com/canonical/sqlair#TX).

To start the query you run
[`DB.Query`](https://pkg.go.dev/github.com/canonical/sqlair#DB.Query) or
[`TX.Query`](https://pkg.go.dev/github.com/canonical/sqlair#TX.Query). This
creates a [`Query`](https://pkg.go.dev/github.com/canonical/sqlair#Query)
object.

You can then do
[`Run`](https://pkg.go.dev/github.com/canonical/sqlair#Query.Run),
[`Get`](https://pkg.go.dev/github.com/canonical/sqlair#Query.Get),
[`GetAll`](https://pkg.go.dev/github.com/canonical/sqlair#Query.GetAll) or
[`Iter`](https://pkg.go.dev/github.com/canonical/sqlair#Query.Iter) on that
[`Query`](https://pkg.go.dev/github.com/canonical/sqlair#Query) object to
execute the statement on the database.