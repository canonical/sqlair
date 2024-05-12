# SQLair
[![Go Reference](https://pkg.go.dev/badge/github.com/canonical/sqlair)](https://pkg.go.dev/github.com/canonical/sqlair) [![Go Report Card](https://goreportcard.com/badge/github.com/canonical/sqlair)](https://goreportcard.com/report/github.com/canonical/sqlair) [![CI](https://github.com/canonical/sqlair/actions/workflows/go-test.yml/badge.svg)](https://github.com/canonical/sqlair/actions/workflows/go-test.yml)

_Friendly type mapping in Go for SQL databases._

## Features

SQLair extends the SQL syntax to allow Go types to be referenced directly in the SQL query.
The full power of SQL is retained but with all the benefits of convenient type mapping.

SQLair allows you to:

 - Map database rows directly into Go structs and maps
 - Cite Go structs and maps in the query parameters
 - Write rich queries in SQL
 - Organise your code conveniently

Things SQLair does *not* do:
 - Act as a traditional ORM 
 - Optimise queries

For example, instead of the pure SQL query:
```
	SELECT name, id, team
	FROM person
	WHERE manager_name = ?
```
With SQLair one would write:
```
	SELECT &Person.*
	FROM person
	WHERE manager_name = $Manager.name
```
Where `Person` and `Manager` are Go structs. 

The SQL syntax is expanded with SQLair input and output expressions (indicated with `$` and `&` respectively) which indicate parts of the query that correspond to Go structs.
This package also provides an alternative API for reading the rows from the database.
SQLair relies on [database/sql](https://pkg.go.dev/database/sql) for all the underlying operations.

For more details please see the [SQLair introduction](https://github.com/canonical/sqlair/wiki/SQLair-Introduction) and the [Go package documentation](https://pkg.go.dev/github.com/canonical/sqlair).

## Contributing

See our [code and contribution guidelines](CONTRIBUTING.md)

