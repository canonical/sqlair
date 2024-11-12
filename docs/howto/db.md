(db)=
# Use a SQLair database
To create a SQLair database and run SQLair queries on it, you need to wrap a
`sql.DB` database object from the `database/sql` package. SQLair is a
convenience wrapper around the database, it does not create the DB itself.
Operations such as opening, closing and managing the DB settings are done
through `database/sql`.

## Wrap a database with SQLair
To wrap a `sql.DB` with SQLair, use the `sqlair.NewDB` method. This will return
a `sqlair.DB`.

> See more:
[`sqlair.NewDB`](https://pkg.go.dev/github.com/canonical/sqlair#NewDB),
[`sqlair.DB`](https://pkg.go.dev/github.com/canonical/sqlair#DB),
[`database/sql.DB`](https://pkg.go.dev/database/sql#DB)

## Query a SQLair database

See: {ref}`query`.

## Unwrap a SQLair database

To unwrap a SQLair database and get out the `sql.DB`, use `DB.PlainDB`. SQLair
does not handle closing the database -- this should be done through the
`database/sql` package.

> See more:
[`DB.PlainDB`](https://pkg.go.dev/github.com/canonical/sqlair#DB.PlainDB)
