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

```{admonition} See more
:class: tip
[`sqlair.NewDB`](https://pkg.go.dev/github.com/canonical/sqlair#NewDB),
[`sqlair.DB`](https://pkg.go.dev/github.com/canonical/sqlair#DB),
[`database/sql.DB`](https://pkg.go.dev/database/sql#DB)
```

## Check database support

SQLair generates the SQL that is sent to the database. It passes query inputs
as numbered placeholders (`$1`, `$2`, ...) followed by the argument values in
the same order. A placeholder can appear more than once in the generated SQL,
for example when the same value is inserted into every row of a bulk insert.

SQLite and PostgreSQL accept this placeholder syntax. SQLair is tested with
the `github.com/mattn/go-sqlite3` and `github.com/jackc/pgx/v5/stdlib`
drivers. Databases and drivers that do not accept numbered placeholders, such
as MySQL and SQL Server, are not supported.

Queries must use SQLair input expressions instead of their own placeholders.
A query that contains a placeholder such as `$1` is rejected when it is
prepared. See {ref}`input-expression-syntax`.

## Query a SQLair database

See: {ref}`query`.

## Unwrap a SQLair database

To unwrap a SQLair database and get out the `sql.DB`, use `DB.PlainDB`. SQLair
does not handle closing the database -- this should be done through the
`database/sql` package.

```{admonition} See more
:class: tip
[`DB.PlainDB`](https://pkg.go.dev/github.com/canonical/sqlair#DB.PlainDB)
```