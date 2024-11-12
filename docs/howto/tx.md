(tx)=
# Manage SQLair transactions
SQLair transactions are created on a `sqlair.DB`.

```{seealso}
{ref}`db`.
```
## Begin a SQLair transaction

To begin a transaction, on your database, call `DB.Begin`, passing a context and
reference to a `sqlair.TXOptions` struct, or nil if you wish to use the default
settings from the driver.

The `sqlair.TXOptions` struct is used to set the isolation level of the
transaction and the read-only boolean.

For example:
```go
txOpts := sqlair.TXOptions{
    ReadOnly: true,
}

tx, err := db.Begin(ctx, &txOpts)
if err != nil {
    return err
}
```

```{seealso}
[DB.Begin](https://pkg.go.dev/github.com/canonical/sqlair#DB.Begin),
[sqlair.TXOptions](https://pkg.go.dev/github.com/canonical/sqlair#TXOptions)
```

## Query a SQLair transaction
See {ref}`query`.

## Commit or roll back a SQLair transaction

To commit a transaction or roll it back, use `TX.Commit` or `TX.Rollback`. Once
a transaction has been committed or rolled back, any further operations on it
will return `sqlair.ErrTXDone`.

For example:
```go
err := tx.Commit()
if err != nil {
    return err
}
```

```{seealso}
[TX.Commit](https://pkg.go.dev/github.com/canonical/sqlair#TX.Commit),
[TX.Rollback](https://pkg.go.dev/github.com/canonical/sqlair#TX.Rollback),
[sqlair.ErrTXDone](https://pkg.go.dev/github.com/canonical/sqlair#ErrTXDone)
```
