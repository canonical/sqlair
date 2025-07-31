# Security

A software layer between your application and database, such as SQLair, can be a
potential security threat. Issues with SQLair could lead to exploits such as SQL
injection or Denial-of-Service through panics or high CPU usage. SQLair
takes a variety of measures to protect users against these attack vectors.

## Security features

### Argument sanitization
The contents of query arguments extracted by SQLair are passed as parameters to
the database rather than injected into the query as strings. This mitigates
against SQL injection as data passed as an argument cannot be executed as SQL.

### Struct tag sanitization
Struct tags which are injected into the query as column names are validated
to ensure that they only contain alphanumeric characters and do not contain
spaces. This mitigates against SQL injection.

### Query processing
In queries passed to SQLair, SQL not part of a SQLair expressions is not changed
by SQLair. This mitigates against attacks involving SQLair that touch parts of
the query outside the SQLair expressions.

### Testing
All query processing done by SQLair is extensively tested to ensure the expected
semantics. Furthermore, the internal cache is extensively tested to mitigate
against panics and memory leaks.

## Secure product usage
While SQLair provides protection against SQL injection, it is still possible to
write insecure code if SQLair is not used correctly.

The primary way to prevent SQL injection is to always use parameters for data 
and never build query strings with user-provided content. SQLair is designed to
make this easy.

For example, instead of writing code like this, which is vulnerable to SQL 
injection:

```go
// Vulnerable code
q := "SELECT * FROM users WHERE name = '" + userName + "'"
```

you should use SQLair to pass the `userName` as a parameter:

```go
// Secure code
type User struct {
    Name string    `db:"name"`
	Surname string `db:"surname"`
}

concreteUser := User{
    Name: "Ivan",
}

// Prepare a statement with a SQLair expression
stmt, err = sqlair.Prepare(`
    SELECT &User.* 
    FROM users
    WHERE name = $User.name
`, User{})
if err != nil {
    return Customer{}, fmt.Errorf("preparing select user statement: %w", err)
}

// Get returns the first query result from the database.
err = db.Query(context.Background(), stmt, concreteUser).Get(&concreteUser)
if err != nil {
    return User{}, fmt.Errorf(
        "selecting user %s from the database: %w",
        name, err,
    )
}

fmt.Printf("Customer record of concrete User: %#v\n", concreteUser)
```

SQLair ensures that the `User.Name` is passed as a parameter to the database,
not as part of the query string, which prevents it from being executed as SQL.

## Cryptographic guidance
SQLair does not directly implement or manage cryptographic functionalities such
as encryption, hashing, or digital signatures. Instead, it delegates these 
tasks to the underlying database drivers.

Users are responsible for configuring their database and database drivers to 
ensure data is handled securely. This includes, but is not limited to, 
enabling Transport Layer Security (TLS) for data in transit and utilizing 
database-level encryption for data at rest.

For detailed instructions on implementing these security measures, please 
refer to the official documentation of the database and the specific Go 
database driver that you've chosen to use.
