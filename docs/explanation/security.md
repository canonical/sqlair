# Security

A software layer between your application and database, such as SQLair, can be a
potential security threat. Issues with SQLair could lead to exploits such as SQL
injection or Denial-of-Service through panics or high CPU usage. SQLair
takes a variety of measures to protect users against these attack vectors.

## Security Features

### Argument sanitization
The contents of query arguments extracted by SQLair are passed as parameters to
the database rather than injected into the query as strings. This mitigates
against SQL injection as data passed as an argument can not be executed as SQL.

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

## Secure Product Usage
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

You should use SQLair to pass the `userName` as a parameter:

```go
// Secure code
type User struct {
    Name string `db:"name"`
}
var user User
err := db.Query(ctx, "SELECT * FROM users WHERE name = $Name", &user).Run()
```

SQLair ensures that the `userName` is passed as a parameter to the database,
not as part of the query string, which prevents it from being executed as SQL.

## Cryptographic Guidance
SQLair does not directly implement or manage cryptographic functionalities such
as encryption, hashing, or digital signatures. Instead, it leverages the 
capabilities of the underlying database drivers to handle these operations 
securely.

Users are responsible for configuring their database and database drivers to 
ensure data is handled securely. This includes, but is not limited to, 
enabling Transport Layer Security (TLS) for data in transit and utilizing 
database-level encryption for data at rest.

For detailed instructions on implementing these security measures, please 
refer to the official documentation of the database and the specific Go 
database driver in use.
