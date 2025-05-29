# Security

A software layer between your application and database, such as SQLair, can be a
potential security threat. Issues with SQLair could lead to exploits such as SQL
injection or Denial-of-Service through panics or high CPU usage. SQLair
takes a variety of measures to protect users against these attack vectors.

## Argument sanitization
The contents of query arguments extracted by SQLair are passed as parameters to
the database rather than injected into the query as strings. This mitigates
against SQL injection as data passed as an argument can not be executed as SQL.

## Struct tag sanitization
Struct tags which are injected into the query as column names are validated
to ensure that they only contain alphanumeric characters and do not contain
spaces. This mitigates against SQL injection.

## Query processing
In queries passed to SQLair, SQL not part of a SQLair expressions is not changed
by SQLair. This mitigates against attacks involving SQLair that touch parts of
the query outside the SQLair expressions.

## Testing
All query processing done by SQLair is extensively tested to ensure the expected
semantics. Furthermore, the internal cache is extensively tested to mitigate
against panics and memory leaks.
