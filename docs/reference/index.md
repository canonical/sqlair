(reference)=
# Reference

SQLair is made up of two parts, the SQLair Domain Specific Language (DSL) which
extends SQL and the SQLair Go package.

The DSL is the special syntax written into your SQL queries that tells SQLair
how to map your Go objects to the database inputs and outputs. The API is the
functional interface of SQLair.

For more information about the specifics of the DSL see the following:
```{toctree}
:titlesonly:

input-expression-syntax
output-expression-syntax
types
```
To read more about the API, please see our
[pkg.go.dev](https://pkg.go.dev/github.com/canonical/sqlair).