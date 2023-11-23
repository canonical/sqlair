/*
Package expr processes the SQLair query string, generates the SQL, and maps the
SQLair query’s input/output arguments to the generated SQL query’s
parameters/results. To put it another way, expr covers all functionality
relating to SQLair expressions, it does not cover interaction with the
databases.

The expr package is split up into three stages: the Parse stage, the Type
Binding stage, and the Query stage.

# Parsing stage

The parsing stage takes a SQLair query string and parses it into an AST. The
parser only processes information already encoded in the syntax of the SQLair
expressions.

# Type Binding stage

The Type Binding stage binds concrete Go types to the type names in the SQLair
expressions. Type samples are provided by the user. These are used to
validate the SQLair expressions, and to generate an intermediate
representation with all the type information needed by SQLair.

The output of this stage retains the shape of the AST from the parse stage.

This stage only processes information contained in types. The query should not
be specialised at this stage to a strict subset of well typed input arguments
(for example, be only valid for slices arguments of length n).

# Input Binding and Query stage

The Input Binding and Query stage generates the concrete values needed to run
the SQLair query on a database. This includes the SQL, the query arguments and
info for scanning the results into SQLair output arguments.

This stage does not analyse the arguments types beyond validating they match
those seen in the Type Binding stage. Neither does it interact with the actual
database.
*/
package expr
