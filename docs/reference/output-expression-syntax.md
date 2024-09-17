(output-expression-syntax)=
# Output expression syntax


SQLair expressions can be either input expressions or output expressions. The
output expressions specify how SQLair will scan the query results into the
provided structs/maps. Output expressions are designed to replace the column
selection part of a SQL query.

There are several types of output expression though all use an ampersand (`&`)
in front of the types.

## Output a single column
The syntax `&<type-name>.<column-name>` with fetch the column `<column-name>`
from the database and set it in a struct/map. If setting it in struct, it will
set the field with the `db` tag `<column-name>` and if setting a value in a map
it will use the key `<column-name>`.

Multiple output expressions can be used in a single query. For example:
```sql
SELECT &Person.name,
       &MyMap.age
FROM   table
```
This will fetch the `name` and `age` column. It will set the field in `Person`
tagged with `name` to the value for the `name` column from the database and the
key `age` in the map `MyMap` to the value from the database for the `age`
column.


## Output a whole struct
The syntax `&<struct-name>.*` fetches and sets all the structs tagged fields.
SQLair will expand the type into all the tagged column names and insert the
results into the struct.
For example:
```sql
SELECT &Person.*
FROM   people
```
## Output a whole struct from a particular table
The syntax `table.* AS &<struct-name>.*` does the same as getting all the fields
of a struct (above) but prepends all columns with the table name. The tags on
the struct should not include the table name.

For example:
```sql
SELECT     p.* AS &Person.*,
           a.* AS &Address.*
FROM       p
INNER JOIN a
```

## Output specific columns into a type
The syntax `(<table-name>.<column-name>, ...) AS &<type-name>.*` fetches and
sets only the specified columns. This type name can be a struct or a map. The
table names are optional. If a table name is included the map key or the struct
tag do not include the table name, it is only mentioned in the query.

For example:
```sql
SELECT     (p.name, a.address_id) AS (&Person.*), 
           (a.postcode, p.person_id) AS (&Address.*)
FROM       p
INNER JOIN a
```

## Output specific columns into specific places
The syntax `(<column-name>, ...) AS (&<type-name>.<column-name>, ...)` will
fetch the specified columns and put them in the structs/maps.

This form should only be used if selecting from a different to the one the
struct normally maps to. The tags on the fields of the struct should generally
match the columns in the database.

For example
```sql
SELECT (other_person_name, other_person_id) AS (&Person.name, &Person.id)
FROM   other_people
```

# Formal BNF specification of output syntax

This is the [BNF](https://en.wikipedia.org/wiki/Backus%E2%80%93Naur_form) description of the syntax of output expressions:

```bnf
<output-expression> ::= <as-expression> | <output-type>

<as-expression> ::= <as-expression-multiple> | <as-expression-single> 
<as-columns> ::= <as-columns-multiple> | <as-columns-single>
<as-columns-multiple> ::= "(" <columns> ") AS (" <output-types> ")"
<as-columns-single> ::= <column> AS <output-type>
<as-asterisk> ::= <as-asterisk-multiple> | <as-asterisk-single>
<as-asterisk-multiple> ::= "(" <asterisk> ") AS (" <output-types> ")"
<as-asterisk-single> ::= <asterisk> AS <output-type>

<output-types> ::= <output-type> | ", " <output-types>
<output-type> ::= <asterisk-output-type> | <member-output-type>
<member-output-type> ::= "&" <type-name> ".*" <type-member>
<asterisk-output-type> ::= "&" <struct-name> ".*"

<type-name> ::= <struct-name> | <map-name>

<asterisk> ::= <table-name> ".*" | "*"

<columns> ::= <column> | ", " <columns>
<column> ::= <column-name> | <table-name> "." <column-name>
```

The syntax for the symbols that are not fully expanded above are as follows:
- `<column-name>` - Any valid SQL column name.
- `<table-name>` - Any valid SQL table name.
- `<struct-name>` - Any valid Golang struct name.
- `<map-name>` - Any valid Golang map name.
