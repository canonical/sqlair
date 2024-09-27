(output-expression-syntax)=
# Output expression syntax

Output expressions specify how SQLair will write the query results into the
provided structs/maps. Output expressions are designed to replace the column
selection part of a SQL query.

There are several types of output expression though all use an ampersand (`&`)
in front of the types.

The output expression syntax is given in [Backus Naur
form](https://en.wikipedia.org/wiki/Backus%E2%80%93Naur_form). In the syntax
definitions, the following symbols have the given meanings:
- `<column-name>` - Any valid SQL column name.
- `<table-name>` - Any valid SQL table name.
- `<struct-name>` - Any valid Golang struct name.
- `<map-name>` - Any valid Golang map name.
- `<slice-name>` - Any valid Golang slice name.


## Single column syntax
A column can be fetched from the database and written into a map or struct via
the syntax below:
```bnf
<column-output> ::= "&" <type-name> "." <column-name>
<type-name> ::= <struct-name> | <map-name>
```
This will fetch the column `<column-name>` from the database and set it in the
type `<type-name>`. If setting it in struct, it will set the field with the `db`
tag `<column-name>` and if setting a value in a map it will use the key
`<column-name>`.

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

## Whole struct syntax
All the tagged fields in a struct can be fetched from the database and written
into a struct via the syntax:
```bnf
<asterisk-output> ::= "&" <struct-name> ".*"
```
SQLair will expand the type `<struct-name>` into a comma separated list of the
column names in its tags and write the results into the struct.
For example:
```sql
SELECT &Person.*
FROM   people
```
## Struct from table syntax
Columns from particular tables can be selected into specified structs using the
syntax below:
```bnf
<table-asterisk> ::= <table-asterisk-multiple> | <table-asterisk-single>
<table-asterisk-multiple> ::= "(" <table-name> ".*) AS (" <output-types> ")"
<table-asterisk-single> ::= <table-name> ".* AS " <output-type>

<output-types> ::= <output-type> | ", " <output-types>
<output-type> ::= <asterisk-output> | <column-output>
```
The `<asterisk-output>` and `<column-output>` reference the syntax above. All
the columns of the `<output-types>` will be prepended with the table name in the
generated SQL. The resulting columns will then be written into the maps/structs
of the `<output-types>`

```{note}
The output expression will always be replaced with a list of columns, a SQLair
output expression will never generate a SQL wildcard (*). For example, `p.* AS
$Person.*` is replaced with a comma separated list of all db tags on the field
of the `Person` struct.
```

For example:
```sql
SELECT     p.* AS &Person.*,
           (a.*) AS (&Address.*, &Country.country_name)
FROM       p
INNER JOIN a
```

This query will select all the column names specified on the db tags on the
fields in `Person` and `Address` as well as the column `country_name`. It will
prepend the columns from `Person` with `p.` and prepend `Address` and
`country_name` with `a.`

## Columns from table syntax
Specific columns from a table can be selected into the types on the right using
the syntax below:
```bnf
<columns-output> ::= "(" <columns> ") AS (&" <type-name> ".*)"

<columns> ::= <column> | ", " <columns>
<column> ::= <column-name> | <table-name> "." <column-name>

<type-name> ::= <struct-name> | <map-name>
```
The columns on are selected into the type on the right. If the type is a struct,
they are written to the fields with the matching tag and if it is a map the
result values are set with the column names as keys. The table name is not
included in the map key or the struct tag.

For example:
```sql
SELECT     (p.name, a.address_id) AS (&Person.*), 
           (a.postcode, p.person_id) AS (&M.*)
FROM       p
INNER JOIN a
```

This will set the fields tagged `name` and `address_id` in the struct `Person`
and set the keys `postcode` and `person_id` in the map `M`.

## Columns into specific struct tags/map keys syntax
Columns can be written into fields that are tagged with a different tag name to
the column using the syntax below:
```bnf
<columns-output> ::= <columns-output-single> | <columns-output-multiple>
<columns-output-multiple> ::= "(" <columns> ") AS (" <output-types> ")"
<columns-output-single> ::= <column> AS <output-type>

<column-outputs> ::= <column-outputs> | ", " <column-output>
<column-output> ::= "&" <type-name> "." <column-name>
<type-name> ::= <struct-name> | <map-name>

<columns> ::= <column> | ", " <columns>
<column> ::= <column-name> | <table-name> "." <column-name>
```

This form should only be used if selecting from a different to the one the
struct normally maps to. The tags on the fields of the struct should generally
match the columns in the database.

For example:
```sql
SELECT (other_person_name, other_person_id) AS (&Person.name, &Person.id),
       other_city AS &M.city
FROM   other_people
```
In `Person`, this will set the `name` field with the content of the column
`other_person_name` and the `id` field with the content of `other_person_id`. In
the map `M`, it will set the key `city` to the value from the column
`other_city`.