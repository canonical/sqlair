(input-expression-syntax)=
# Input expression syntax

Input expressions specify how SQLair will read values from the objects passed
to the `Query` method to pass to the database as the query arguments. Input
expressions replace SQL input placeholders (such as `?` or `$1`).

There are several different types of input expression though all use a dollar
sign ($) in front of the types.

The input expressions syntax is given in [Backus Naur
form](https://en.wikipedia.org/wiki/Backus%E2%80%93Naur_form). In the syntax
definitions, the following symbols have the given meanings:
- `<column-name>` - Any valid SQL column name.
- `<table-name>` - Any valid SQL table name.
- `<struct-name>` - Any valid Golang struct name.
- `<map-name>` - Any valid Golang map name.
- `<slice-name>` - Any valid Golang slice name.


## Struct field and map key syntax

A value in a struct field or map key can be input via the syntax:
```bnf
<input> ::= "$" <type-name> "." <column-name>

<type-name> ::= <struct-name> | <map-name>
<column> ::= <column-name> | <table-name> "." <column-name>
```
The struct or map can be passed to the `Query` function and the specified
value will be extracted, and passed to the driver as a query argument.
For example in this snippet:
```
...
WHERE manager_name = $Manager.name
```

`Manager` is a struct and `name` is the "db" tag on one of its fields.

(slice-inputs)=
## Slice syntax

A slice of values can be input via the syntax below. The slice name must be a
named slice type.
```bnf
<slice-input> ::= "$" <slice-name> "[:]"
```
The values in the slice passed to `Query` that corresponds to this expression
will be expanded into a comma separated list of input placeholders. SQLair does
not insert the parentheses around the values.
For example, the slice can be used with an `IN` clause:
```
...
WHERE name IN ($Names[:])
```
(insert-statements)=
## Insert syntax

To insert values from a type into the database SQLair provides specialised
syntax:
```bnf
<asterisk-insert> ::= "INSERT INTO <table-name> (*) VALUES (" <input-types> ")"

<input-types> ::= <input-type> | ", " <input-types>
<input-type> ::= <asterisk-input-type> | <member-input-type>

<asterisk-input-type> ::= "$" <struct-name> ".*"
<member-input-type> ::= "$" <type-name> "." <column-name>

<type-name> ::= <struct-name> | <map-name>
```
This syntax will insert all values specified on the left hand side into the
database. Values can be inserted from structs and maps. A struct name can be
followed by an asterisk (*) to insert all tagged fields of the struct.
For example, the statement below will insert all tagged fields in the `Person`
struct:
```
INSERT INTO person (*) VALUES ($Person.*)
```
Specific fields/keys on structs/maps can also be used:
```
INSERT INTO person (*) VALUES ($Person.name, $PersonDetailsMap.age)
```

SQLair also has syntax to insert specific columns from the types on the right.
This syntax will only insert the column names specified in the list of column
names on the left.
```bnf
<column-insert> ::= "INSERT INTO <table-name> (" <columns> ") VALUES (" <input-types> ")"

<columns> ::= <column-name> | ", " <columns>

<input-types> ::= <input-type> | ", " <input-types>
<input-type> ::= <asterisk-input-type> | <member-input-type>

<member-input-type> ::= "$" <type-name> "." <column-name>
<asterisk-input-type> ::= "$" <struct-name> ".*"

<type-name> ::= <struct-name> | <map-name>
```
For example, this will only insert the `name` and `postcode`:
```
INSERT INTO person (name, postcode) VALUES ($Person.*, $Address.*)
```