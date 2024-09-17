(input-expression-syntax)=
# Input expression syntax

SQLair expressions can be either input expressions or output expressions. The
input expressions specify how SQLair will extract the query arguments from the
values passed to the `Query` method. Input expressions replace SQL input
placeholders (such as `?` or `$1`).

There are several different types of input expression though all use a dollar
sign ($) in front of the types.

## Input a query parameter

A value in a struct field or map key can be input via the syntax:
```
$<type-name>.<tag/key>
```
The struct or map can be passed to the `Query` function and the specified
value will be extracted, and passed to the driver as a query argument.
For example in the snippet of SQL:
```
...
WHERE manager_name = $Manager.name`,
```

`Manager` is a struct and `name` is the "db" tag on one of its fields.

## Input a slice

A slice of values can be input via the syntax below. The slice name must be a
named slice type.
```
$<slice-name>[:]
```
The values in the slice passed to `Query` that corresponds to this expression
will be expanded into a comma separated list of input placeholders. SQLair does
not insert the parentheses around the values.
For example, the slice can be used with an `IN` clause:
```
...
WHERE name IN ($Names[:])
```
## Insert statements

To insert values from a type into the database SQLair provides specialised
syntax.
```
INSERT INTO <table-name> (*) VALUES ($<type-name>.<key/tag/*>, ...)
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
```
INSERT INTO table (<column-name>, ...) VALUES ($<type-name>.<key/tag/*>, ...)
```
For example, this will only insert the `name` and `postcode`:
```
INSERT INTO person (name, postcode) VALUES ($Person.*, $Address.*)
```

# Formal BNF specification of input syntax
This is the [BNF](https://en.wikipedia.org/wiki/Backus%E2%80%93Naur_form)
description of the syntax of input expressions. The grammar below contains the
three types of input expression given above.

```bnf
<input-expression> ::= <member-input-type> | <slice-input-type> | <insert-expression>

<member-input-type> ::= "$" <type-name> "." <column-name>

<slice-input-type> ::= "$" <slice-name> "[:]"

<insert-expression-asterisk> ::= "(*) VALUES (" <input-types> ")"
<insert-expression-columns> ::= "(" <columns> ") VALUES (" <input-types> ")"

<input-types> ::= <input-type> | ", " <input-types>
<input-type> ::= <asterisk-input-type> | <member-input-type>
<asterisk-input-type> ::= "$" <struct-name> ".*"

<type-name> ::= <struct-name> | <map-name>

<columns> ::= <column> | ", " <columns>
<column> ::= <column-name> | <table-name> "." <column-name>
```

The syntax for the symbols that are not fully expanded above are as follows:
- `<column-name>` - Any valid SQL column name.
- `<table-name>` - Any valid SQL table name.
- `<struct-name>` - Any valid Golang struct name.
- `<map-name>` - Any valid Golang map name.
- `<slice-name>` - Any valid Golang slice name.

