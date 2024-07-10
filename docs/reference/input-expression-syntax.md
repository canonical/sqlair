(input-expression-syntax)=
# Input expression syntax

This is the [BNF](https://en.wikipedia.org/wiki/Backus%E2%80%93Naur_form) description of the syntax of input expressions:

```bnf
<input-expression> ::= <member-input-type> | <slice-input-type> | <insert-expression>

<member-input-type> ::= "$" <type-name> ".*" <type-member>

<slice-input-type> ::= "$" <slice-name> "[:]"

<insert-expression-asterisk> ::= "(*) VALUES (" <input-types> ")"
<insert-expression-columns> ::= "(" <columns> ") VALUES (" <input-types> ")"

<input-types> ::= <input-type> | ", " <input-types>
<input-type> ::= <asterisk-input-type> | <member-input-type>
<member-input-type> ::= "$" <type-name> ".*" <type-member>
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

