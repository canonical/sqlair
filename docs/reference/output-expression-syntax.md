(output-expression-syntax)=
# Output expression syntax

TODO add verbal descriptions of each of these.

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
