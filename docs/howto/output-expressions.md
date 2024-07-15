(output-expressions)=
# How to output
SQLair output expressions are designed to replace the column selection part of a SQL query. They always start with an ampersand (`&`). For example:
```
SELECT &Person.*
FROM   person
```
The `Person` object here should be tagged with the column names. SQLair will then expand the struct into the names of the columns it is tagged with and send the generated SQL to the database. When you get the results, it will automatically scan the columns into the correct fields of the person struct.

There are several different forms of output expression. Their forms and functions are described below.

See {ref}`output-expression-syntax` for the exact valid syntax.

## Get a single column in an object
The syntax `&Struct.col_name` with fetch and set the field of the type `Struct` tagged with `col_name`. This can also be done with maps, `&Map.key` will fetch the column key from the database and insert it into the map with the key `"key"`.

Example:
```
SELECT &Struct.col_name,
       &Map.key
FROM   table
```

## Get all the columns in a struct
The syntax `&Struct.*` fetches and sets all the tagged fields of `Struct`. SQLair will expand the type into all the tagged column names and insert the results into the struct when it gets the results.

Example:
```
SELECT &Struct.*
FROM   table
```

## Get all the columns in a struct from a particular table
The syntax `table.* AS &Struct.*` does the same as getting all the fields of a struct (above) but prepends all columns with the table name. The tags on the struct should not include the table name.

Example:
```
SELECT     t.* AS &Struct1.*,
           s.* AS &Struct2.*
FROM       t
INNER JOIN s
```

## Get a subset of an objects columns
The syntax `(table1.col_name1, table2.col_name2) AS &Struct.*` fetches and sets only the specified columns (the table names are optional). This syntax can also be used with maps. If a table name is included the map key or the struct tag do not include the table name, it is only mentioned in the query.

Example:
```
SELECT     (t.col_name1, s.col_name2) AS (&Struct1.*), 
           (t.col_name3, s.col_name4) AS (&Struct2.*)
FROM       t
INNER JOIN s
```

## Put particular columns in particular places in an object
The syntax `(col_name1, col_name2) AS (&Type.other_col1, &Type.other_col2)` will fetch the specified columns and put them in the type locations specified. If `Type` is a struct this will be the fields tagged with `other_col1` and `other_col2`, if it was a map it would be the fields with that name.

This form should only really be used if you are selecting from a table you would not usually select from. The tags on the fields of the struct should match the columns in the database.

Example
```
SELECT (col_name1, col_name2) AS (&Struct.other_col1, &Map.other_col2)
FROM   table
```


