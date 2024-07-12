(input-expressions)=
# How to input 
SQLair input expressions can be used anywhere in a SQL query you would normally use an input placeholder (such as `?`, or `$1`). They always start with a doller sign (`$`). For example:
```
INSERT INTO Person (name, age) 
VALUES ($Person.*)
```

The `Person` object here should be tagged with the column names (see {doc}`preparing-go-objects` for more). SQLair will then expand it into a series of input placeholders corresponding to the number of columns being inserted and send the generated SQL to the database, along with the argument values extracted from the struct.

This is not the only form of input expression. There are several ways you can input values with SQLair. This how-to guide takes you through the different forms.

See {ref}`output-expression-syntax` for the exact valid syntax.

##### Input a single parameter
The syntax `$Struct.col_name` can be used to input the field of the struct tagged with `col_name` in the query. This also works with key of maps. 

Example:
```
SELECT &Struct.*
FROM   table
WHERE  col_name1 = $Struct.col_name1
AND    col_name2 = $Map.col_name2
```



##### Input a slice
The syntax `$Slice[:]` can be used to pass all the values of a slice as a parameter. The `Slice` object here must be a named slice type.

Example:
```
SELECT &Struct.*
FROM   table
WHERE  name IN ($Names[:])
```

##### Insert all columns of objects
The syntax `INSERT INTO table (*) VALUES ($Type1.*, $Type2.col_name2, ...)` can be used to insert columns from structs into the database. SQLair will expand the asterisk on the left hand side into all the column names specified on the right. If a struct on the left is followed by an asterisk it will insert all tagged fields on the struct. Types with a single field/key specifed will only have just that member inserted.

It will also insert the corrent number of parameter palceholders on the right. The values specified on the right will then be passed to the database as query arguments and be inserted into the database.

Example:
```
INSERT INTO table (*)
VALUES ($Struct1.*, $Struct2.col_name1, $Struct2.col_name2, $Map.key)
```

##### Insert particular columns from objects
The syntax `(col_name1, col_name2, ...) VALUES ($Type1.*, $Type2.col_name2, ...)` can be used to insert specified columns from the collection of objects on the right. It works the same way as inserting all the columns of objects (above) excepet it will only insert those specified on the left.

Example:
```
INSERT INTO TABLE (col_name1, col_name2, col_name4)
VALUES ($Struct1.*, $Struct2.col_name2, $Map.*)
```
