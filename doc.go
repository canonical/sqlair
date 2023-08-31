/*
SQLair is a convenience layer for SQL databases that embeds Go types directly into SQL queries.

The SQL syntax is expanded with SQLair input and output expressions which indicate the parts of the query that correspond to Go types.
This allows the user to specify the Go types they want in the SQL query itself whilst allowing the full power of SQL to be utilised.
The Go types in the input and output expressions can be structs, maps or primitive types.
SQLair is designed around for use with the struct type.
Struct fields are tagged with column names allowing query results to be read directly into them.
This package also provides an alternative API for reading the rows from the database.
SQLair relies on database/sql for all the underlying operations.

# Basics

The characters $ and & are used to specify SQLair input and outputs expressions respectively.
For example, given the following tagged struct "Person":

	type Person struct {
		Name	string	`db:"name"`
		ID	int	`db:"id"`
		Team	string  `db:"team"`
	}

Instead of the SQL query:

	SELECT name, id, team
	FROM person
	WHERE manager_name = ?

With SQLair one would write:

	SELECT &Person.*
	FROM person
	WHERE manager_name = $Manager.name

Note that in the SQLair `db` tags (i.e. the column names) appear in the input/output expressions, not the field names.

# Syntax

SQLair input expressions take the format:

	$Type.col_name

If Type is a struct then col_name is a `db` tag on one of the structs fields.
If Type is a map then col_name is a key in the map.
If Type is a primitive type then the col_name is dropped e.g. "$string".

SQLair output expressions can take the following formats:

 1. &Type.col_name
    - Fetches col_name and sets it in Type.
    - If Type is a struct this will be the field tagged with col_name.
    - If Type is a map this will be the value with key "col_name".
    - This form cannot be used with primitive types, format 4 must be used.

 2. &Type.*
    - Fetches and sets all the tagged fields of Type.
    - This form cannot be used with maps or primitive types.

 3. table.* AS &Type.*
    - Does the same as 2 but prepends all columns with the table name.

 4. (t1.col_name1, t2.col_name2) AS &Type.*
    - Fetches and sets only the specified columns (the table is optional).
    - If Type is a map they will be stored at "col_name1" and "col_name2".
	- This form cannot be used with primitive types.

 5. (col_name1, col_name2) AS (&Type.other_col1, &Type.other_col2)
    - Fetches the columns from the database and stores them at other_col1 and other_col2 in Type.
	- If Type is a primitive type then the "other_col" is dropped e.g. "(col_name AS &string)".


Multiple input and output expressions can be written in a single query.
*/
package sqlair
