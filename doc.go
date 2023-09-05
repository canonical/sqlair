/*
SQLair is a convenience layer for SQL databases that embeds Go structs and maps directly into SQL queries.

The SQL syntax is expanded with SQLair input and output expressions which indicate the parts of the query that correspond to Go structs or maps.
This allows the user to specify the Go structs/maps they want in the SQL query itself whilst allowing the full power of SQL to be utilised.
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

If Type is a struct then col_name is a `db` tag on one of the structs fields.
If Type is a map then col_name is a key in the map.

SQLair input expressions can take the following formats:

 1. $Type.col_name
 	- Can be used anywhere in the SQL statement.
	- If Type is a struct the field tagged with col_name will be passed as a parameter here.
	- If Type is a map the value with the key col_name will be passed as a parameter here.

 2. (*) VALUES ($Type1.*, $Type2.col_name2, ...)
 	- Follows an INSERT INTO ... clause.
	- Types followed by an asterisk must be a struct.
	- Types followed by an asterisk insert all tagged fields in the Type into the columns specified
          in the field's the db tag.
	- Types followed by a column name insert the field with the matching tag in structs and the
          value associated with that key in maps.

 3. (col_name1, col_name2, ...) VALUES ($Type.*)
 	- Follows an INSERT INTO ... clause.
	- Inserts the columns from Type.

 4. (col_name1, col_name2, ...) VALUES ($Type.other_col1, $Type.other_col2)
 	- Follows an INSERT INTO ... clause.
	- Inserts other_col1 and other_col2 from Type into the columns col_name1 and col_name2

SQLair output expressions can take the following formats:

 1. &Type.col_name
    - Fetches col_name and sets it in Type.
    - If Type is a struct this will be the field tagged with col_name.
    - If Type is a map this will be the value with key "col_name".

 2. &Type.*
    - Fetches and sets all the tagged fields of Type.
    - This form cannot be used with maps.

 3. table.* AS &Type.*
    - Does the same as 2 but prepends all columns with the table name.

 4. (t1.col_name1, t2.col_name2) AS (&Type.*)
    - Fetches and sets only the specified columns (the table is optional).
    - If Type is a map they will be stored at "col_name1" and "col_name2".

 5. (col_name1, col_name2) AS (&Type.other_col1, &Type.other_col2)
    - Fetches the columns from the database and stores them at other_col1 and other_col2 in Type.

Multiple input and output expressions can be written in a single query.
*/
package sqlair
