/*
SQLair is a convenience layer for SQL databases that embeds Go structs directly into SQL queries.

The SQL syntax is expanded with SQLair input and output expressions which indicate parts of the query that correspond to Go structs.
This allows the user to specify the Go structs they want in the SQL query itself whilst allowing the full power of SQL to be utilised.
This package also provides an alternative API for reading the rows from the database.
It does not copy database/sql, it instead improves upon it and removes inconsistencies.

# Basics

To read the results of a query into a struct, the columns listed in the SQL are replaced by a SQLair output expression.
The output expressions are parsed and replaced with the column names found in the `db` tags of the struct.
Similarly, to use a query argument directly from a Go struct the question mark is replaced by a SQLair input expression.
This package does not parse the SQL query, only the SQLair input/output expressions found within it.

The characters $ and & are used to specify input and outputs expressions respectively.
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

SQLair substitutes &Person.* for all columns in the `db` tags of Person.
The input expression, $Manager.name, instructs SQLair to pass the Name field of the struct Manager as a query argument.

Note that in the SQLair `db` tags (i.e. the column names) appear in the input/output expressions, not the field names.

# Syntax

SQLair input expressions only take one format:

	$Type.col_name

Where Type is a struct and col_name is a `db` tag on one of the structs fields.

SQLair Output expressions, however, can take many forms:

 1. &Type.col_name
    - Fetches col_name and sets the corresponding field of Type.

 2. &Type.*
    - Fetches and sets all the tagged fields of Type.

 3. table.* AS &Type.*
    - Does the sames 2 but prepends all columns with the table name.

 4. (col_name1, col_name2) AS &Type.*
    - Fetches and sets only the specified columns.

 5. (renamed_col1, renamed_col2) AS (&Type.col_name1, &Type.col_name2)
    - Fetches the renamed columns from the database and sets them to the fields of the named columns.

Multiple input and output expressions can be written in a single query.

A SQLair expression will never be substituted for a wildcard asterisk in a SQL query.
*/
package sqlair
