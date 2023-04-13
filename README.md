# SQLair

SQLair is a package for Go that acts as a compatibility layer between Go and SQL databases. It allows for easy mapping between Go objects and query arguments and results.

SQLair allows you to write SQL with SQLair input/output expressions included where mapping is needed. These expressions indicate the Go objects to map the database arguments/results into. SQLair is not an ORM and will leave all parts of the query outside of the expressions untouched.

### Motivation
When writing an SQL query with `database/sql` in Go there are multiple points of redundency/failure:

- The order of the columns in the Query must match the order of columns in `Scan`
- The columns from the Query must be manuelly match to their destinations
- If the columns needed change all queries must be changed

For example, when selecting a particular `Person` from a database, instead of the query: 
```SQL
SELECT name_col, id_col, gender_col FROM person WHERE manager_col = ?
```
In SQLair you could write:
```SQL
SELECT &Person.* FROM person WHERE manager_col = $Manager.name
```
This results from this second query could then be directly decoded in the the `Person` struct.


# Usage
The first step when using SQLair is to tag your structs. The "`db`" tag is used to map between the database column names and struct fields.

For example:
```Go
type Person struct {
	Name	string	`db:"name_col"`
	ID 	int64 	`db:"id_col"`
	Gender	string  `db:"gender_col"`
}
```
It is important to note that SQLair __needs__ the fields to be public in order read from them and write to them.

To run a query with SQLair you need to create your `sqlair.DB` object. This is done by wrapping a `sql.DB` with:
```Go
db := sqlair.NewDB(sqldb)
```

Now to get a `Person` from the database.

```Go
stmt := sqlair.MustPrepare(
    "SELECT &Person.* FROM people",
    Person{},
)

var person := Person{}
err := db.Query(stmt).One(&person)
```

Or maybe we want all the people from the database

```Go
var people := []Person{}
err := db.Query(stmt).All(&people)
```

Or maybe some more precise control is needed

```Go
iter := db.Query(stmt).Iter()
for iter.Next() {
    var p := Person{}
    if !iter.Decode(&p) {
        break
    }
    doSomethingWithPerson(p)
}
err := iter.Close()
```
### Example
For a full example see [the demo](demo/demo.go).

## Writing the SQL
### Input Expressions
To specify SQLair inputs and outputs, the characters `$` and `&` are used.

For now, input expressions are limited to `$Type.col_name`. In the case of the `Person` struct above we could write:
```SQL
SELECT name_col FROM person WHERE id_col = $Person.id_col
```
When we run `DB.Query(ctx, stmt, &person) the value in the `ID` field will be used as the query argument.
 
### Output Expressions
With output expressions we can do much more. 

|Output expressions| Result |
| --- | --- |
| &Person.name\_col | The Name field of Person is set to the result from the name column |
| &Person.\* | All columns mentioned in the field tags of Person are set to the result of their tagged column |
| t.\* AS &Person.\* | All columns mentioned in the field tags of Person are set to the results of the tagged column from table `t` |
| (client\_name, client\_id) AS (&Person.name\_col, &Person.id\_col) | The Name and ID fields of Person will be set with the results from client\_name and client\_id |
| (gender\_col, name\_col) AS &Person.\* | The Gender and Name fields of Person will be set with the results from gender\_col and name\_col |

# FAQ


## Contributing

See our [code and contribution guidelines](CONTRIBUTING.md)

