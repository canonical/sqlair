(types)=
# How to prepare your Go types to use with SQLair 

To use your Go types with SQLair you will need to do some preparation. SQLair
supports inputs from structs maps and slices and can output into structs
and maps. 

In general, all types used with SQLair need to be named, otherwise there is no
way to reference them in the query. The rest of the preparation of the type
depends on what kind it is.

## How use a struct with SQLair

To use a struct with SQLair tag all the fields that correspond to a database
column with a `db` tag specifying the column name. The fields must be public
(i.e. start with a capital letter), though the struct does not have to be. Any
fields without a tag will be ignored.

For example:
```go
type Person struct {
    Name    string  `db:"name"`
    ID      int     `db:"id_number"`
    DoB     string  `db:"date_of_birth"`
    Weight  int
    height  int
}
```
In this example the `Weight` and `height` fields will be ignored by SQLair.

This struct and its corresponding columns can be referenced from a SQLair input
or output expression.

If a struct contains an embedded struct then SQLair will treat the fields of the
embedded structs as if they were fields in the parent struct.

## How to use a map with SQLair

To use a map with SQLair, make sure the type is named and that the base key type
is string. The value type of the map can be anything.

The key type must be string because SQLair uses the column names as keys to the
map for inputting and outputting data. The type needs to be named as the name is
used to reference it in the queries input and output expressions.

For example:
```go
type Cols map[string]string
```

For convenience, SQLair provides a named map type `sqlair.M` which has the type
`map[string]any`.


## How to use a slice with SQLair

Slices can only be used as inputs and will expand into a comma separated list of
input placeholders for each value in the slice. The slice must be named and can
be of any type.

For example:
```go
type Names []string
```

