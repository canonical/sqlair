(functions)=
# Get SQL function results

To output the result of a SQL function such as `COUNT`, `MAX` or `SUM`, use the
`AS` keyword to select the function result into an output variable, then run
the query.

For example:

```go
type Count struct {
    Num int `db:"num"`
}

stmt, err := sqlair.Prepare(
    "SELECT COUNT(*) AS &Count.num FROM employees", 
    Count{},
)
if err != nil {
    return err
}

var count Count
err := db.Query(ctx, stmt).Get(&count)
if err != nil {
    return err
}

fmt.Printf("Number of employees: %d", count.Num)
```

```{admonition} See more
:class: tip
{ref}`output-expression-syntax`
```
