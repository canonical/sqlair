(custom-types)=
# Use custom types in structs
SQLair makes it very easy to serialise to and from structs. But one case that
may cause issue, is when you have a user-defined type in your struct. For
example:
```go
type MyModel struct {
  ID  int    `db:"uuid"`
  Key *MyKey `db:"key"`
}

type MyKey [32]int
```
For this example, `MyKey` could be any user-defined type that can be serialised
to a database row type. The problem is that SQLair and its underlying libraries
do not know how to serialise the `MyKey` type.

Luckily, there are two very useful interfaces you can use to get around this
problem: the [Valuer](https://pkg.go.dev/database/sql/driver#Valuer) and
[Scanner](https://pkg.go.dev/database/sql#Scanner). The `Valuer` interface tells
the driver how to serialise the type for putting in the database and the Scanner
tells it how to deserialise it.

For example:
```go
func (k *MyKey) Value() (driver.Value, error) {
  return k[:], nil
}

func (k *MyKey) Scan(src any) error {
  bs, ok := src.([]byte)
  if !ok {
    return fmt.Errorf("unexpected type %T", src)
  }
  if len(bs) != len(k) {
    return fmt.Errorf("invalid key found in db")
  }
  copy(k[:], bs) 
  return nil
}
```

```{admonition} See more
:class: tip
[Go | Valuer](https://pkg.go.dev/database/sql/driver#Valuer), [Go | Scanner](https://pkg.go.dev/database/sql#Scanner) 
```
