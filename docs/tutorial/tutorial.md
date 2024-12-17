(tutorial)=
# Get started with SQLair

Imagine you are writing a Go program that handles your customers and their
orders. The business logic is contained in the Go program but the customer and
order data is stored in a SQL database. In this tutorial, you'll see how easy
SQLair makes it to map between the database and the Go program.
Let's get started!


## Set things up
For this tutorial you will need Go version 1.18. To use SQLair you will need a
Go project that sets up a database. 

To create the project run:
```bash
mkdir tutorial 
cd tutorial
go mod init sqlair-tutorial
```

To set up the database, in the tutorial folder, create a new `main.go` file,
open it in your favourite editor and copy in the code below:
```go
package main

import (
    // database/sql is used to open the database and set up the schema.
    "database/sql"
    "fmt"

    // go-sqlite3 provides the SQLite driver. Importing it registers the driver 
    // so that sql.Open can find it.
    // SQLair can be used with any database/driver that works with the 
    // database/sql package.
    _ "github.com/mattn/go-sqlite3"
)

// CreateDB opens a SQLite database and adds a customer and order schema.
func CreateDB() (*sql.DB, error) {
    // sql.Open will create an in process, in memory database that will disappear 
    // when the program finishes running.
    sqlDB, err := sql.Open(
        "sqlite3", 
        "file:sqlair-tutorial.db?cache=shared&mode=memory",
    )
    if err != nil {
        return nil, fmt.Errorf("opening SQLite database: %w", err)
    }

    // Use database/sql to create the database schema.
    _, err = sqlDB.Exec(`
    CREATE TABLE customers (
        id integer PRIMARY KEY,
        name text,
        address text,
        city text, 
        postal_code text,
        country text
    );

    CREATE TABLE orders (
        id integer PRIMARY KEY,
        customer_id integer,
        order_date timestamp,
        FOREIGN KEY (customer_id) REFERENCES customer (id)
    );
`)
    if err != nil {
        return nil, fmt.Errorf("creating tables: %w", err)
    }
    
    return sqlDB, nil
}

func tutorial() error {
    // Create the database.
    _, err := CreateDB()
    if err != nil {
        return err
    }
    
    return nil
}

func main() {
    err := tutorial()
    if err != nil {
        fmt.Printf("ERROR: %s\n", err)
    }
    fmt.Println("Finished without errors!")
}
```
Now, to test the program, install the dependencies and run:
```bash
go mod tidy
go run .
```
You should get the output: `"Finished without errors!"`.

## Wrap the database with SQLair

To get started with SQLair, wrap your `database/sql` database with SQLair. 

In `main.go`, at the top of the file, add SQLair to the imported functions.
```go
import (
    ...
    "github.com/canonical/sqlair"
    ...
)
```
In `main.go`, replace the `tutorial` function with the new version below:
```go
func tutorial() error {
    // Create the database.
    sqlDB, err := CreateDB()
    if err != nil {
    return err
    }

    // Wrap the DB with SQLair
    db := sqlair.NewDB(sqlDB)

    return nil
}
```
Install the dependencies and run:
```bash
go mod tidy
go run .
```
Again, you should get the output: `"Finished without errors!"`.

## SQLair-proof your types

Next, define structs to represent customers and orders. These correspond to the
rows of the tables in the database. 

In the tutorial project, in the `main.go` file add these types at the top of the
file, below the imports. Note the db tags (e.g., `db:"id"`) -- SQLair will use
them to work out how the struct fields map to the table columns in the database.
```go
type Customer struct {
    ID         int    `db:"id"`
    Name       string `db:"name"`
    Address    string `db:"address"`
    City       string `db:"city"`
    PostalCode string `db:"postal_code"`
    Country    string `db:"country"`
}

type Order struct {
    ID         int       `db:"id,omitempty"`
    CustomerID int       `db:"customer_id"`
    OrderDate  time.Time `db:"order_date"`
}
```

```{admonition} See more
:class: tip
{ref}`types`, [Go | Struct Tags](https://go.dev/wiki/Well-known-struct-tags)
```

## Put your struct in the database

Now that you have the scaffolding let's populate our database with some data. To
do this build an insert statement. 

To insert a `Customer` struct you can write the following SQLair query:
```
INSERT INTO customers (*) VALUES ($Customer.*)
```
The special syntax `$Customer.*` is called a SQLair input expression. These are
marked by the dollar sign (`$`). This tells SQLair that we want to insert all
tagged fields from the `Customer` struct. The asterisk (`*`) on the left had
side of the `VALUES` keyword tells SQLair that you want to insert all the values
listed on the right.

Now that we have written our query the next stage is to prepare it for execution
with [`sqlair.Prepare`](https://pkg.go.dev/github.com/canonical/sqlair#Prepare).


In the `main.go` file, just above the `tutorial` function definition, paste the
code below. Note that
[`sqlair.Prepare`](https://pkg.go.dev/github.com/canonical/sqlair#Prepare) also
takes samples of all the Go types mentioned in the query. 
```go
func populateDB(db *sqlair.DB) error {
    // Prepare uses the reflection information from type samples to verify the
    // SQLair expressions and generate the SQL to send to the database.    
    insertCustomerStmt, err := sqlair.Prepare(`
        INSERT INTO customers (*) 
        VALUES ($Customer.*)
        `, Customer{},
    )
    if err != nil {
        return fmt.Errorf("preparing insert customer statement: %w", err)
    }
    
    return nil
}
```

```{admonition} See more
:class: tip
{ref}`input-expression-syntax`, {ref}`query`
```

To insert the data into the database, create a `Query` on the database using the
[`DB.Query`](https://pkg.go.dev/github.com/canonical/sqlair#Query) method and
run it with
[`Query.Run`](https://pkg.go.dev/github.com/canonical/sqlair#Query.Run).

In the `populateDB` function, under the previous code but above the
`return nil`, paste the code below:

```go
// Define a customer to insert.
customer := Customer{
    ID:         1,
    Name:       "Joe",
    Address:    "15 Westmeade Road",
    City:       "Hounslow",
    PostalCode: "TW4 7EY",
    Country:    "England",
}

// Create the query object on the database and Run it without expecting any 
// results back.
err = db.Query(context.Background(), insertCustomerStmt, customer).Run()
if err != nil {
    return fmt.Errorf("inserting initial customer: %w", err)
}
```
Congratulations! You have just learnt how to insert a struct into a database
with SQLair.

## Put all the structs in the database

To fill up our database, you don't just want to insert a single customer, you
want to insert ALL the customers. Instead of passing a single customer struct as
an argument to `DB.Query`, you can pass a slice of customer structs, and SQLair
will automatically turn your query into a bulk insert.

In `main.go` in the `populateDB` function, replace the `customer` variable
declaration with the slices of customers below, and change the `customer` in the
query to `customers`.

```go
// Define the customers to insert.
customers := []Customer{{
    ID:         1,
    Name:       "Joe",
    Address:    "15 Westmeade Road",
    City:       "Hounslow",
    PostalCode: "TW4 7EY",
    Country:    "England",
}, {
    ID:         2,
    Name:       "Simon",
    Address:    "11 Pearch Avenue",
    City:       "Birmingham",
    PostalCode: "B37 5NA",
    Country:    "England",
}, {
    ID:         3,
    Name:       "Heather",
    Address:    "6 Moorland Close",
    City:       "Inverness",
    PostalCode: "IV1 6PA",
    Country:    "Scotland",
}}

// Insert the slice of customers.
err = db.Query(context.Background(), insertCustomerStmt, customers).Run()
if err != nil {
    return fmt.Errorf("inserting initial customers: %w", err)
}
```

To compile the code, in at the top of `main.go` in the `import` section, add the
`time` and `context` packages.
```go
import (
    // database/sql is used to open the database and set up the schema.
    "database/sql"
    "fmt"
    "time"
    "context"
    ...
)
```
Now delete the old `tutorial` function and replace it with the one below: 
```
func tutorial() error {
    db, err := NewDB()
    if err != nil {
        return err
    }

    err = populateDB(db)
    if err != nil {
        return fmt.Errorf("populating database %w", err)
    }

    return nil
}
```
Now let's check it works:
```go
go run .
```
You should get the output: `"Finished without errors!"`.
### Put the orders in the database
Now you've added your customers, it's time to add some orders! 

At the bottom of the `populateDB` function but before the `return nil` add the
order data.
```go
orders = []Order{{
    CustomerID: 1,
    OrderDate:  time.Date(2024, time.January, 10, 0, 0, 0, 0, time.UTC),
}, {
    CustomerID: 1,
    OrderDate:  time.Date(2024, time.September, 20, 0, 0, 0, 0, time.UTC),
}, {
    CustomerID: 2,
    OrderDate:  time.Date(2024, time.August, 23, 0, 0, 0, 0, time.UTC),
}, {
    CustomerID: 3,
    OrderDate:  time.Date(2024, time.January, 3, 0, 0, 0, 0, time.UTC),
}, {
    CustomerID: 3,
    OrderDate:  time.Date(2024, time.April, 11, 0, 0, 0, 0, time.UTC),
}, {
    CustomerID: 3,
    OrderDate:  time.Date(2024, time.June, 8, 0, 0, 0, 0, time.UTC),
}}
```

**It's your turn now!** Have a go at adding the orders to the database. It should
look very similar to the adding the customers.

If you want to see the answer, click below.
```{dropdown} How to insert the orders
```go
orders = []Order{{
    CustomerID: 1,
    OrderDate:  time.Date(2024, time.January, 10, 0, 0, 0, 0, time.UTC),
}, {
    CustomerID: 1,
    OrderDate:  time.Date(2024, time.September, 20, 0, 0, 0, 0, time.UTC),
}, {
    CustomerID: 2,
    OrderDate:  time.Date(2024, time.August, 23, 0, 0, 0, 0, time.UTC),
}, {
    CustomerID: 3,
    OrderDate:  time.Date(2024, time.January, 3, 0, 0, 0, 0, time.UTC),
}, {
    CustomerID: 3,
    OrderDate:  time.Date(2024, time.April, 11, 0, 0, 0, 0, time.UTC),
}, {
    CustomerID: 3,
    OrderDate:  time.Date(2024, time.June, 8, 0, 0, 0, 0, time.UTC),
}}

insertOrdersStmt, err := sqlair.Prepare(
    "INSERT INTO orders (*) VALUES ($Order.*)",
    Order{},
)
if err != nil {
    return fmt.Errorf("preparing insert orders statement: %w", err)
}

err = db.Query(context.Background(), insertOrdersStmt, orders).Run()
if err != nil {
    return fmt.Errorf("inserting initial orders: %w", err)
}
```
## Get structs from the database

Now that we have the customer and order data it is time to query it.

### Get a customer
Somebody has asked you get all the information you have about the customer with
the name `"Joe"`. You need to put all his information in a `Customer` struct and
return this to the user.

In `main.go`, in the `tutorial` function under the call to `populateDB`, add the
code below.
```go
stmt, err = sqlair.Prepare(`
    SELECT &Customer.* 
    FROM customers
    WHERE name = $Customer.name
`, Customer{})
if err != nil {
    return Customer{}, fmt.Errorf("preparing select customer statement: %w", err)
}
```
The special syntax in the query, `&Customer.*`, is an output expression. The
ampersand (`&`) marks output expressions. This tells SQLair that you want to
fetch all columns given in the tags of the `Customer` struct and use them to
fill the struct in.

In the `WHERE` clause at the bottom, there is an input expression. This is
telling SQLair to pass the value in the field tagged with `name` in `Customer`
as an argument to the database.

```{note}
The column name from the tag rather than the field name is used when specifying
a value in a struct e.g. `$Customer.name`.
```

To get the results out, we can use
[`Query.Get`](https://pkg.go.dev/github.com/canonical/sqlair#Query.Get). In the
`tutorial` function, below the `sqlair.Prepare`, add the code below:
```go
// Define a customer struct with only the name specified. This can be used for 
// both the input and output of the query.
joe := Customer{
    Name: "Joe",
}

// Get returns the first query result from the database.
err = db.Query(context.Background(), stmt, joe).Get(&joe)
if err != nil {
    return Customer{}, fmt.Errorf(
        "selecting customer %s from the database: %w", 
        name, err,
    )
}

fmt.Printf("Customer record of Joe: %#v\n", joe)
```

Run the program, and you should get the output below:
```bash
$ go run .
Customer record of Joe: main.Customer{ID:1, Name:"Joe", Address:"15 Westmeade Road", City:"Hounslow", PostalCode:"TW4 7EY", Country:"England"}
Finished without errors!
```

```{admonition} See more
:class: tip
{ref}`output-expression-syntax`, {ref}`input-expression-syntax`, [`Query.Get`](https://pkg.go.dev/github.com/canonical/sqlair#Query.Get)
```

### Get all the orders

You've just been asked to get all the orders ever made from the
database. They want a slice of `Order` structs representing this information.
Luckily, SQLair has your back with the `Query.GetAll` function.

Let's first prepare a query to select the orders. In the `tutorial` function,
copy the code below:
```go
stmt, err := sqlair.Prepare(`
    SELECT &Order.*
    FROM Orders
`, Order{})
if err != nil {
    return fmt.Errorf("preparing select orders statement: %w", err)
}

// Define a slice of orders to hold all the orders from the database.
var orders []Order
// Get all the orders.
err = db.Query(context.Background(), stmt).GetAll(&orders)
if err != nil {
    return fmt.Errorf("getting all orders from the database: %w", err)
}
// Print out the orders.
for i, order := range orders {
    fmt.Printf("Order %d: %#v\n", i, order)
}

```
Now run the program and you should get the output below.
```bash
go run .
```
```bash
Order 0: main.Order{ID:1, CustomerID:1, OrderDate:time.Date(2024, time.January, 10, 0, 0, 0, 0, time.UTC)}
Order 1: main.Order{ID:2, CustomerID:1, OrderDate:time.Date(2024, time.September, 20, 0, 0, 0, 0, time.UTC)}
Order 2: main.Order{ID:3, CustomerID:2, OrderDate:time.Date(2024, time.August, 23, 0, 0, 0, 0, time.UTC)}
Order 3: main.Order{ID:4, CustomerID:3, OrderDate:time.Date(2024, time.January, 3, 0, 0, 0, 0, time.UTC)}
Order 4: main.Order{ID:5, CustomerID:3, OrderDate:time.Date(2024, time.April, 11, 0, 0, 0, 0, time.UTC)}
Order 5: main.Order{ID:6, CustomerID:3, OrderDate:time.Date(2024, time.June, 8, 0, 0, 0, 0, time.UTC)}
Finished without errors!
```
You have all the orders. Congratulations! You have learnt the basics of SQLair!

```{admonition} See more
:class: tip
[`Query.GetAll`](https://pkg.go.dev/github.com/canonical/sqlair#Query.GetAll)
```

## Tear things down
To restore your machine to the state it had before you started this tutorial,
simply delete the directory. No further steps required.
```bash
cd ..
rm -r tutorial
```
## Next steps
This tutorial has introduced you to the basics, but there is a lot more to
explore!
```{admonition} See more
:class: tip
{ref}`howto`, {ref}`reference`,
[pkg.go.dev](https://pkg.go.dev/github.com/canonical/sqlair)
```
