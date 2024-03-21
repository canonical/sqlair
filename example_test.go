package sqlair_test

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/canonical/sqlair"
)

func ExampleQuery_Get() {
	type Employee struct {
		ID   int    `db:"employee_id"`
		Name string `db:"name"`
		Team string `db:"team_name"`
	}

	db, err := employeeDB()
	if err != nil {
		return
	}

	stmt, err := sqlair.Prepare("SELECT &Employee.* FROM employees", Employee{})
	if err != nil {
		return
	}

	var e Employee
	err = db.Query(context.Background(), stmt).Get(&e)
	if err != nil {
		return
	}

	fmt.Printf("Employee: %+v", e)

	// Output: Employee: {ID:1 Name:Alastair Team:Juju}
}

func ExampleQuery_Get_withInput() {
	type Employee struct {
		ID   int    `db:"employee_id"`
		Name string `db:"name"`
		Team string `db:"team_name"`
	}

	type Team struct {
		ID   int    `db:"team_id"`
		Name string `db:"name"`
	}

	db, err := employeeDB()
	if err != nil {
		return
	}

	stmt, err := sqlair.Prepare(
		"SELECT &Employee.* FROM employees WHERE team_name = $Team.name",
		Employee{}, Team{},
	)
	if err != nil {
		return
	}

	var e Employee
	team := Team{Name: "Juju", ID: 1}
	err = db.Query(context.Background(), stmt, team).Get(&e)
	if err != nil {
		return
	}

	fmt.Printf("Employee: %+v", e)

	// Output: Employee: {ID:1 Name:Alastair Team:Juju}
}

func ExampleQuery_GetAll() {
	type Employee struct {
		ID   int    `db:"employee_id"`
		Name string `db:"name"`
		Team string `db:"team_name"`
	}

	db, err := employeeDB()
	if err != nil {
		return
	}

	stmt, err := sqlair.Prepare("SELECT &Employee.* FROM employees", Employee{})
	if err != nil {
		return
	}

	var es []Employee
	err = db.Query(context.Background(), stmt).GetAll(&es)
	if err != nil {
		return
	}

	fmt.Printf("Employees: %+v", es)

	// Output: Employees: [{ID:1 Name:Alastair Team:Juju} {ID:2 Name:Alberto Team:OCTO}]
}

func ExampleQuery_Iter() {
	type Employee struct {
		ID   int    `db:"employee_id"`
		Name string `db:"name"`
		Team string `db:"team_name"`
	}

	db, err := employeeDB()
	if err != nil {
		return
	}

	stmt, err := sqlair.Prepare("SELECT &Employee.* FROM employees", Employee{})
	if err != nil {
		return
	}

	var es []Employee
	iter := db.Query(context.Background(), stmt).Iter()
	for iter.Next() {
		var e Employee
		err := iter.Get(&e)
		if err != nil {
			return
		}
		es = append(es, e)
	}
	err = iter.Close()
	if err != nil {
		return
	}

	fmt.Printf("Employees: %+v", es)
	// Output: Employees: [{ID:1 Name:Alastair Team:Juju} {ID:2 Name:Alberto Team:OCTO}]
}

func ExampleQuery_Run() {
	type Employee struct {
		ID   int    `db:"employee_id"`
		Name string `db:"name"`
		Team string `db:"team_name"`
	}

	db, err := employeeDB()
	if err != nil {
		return
	}

	insertStmt, err := sqlair.Prepare(`INSERT INTO employees (*) VALUES ($Employee.*)`, Employee{})
	if err != nil {
		return
	}

	alastair := Employee{ID: 1, Name: "Alastair", Team: "Juju"}

	err = db.Query(context.Background(), insertStmt, alastair).Run()
	if err != nil {
		return
	}

	fmt.Printf("Employee inserted: %+v", alastair)

	// Output: Employee inserted: {ID:1 Name:Alastair Team:Juju}
}

func employeeDB() (*sqlair.DB, error) {
	type Employee struct {
		ID   int    `db:"employee_id"`
		Name string `db:"name"`
		Team string `db:"team_name"`
	}

	sqldb, err := sql.Open("sqlite3", "file:example.db?mode=memory")
	if err != nil {
		return nil, err
	}
	db := sqlair.NewDB(sqldb)

	createStmt, err := sqlair.Prepare(`CREATE TABLE employees (employee_id integer, team_name string, name text);`)
	if err != nil {
		return nil, err
	}

	err = db.Query(context.Background(), createStmt).Run()
	if err != nil {
		return nil, err
	}

	insertStmt, err := sqlair.Prepare(`INSERT INTO employees (*) VALUES ($Employee.*)`, Employee{})
	if err != nil {
		return nil, err
	}

	alastair := Employee{ID: 1, Name: "Alastair", Team: "Juju"}
	alberto := Employee{ID: 2, Name: "Alberto", Team: "OCTO"}

	err = db.Query(context.Background(), insertStmt, alastair).Run()
	if err != nil {
		return nil, err
	}

	err = db.Query(context.Background(), insertStmt, alberto).Run()
	if err != nil {
		return nil, err
	}

	return db, nil
}
