// Copyright 2026 Canonical Ltd.
// Licensed under Apache 2.0, see LICENCE file for details.

package postgresql_test

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/canonical/sqlair"
	"github.com/canonical/sqlair/tests/postgresql"
)

type Person struct {
	ID   int    `db:"id"`
	Name string `db:"name"`
}

// createPersonTable creates the person table and drops it when the test
// finishes.
func createPersonTable(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.ExecContext(t.Context(), `CREATE TABLE person (id integer PRIMARY KEY, name text NOT NULL)`); err != nil {
		t.Fatalf("cannot create table: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if _, err := db.ExecContext(ctx, `DROP TABLE person`); err != nil {
			t.Errorf("cannot drop table: %v", err)
		}
	})
}

func TestOutputs(t *testing.T) {
	sqldb := postgresql.NewDB(t)
	createPersonTable(t, sqldb)
	if _, err := sqldb.ExecContext(t.Context(), `INSERT INTO person (id, name) VALUES (1, 'Fred'), (2, 'Mark')`); err != nil {
		t.Fatalf("cannot insert rows: %v", err)
	}
	db := sqlair.NewDB(sqldb)

	stmt := sqlair.MustPrepare(`SELECT &Person.* FROM person ORDER BY id`, Person{})
	var people []Person
	if err := db.Query(t.Context(), stmt).GetAll(&people); err != nil {
		t.Fatalf("cannot get people: %v", err)
	}
	want := []Person{{ID: 1, Name: "Fred"}, {ID: 2, Name: "Mark"}}
	if !reflect.DeepEqual(people, want) {
		t.Errorf("got %v, want %v", people, want)
	}
}

type IDs []int

func TestRejectAmbiguousPostgreSQLSyntax(t *testing.T) {
	db := postgresql.NewDB(t)
	tests := []struct {
		name, query, postgresQuery, message string
	}{
		{
			"escape string",
			`SELECT $M.id, E'foo\'bar', $1 -- '`,
			`SELECT $1, E'foo\'bar', $1 -- '`,
			"backslash before a quote is not supported",
		},
		{
			"dollar quote",
			`SELECT $M.id, $$'$$, $1 -- '`,
			`SELECT $1, $$'$$, $1 -- '`,
			"dollar-quoted strings are not supported",
		},
		{
			"nested comment",
			`SELECT $M.id, /* outer /* inner */ ' */ $1 -- '`,
			`SELECT $1, /* outer /* inner */ ' */ $1 -- '`,
			"nested block comments are not supported",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := db.ExecContext(t.Context(), test.postgresQuery, "proof"); err != nil {
				t.Fatalf("PostgreSQL did not accept the query: %v", err)
			}
			_, err := sqlair.Prepare(test.query, sqlair.M{})
			if err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("Prepare error = %v, want %q", err, test.message)
			}
		})
	}
}

func TestInputs(t *testing.T) {
	sqldb := postgresql.NewDB(t)
	createPersonTable(t, sqldb)
	db := sqlair.NewDB(sqldb)
	ctx := t.Context()

	// The name column has a single value that is reused in every row.
	insertStmt := sqlair.MustPrepare(`INSERT INTO person (id, name) VALUES ($Person.id, $M.name)`, Person{}, sqlair.M{})
	err := db.Query(ctx, insertStmt, []Person{{ID: 1}, {ID: 2}}, sqlair.M{"name": "Fred"}).Run()
	if err != nil {
		t.Fatalf("cannot bulk insert: %v", err)
	}

	tx, err := db.Begin(ctx, nil)
	if err != nil {
		t.Fatalf("cannot begin transaction: %v", err)
	}
	// Release locks held by the transaction if the test fails before Commit.
	defer tx.Rollback()
	insertOne := sqlair.MustPrepare(`INSERT INTO person (*) VALUES ($Person.*)`, Person{})
	if err := tx.Query(ctx, insertOne, Person{ID: 3, Name: "Mark"}).Run(); err != nil {
		t.Fatalf("cannot insert in transaction: %v", err)
	}
	getOne := sqlair.MustPrepare(`SELECT &Person.* FROM person WHERE id = $Person.id`, Person{})
	mark := Person{ID: 3}
	if err := tx.Query(ctx, getOne, mark).Get(&mark); err != nil {
		t.Fatalf("cannot get person in transaction: %v", err)
	}
	if mark.Name != "Mark" {
		t.Errorf("got %v in transaction, want name Mark", mark)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("cannot commit: %v", err)
	}

	// A slice expands to several placeholders before the next input.
	selectStmt := sqlair.MustPrepare(`SELECT &Person.* FROM person WHERE id IN ($IDs[:]) AND name = $M.name ORDER BY id`, Person{}, IDs{}, sqlair.M{})
	var people []Person
	err = db.Query(ctx, selectStmt, IDs{1, 2, 3}, sqlair.M{"name": "Fred"}).GetAll(&people)
	if err != nil {
		t.Fatalf("cannot get people: %v", err)
	}
	want := []Person{{ID: 1, Name: "Fred"}, {ID: 2, Name: "Fred"}}
	if !reflect.DeepEqual(people, want) {
		t.Errorf("got %v, want %v", people, want)
	}
}
