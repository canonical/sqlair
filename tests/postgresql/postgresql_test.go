// Copyright 2026 Canonical Ltd.
// Licensed under Apache 2.0, see LICENCE file for details.

package postgresql_test

import (
	"context"
	"database/sql"
	"reflect"
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
