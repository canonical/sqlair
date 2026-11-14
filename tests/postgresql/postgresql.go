// Copyright 2026 Canonical Ltd.
// Licensed under Apache 2.0, see LICENCE file for details.

// Package postgresql runs SQLair tests against a PostgreSQL server. It is a
// separate module so that SQLair itself does not depend on a PostgreSQL
// driver.
//
// The tests are skipped unless SQLAIR_POSTGRESQL_TEST_URL is set to the URL of
// a server, for example postgres://postgres@localhost:5432/postgres. Each test
// runs in a new schema, which is dropped when the test finishes.
package postgresql

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
)

// URLEnv is the environment variable that holds the PostgreSQL server URL.
const URLEnv = "SQLAIR_POSTGRESQL_TEST_URL"

// NewDB returns a database connected to a new, empty schema. The schema is
// dropped when the test finishes. Tests must drop the objects they create;
// the test fails if the schema is not empty.
func NewDB(t testing.TB) *sql.DB {
	t.Helper()
	url := os.Getenv(URLEnv)
	if url == "" {
		t.Skipf("%s is not set", URLEnv)
	}
	config, err := pgx.ParseConfig(url)
	if err != nil {
		t.Fatalf("cannot parse %s: %v", URLEnv, err)
	}
	suffix := make([]byte, 4)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatalf("cannot generate schema name: %v", err)
	}
	schema := fmt.Sprintf("sqlair_test_%d_%s", time.Now().Unix(), hex.EncodeToString(suffix))
	config.RuntimeParams["search_path"] = schema

	db := stdlib.OpenDB(*config)
	db.SetMaxOpenConns(2)
	if _, err := db.ExecContext(t.Context(), "CREATE SCHEMA "+schema); err != nil {
		db.Close()
		t.Fatalf("cannot create schema %s: %v", schema, err)
	}
	t.Cleanup(func() {
		defer db.Close()
		// The test context is canceled before cleanup functions run.
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, err := db.ExecContext(ctx, "DROP SCHEMA "+schema)
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "2BP01" {
			// Avoid adding noise to an earlier failure.
			if !t.Failed() {
				t.Errorf("schema %s is not empty at the end of the test: %v", schema, err)
			}
			_, err = db.ExecContext(ctx, "DROP SCHEMA "+schema+" CASCADE")
		}
		if err != nil {
			t.Errorf("cannot drop schema %s: %v", schema, err)
		}
	})
	return db
}
