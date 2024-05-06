// Copyright 2024 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.
package sqlair

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"sync"
	"unsafe"

	"github.com/mattn/go-sqlite3"
)

// This file contains a wrapper sql.Driver over the SQLite driver which
// monitors the creation and closing of prepared statements and stores the
// references to said statements. We can later use that information to check
// for statement leaks.

// openedStmts and closedStmts store the pointers to the created/closed
// statements indexed by test case. We use unsafe pointers instead of references
// to the objects because if we stored a reference the runtime.Finalizer would
// not be able to run.
var openedStmts = map[string]map[uintptr]string{}
var closedStmts = map[string]map[uintptr]bool{}
var stmtRegistryMutex sync.RWMutex

// dbQueriesRun and stmtQueriesRun count the number of queries run directly
// against the database and queries that are run through a prepared statement.
// The maps are indexed by the test name. The queriesRunMutex must be used when
// accessing the counts.
var dbQueriesRun = map[string]int{}
var stmtQueriesRun = map[string]int{}
var queriesRunMutex sync.RWMutex

type Driver struct {
	driver.Driver
}

type Conn struct {
	testName string
	*sqlite3.SQLiteConn
}

type Stmt struct {
	testName string
	*sqlite3.SQLiteStmt
}

func (s *Stmt) Close() error {
	stmtRegistryMutex.Lock()
	defer stmtRegistryMutex.Unlock()
	_, ok := closedStmts[s.testName]
	if !ok {
		closedStmts[s.testName] = map[uintptr]bool{}
	}
	closedStmts[s.testName][uintptr(unsafe.Pointer(s))] = true

	return s.SQLiteStmt.Close()
}

func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	s, err := c.SQLiteConn.PrepareContext(ctx, query)
	if sm, ok := s.(*sqlite3.SQLiteStmt); ok {
		sPtr := &Stmt{SQLiteStmt: sm, testName: c.testName}

		stmtRegistryMutex.Lock()
		defer stmtRegistryMutex.Unlock()
		_, ok := openedStmts[c.testName]
		if !ok {
			openedStmts[c.testName] = map[uintptr]string{}
		}
		openedStmts[c.testName][uintptr(unsafe.Pointer(sPtr))] = query

		return sPtr, err
	} else {
		panic(fmt.Sprintf("internal error: base driver is not SQLite, got %T", s))
	}
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *Conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	rows, err := c.SQLiteConn.Query(query, args)
	if err == nil {
		queriesRunMutex.Lock()
		defer queriesRunMutex.Unlock()
		if _, ok := dbQueriesRun[c.testName]; ok {
			dbQueriesRun[c.testName] += 1
		} else {
			dbQueriesRun[c.testName] = 1
		}
	}
	return rows, err
}

func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.SQLiteConn.QueryContext(ctx, query, args)
	if err == nil {
		queriesRunMutex.Lock()
		defer queriesRunMutex.Unlock()
		if _, ok := dbQueriesRun[c.testName]; ok {
			dbQueriesRun[c.testName] += 1
		} else {
			dbQueriesRun[c.testName] = 1
		}
	}
	return rows, err
}

func (c *Conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	res, err := c.SQLiteConn.Exec(query, args)
	if err == nil {
		queriesRunMutex.Lock()
		defer queriesRunMutex.Unlock()
		if _, ok := dbQueriesRun[c.testName]; ok {
			dbQueriesRun[c.testName] += 1
		} else {
			dbQueriesRun[c.testName] = 1
		}
	}
	return res, err
}

func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	res, err := c.SQLiteConn.ExecContext(ctx, query, args)
	if err == nil {
		queriesRunMutex.Lock()
		defer queriesRunMutex.Unlock()
		if _, ok := dbQueriesRun[c.testName]; ok {
			dbQueriesRun[c.testName] += 1
		} else {
			dbQueriesRun[c.testName] = 1
		}
	}
	return res, err
}

func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	rows, err := s.SQLiteStmt.Query(args)
	if err == nil {
		queriesRunMutex.Lock()
		defer queriesRunMutex.Unlock()
		if _, ok := stmtQueriesRun[s.testName]; ok {
			stmtQueriesRun[s.testName] += 1
		} else {
			stmtQueriesRun[s.testName] = 1
		}
	}
	return rows, err
}

func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := s.SQLiteStmt.QueryContext(ctx, args)
	if err == nil {
		queriesRunMutex.Lock()
		defer queriesRunMutex.Unlock()
		if _, ok := stmtQueriesRun[s.testName]; ok {
			stmtQueriesRun[s.testName] += 1
		} else {
			stmtQueriesRun[s.testName] = 1
		}
	}
	return rows, err
}

func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	res, err := s.SQLiteStmt.Exec(args)
	if err == nil {
		queriesRunMutex.Lock()
		defer queriesRunMutex.Unlock()
		if _, ok := stmtQueriesRun[s.testName]; ok {
			stmtQueriesRun[s.testName] += 1
		} else {
			stmtQueriesRun[s.testName] = 1
		}
	}
	return res, err
}

func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	fmt.Println(s.SQLiteStmt)
	res, err := s.SQLiteStmt.ExecContext(ctx, args)
	if err == nil {
		queriesRunMutex.Lock()
		defer queriesRunMutex.Unlock()
		if _, ok := stmtQueriesRun[s.testName]; ok {
			stmtQueriesRun[s.testName] += 1
		} else {
			stmtQueriesRun[s.testName] = 1
		}
	}
	return res, err
}

const TestNameTag = "testName"

// Open expects the DSN to contain the test name using the testNameTag
// attribute.
func (d *Driver) Open(name string) (driver.Conn, error) {
	var testName string
	parameters := strings.Split(name, "?")[1]
	for _, p := range strings.Split(parameters, "&") {
		if strings.HasPrefix(p, TestNameTag) {
			testName = strings.Split(p, "=")[1]
		}
	}
	if testName == "" {
		panic("internal error: testName is not found in the db DSN")
	}

	baseConn, err := d.Driver.Open(name)
	if err != nil {
		return nil, err
	}
	if baseConn, ok := baseConn.(*sqlite3.SQLiteConn); ok {
		return &Conn{SQLiteConn: baseConn, testName: testName}, err
	} else {
		panic("internal error: base driver is not SQLite")
	}
}

func init() {
	sql.Register("sqlite3_stmtChecked", &Driver{
		&sqlite3.SQLiteDriver{},
	})
}
