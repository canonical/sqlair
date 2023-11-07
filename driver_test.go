package sqlair_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"strings"
	"sync"
	"unsafe"

	"github.com/mattn/go-sqlite3"
)

// This file contains a wrapper sql.Driver over the SQLite driver which
// monitors the creation and closing of prepared statements and stores the
// references to said statements. We can later use that information to check
// for statement leaks.

// The stmt registry keeps the pointers for the open and closed statements to
// detect resource leaks. It uses unsafe pointers instead of references to the
// object because if we stored a reference the runtime.Finalizer would not be
// able to run.
var openedStmts = map[string]map[uintptr]string{}
var closedStmts = map[string]map[uintptr]bool{}
var stmtRegistryMutex sync.RWMutex

// Structs used for wrapping the underlying SQLite driver.
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
	if s, ok := s.(*sqlite3.SQLiteStmt); ok {
		sPtr := &Stmt{SQLiteStmt: s, testName: c.testName}

		stmtRegistryMutex.Lock()
		defer stmtRegistryMutex.Unlock()
		_, ok := openedStmts[c.testName]
		if !ok {
			openedStmts[c.testName] = map[uintptr]string{}
		}
		openedStmts[c.testName][uintptr(unsafe.Pointer(sPtr))] = query

		return sPtr, err
	} else {
		panic("internal error: base driver is not SQLite")
	}
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

const testNameTag = "testName"

func (d *Driver) Open(name string) (driver.Conn, error) {
	var testName string
	parameters := strings.Split(name, "?")[1]
	for _, p := range strings.Split(parameters, "&") {
		if strings.HasPrefix(p, testNameTag) {
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
