package sqlair_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync"
	"unsafe"

	"github.com/mattn/go-sqlite3"
)

// This file contains a wrapper sql.Driver over the SQLite driver which does
// the real work behind the scenes. On top of it, it monitors the creation and
// closing of prepared statements and stores the references to said statements.
// We can later use that information to check for leaks.

// The stmt registry keeps the pointers for the open and closed statements to
// detect resource leaks. It uses pointers instead of references to the object
// because if we stored a reference the runtime.Finalizer would not be able to
// run.
var openStmts map[uintptr]string = map[uintptr]string{}
var closedStmts map[uintptr]bool = map[uintptr]bool{}
var stmtRegistryMutex sync.RWMutex

// Structs used for wrapping the underlying SQLite driver.
type Driver struct {
	driver.Driver
}
type Conn struct {
	*sqlite3.SQLiteConn
}
type Stmt struct {
	*sqlite3.SQLiteStmt
}

func (s *Stmt) Close() error {
	stmtRegistryMutex.Lock()
	closedStmts[uintptr(unsafe.Pointer(s))] = true
	stmtRegistryMutex.Unlock()

	return s.SQLiteStmt.Close()
}

func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	s, err := c.SQLiteConn.PrepareContext(ctx, query)
	if s, ok := s.(*sqlite3.SQLiteStmt); ok {
		sPtr := &Stmt{s}

		stmtRegistryMutex.Lock()
		openStmts[uintptr(unsafe.Pointer(sPtr))] = query
		stmtRegistryMutex.Unlock()

		return sPtr, err
	} else {
		panic("internal error: base driver is not SQLite")
	}
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	baseConn, err := d.Driver.Open(name)
	if err != nil {
		return nil, err
	}
	if baseConn, ok := baseConn.(*sqlite3.SQLiteConn); ok {
		return &Conn{baseConn}, err
	} else {
		panic("internal error: base driver is not SQLite")
	}
}

func init() {
	sql.Register("sqlite3_stmtChecked", &Driver{
		&sqlite3.SQLiteDriver{},
	})
}
