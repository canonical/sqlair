// Copyright 2024 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package sqlair

import (
	"context"
	"database/sql"
	"runtime"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func TestPackage(t *testing.T) { TestingT(t) }

type CacheSuite struct{}

var _ = Suite(&CacheSuite{})

func (s *CacheSuite) TearDownTest(c *C) {
	// Check every test finishes cleanly.
	s.triggerFinalizers()
	s.checkCacheEmpty(c)
	s.checkDriverStmtsAllClosed(c)
}

func (s *CacheSuite) TearDownSuite(_ *C) {
	stmtRegistryMutex.Lock()
	defer stmtRegistryMutex.Unlock()

	// Reset prepared statements trackers.
	closedStmts = map[string]map[uintptr]bool{}
	openedStmts = map[string]map[uintptr]string{}

	// Reset prepared statements trackers.
	dbQueriesRun = map[string]int{}
	stmtQueriesRun = map[string]int{}
}

func (s *CacheSuite) TestPreparedStatementReuse(c *C) {
	db := s.openDB(c)

	var stmtID uint64
	// For a Statement or DB to be removed from the cache it needs to go out of
	// scope and be garbage collected. A function is used to "forget" the
	// statement.
	func() {
		stmt, err := Prepare(`SELECT 'test'`)
		c.Assert(err, IsNil)
		stmtID = stmt.cacheID

		// Start a query with stmt on db. This will prepare the stmt on the db.
		err = db.Query(nil, stmt).Run()
		c.Assert(err, IsNil)

		// Check a statement is in the cache and a prepared statement has been
		// opened on the DB.
		s.checkStmtInCache(c, db.cacheID, stmt.cacheID)
		s.checkNumDBStmts(c, db.cacheID, 1)
		s.checkDriverStmtsOpened(c, 1)

		// Run the query again.
		err = db.Query(nil, stmt).Run()
		c.Assert(err, IsNil)

		// Check that running a second time does not prepare a second statement.
		s.checkNumDBStmts(c, db.cacheID, 1)
		s.checkDriverStmtsOpened(c, 1)
	}()

	s.triggerFinalizers()

	// Check the prepared statement has been removed from the cache and closed.
	s.checkStmtNotInCache(c, stmtID)
	s.checkDriverStmtsAllClosed(c)
}

func (s *CacheSuite) TestClosingDB(c *C) {
	stmt, err := Prepare(`SELECT 'test'`)
	c.Assert(err, IsNil)

	var dbID uint64
	// For a Statement or DB to be removed from the cache it needs to go out of
	// scope and be garbage collected. A function is used to "forget" the
	// statement.
	func() {
		db := s.openDB(c)
		dbID = db.cacheID

		// Start a query with stmt on db. This will prepare the stmt on the db.
		err = db.Query(nil, stmt).Run()
		c.Assert(err, IsNil)

		// Check a statement is in the cache and a prepared statement has been
		// opened on the DB.
		s.checkStmtInCache(c, db.cacheID, stmt.cacheID)
		s.checkNumDBStmts(c, db.cacheID, 1)
		s.checkDriverStmtsOpened(c, 1)
	}()

	s.triggerFinalizers()
	s.checkDBNotInCache(c, dbID)
	s.checkDriverStmtsAllClosed(c)

	// Check that the statement runs fine on a new DB.
	db := s.openDB(c)
	err = db.Query(nil, stmt).Run()
	c.Assert(err, IsNil)

	// Check the statement has been added to the cache for the new DB.
	s.checkStmtInCache(c, db.cacheID, stmt.cacheID)
	s.checkNumDBStmts(c, db.cacheID, 1)
	s.checkDriverStmtsOpened(c, 2)
}

func (s *CacheSuite) TestStatementPreparedAndClosed(c *C) {
	db := s.openDB(c)

	// For a Statement or DB to be removed from the cache it needs to go out of
	// scope and be garbage collected. A function is used to "forget" the
	// statement.
	func() {
		stmt, err := Prepare(`SELECT 'test'`)
		c.Assert(err, IsNil)

		// Start a query with stmt on db. This will prepare the stmt on the db.
		err = db.Query(nil, stmt).Run()
		c.Assert(err, IsNil)

		// Check a prepared statement has been opened on the DB.
		s.checkDriverStmtsOpened(c, 1)
	}()
	s.triggerFinalizers()
	s.checkDriverStmtsAllClosed(c)
}

func (s *CacheSuite) TestPreparedStatementsClosedWithDB(c *C) {
	stmt, err := Prepare(`SELECT 'test'`)
	c.Assert(err, IsNil)

	// For a Statement or DB to be removed from the cache it needs to go out of
	// scope and be garbage collected. A function is used to "forget" the
	// statement.
	func() {
		db := s.openDB(c)

		// Start a query with stmt on db. This will prepare the stmt on the db.
		err = db.Query(context.Background(), stmt).Run()
		c.Assert(err, IsNil)

		s.checkStmtInCache(c, db.cacheID, stmt.cacheID)
	}()
	s.triggerFinalizers()
	s.checkStmtNotInCache(c, stmt.cacheID)
}

func (s *CacheSuite) TestPreparedStatementsInTX(c *C) {
	db := s.openDB(c)

	stmt, err := Prepare(`SELECT 'test'`)
	c.Assert(err, IsNil)

	// Start a new transaction.
	tx, err := db.Begin(context.Background(), nil)
	c.Assert(err, IsNil)

	// A query executed on a transaction will reuse a prepared statement if it
	// exists, but it will not create one if it does not. The query below should
	// run directly on the DB, not use a prepared statement.
	err = tx.Query(context.Background(), stmt).Run()
	c.Assert(err, IsNil)
	// Check no new statement has been added to the driver cache.
	s.checkNumDBStmts(c, db.cacheID, 0)
	s.checkQueriesRunOnDB(c, 1)
	s.checkQueriesRunOnStmt(c, 0)

	// Prepare the query on the database by running it.
	err = db.Query(context.Background(), stmt).Run()
	c.Assert(err, IsNil)
	s.checkStmtInCache(c, db.cacheID, stmt.cacheID)
	s.checkNumDBStmts(c, db.cacheID, 1)
	s.checkQueriesRunOnDB(c, 1)
	s.checkQueriesRunOnStmt(c, 1)

	// Run the statement on the transaction. This should reuse the prepared
	// statement.
	err = tx.Query(context.Background(), stmt).Run()
	c.Assert(err, IsNil)
	// Check no new statement has been added to the driver cache.
	s.checkQueriesRunOnDB(c, 1)
	s.checkQueriesRunOnStmt(c, 2)

	err = tx.Commit()
	c.Assert(err, IsNil)
}

// TestLateQuery checks that a Query that outlives a Statement does not throw a
// statement is closed error.
func (s *CacheSuite) TestLateQuery(c *C) {
	var q *Query
	// Drop all the values except the query itself.
	func() {
		db := s.openDB(c)

		selectStmt, err := Prepare(`SELECT 'hello'`)
		c.Assert(err, IsNil)
		q = db.Query(nil, selectStmt)
	}()

	s.triggerFinalizers()

	// Assert that sql.Stmt was not closed early.
	c.Assert(q.Run(), IsNil)
}

// TestLateQueryTX checks that a Query on a transaction that outlives a
// Statement does not throw a statement is closed error.
func (s *CacheSuite) TestLateQueryTX(c *C) {
	var q *Query

	// Drop all the values except the query itself.
	func() {
		db := s.openDB(c)

		selectStmt, err := Prepare(`SELECT 'hello'`)
		c.Assert(err, IsNil)
		tx, err := db.Begin(nil, nil)
		c.Assert(err, IsNil)
		q = tx.Query(nil, selectStmt)
	}()

	s.triggerFinalizers()

	// Assert that sql.Stmt was not closed early.
	c.Assert(q.Run(), IsNil)
}

// TestQueryWithBulkAndSlice checks that a sqlair.Statements that generate
// different SQL strings when given different arguments work with the cache.
func (s *CacheSuite) TestQueryWithBulkAndSlice(c *C) {
	db := s.openDB(c)
	createStmt, err := Prepare(`
CREATE TABLE t (
	col integer
);`)
	c.Assert(err, IsNil)
	err = db.Query(context.Background(), createStmt).Run()
	createStmt = nil
	c.Assert(err, IsNil)

	type dbCol struct {
		Col int `db:"col"`
	}

	insertStmt, err := Prepare(`INSERT INTO t (*) VALUES ($dbCol.*)`, dbCol{})
	c.Assert(err, IsNil)

	// Bulk insert some columns
	insertCols := []dbCol{{Col: 1}, {Col: 2}}
	err = db.Query(context.Background(), insertStmt, insertCols).Run()
	c.Assert(err, IsNil)

	// Bulk insert a different number of columns using the same statement.
	insertCols = []dbCol{{Col: 3}, {Col: 4}, {Col: 5}}
	err = db.Query(context.Background(), insertStmt, insertCols).Run()
	c.Assert(err, IsNil)

	type dbCols []int
	selectStmt, err := Prepare(`SELECT col AS &dbCol.* FROM t WHERE col IN ($dbCols[:])`, dbCols{}, dbCol{})
	c.Assert(err, IsNil)

	cols := dbCols{1, 3, 5}
	colsFromDB := []dbCol{}
	err = db.Query(context.Background(), selectStmt, cols).GetAll(&colsFromDB)
	c.Assert(err, IsNil)
	c.Assert(colsFromDB, DeepEquals, []dbCol{{Col: 1}, {Col: 3}, {Col: 5}})

	cols = dbCols{2, 4}
	colsFromDB = []dbCol{}
	err = db.Query(context.Background(), selectStmt, cols).GetAll(&colsFromDB)
	c.Assert(err, IsNil)
	c.Assert(colsFromDB, DeepEquals, []dbCol{{Col: 2}, {Col: 4}})
}

func (s *CacheSuite) openDB(c *C) *DB {
	db, err := sql.Open("sqlite3_stmtChecked", "file:test.db?cache=shared&mode=memory&testName="+c.TestName())
	c.Assert(err, IsNil)
	return NewDB(db)
}

func (s *CacheSuite) triggerFinalizers() {
	// Try to run finalizers by calling GC several times.
	for i := 0; i <= 10; i++ {
		runtime.GC()
		time.Sleep(0)
	}
}

func (s *CacheSuite) checkStmtInCache(c *C, dbID, stmtID uint64) {
	stmtCache.mutex.RLock()
	defer stmtCache.mutex.RUnlock()
	_, ok := stmtCache.stmtDBCache[stmtID][dbID]
	c.Check(ok, Equals, true)
	_, ok = stmtCache.dbStmtCache[dbID][stmtID]
	c.Check(ok, Equals, true)
}

func (s *CacheSuite) checkStmtNotInCache(c *C, stmtID uint64) {
	stmtCache.mutex.RLock()
	defer stmtCache.mutex.RUnlock()
	dbc, ok := stmtCache.stmtDBCache[stmtID]
	if ok {
		c.Check(dbc, HasLen, 0)
	}

	for _, dbc := range stmtCache.dbStmtCache {
		_, ok := dbc[stmtID]
		c.Check(ok, Equals, false)
	}
}

func (s *CacheSuite) checkDBNotInCache(c *C, dbID uint64) {
	stmtCache.mutex.RLock()
	defer stmtCache.mutex.RUnlock()
	_, ok := stmtCache.dbStmtCache[dbID]
	c.Check(ok, Equals, false)

	for _, sc := range stmtCache.stmtDBCache {
		_, ok := sc[dbID]
		c.Check(ok, Equals, false)
	}
}

func (s *CacheSuite) checkNumDBStmts(c *C, dbID uint64, n int) {
	stmtCache.mutex.RLock()
	defer stmtCache.mutex.RUnlock()
	sc, ok := stmtCache.dbStmtCache[dbID]
	c.Check(ok, Equals, true)
	c.Check(sc, HasLen, n)

	numDBStmts := 0
	for _, dbc := range stmtCache.stmtDBCache {
		if _, ok := dbc[dbID]; ok {
			numDBStmts += 1
		}
	}
	c.Check(numDBStmts, Equals, n)
}

func (s *CacheSuite) checkCacheEmpty(c *C) {
	stmtCache.mutex.RLock()
	defer stmtCache.mutex.RUnlock()
	c.Check(stmtCache.stmtDBCache, HasLen, 0)
	c.Check(stmtCache.dbStmtCache, HasLen, 0)
}

func (s *CacheSuite) checkDriverStmtsAllClosed(c *C) {
	stmtRegistryMutex.RLock()
	defer stmtRegistryMutex.RUnlock()
	c.Check(len(openedStmts[c.TestName()]), Equals, len(closedStmts[c.TestName()]))
}

func (s *CacheSuite) checkDriverStmtsOpened(c *C, n int) {
	stmtRegistryMutex.RLock()
	defer stmtRegistryMutex.RUnlock()
	c.Check(openedStmts[c.TestName()], HasLen, n)
}

func (s *CacheSuite) checkQueriesRunOnDB(c *C, n int) {
	queriesRunMutex.RLock()
	defer queriesRunMutex.RUnlock()
	c.Check(dbQueriesRun[c.TestName()], Equals, n)
}

func (s *CacheSuite) checkQueriesRunOnStmt(c *C, n int) {
	queriesRunMutex.RLock()
	defer queriesRunMutex.RUnlock()
	c.Check(stmtQueriesRun[c.TestName()], Equals, n)
}
