// Copyright 2024 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package sqlair

import (
	"context"
	"database/sql"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/canonical/sqlair/internal/expr"
)

// statementCache caches driver-prepared sql.Stmt objects associated with
// sqlair.Statement objects. The driver-prepared sql.Stmt objects corresponding
// to a single sqlair.Statement can be prepared on different databases and have
// different generated SQL (depending on query arguments).
//
// Entries in the cache are indexed by the sqlair.Statement cache ID and the
// sqlair.DB cache ID. Each entry contains the sql.Stmt and the SQL string that
// was used to create it.
//
// A finalizer is set on sqlair.Statement objects to close the associated
// sql.Stmt objects and remove them from the cache. Similarly, a finalizer is
// set on sqlair.DB objects to close all sql.Stmt objects prepared on the DB,
// close the DB, and remove the DB cache ID from the cache.
//
// Only a single driver-prepared sql.Stmt is cached for each sqlair.DB/
// sqlair.Statement pair. If the sqlair.Statement is re-prepared with different
// generated SQL then the previous sql.Stmt is evicted from the cache. A
// finalizer is set on the evicted sql.Stmt to ensure it is closed once all
// references die.
//
// The mutex must be locked when accessing either the stmtDBCache or the
// dbStmtCache.
type statementCache struct {
	// stmtDBCache stores driverStmt objects containing a sql.Stmt and the sql
	// used to generate it. These are addressed via the cache ID of the
	// corresponding sqlair.Statement and the cache ID of the sqlair.DB they are
	// prepared against.
	stmtDBCache map[uint64]map[uint64]*driverStmt

	// dbStmtCache indicates when a sqlair.Statement has been prepared on a
	// particular sqlair.DB.
	dbStmtCache map[uint64]map[uint64]bool

	// stmtIDCount and dbIDCount are monotonically increasing counters used to
	// generate unique new cache IDs.
	stmtIDCount uint64
	dbIDCount   uint64

	mutex sync.RWMutex
}

// driverStmt represents a SQL statement prepared against a database driver.
type driverStmt struct {
	stmt *sql.Stmt
	sql  string
}

var once sync.Once
var singleStmtCache *statementCache

// newStatementCache returns the single instance of the statement cache.
func newStatementCache() *statementCache {
	once.Do(func() {
		singleStmtCache = &statementCache{
			stmtDBCache: map[uint64]map[uint64]*driverStmt{},
			dbStmtCache: map[uint64]map[uint64]bool{},
		}
	})
	return singleStmtCache
}

// newStatement returns a new sqlair.Statement and adds it to the cache. A
// finalizer is set on the sqlair.Statement to remove its ID from the cache and
// close all associated sql.Stmt objects.
func (sc *statementCache) newStatement(te *expr.TypeBoundExpr) *Statement {
	cacheID := atomic.AddUint64(&sc.stmtIDCount, 1)
	sc.mutex.Lock()
	sc.stmtDBCache[cacheID] = map[uint64]*driverStmt{}
	sc.mutex.Unlock()
	s := &Statement{te: te, cacheID: cacheID}
	// This finalizer is run after the Statement is garbage collected.
	runtime.SetFinalizer(s, sc.removeAndCloseStmtFunc)
	return s
}

// newDB returns a new sqlair.DB and allocates the necessary resources in the
// statementCache. A finalizer is set on the sqlair.DB to remove references to
// it from the cache, close all sql.Stmt objects on it, and close the sql.DB
func (sc *statementCache) newDB(sqldb *sql.DB) *DB {
	cacheID := atomic.AddUint64(&sc.dbIDCount, 1)
	sc.mutex.Lock()
	sc.dbStmtCache[cacheID] = map[uint64]bool{}
	sc.mutex.Unlock()
	db := &DB{sqldb: sqldb, cacheID: cacheID}
	// This finalizer is run after the DB is garbage collected.
	runtime.SetFinalizer(db, sc.removeAndCloseDBFunc)
	return db
}

// lookupStmt checks if a Statement has been prepared on the db driver with the
// given primedSQL. If it has, the driverStmt is returned.
func (sc *statementCache) lookupStmt(db *DB, s *Statement, primedSQL string) (dStmt *driverStmt, ok bool) {
	// The Statement cache ID is only removed from stmtDBCache when the
	// finalizer is run. The Statement's cache ID must be in the stmtDBCache
	// since we hold a reference to the Statement. It is therefore safe to
	// access in it in the map without first checking it exists.
	sc.mutex.RLock()
	ds, ok := sc.stmtDBCache[s.cacheID][db.cacheID]
	sc.mutex.RUnlock()
	// Check if the sql of the driver statement matches the requested primedSQL.
	if !ok || ds.sql != primedSQL {
		return nil, false
	}
	return ds, ok
}

// driverPrepareStatement prepares a statement on the database and then stores
// the prepared *sql.Stmt in the cache.
func (sc *statementCache) driverPrepareStmt(ctx context.Context, db *DB, s *Statement, primedSQL string) (*driverStmt, error) {
	sqlstmt, err := db.sqldb.PrepareContext(ctx, primedSQL)
	if err != nil {
		return nil, err
	}

	sc.mutex.Lock()
	defer sc.mutex.Unlock()

	// If there is already a statement in the cache, set a finalizer on it to
	// close it once concurrent users have finished with it and replace it with
	// ours.
	if ds, ok := sc.stmtDBCache[s.cacheID][db.cacheID]; ok {
		runtime.SetFinalizer(ds, func(ds *driverStmt) {
			ds.stmt.Close()
		})
	}
	ds := &driverStmt{sql: primedSQL, stmt: sqlstmt}
	sc.stmtDBCache[s.cacheID][db.cacheID] = ds
	sc.dbStmtCache[db.cacheID][s.cacheID] = true
	return ds, nil
}

// removeAndCloseStmtFunc removes and closes all sql.Stmt objects associated
// with the argument Statement from the statement caches of each DB.
func (sc *statementCache) removeAndCloseStmtFunc(s *Statement) {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()
	dbCache := sc.stmtDBCache[s.cacheID]
	for dbCacheID, ds := range dbCache {
		ds.stmt.Close()
		delete(sc.dbStmtCache[dbCacheID], s.cacheID)
	}
	delete(sc.stmtDBCache, s.cacheID)
}

// removeAndCloseDBFunc closes and removes from the cache all sql.Stmt objects
// prepared on the database, removes the database from then cache.
func (sc *statementCache) removeAndCloseDBFunc(db *DB) {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()
	stmtCache := sc.dbStmtCache[db.cacheID]
	for statementCacheID := range stmtCache {
		dbCache := sc.stmtDBCache[statementCacheID]
		dbCache[db.cacheID].stmt.Close()
		delete(dbCache, db.cacheID)
	}
	delete(sc.dbStmtCache, db.cacheID)
}
