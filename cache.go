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

// statementCache caches the driver prepared sql.Stmt objects associated with
// each sqlair.Statement. A sqlair.Statement can correspond to multiple sql.Stmt
// objects prepared on different databases. Entries in the cache are therefore
// indexed by the sqlair.Statement cache ID and the sqlair.DB cache ID.
//
// A finalizer is set on sqlair.Statement objects to close the associated
// sql.Stmt objects. Similarly, a finalizer is set on sqlair.DB objects to close
// all sql.Stmt objects prepared on the DB, close the DB, and remove the DB
// cache ID from the cache.
//
// The mutex must be locked when accessing either the stmtDBCache or the
// dbStmtCache.
type statementCache struct {
	// stmtDBCache stores sql.Stmt objects addressed via the cache ID of the
	// sqlair.Statement they built from and the sqlair.DB they are prepared on.
	stmtDBCache map[uint64]map[uint64]*sql.Stmt

	// dbStmtCache indicates when a sqlair.Statement has been prepared on a particular sqlair.DB.
	dbStmtCache map[uint64]map[uint64]bool

	// stmtIDCount and dbIDCount are monotonically increasing counters used to
	// generate unique new cache IDs.
	stmtIDCount uint64
	dbIDCount   uint64

	mutex sync.RWMutex
}

var once sync.Once
var singleStmtCache *statementCache

// newStatementCache returns the single instance of the statement cache.
func newStatementCache() *statementCache {
	once.Do(func() {
		singleStmtCache = &statementCache{
			stmtDBCache: map[uint64]map[uint64]*sql.Stmt{},
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
	sc.stmtDBCache[cacheID] = map[uint64]*sql.Stmt{}
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

// lookupStmt checks if a *sql.Stmt corresponding to s has been prepared on db
// and stored in the cache.
func (sc *statementCache) lookupStmt(db *DB, s *Statement) (*sql.Stmt, bool) {
	// The Statement cache ID is only removed from stmtDBCache when the
	// finalizer is run. The Statements cache ID must be in the stmtDBCache
	// since we hold a reference to the Statement. It is therefore safe to
	// access in it in the map without first checking it exists.
	sc.mutex.RLock()
	sqlstmt, ok := sc.stmtDBCache[s.cacheID][db.cacheID]
	sc.mutex.RUnlock()
	return sqlstmt, ok
}

// driverPrepareStatement prepares a statement on the database and then stores
// the prepared *sql.Stmt in the cache.
func (sc *statementCache) driverPrepareStmt(ctx context.Context, db *DB, s *Statement, sql string) (*sql.Stmt, error) {
	sqlstmt, err := db.sqldb.PrepareContext(ctx, sql)
	if err != nil {
		return nil, err
	}
	sc.mutex.Lock()
	defer sc.mutex.Unlock()
	// Check if sqlstmt has been inserted by another process else already.
	sqlstmtAlt, ok := sc.stmtDBCache[s.cacheID][db.cacheID]
	if ok {
		err = sqlstmt.Close()
		if err != nil {
			return nil, err
		}
		sqlstmt = sqlstmtAlt
	} else {
		sc.stmtDBCache[s.cacheID][db.cacheID] = sqlstmt
		sc.dbStmtCache[db.cacheID][s.cacheID] = true
	}
	return sqlstmt, nil
}

// removeAndCloseStmtFunc removes and closes all sql.Stmt objects associated
// with the argument Statement from the statement caches of each DB.
func (sc *statementCache) removeAndCloseStmtFunc(s *Statement) {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()
	dbCache := sc.stmtDBCache[s.cacheID]
	for dbCacheID, sqlstmt := range dbCache {
		sqlstmt.Close()
		delete(sc.dbStmtCache[dbCacheID], s.cacheID)
	}
	delete(sc.stmtDBCache, s.cacheID)
}

// removeAndCloseDBFunc closes and removes from the cache all sql.Stmt objects
// prepared on the database, removes the database from then cache, then closes
// the sql.DB.
func (sc *statementCache) removeAndCloseDBFunc(db *DB) {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()
	statementCache := sc.dbStmtCache[db.cacheID]
	for statementCacheID := range statementCache {
		dbCache := sc.stmtDBCache[statementCacheID]
		dbCache[db.cacheID].Close()
		delete(dbCache, db.cacheID)
	}
	delete(sc.dbStmtCache, db.cacheID)
	db.sqldb.Close()
}
