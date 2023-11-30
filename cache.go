package sqlair

import (
	"context"
	"database/sql"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/canonical/sqlair/internal/expr"
)

// stmtIDCount and dbIDCount are global variables to used to generate unique
// IDs.
var stmtIDCount int64
var dbIDCount int64

type dbID = int64
type stmtID = int64

// statementCache caches the sql.Stmt objects associated with each
// sqlair.Statement. A sqlair.Statement can correspond to multiple sql.Stmt
// values on different databases. The cache is indexed by the sqlair.Statement
// ID and the sqlair.DB ID.
//
// The cache closes sql.Stmt objects with a finalizer on the sqlair.Statement.
// Similarly a finalizer is set on sqlair.DB objects to close all statements
// prepared on the DB, close the DB, and remove references to the DB from the
// cache.
//
// The mutex must be locked when accessing either the stmtDBCache or the
// dbStmtCache.
type statementCache struct {
	stmtDBCache map[stmtID]map[dbID]*sql.Stmt
	dbStmtCache map[dbID]map[stmtID]bool
	mutex       sync.RWMutex
}

var once sync.Once
var singleStmtCache *statementCache

// newStatementCache returns the single instance of the statement cache.
func newStatementCache() *statementCache {
	once.Do(func() {
		singleStmtCache = &statementCache{
			stmtDBCache: map[stmtID]map[dbID]*sql.Stmt{},
			dbStmtCache: map[dbID]map[stmtID]bool{},
		}
	})
	return singleStmtCache
}

// newStatement returns a new sqlair.Statement and allocates it in the cache. A
// finalizer is set on the sqlair.Statement to remove all sql.Stmt values
// associated with it from the cache and then run Close on the sql.Stmt values.
// The finalizer is run after the sqlair.Statement is garbage collected.
func (sc *statementCache) newStatement(te *expr.TypedExpr) *Statement {
	cacheID := atomic.AddInt64(&stmtIDCount, 1)
	s := &Statement{te: te, cacheID: cacheID}
	sc.mutex.Lock()
	sc.stmtDBCache[cacheID] = map[dbID]*sql.Stmt{}
	sc.mutex.Unlock()
	runtime.SetFinalizer(s, sc.getStmtFinalizer(s))
	return s
}

// newDB returns a new sqlair.DB and allocates it in the cache. A finalizer is
// set on the sqlair.DB which removes it from the cache, closes all sql.Stmt
// values prepared upon it and then closes the DB. The finalizer is run after
// the sqlair.DB is garbage collected.
func (sc *statementCache) newDB(sqldb *sql.DB) *DB {
	cacheID := atomic.AddInt64(&dbIDCount, 1)
	sc.mutex.Lock()
	sc.dbStmtCache[cacheID] = map[stmtID]bool{}
	sc.mutex.Unlock()
	db := &DB{sqldb: sqldb, cacheID: cacheID}
	runtime.SetFinalizer(db, sc.getDBFinalizer(db))
	return db
}

// prepareSubstrate is an object that queries can be prepared on, e.g. a sql.DB
// or sql.Conn. It is used in prepareStmt.
type prepareSubstrate interface {
	PrepareContext(context.Context, string) (*sql.Stmt, error)
}

// prepareStmt prepares a Statement on a prepareSubstrate. It first checks in
// the cache to see if it has already been prepared on the DB.
// The prepareSubstrate must be associated with the same DB that prepareStmt is
// a method of.
func (sc *statementCache) prepareStmt(ctx context.Context, dbID dbID, ps prepareSubstrate, s *Statement) (*sql.Stmt, error) {
	var err error
	sc.mutex.RLock()
	// The statement ID is only removed from the cache when the finalizer is
	// run, so it is always in stmtDBCache.
	sqlstmt, ok := sc.stmtDBCache[s.cacheID][dbID]
	sc.mutex.RUnlock()
	if !ok {
		sqlstmt, err = ps.PrepareContext(ctx, s.te.SQL())
		if err != nil {
			return nil, err
		}
		sc.mutex.Lock()
		// Check if a statement has been inserted by someone else since we last
		// checked.
		sqlstmtAlt, ok := sc.stmtDBCache[s.cacheID][dbID]
		if ok {
			sqlstmt.Close()
			sqlstmt = sqlstmtAlt
		} else {
			sc.stmtDBCache[s.cacheID][dbID] = sqlstmt
			sc.dbStmtCache[dbID][s.cacheID] = true
		}
		sc.mutex.Unlock()
	}
	return sqlstmt, nil
}

// getStmtFinalizer returns a finalizer that removes a Statement from the
// statement caches and closes it.
func (sc *statementCache) getStmtFinalizer(s *Statement) func(*Statement) {
	return func(s *Statement) {
		sc.mutex.Lock()
		defer sc.mutex.Unlock()
		dbCache := sc.stmtDBCache[s.cacheID]
		for dbCacheID, sqlstmt := range dbCache {
			sqlstmt.Close()
			delete(sc.dbStmtCache[dbCacheID], s.cacheID)
		}
		delete(sc.stmtDBCache, s.cacheID)
	}
}

// getDBFinalizer returns a finalizer that closes and removes from the cache
// all sql.Stmt values prepared on the database, removes the database from then
// cache, then closes the sql.DB.
func (sc *statementCache) getDBFinalizer(db *DB) func(*DB) {
	return func(db *DB) {
		sc.mutex.Lock()
		defer sc.mutex.Unlock()
		statementCache := sc.dbStmtCache[db.cacheID]
		for statementCacheID, _ := range statementCache {
			dbCache := sc.stmtDBCache[statementCacheID]
			dbCache[db.cacheID].Close()
			delete(dbCache, db.cacheID)
		}
		delete(sc.dbStmtCache, db.cacheID)
		db.sqldb.Close()
	}
}
