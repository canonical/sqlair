package sqlair

import (
	"context"
	"database/sql"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/canonical/sqlair/internal/expr"
)

// statementCache caches the sql.Stmt objects associated with each
// sqlair.Statement. A sqlair.Statement can correspond to multiple sql.Stmt
// objects prepared on different databases. Entries in the cache are therefore
// indexed by the sqlair.Statement cache ID and the sqlair.DB cache ID.
//
// A finalizer is set on sqlair.Statement objects to close the assosiated
// sql.Stmt objects. Similarly a finalizer is set on sqlair.DB objects to close
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
func (sc *statementCache) newStatement(te *expr.TypedExpr) *Statement {
	cacheID := atomic.AddUint64(&sc.stmtIDCount, 1)
	sc.mutex.Lock()
	sc.stmtDBCache[cacheID] = map[uint64]*sql.Stmt{}
	sc.mutex.Unlock()
	s := &Statement{te: te, cacheID: cacheID}
	// This finalizer is run after the Statement is garbage collected.
	runtime.SetFinalizer(s, sc.getStmtFinalizer(s))
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
func (sc *statementCache) prepareStmt(ctx context.Context, dbID uint64, ps prepareSubstrate, s *Statement) (*sql.Stmt, error) {
	var err error
	sc.mutex.RLock()
	// The Statement cache ID is only removed from stmtDBCache when the
	// finalizer is run. The Statements cache ID must be in the stmtDBCache
	// since we hold a reference to the Statement. It is therefore safe to
	// access in it in the map without first checking it exists.
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
// all sql.Stmt objects prepared on the database, removes the database from then
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
