package sqlair

import (
	"database/sql"
	"sync"
)

func (s *Statement) CacheID() uint64 {
	return s.cacheID
}

func (db *DB) CacheID() uint64 {
	return db.cacheID
}

func Cache() (map[uint64]map[uint64]*sql.Stmt, map[uint64]map[uint64]bool, *sync.RWMutex) {
	return stmtCache.stmtDBCache, stmtCache.dbStmtCache, &stmtCache.mutex
}
