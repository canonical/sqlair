package sqlair

import (
	"database/sql"
	"sync"
)

func (s *Statement) CacheID() int64 {
	return s.cacheID
}

func (db *DB) CacheID() int64 {
	return db.cacheID
}

func Cache() (map[int64]map[int64]*sql.Stmt, map[int64]map[int64]bool, *sync.RWMutex) {
	return stmtDBCache, dbStmtCache, &cacheMutex
}
