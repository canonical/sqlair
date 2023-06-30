package sqlair

import (
	"database/sql"
	"sync"
)

func (s *Statement) CacheID() int64 {
	return s.cacheID
}

func (tx *TX) CacheID() int64 {
	return tx.cacheID
}

func (db *DB) CacheID() int64 {
	return db.cacheID
}

func Cache() (map[int64]map[int64]*sql.Stmt, *sync.RWMutex) {
	return stmtCache, &cacheMutex
}
