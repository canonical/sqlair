package sqlair

import (
	"database/sql"
	"sync"

	"github.com/google/uuid"
)

func (s *Statement) ID() uuid.UUID {
	return s.id
}

func (db *DB) Cache() (map[uuid.UUID]*sql.Stmt, *sync.RWMutex) {
	return db.stmtCache.c, &db.stmtCache.m
}

func (tx *TX) Cache() (map[uuid.UUID]*sql.Stmt, *sync.RWMutex) {
	return tx.stmtCache.c, &tx.stmtCache.m
}
