package expr

import (
	"context"
	"database/sql"
)

type ResultExpr struct {
	outputs []field
	rows    *sql.Rows
}

type DB struct {
	*sql.DB
}

func NewDB(db *sql.DB) *DB {
	return &DB{db}
}

func (db *DB) Query(ce *CompletedExpr) (*ResultExpr, error) {
	return db.QueryContext(ce, context.Background())
}

func (db *DB) QueryContext(ce *CompletedExpr, ctx context.Context) (*ResultExpr, error) {
	rows, err := db.DB.QueryContext(ctx, ce.sql, ce.args...)
	if err != nil {
		return nil, err
	}
	return &ResultExpr{outputs: ce.outputs, rows: rows}, nil
}

func (db *DB) Exec(ce *CompletedExpr) (sql.Result, error) {
	return db.ExecContext(ce, context.Background())
}

func (db *DB) ExecContext(ce *CompletedExpr, ctx context.Context) (sql.Result, error) {
	res, err := db.DB.ExecContext(ctx, ce.sql, ce.args...)
	if err != nil {
		return nil, err
	}
	return res, nil
}
