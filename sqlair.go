package sqlair

import (
	"context"
	"database/sql"

	"github.com/canonical/sqlair/internal/expr"
)

type Statement struct {
	pe *expr.PreparedExpr
}

type DB struct {
	db *sql.DB
}

type Query struct {
	re  *expr.ResultExpr
	err error
}

func NewDB(db *sql.DB) *DB {
	return &DB{db: db}
}

func (db *DB) sqlDB() *sql.DB {
	return db.db
}

func Prepare(query string, typeInstantiations ...any) (*Statement, error) {
	parser := expr.NewParser()
	parsedExpr, err := parser.Parse(query)
	if err != nil {
		return nil, err
	}
	preparedExpr, err := parsedExpr.Prepare(typeInstantiations...)
	if err != nil {
		return nil, err
	}
	return &Statement{pe: preparedExpr}, nil
}

func MustPrepare(query string, typeInstantiations ...any) *Statement {
	s, err := Prepare(query, typeInstantiations...)
	if err != nil {
		panic(err)
	}
	return s
}

func (db *DB) QueryContext(s *Statement, ctx context.Context, inputStructs ...any) (*Query, error) {
	ce, err := s.pe.Complete(inputStructs...)
	if err != nil {
		return nil, err
	}

	rows, err := db.db.QueryContext(ctx, expr.GetCompletedSQL(ce), expr.GetCompletedArgs(ce)...)
	if err != nil {
		return nil, err
	}

	re := expr.NewResultExpr(ce, rows)

	return &Query{re: re}, nil
}

func (db *DB) Query(s *Statement, inputStructs ...any) (*Query, error) {
	return db.QueryContext(s, context.Background(), inputStructs...)
}

func (db *DB) ExecContext(s *Statement, ctx context.Context, inputStructs ...any) (sql.Result, error) {
	ce, err := s.pe.Complete(inputStructs...)
	if err != nil {
		return nil, err
	}

	res, err := db.db.ExecContext(ctx, expr.GetCompletedSQL(ce), expr.GetCompletedArgs(ce)...)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (db *DB) Exec(s *Statement, inputStructs ...any) (sql.Result, error) {
	return db.ExecContext(s, context.Background())
}

func (q *Query) Next() bool {
	if q.err != nil {
		return false
	}
	ok, err := q.re.Next()
	if err != nil {
		q.err = err
		return false
	}
	return ok
}

func (q *Query) Decode(outputStructs ...any) bool {
	if q.err != nil {
		return false
	}
	err := q.re.Decode(outputStructs...)
	if err != nil {
		q.err = err
		return false
	}
	return true
}

func (q *Query) Close(outputStructs ...any) error {
	if q.err != nil {
		q.re.Close() // Which error should we return here? We have two and are currently ignoring the error of q.Close()
		return q.err
	}
	err := q.re.Close()
	if err != nil {
		return err
	}
	return nil
}
