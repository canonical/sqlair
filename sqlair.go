package sqlair

import (
	"context"
	"database/sql"

	"github.com/canonical/sqlair/internal/expr"
)

// Statement is a sql statement with valid sqlair DML.
type Statement struct {
	pe *expr.PreparedExpr
}

// Query stores a database query and iterates over the results.
type Query struct {
	pe  *expr.PreparedExpr
	re  *expr.ResultExpr
	err error
	q   func() (*sql.Rows, error)
}

// Err returns the error, if any, that was encountered during iteration.
// It should only be called after Close, or after Next has returned false (implicit close).
func (q *Query) Err() error {
	return q.err
}

type DB struct {
	db *sql.DB
}

func NewDB(db *sql.DB) *DB {
	return &DB{db: db}
}

// sqlDB returns the underlying database object.
func (db *DB) sqlDB() *sql.DB {
	return db.db
}

// Prepare checks that SQLair DML statements in a SQL query are well formed.
// typeInstantiations must contain an instance of every type mentioned in the
// SQLair DML of the query. These are used only for type information.
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

// Query takes a prepared SQLair Statement and returns a Query object for
// iterating over the results.
func (db *DB) Query(s *Statement, inputStructs ...any) (*Query, error) {
	return db.QueryContext(s, context.Background(), inputStructs...)
}

func (db *DB) QueryContext(s *Statement, ctx context.Context, inputStructs ...any) (*Query, error) {
	ce, err := s.pe.Complete(inputStructs...)
	if err != nil {
		return nil, err
	}

	q := func() (*sql.Rows, error) {
		return db.db.QueryContext(ctx, expr.GetCompletedSQL(ce), expr.GetCompletedArgs(ce)...)
	}

	return &Query{pe: s.pe, q: q}, nil
}

// Exec executes a query without returning any rows.
func (db *DB) Exec(s *Statement, inputStructs ...any) (sql.Result, error) {
	return db.ExecContext(s, context.Background())
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

// Next prepares the next row for decoding.
// The first call to Next will execute the query.
// If an error occours it can be checked with Query.Err(), Next will return false and Close will be called implicitly.
// If an error has previously occoured Next will return false.
func (q *Query) Next() bool {
	if q.err != nil {
		return false
	}

	if q.re == nil {
		rows, err := q.q()
		if err != nil {
			q.err = err
			return false
		}
		q.re = expr.NewResultExpr(q.pe, rows)
	}

	ok, err := q.re.Next()
	if err != nil {
		q.err = err
		return false
	}
	return ok
}

// Decode stores a result row from the query into the structs specified in its SQLair DML.
// outputStructs must contains all the structs mentioned in the query.
// If an error occours during decode it will return false and Close will be called implicitly.
// The error can be checked with Query.Err().
// If an error has previously occoured Decode will return false.
func (q *Query) Decode(outputStructs ...any) bool {
	if q.err != nil {
		return false
	}
	err := q.re.Decode(outputStructs...)
	if err != nil {
		q.err = err
		// We must close the rows if an error occours.
		// The error, if any, from Rows.Close is ignored.
		_ = q.re.Close()
		return false
	}
	return true
}

// Close finishes iteration of the results.
func (q *Query) Close(outputStructs ...any) error {
	if q.err != nil {
		return q.err
	}
	err := q.re.Close()
	if err != nil {
		return err
	}
	q.re = nil
	return nil
}

// One is shorthand for q.Next(); q.Decode(outputStructs...); q.Close().
func (q *Query) One(outputStructs ...any) error {
	return q.re.One(outputStructs...)
}

// All iterates over the query and decodes all the rows.
// It fabricates all struct instansiations needed.
func (q *Query) All() ([][]any, error) {
	return q.re.All()
}
