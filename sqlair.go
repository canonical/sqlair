package sqlair

import (
	"context"
	"database/sql"

	"github.com/canonical/sqlair/internal/expr"
)

// Statement represents a SQL statemnt with valid SQLair expressions.
// It is ready to be run on a SQLair DB.
type Statement struct {
	pe *expr.PreparedExpr
}

// Query holds a database query and is used to iterate over the results.
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

// Prepare expands the types mentioned in the SQLair expressions and checks
// the SQLair parts of the query are well formed.
// typeInstantiations must contain an instance of every type mentioned in the
// SQLair expressions of the query. These are used only for type information.
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

// MustPrepare is the same as prepare except that it panics on error.
func MustPrepare(query string, typeInstantiations ...any) *Statement {
	s, err := Prepare(query, typeInstantiations...)
	if err != nil {
		panic(err)
	}
	return s
}

// Query takes a prepared SQLair Statement and returns a Query object for
// iterating over the results.
// Query uses QueryContext with context.Background internally.
func (db *DB) Query(s *Statement, inputStructs ...any) (*Query, error) {
	return db.QueryContext(s, context.Background(), inputStructs...)
}

// QueryContext takes a prepared SQLair Statement and returns a Query object for
// iterating over the results.
func (db *DB) QueryContext(s *Statement, ctx context.Context, inputStructs ...any) (*Query, error) {
	ce, err := s.pe.Complete(inputStructs...)
	if err != nil {
		return nil, err
	}

	q := func() (*sql.Rows, error) {
		return db.db.QueryContext(ctx, ce.CompletedSQL(), ce.CompletedArgs()...)
	}

	return &Query{pe: s.pe, q: q}, nil
}

// Exec executes an SQLair Statement without returning any rows.
// Exec uses ExecContext with context.Background internally.
func (db *DB) Exec(s *Statement, inputStructs ...any) (sql.Result, error) {
	return db.ExecContext(s, context.Background())
}

// ExecContext executes an SQLair Statement without returning any rows.
func (db *DB) ExecContext(s *Statement, ctx context.Context, inputStructs ...any) (sql.Result, error) {
	ce, err := s.pe.Complete(inputStructs...)
	if err != nil {
		return nil, err
	}

	res, err := db.db.ExecContext(ctx, ce.CompletedSQL(), ce.CompletedArgs()...)
	if err != nil {
		return nil, err
	}

	return res, nil
}

// Next prepares the next row for decoding.
// The first call to Next will execute the query.
// If an error occurs it can be checked with Query.Err(), Next will return false and Close will be called implicitly.
// If an error has previously occured Next will return false.
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

// Decode stores a result row from the query into the structs specified in its
// SQLair expressions.
// outputStructs must contains all the structs mentioned in the query.
// If an error occurs during decode it will return false and Close will be called implicitly.
// If an error has previously occured Decode will return false.
// In this case the error can be checked with Query.Err().
func (q *Query) Decode(outputStructs ...any) bool {
	if q.err != nil {
		return false
	}
	err := q.re.Decode(outputStructs...)
	if err != nil {
		q.err = err
		// We must close the rows if an error occurs.
		// The error, if any, from Rows.Close is ignored.
		_ = q.re.Close()
		return false
	}
	return true
}

// Close finishes iteration of the results.
func (q *Query) Close() error {
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

// One runs a query and decodes the first row into outputStructs.
// One is shorthand for q.Next(); q.Decode(outputStructs...); q.Close().
func (q *Query) One(outputStructs ...any) error {
	return q.re.One(outputStructs...)
}

// All iterates over the query and decodes all the rows.
// It fabricates all struct instantiations needed.
func (q *Query) All() ([][]any, error) {
	return q.re.All()
}
