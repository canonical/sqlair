package sqlair

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/canonical/sqlair/internal/expr"
)

// Statement represents a SQL statemnt with valid SQLair expressions.
// It is ready to be run on a SQLair DB.
type Statement struct {
	pe *expr.PreparedExpr
}

// Prepare expands the types mentioned in the SQLair expressions and checks
// the SQLair parts of the query are well formed.
// typeSamples must contain an instance of every type mentioned in the
// SQLair expressions of the query. These are used only for type information.
func Prepare(query string, typeSamples ...any) (*Statement, error) {
	parser := expr.NewParser()
	parsedExpr, err := parser.Parse(query)
	if err != nil {
		return nil, err
	}
	preparedExpr, err := parsedExpr.Prepare(typeSamples...)
	if err != nil {
		return nil, err
	}
	return &Statement{pe: preparedExpr}, nil
}

// MustPrepare is the same as prepare except that it panics on error.
func MustPrepare(query string, typeSamples ...any) *Statement {
	s, err := Prepare(query, typeSamples...)
	if err != nil {
		panic(err)
	}
	return s
}

type DB struct {
	db *sql.DB
}

func NewDB(db *sql.DB) *DB {
	return &DB{db: db}
}

// Unwrap returns the underlying database object.
func (db *DB) Unwrap() *sql.DB {
	return db.db
}

// Query holds the results of a database query.
type Query struct {
	qe   *expr.QueryExpr
	q    func() (*sql.Rows, error)
	rows *sql.Rows
	err  error
}

// Iterator is used to iterate over the results of the query.
type Iterator struct {
	qe   *expr.QueryExpr
	rows *sql.Rows
	cols []string
	err  error
}

// Query takes a prepared SQLair Statement and returns a Query object for iterating over the results.
// If an error occurs it will be returned with Query.Close().
// Query uses QueryContext with context.Background internally.
func (db *DB) Query(s *Statement, inputArgs ...any) *Query {
	return db.QueryContext(context.Background(), s, inputArgs...)
}

// QueryContext takes a prepared SQLair Statement and returns a Query object for iterating over the results.
// If an error occurs it will be returned with Query.Close().
func (db *DB) QueryContext(ctx context.Context, s *Statement, inputArgs ...any) *Query {
	qe, err := s.pe.Query(inputArgs...)
	q := func() (*sql.Rows, error) {
		return db.db.QueryContext(ctx, qe.QuerySQL(), qe.QueryArgs()...)
	}
	return &Query{qe: qe, q: q, err: err}
}

// Iter returns an Iterator to iterate through the results row by row.
func (q *Query) Iter() *Iterator {
	rows, err := q.q()
	if err != nil {
		return &Iterator{err: err}
	}
	cols, err := rows.Columns()
	if err != nil {
		return &Iterator{err: err}
	}
	return &Iterator{qe: q.qe, rows: rows, cols: cols, err: err}
}

// Next prepares the next row for decoding.
// The first call to Next will execute the query.
// If an error occurs it will be returned with Iter.Close().
func (iter *Iterator) Next() bool {
	if iter.err != nil || iter.rows == nil {
		return false
	}
	return iter.rows.Next()
}

// Decode decodes the current result into the structs in outputValues.
// outputArgs must contain all the structs mentioned in the query.
// If an error occurs it will be returned with Iter.Close().
func (iter *Iterator) Decode(outputArgs ...any) (ok bool) {
	if iter.err != nil {
		return false
	}
	defer func() {
		if !ok {
			iter.err = fmt.Errorf("cannot decode result: %s", iter.err)
		}
	}()

	if iter.rows == nil {
		iter.err = fmt.Errorf("iteration ended or not started")
		return false
	}

	ptrs, err := iter.qe.ScanArgs(iter.cols, outputArgs)
	if err != nil {
		iter.err = err
		return false
	}
	if err := iter.rows.Scan(ptrs...); err != nil {
		iter.err = err
		return false
	}
	return true
}

// Close finishes the iteration and returns any errors encountered.
func (iter *Iterator) Close() error {
	if iter.rows == nil {
		return iter.err
	}
	err := iter.rows.Close()
	iter.rows = nil
	if iter.err != nil {
		return iter.err
	}
	return err
}

// One runs a query and decodes the first row into outputArgs.
func (q *Query) One(outputArgs ...any) error {
	iter := q.Iter()
	if !iter.Next() {
		return fmt.Errorf("cannot return one row: no results")
	}
	iter.Decode(outputArgs...)
	return iter.Close()
}
