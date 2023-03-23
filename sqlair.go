package sqlair

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/canonical/sqlair/internal/expr"
)

// sqlair-provided M-type map.
// The M type can be used in querys to pass arbitrary values referenced by their key.
//
// For example:
//
//  stmt := sqlair.MustPrepare("SELECT (name, postcode) AS &M.* FROM p WHERE id = $M.id", sqlair.M{})
//  q := db.Query(stmt, sqlair.M{"id": 10})
//  var resultMap = sqlair.M{}
//  err := q.One{resultMap}
//  // resultMap == sqlair.M{"name": "Fred", "postcode": 10031}

type M map[string]any

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

// Query holds a database query and is used to iterate over the results.
type Query struct {
	qe   *expr.QueryExpr
	q    func() (*sql.Rows, error)
	rows *sql.Rows
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

// One runs a query and decodes the first row into outputArgs.
func (q *Query) One(outputArgs ...any) error {
	if !q.Next() {
		return fmt.Errorf("cannot return one row: no results")
	}
	q.Decode(outputArgs...)
	return q.Close()
}

// Next prepares the next row for decoding.
// The first call to Next will execute the query.
// If an error occurs it will be returned with Query.Close().
func (q *Query) Next() bool {
	if q.err != nil {
		return false
	}
	if q.rows == nil {
		rows, err := q.q()
		if err != nil {
			q.err = err
			return false
		}
		q.rows = rows
	}
	return q.rows.Next()
}

// Decode decodes the current result into the structs in outputValues.
// outputArgs must contain all the structs mentioned in the query.
// If an error occurs it will be returned with Query.Close().
func (q *Query) Decode(outputArgs ...any) (ok bool) {
	if q.err != nil {
		return false
	}
	defer func() {
		if !ok {
			q.err = fmt.Errorf("cannot decode result: %s", q.err)
		}
	}()

	if q.rows == nil {
		q.err = fmt.Errorf("iteration ended or not started")
		return false
	}
	cols, err := q.rows.Columns()
	if err != nil {
		q.err = err
		return false
	}
	ptrs, mapDecodeInfos, err := q.qe.ScanArgs(cols, outputArgs)
	if err != nil {
		q.err = err
		return false
	}
	if err := q.rows.Scan(ptrs...); err != nil {
		q.err = err
		return false
	}
	for _, m := range mapDecodeInfos {
		m.Populate()
	}
	return true
}

// Close closes the query and returns any errors encountered during iteration.
func (q *Query) Close() error {
	if q.rows == nil {
		return q.err
	}
	err := q.rows.Close()
	q.rows = nil
	if q.err != nil {
		return q.err
	}
	return err
}
