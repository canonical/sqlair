package sqlair

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

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

// sqlDB returns the underlying database object.
func (db *DB) SQLdb() *sql.DB {
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

	outputVals := []reflect.Value{}
	for _, outputArg := range outputArgs {
		if outputArg == nil {
			q.err = fmt.Errorf("need pointer to struct, got nil")
			return false
		}
		outputVal := reflect.ValueOf(outputArg)
		if outputVal.Kind() != reflect.Pointer {
			q.err = fmt.Errorf("need pointer to struct, got %s", outputVal.Kind())
			return false
		}
		if outputVal.IsNil() {
			q.err = fmt.Errorf("got nil pointer")
			return false
		}
		outputVal = reflect.Indirect(outputVal)
		if outputVal.Kind() != reflect.Struct {
			q.err = fmt.Errorf("need pointer to struct, got pointer to %s", outputVal.Kind())
			return false
		}
		outputVals = append(outputVals, outputVal)
	}

	cols, err := q.rows.Columns()
	if err != nil {
		q.err = err
		return false
	}
	ptrs, err := q.qe.ScanArgs(cols, outputVals)
	if err != nil {
		q.err = err
		return false
	}
	if err := q.rows.Scan(ptrs...); err != nil {
		q.err = err
		return false
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
