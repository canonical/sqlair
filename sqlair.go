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
	err     error
	rows    *sql.Rows
	outputs expr.Outputs
	q       func() (*sql.Rows, error)
}

// Query takes a prepared SQLair Statement and returns a Query object for iterating over the results.
// If an error occurs it will be returned with Query.Close().
// Query uses QueryContext with context.Background internally.
func (db *DB) Query(s *Statement, inputStructs ...any) *Query {
	return db.QueryContext(s, context.Background(), inputStructs...)
}

// QueryContext takes a prepared SQLair Statement and returns a Query object for iterating over the results.
// If an error occurs it will be returned with Query.Close().
func (db *DB) QueryContext(s *Statement, ctx context.Context, inputStructs ...any) *Query {
	q := &Query{outputs: s.pe.Outputs()}

	ce, err := s.pe.Complete(inputStructs...)
	if err != nil {
		q.err = err
	}
	q.q = func() (*sql.Rows, error) {
		return db.db.QueryContext(ctx, ce.CompletedSQL(), ce.CompletedArgs()...)
	}
	return q
}

// One runs a query and decodes the first row into outputs.
func (q *Query) One(outputs ...any) error {
	if !q.Next() {
		return fmt.Errorf("cannot return one row: no results")
	}
	q.Decode(outputs...)
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
// outputs must contain all the structs mentioned in the query.
// If an error occurs it will be returned with Query.Close().
func (q *Query) Decode(outputs ...any) (ok bool) {
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
	for _, output := range outputs {
		if output == nil {
			q.err = fmt.Errorf("need valid struct, got nil")
			return false
		}
		outputVal := reflect.ValueOf(output)
		if outputVal.Kind() != reflect.Pointer {
			q.err = fmt.Errorf("need pointer to struct, got %s", outputVal.Kind())
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
	ptrs, err := expr.OutputAddrs(cols, q.outputs, outputVals)
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
	err := q.rows.Close()
	q.rows = nil
	if q.err != nil {
		return q.err
	}
	return err
}
