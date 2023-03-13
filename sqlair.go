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

	return &Query{outputs: ce.Outputs(), q: q}, nil
}

// Query holds a database query and is used to iterate over the results.
type Query struct {
	err     error
	rows    *sql.Rows
	outputs expr.Outputs
	q       func() (*sql.Rows, error)
}

// One runs a query and decodes the first row into outputStructs.
func (q *Query) One(outputStructs ...any) error {
	if !q.Next() {
		return fmt.Errorf("cannot return one row: no results")
	}
	q.Decode(outputStructs...)
	return q.Close()
}

// All iterates over the query and decodes all the rows.
// It fabricates all struct instantiations needed.
// It returns a list of rows, each row is a list of pointers to structs.
func (q *Query) All() ([][]any, error) {
	var rows [][]any

	for q.Next() {
		cols, err := q.rows.Columns()
		if err != nil {
			return [][]any{}, err
		}

		ptrs, row, err := expr.FabricatedOutputAddrs(cols, q.outputs)
		if err != nil {
			return [][]any{}, err
		}
		if err := q.rows.Scan(ptrs...); err != nil {
			return [][]any{}, err
		}

		/*
			row := make([]any, len(ptrs))
			for i, p := range ptrs {
				row[i] = *p
			}
			rows = append(rows, row)
		*/
		rows = append(rows, row)
	}

	if err := q.Close(); err != nil {
		return [][]any{}, err
	}
	return rows, nil
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

// Decode decodes the current result into the dests struct values.
// dests must contain all the structs mentioned in the query.
// If an error occurs it will be returned with Query.Close().
func (q *Query) Decode(dests ...any) (ok bool) {
	if q.err != nil {
		return false
	}
	defer func() {
		if !ok {
			q.err = fmt.Errorf("cannot decode result: %s", q.err)
		}
	}()

	if q.rows == nil {
		q.err = fmt.Errorf("Decode called without calling Next")
		return false
	}

	destVals := []reflect.Value{}
	for _, dest := range dests {
		if dest == nil {
			q.err = fmt.Errorf("need valid struct, got nil")
			return false
		}

		destVal := reflect.ValueOf(dest)
		if destVal.Kind() != reflect.Pointer {
			q.err = fmt.Errorf("need pointer to struct, got non-pointer of kind %s", destVal.Kind())
			return false
		}

		destVal = reflect.Indirect(destVal)
		if destVal.Kind() != reflect.Struct {
			q.err = fmt.Errorf("need pointer to struct, got pointer to %s", destVal.Kind())
			return false
		}

		destVals = append(destVals, destVal)
	}

	cols, err := q.rows.Columns()
	if err != nil {
		q.err = err
		return false
	}

	ptrs, err := expr.OutputAddrs(cols, q.outputs, destVals)
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

func (q *Query) Close() error {
	if q.err != nil {
		_ = q.rows.Close()
		return q.err
	}
	err := q.rows.Close()
	q.rows = nil
	return err
}
