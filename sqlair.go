package sqlair

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/canonical/sqlair/internal/expr"
)

var ErrNoRows = sql.ErrNoRows

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

// PlainDB returns the underlying database object.
func (db *DB) PlainDB() *sql.DB {
	return db.db
}

// querySubstrate abstracts the different surfaces that the query can be run on.
// For example, the database or a transaction.
type querySubstrate interface {
	QueryContext(ctx context.Context, sql string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, sql string, args ...any) (sql.Result, error)
}

// Query holds the results of a database query.
type Query struct {
	qe      *expr.QueryExpr
	qs      querySubstrate
	ctx     context.Context
	outcome *Outcome
	err     error
}

// Iterator is used to iterate over the results of the query.
type Iterator struct {
	qe   *expr.QueryExpr
	rows *sql.Rows
	cols []string
	err  error
}

// Query takes a context, prepared SQLair Statement and the structs mentioned in the query arguments.
// It returns a Query object for iterating over the results.
func (db *DB) Query(ctx context.Context, s *Statement, inputArgs ...any) *Query {
	if ctx == nil {
		ctx = context.Background()
	}

	var outcome *Outcome
	if len(inputArgs) > 0 {
		if oc, ok := inputArgs[0].(*Outcome); ok {
			outcome = oc
			inputArgs = inputArgs[1:]
		}
	}
	qe, err := s.pe.Query(inputArgs...)
	return &Query{qs: db.db, qe: qe, ctx: ctx, outcome: outcome, err: err}
}

// Run will execute the query.
// Any rows returned by the query are ignored.
func (q *Query) Run() error {
	if q.err != nil {
		return q.err
	}
	res, err := q.qs.ExecContext(q.ctx, q.qe.QuerySQL(), q.qe.QueryArgs()...)
	if err != nil {
		return err
	}
	if q.outcome != nil {
		q.outcome.result = res
	}

	return nil
}

// Iter returns an Iterator to iterate through the results row by row.
func (q *Query) Iter() *Iterator {
	if q.err != nil {
		return &Iterator{err: q.err}
	}

	rows, err := q.qs.QueryContext(q.ctx, q.qe.QuerySQL(), q.qe.QueryArgs()...)
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
	if iter.err != nil || iter.rows == nil {
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

// A pointer to an outcome can be passed as the first variadic argument to Query.
// It will be populated with the outcome of the query, if available.
type Outcome struct {
	result sql.Result
}

func (o *Outcome) Result() sql.Result {
	return o.result
}

// One runs a query and decodes the first row into outputArgs.
func (q *Query) One(outputArgs ...any) error {
	err := ErrNoRows
	iter := q.Iter()
	if iter.Next() {
		iter.Decode(outputArgs...)
		err = nil
	}
	if cerr := iter.Close(); cerr != nil {
		return cerr
	}
	return err
}

// All iterates over the query and decodes all rows into the provided slices.
//
// For example:
//
//	var pslice []Person
//	var aslice []*Address
//	err := query.All(&pslice, &aslice)
//
// sliceArgs must contain pointers to slices of each of the output types.
func (q *Query) All(sliceArgs ...any) (err error) {
	if q.err != nil {
		return q.err
	}
	defer func() {
		if err != nil {
			err = fmt.Errorf("cannot populate slice: %s", err)
		}
	}()

	// Check slice inputs
	var slicePtrVals = []reflect.Value{}
	var sliceVals = []reflect.Value{}
	for _, ptr := range sliceArgs {
		ptrVal := reflect.ValueOf(ptr)
		if ptrVal.Kind() != reflect.Pointer {
			return fmt.Errorf("need pointer to slice, got %s", ptrVal.Kind())
		}
		if ptrVal.IsNil() {
			return fmt.Errorf("need pointer to slice, got nil")
		}
		slicePtrVals = append(slicePtrVals, ptrVal)
		sliceVal := ptrVal.Elem()
		if sliceVal.Kind() != reflect.Slice {
			return fmt.Errorf("need pointer to slice, got pointer to %s", sliceVal.Kind())
		}
		sliceVals = append(sliceVals, sliceVal)
	}

	iter := q.Iter()
	for iter.Next() {
		var outputArgs = []any{}
		for _, sliceVal := range sliceVals {
			elemType := sliceVal.Type().Elem()
			var outputArg reflect.Value
			switch elemType.Kind() {
			case reflect.Pointer:
				outputArg = reflect.New(elemType.Elem())
			case reflect.Struct:
				outputArg = reflect.New(elemType)
			default:
				iter.Close()
				return fmt.Errorf("need slice of struct, got slice of %s", elemType.Kind())
			}
			outputArgs = append(outputArgs, outputArg.Interface())
		}
		if !iter.Decode(outputArgs...) {
			break
		}
		for i, outputArg := range outputArgs {
			switch k := sliceVals[i].Type().Elem().Kind(); k {
			case reflect.Pointer:
				sliceVals[i] = reflect.Append(sliceVals[i], reflect.ValueOf(outputArg))
			case reflect.Struct:
				sliceVals[i] = reflect.Append(sliceVals[i], reflect.ValueOf(outputArg).Elem())
			default:
				iter.Close()
				return fmt.Errorf("internal error: output arg has unexpected kind %s", k)
			}
		}
	}
	err = iter.Close()
	if err != nil {
		return err
	}

	for i, ptrVal := range slicePtrVals {
		ptrVal.Elem().Set(sliceVals[i])
	}

	return nil
}
