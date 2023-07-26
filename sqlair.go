// Copyright 2023 Canonical Ltd.
// Licensed under Apache 2.0, see LICENCE file for details.

package sqlair

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sync/atomic"

	"github.com/canonical/sqlair/internal/expr"
)

// M is a convenience type that can be used in input and output expressions to
// pass values referenced by their key. M is not a special type, any named map
// type with string keys can be used with SQLair.
//
// Example 1:
//     stmt := sqlair.MustPrepare("UPDATE people SET name=$M.name", sqlair.M{})
//     err := db.Query(ctx, stmt, sqlair.M{"id": 10}).Run()
//
// Example 2:
//     stmt := sqlair.MustPrepare("SELECT (name, postcode) AS &M.* FROM people id", sqlair.M{})
//     m := sqlair.M{}
//     err := db.Query(ctx, stmt).Get(m) // => sqlair.M{"name": "Fred", "postcode": 10031}

type M map[string]any

// S is a slice type that, as with other named slice types, can be used with
// SQLair to pass a slice of input values.
type S []any

var ErrNoRows = sql.ErrNoRows
var ErrTXDone = sql.ErrTxDone

// Statement represents a parsed SQLair statement ready to be run on a database.
// A statement can be used with any database.
type Statement struct {
	// te is the type bound SQLair query. It contains information used to
	// generate query values from the input arguments when the Statement is run
	// on a database.
	te *expr.TypeBoundExpr
}

// Prepare validates SQLair expressions in the query and generates a SQLair
// statement Statement.
// typeSamples must contain an instance of every type mentioned in the
// SQLair expressions in the query. These are used only for type information.
func Prepare(query string, typeSamples ...any) (*Statement, error) {
	parser := expr.NewParser()
	parsedExpr, err := parser.Parse(query)
	if err != nil {
		return nil, err
	}
	typedExpr, err := parsedExpr.BindTypes(typeSamples...)
	if err != nil {
		return nil, err
	}

	return &Statement{te: typedExpr}, nil
}

// MustPrepare is the same as Prepare except that it panics on error.
func MustPrepare(query string, typeSamples ...any) *Statement {
	s, err := Prepare(query, typeSamples...)
	if err != nil {
		panic(err)
	}
	return s
}

type DB struct {
	sqldb *sql.DB
}

// NewDB creates a new SQLair DB from a sql.DB.
func NewDB(sqldb *sql.DB) *DB {
	return &DB{sqldb: sqldb}
}

// PlainDB returns the underlying database object.
func (db *DB) PlainDB() *sql.DB {
	return db.sqldb
}

// Query represents a query on a database. It is designed to be run once.
type Query struct {
	// run executes the Query against the db or the tx.
	run func(context.Context) (*sql.Rows, sql.Result, error)
	ctx context.Context
	err error
	pq  *expr.PrimedQuery
}

// Iterator is used to iterate over the results of the query.
type Iterator struct {
	pq      *expr.PrimedQuery
	rows    *sql.Rows
	cols    []string
	err     error
	result  sql.Result
	started bool
}

// Query builds a new query from a context, a SQLair Statement and the input
// arguments. The query is run on the database when one of Iter, Run, Get or
// GetAll is executed on the Query.
func (db *DB) Query(ctx context.Context, s *Statement, inputArgs ...any) *Query {
	if ctx == nil {
		ctx = context.Background()
	}

	pq, err := s.te.BindInputs(inputArgs...)
	if err != nil {
		return &Query{ctx: ctx, err: err}
	}

	run := func(innerCtx context.Context) (rows *sql.Rows, result sql.Result, err error) {
		if pq.HasOutputs() {
			rows, err = db.sqldb.QueryContext(innerCtx, pq.SQL(), pq.Params()...)
		} else {
			result, err = db.sqldb.ExecContext(innerCtx, pq.SQL(), pq.Params()...)
		}
		return rows, result, err
	}

	return &Query{pq: pq, run: run, ctx: ctx, err: nil}
}

// Run is an alias for Get that takes no arguments. Run is used to run a query
// on a database and disregard any results.
func (q *Query) Run() error {
	return q.Get()
}

// Get runs the query and decodes the first result into the provided output
// arguments. It returns ErrNoRows if output arguments were provided but no
// results were found.
//
// An Outcome struct may be provided as the first output variable to fill it
// with information about query execution.
func (q *Query) Get(outputArgs ...any) error {
	if q.err != nil {
		return q.err
	}
	var outcome *Outcome
	if len(outputArgs) > 0 {
		if oc, ok := outputArgs[0].(*Outcome); ok {
			outcome = oc
			outputArgs = outputArgs[1:]
		}
	}
	if !q.pq.HasOutputs() && len(outputArgs) > 0 {
		return fmt.Errorf("cannot get results: output variables provided but not referenced in query")
	}

	var err error
	iter := q.Iter()
	if outcome != nil {
		err = iter.Get(outcome)
	}
	if err == nil && !iter.Next() {
		err = iter.Close()
		if err == nil && q.pq.HasOutputs() {
			err = ErrNoRows
		}
		return err
	}
	if err == nil {
		err = iter.Get(outputArgs...)
	}
	if cerr := iter.Close(); err == nil {
		err = cerr
	}
	return err
}

// Iter returns an Iterator to iterate through the results row by row.
// Close must be run once iteration is finished.
func (q *Query) Iter() *Iterator {
	if q.err != nil {
		return &Iterator{err: q.err}
	}

	var cols []string
	rows, result, err := q.run(q.ctx)
	if q.pq.HasOutputs() {
		if err == nil { // if err IS nil
			cols, err = rows.Columns()
		}
	}
	if err != nil {
		return &Iterator{pq: q.pq, err: err}
	}

	return &Iterator{pq: q.pq, rows: rows, cols: cols, err: err, result: result}
}

// Next prepares the next row for Get.
// If an error occurs during iteration it will be returned with Iter.Close.
func (iter *Iterator) Next() bool {
	iter.started = true
	if iter.err != nil || iter.rows == nil {
		return false
	}
	return iter.rows.Next()
}

// Get decodes the result from the previous Next call into the provided output
// arguments.
//
// Before the first call of Next an Outcome struct may be passed to Get as the
// only argument to fill it information about query execution.
func (iter *Iterator) Get(outputArgs ...any) (err error) {
	if iter.err != nil {
		return iter.err
	}
	defer func() {
		if err != nil {
			err = fmt.Errorf("cannot get result: %s", err)
		}
	}()

	if !iter.started {
		if len(outputArgs) == 1 {
			if oc, ok := outputArgs[0].(*Outcome); ok {
				oc.result = iter.result
				return nil
			}
		}
		return fmt.Errorf("cannot call Get before Next unless getting outcome")
	}

	if iter.rows == nil {
		return fmt.Errorf("iteration ended")
	}

	ptrs, onSuccess, err := iter.pq.ScanArgs(iter.cols, outputArgs)
	if err != nil {
		return err
	}
	if err := iter.rows.Scan(ptrs...); err != nil {
		return err
	}
	onSuccess()
	return nil
}

// Close finishes the iteration and returns any errors encountered. Close can
// be called multiple times on the Iterator and the same error will be
// returned.
func (iter *Iterator) Close() error {
	iter.started = true
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

// Outcome holds metadata about executed queries, and can be provided as the
// first output argument to any of the Get methods to populate it with
// information about the query execution.
type Outcome struct {
	result sql.Result
}

// Result returns a sql.Result containing information about the query
// execution. If no result is set then Result returns nil.
func (o *Outcome) Result() sql.Result {
	return o.result
}

// GetAll iterates over the query and scans all rows into the provided slices.
// sliceArgs must contain pointers to slices of each of the output types.
// An &Outcome{} struct may be provided as the first output variable to get
// information about query execution.
//
// ErrNoRows will be returned if no rows are found.
func (q *Query) GetAll(sliceArgs ...any) (err error) {
	if q.err != nil {
		return q.err
	}
	defer func() {
		if err != nil {
			err = fmt.Errorf("cannot populate slice: %s", err)
		}
	}()

	if len(sliceArgs) > 0 {
		if outcome, ok := sliceArgs[0].(*Outcome); ok {
			outcome.result = nil
			sliceArgs = sliceArgs[1:]
		}
	}
	if !q.pq.HasOutputs() && len(sliceArgs) > 0 {
		return fmt.Errorf("output variables provided but not referenced in query")
	}
	// Check slice inputs are valid using reflection.
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

	// Iterate over the query results.
	rowsReturned := false
	iter := q.Iter()
	for iter.Next() {
		rowsReturned = true
		var outputArgs = []any{}
		for _, sliceVal := range sliceVals {
			elemType := sliceVal.Type().Elem()
			var outputArg reflect.Value
			switch elemType.Kind() {
			case reflect.Pointer:
				if elemType.Elem().Kind() != reflect.Struct {
					iter.Close()
					return fmt.Errorf("need slice of structs/maps, got slice of pointer to %s", elemType.Elem().Kind())
				}
				outputArg = reflect.New(elemType.Elem())
			case reflect.Struct:
				outputArg = reflect.New(elemType)
			case reflect.Map:
				outputArg = reflect.MakeMap(elemType)
			default:
				iter.Close()
				return fmt.Errorf("need slice of structs/maps, got slice of %s", elemType.Kind())
			}
			outputArgs = append(outputArgs, outputArg.Interface())
		}
		if err := iter.Get(outputArgs...); err != nil {
			iter.Close()
			return err
		}
		for i, outputArg := range outputArgs {
			switch k := sliceVals[i].Type().Elem().Kind(); k {
			case reflect.Pointer, reflect.Map:
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
	} else if !rowsReturned && q.pq.HasOutputs() {
		return ErrNoRows
	}

	for i, ptrVal := range slicePtrVals {
		ptrVal.Elem().Set(sliceVals[i])
	}

	return nil
}

// TX represents a transaction on the database.
type TX struct {
	sqltx *sql.Tx
	done  int32
}

func (tx *TX) isDone() bool {
	return atomic.LoadInt32(&tx.done) == 1
}

func (tx *TX) setDone() error {
	if !atomic.CompareAndSwapInt32(&tx.done, 0, 1) {
		return ErrTXDone
	}
	return nil
}

// Begin starts a transaction. A transaction must be ended
// with a Commit or Rollback.
func (db *DB) Begin(ctx context.Context, opts *TXOptions) (*TX, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	sqltx, err := db.sqldb.BeginTx(ctx, opts.plainTXOptions())
	if err != nil {
		return nil, err
	}
	return &TX{sqltx: sqltx}, nil
}

// Commit commits the transaction.
func (tx *TX) Commit() error {
	err := tx.setDone()
	if err == nil {
		err = tx.sqltx.Commit()
	}
	return err
}

// Rollback aborts the transaction.
func (tx *TX) Rollback() error {
	err := tx.setDone()
	if err == nil {
		err = tx.sqltx.Rollback()
	}
	return err
}

// TXOptions holds the transaction options to be used in DB.Begin.
type TXOptions struct {
	// Isolation is the transaction isolation level.
	// If zero, the driver or database's default level is used.
	Isolation sql.IsolationLevel
	ReadOnly  bool
}

func (txopts *TXOptions) plainTXOptions() *sql.TxOptions {
	if txopts == nil {
		return nil
	}
	return &sql.TxOptions{Isolation: txopts.Isolation, ReadOnly: txopts.ReadOnly}
}

// Query builds a new query from a context, a SQLair Statement and the input
// arguments. The query is run on the transaction when one of Iter, Run, Get or
// GetAll is executed on the Query.
func (tx *TX) Query(ctx context.Context, s *Statement, inputArgs ...any) *Query {
	if ctx == nil {
		ctx = context.Background()
	}
	if tx.isDone() {
		return &Query{ctx: ctx, err: ErrTXDone}
	}

	pq, err := s.te.BindInputs(inputArgs...)
	if err != nil {
		return &Query{ctx: ctx, err: err}
	}

	run := func(innerCtx context.Context) (rows *sql.Rows, result sql.Result, err error) {
		if pq.HasOutputs() {
			rows, err = tx.sqltx.QueryContext(innerCtx, pq.SQL(), pq.Params()...)
		} else {
			result, err = tx.sqltx.ExecContext(innerCtx, pq.SQL(), pq.Params()...)
		}
		return rows, result, err
	}

	return &Query{pq: pq, ctx: ctx, run: run, err: nil}
}
