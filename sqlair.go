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
	qe  *expr.QueryExpr
	qs  querySubstrate
	ctx context.Context
	err error
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

	qe, err := s.pe.Query(inputArgs...)
	return &Query{qs: db.db, qe: qe, ctx: ctx, err: err}
}

// Run is an alias for Get that takes no arguments.
func (q *Query) Run() error {
	return q.Get()
}

// Query.Get will run the query.
// The first row (if there is one) will be scanned into the outputArgs.
// It will give ErrNoRows if the query contains output expressions and there are no rows.
// An Outcome struct can be passed as the first argument which will be populated with the outcome of the query.
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
	if q.qe.HasOutputs() {
		if outcome != nil {
			outcome.result = nil
		}
		iter := q.Iter()
		defer iter.Close()
		if !iter.Next() {
			if err := iter.Close(); err != nil {
				return err
			}
			return ErrNoRows
		}
		return iter.Get(outputArgs...)
	} else {
		if len(outputArgs) > 0 {
			return fmt.Errorf("cannot get results: query does not return any results")
		}
		res, err := q.qs.ExecContext(q.ctx, q.qe.QuerySQL(), q.qe.QueryArgs()...)
		if outcome != nil {
			outcome.result = res
		}
		return err
	}
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

// Next prepares the next row for Get.
// If an error occurs during iteration it will be returned with Iter.Close().
func (iter *Iterator) Next() bool {
	if iter.err != nil || iter.rows == nil {
		return false
	}
	return iter.rows.Next()
}

// Iterator.Get scans the current result into the structs in outputValues.
// outputArgs must contain all the structs mentioned in the query.
func (iter *Iterator) Get(outputArgs ...any) (err error) {
	if iter.err != nil {
		return iter.err
	}
	defer func() {
		if err != nil {
			err = fmt.Errorf("cannot get result: %s", err)
		}
	}()
	if iter.rows == nil {
		return fmt.Errorf("iteration ended or not started")
	}

	ptrs, err := iter.qe.ScanArgs(iter.cols, outputArgs)
	if err != nil {
		return err
	}
	if err := iter.rows.Scan(ptrs...); err != nil {
		return err
	}
	return nil
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

// A pointer to an outcome can be passed as the first variadic argument to Run.
// It will be populated with the outcome of the query execution.
type Outcome struct {
	result sql.Result
}

func (o *Outcome) Result() sql.Result {
	return o.result
}

// GetAll iterates over the query and scans all rows into the provided slices.
//
// For example:
//
//	var pslice []Person
//	var aslice []*Address
//	err := query.GetAll(&pslice, &aslice)
//
// sliceArgs must contain pointers to slices of each of the output types.
func (q *Query) GetAll(sliceArgs ...any) (err error) {
	if q.err != nil {
		return q.err
	}
	defer func() {
		if err != nil {
			err = fmt.Errorf("cannot populate slice: %s", err)
		}
	}()

	var outcome *Outcome
	if len(sliceArgs) > 0 {
		if oc, ok := sliceArgs[0].(*Outcome); ok {
			outcome = oc
			sliceArgs = sliceArgs[1:]
		}
	}
	if outcome != nil {
		outcome.result = nil
	}
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
		if err := iter.Get(outputArgs...); err != nil {
			iter.Close()
			return err
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

type TX struct {
	tx *sql.Tx
}

// NewTX creates a SQLair transaction from a sql transaction.
func (db *DB) NewTX(tx *sql.Tx) *TX {
	return &TX{tx: tx}
}

// Begin starts a transaction.
func (db *DB) Begin(ctx context.Context, opts *TXOptions) (*TX, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	tx, err := db.db.BeginTx(ctx, opts.plainTXOptions())
	return db.NewTX(tx), err
}

// Commit commits the transaction.
func (tx *TX) Commit() error {
	return tx.tx.Commit()
}

// Rollback aborts the transaction.
func (tx *TX) Rollback() error {
	return tx.tx.Rollback()
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

// Query takes a context, prepared SQLair Statement and the structs mentioned in the query arguments.
// It returns a Query object for iterating over the results.
func (tx *TX) Query(ctx context.Context, s *Statement, inputArgs ...any) *Query {
	if ctx == nil {
		ctx = context.Background()
	}

	qe, err := s.pe.Query(inputArgs...)
	return &Query{qs: tx.tx, qe: qe, ctx: ctx, err: err}
}
