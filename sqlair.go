package sqlair

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sync/atomic"

	"github.com/canonical/sqlair/internal/expr"
)

// M is a type that, as with other map types, can be used with SQLair for more dynamic behavior.
// It can be used in querys to pass arbitrary values referenced by their key.
//
// For example:
//
//	stmt := sqlair.MustPrepare("SELECT (name, postcode) AS &M.* FROM p WHERE id = $M.id", sqlair.M{})
//	q := db.Query(ctx, stmt, sqlair.M{"id": 10})
//	var resultMap = sqlair.M{}
//	err := q.Get(resultMap) // => sqlair.M{"name": "Fred", "postcode": 10031}
type M map[string]any

var ErrNoRows = sql.ErrNoRows
var ErrTXDone = sql.ErrTxDone

var stmtCache = newStatementCache()

// Statement represents a SQL statement with valid SQLair expressions.
// It is ready to be run on a SQLair DB.
type Statement struct {
	cacheID uint64
	// te is the type bound SQLair query. It contains information used to
	// generate query values from the input arguments when the Statement is run
	// on a database.
	te *expr.TypedExpr
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
	typedExpr, err := parsedExpr.BindTypes(typeSamples...)
	if err != nil {
		return nil, err
	}

	s := stmtCache.newStatement(typedExpr)
	return s, nil
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
	cacheID uint64
	sqldb   *sql.DB
}

// NewDB creates a new SQLair DB from a sql.DB.
func NewDB(sqldb *sql.DB) *DB {
	return stmtCache.newDB(sqldb)
}

// PlainDB returns the underlying database object.
func (db *DB) PlainDB() *sql.DB {
	return db.sqldb
}

// Query holds the results of a database query.
type Query struct {
	pq      *expr.PrimedQuery
	sqlstmt *sql.Stmt
	ctx     context.Context
	err     error
	tx      *TX // tx is only set for queries in transactions.
	// Persist statement and db so the sql.Stmt is not closed until the Query is dropped.
	stmt *Statement
	db   *DB
}

// Iterator is used to iterate over the results of the query.
type Iterator struct {
	pq      *expr.PrimedQuery
	rows    *sql.Rows
	cols    []string
	err     error
	result  sql.Result
	started bool
	close   func() error
}

// Query takes a context, prepared SQLair Statement and the structs mentioned in the query arguments.
// It returns a Query object for iterating over the results.
func (db *DB) Query(ctx context.Context, s *Statement, inputArgs ...any) *Query {
	if ctx == nil {
		ctx = context.Background()
	}

	sqlstmt, ok := stmtCache.lookupStmt(db, s)
	var err error
	if !ok {
		sqlstmt, err = db.sqldb.PrepareContext(ctx, s.te.SQL())
		if err != nil {
			return &Query{ctx: ctx, err: err}
		}
		stmtCache.storeStmt(db, s, sqlstmt)
	}

	pq, err := s.te.BindInputs(inputArgs...)
	if err != nil {
		return &Query{ctx: ctx, err: err}
	}

	return &Query{sqlstmt: sqlstmt, stmt: s, db: db, pq: pq, ctx: ctx, err: nil}
}

// Run is an alias for Get that takes no arguments.
func (q *Query) Run() error {
	return q.Get()
}

// Get runs the query and decodes the first result into the provided output arguments.
// It returns ErrNoRows if output arguments were provided but no results were found.
// An &Outcome{} variable may be provided as the first output variable.
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
func (q *Query) Iter() *Iterator {
	if q.err != nil {
		return &Iterator{err: q.err}
	}
	if q.tx != nil && q.tx.isDone() {
		return &Iterator{err: ErrTXDone}
	}

	var result sql.Result
	var rows *sql.Rows
	var err error
	var cols []string
	var close func() error
	sqlstmt := q.sqlstmt
	if q.tx != nil {
		sqlstmt = q.tx.sqltx.Stmt(q.sqlstmt)
		close = sqlstmt.Close
	}
	if q.pq.HasOutputs() {
		rows, err = sqlstmt.QueryContext(q.ctx, q.pq.Params()...)
		if err == nil { // if err IS nil
			cols, err = rows.Columns()
		}
	} else {
		result, err = sqlstmt.ExecContext(q.ctx, q.pq.Params()...)
	}
	if err != nil {
		if close != nil {
			close()
		}
		return &Iterator{pq: q.pq, err: err}
	}

	return &Iterator{pq: q.pq, rows: rows, cols: cols, err: err, result: result, close: close}
}

// Next prepares the next row for Get.
// If an error occurs during iteration it will be returned with Iter.Close().
func (iter *Iterator) Next() bool {
	iter.started = true
	if iter.err != nil || iter.rows == nil {
		return false
	}
	if !iter.rows.Next() {
		if iter.close != nil {
			err := iter.close()
			iter.close = nil
			if iter.err == nil {
				iter.err = err
			}
		}
		return false
	}
	return true
}

// Get decodes the result from the previous Next call into the provided output arguments.
// An &Outcome{} variable may be provided as the single output variable before the first call to Next.
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

// Close finishes the iteration and returns any errors encountered.
func (iter *Iterator) Close() error {
	var cerr error
	if iter.close != nil {
		cerr = iter.close()
		iter.close = nil
	}
	iter.started = true
	if iter.rows == nil {
		return iter.err
	}
	err := iter.rows.Close()
	iter.rows = nil
	if iter.err != nil {
		return iter.err
	}
	if err == nil {
		err = cerr
	}
	return err
}

// Outcome holds metadata about executed queries, and can be provided as the
// first output argument to any of the Get methods.
type Outcome struct {
	result sql.Result
}

func (o *Outcome) Result() sql.Result {
	return o.result
}

// GetAll iterates over the query and scans all rows into the provided slices.
// sliceArgs must contain pointers to slices of each of the output types.
// An &Outcome{} variable may be provided as the first output variable.
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
	}

	for i, ptrVal := range slicePtrVals {
		ptrVal.Elem().Set(sliceVals[i])
	}

	return nil
}

type TX struct {
	sqltx   *sql.Tx
	sqlconn *sql.Conn
	db      *DB
	done    int32
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

// Begin starts a transaction.
func (db *DB) Begin(ctx context.Context, opts *TXOptions) (*TX, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	sqlconn, err := db.sqldb.Conn(ctx)
	if err != nil {
		return nil, err
	}
	sqltx, err := sqlconn.BeginTx(ctx, opts.plainTXOptions())
	if err != nil {
		return nil, err
	}
	return &TX{sqltx: sqltx, sqlconn: sqlconn, db: db}, nil
}

// Commit commits the transaction.
func (tx *TX) Commit() error {
	err := tx.setDone()
	if err == nil {
		err = tx.sqltx.Commit()
	}
	if cerr := tx.sqlconn.Close(); err == nil {
		err = cerr
	}
	return err
}

// Rollback aborts the transaction.
func (tx *TX) Rollback() error {
	err := tx.setDone()
	if err == nil {
		err = tx.sqltx.Rollback()
	}
	if cerr := tx.sqlconn.Close(); err == nil {
		err = cerr
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

// Query takes a context, prepared SQLair Statement and the structs mentioned in the query arguments.
// It returns a Query object for iterating over the results.
func (tx *TX) Query(ctx context.Context, s *Statement, inputArgs ...any) *Query {
	if ctx == nil {
		ctx = context.Background()
	}
	if tx.isDone() {
		return &Query{ctx: ctx, err: ErrTXDone}
	}

	sqlstmt, ok := stmtCache.lookupStmt(tx.db, s)
	var err error
	if !ok {
		sqlstmt, err = tx.sqlconn.PrepareContext(ctx, s.te.SQL())
		if err != nil {
			return &Query{ctx: ctx, err: err}
		}
		stmtCache.storeStmt(tx.db, s, sqlstmt)
	}

	pq, err := s.te.BindInputs(inputArgs...)
	if err != nil {
		return &Query{ctx: ctx, err: err}
	}

	return &Query{sqlstmt: sqlstmt, stmt: s, db: tx.db, pq: pq, tx: tx, ctx: ctx, err: nil}
}
