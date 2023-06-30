package sqlair

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"runtime"
	"sync"
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

var stmtIDCount int64
var dbIDCount int64

type txdbID = int64
type stmtID = int64

var cacheMutex sync.RWMutex
var dbStmts = make(map[stmtID][]txdbID)
var stmtCache = make(map[txdbID]map[stmtID]*sql.Stmt)

// Statement represents a SQL statement with valid SQLair expressions.
// It is ready to be run on a SQLair DB.
type Statement struct {
	cacheID stmtID
	pe      *expr.PreparedExpr
}

func stmtFinalizer(s *Statement) {
	cacheMutex.Lock()
	dbtxIDs := dbStmts[s.cacheID]
	delete(dbStmts, s.cacheID)
	for _, dbtxID := range dbtxIDs {
		dbCache := stmtCache[dbtxID]
		ps, ok := dbCache[s.cacheID]
		if ok {
			ps.Close()
			delete(dbCache, s.cacheID)
		}
	}
	cacheMutex.Unlock()
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

	var s = Statement{pe: preparedExpr, cacheID: atomic.AddInt64(&stmtIDCount, 1)}
	runtime.SetFinalizer(&s, stmtFinalizer)
	return &s, nil
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
	cacheID txdbID
	db      *sql.DB
}

func NewDB(db *sql.DB) *DB {
	cacheID := atomic.AddInt64(&dbIDCount, 1)
	cacheMutex.Lock()
	stmtCache[cacheID] = make(map[stmtID]*sql.Stmt)
	cacheMutex.Unlock()
	return &DB{db: db, cacheID: cacheID}
}

// PlainDB returns the underlying database object.
func (db *DB) PlainDB() *sql.DB {
	return db.db
}

// Query holds the results of a database query.
type Query struct {
	qe   *expr.QueryExpr
	stmt *sql.Stmt
	ctx  context.Context
	err  error
}

// Iterator is used to iterate over the results of the query.
type Iterator struct {
	qe      *expr.QueryExpr
	rows    *sql.Rows
	cols    []string
	err     error
	result  sql.Result
	started bool
}

func (db *DB) Close() error {
	cacheMutex.Lock()
	// There is no need to close the sql.Stmts here, the resources are freed
	// when the database connection is closed.
	delete(stmtCache, db.cacheID)
	cacheMutex.Unlock()
	return db.db.Close()
}

// Query takes a context, prepared SQLair Statement and the structs mentioned in the query arguments.
// It returns a Query object for iterating over the results.
func (db *DB) Query(ctx context.Context, s *Statement, inputArgs ...any) *Query {
	if ctx == nil {
		ctx = context.Background()
	}

	var err error
	var qe *expr.QueryExpr
	qe, err = s.pe.Query(inputArgs...)
	if err != nil {
		return &Query{ctx: ctx, err: err}
	}

	cacheMutex.RLock()
	dbCache, ok := stmtCache[db.cacheID]
	if !ok {
		cacheMutex.RUnlock()
		return &Query{ctx: ctx, err: fmt.Errorf("sql: database is closed")}
	}
	ps, ok := dbCache[s.cacheID]
	cacheMutex.RUnlock()
	if !ok {
		ps, err = db.db.PrepareContext(ctx, qe.QuerySQL())
		if err != nil {
			return &Query{ctx: ctx, err: err}
		}
		cacheMutex.Lock()
		dbStmts[s.cacheID] = append(dbStmts[s.cacheID], db.cacheID)
		dbCache[s.cacheID] = ps
		cacheMutex.Unlock()

	}
	return &Query{stmt: ps, qe: qe, ctx: ctx, err: nil}
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
	if !q.qe.HasOutputs() && len(outputArgs) > 0 {
		return fmt.Errorf("cannot get results: output variables provided but not referenced in query")
	}

	var err error
	iter := q.Iter()
	if outcome != nil {
		err = iter.Get(outcome)
	}
	if err == nil && !iter.Next() {
		err = iter.Close()
		if err == nil && q.qe.HasOutputs() {
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
	var result sql.Result
	var rows *sql.Rows
	var err error
	var cols []string
	if q.qe.HasOutputs() {
		rows, err = q.stmt.QueryContext(q.ctx, q.qe.QueryArgs()...)
		if err == nil { // if err IS nil
			cols, err = rows.Columns()
		}
	} else {
		result, err = q.stmt.ExecContext(q.ctx, q.qe.QueryArgs()...)
	}
	return &Iterator{qe: q.qe, rows: rows, cols: cols, err: err, result: result}
}

// Next prepares the next row for Get.
// If an error occurs during iteration it will be returned with Iter.Close().
func (iter *Iterator) Next() bool {
	iter.started = true
	if iter.err != nil || iter.rows == nil {
		return false
	}
	return iter.rows.Next()
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
		if oc, ok := outputArgs[0].(*Outcome); ok && len(outputArgs) == 1 {
			oc.result = iter.result
			return nil
		}
		return fmt.Errorf("cannot call Get before Next unless getting outcome")
	}

	if iter.rows == nil {
		return fmt.Errorf("iteration ended")
	}

	ptrs, onSuccess, err := iter.qe.ScanArgs(iter.cols, outputArgs)
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
	cacheID txdbID
	tx      *sql.Tx
	db      *DB
}

// Begin starts a transaction.
func (db *DB) Begin(ctx context.Context, opts *TXOptions) (*TX, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	cacheID := atomic.AddInt64(&dbIDCount, 1)
	cacheMutex.Lock()
	stmtCache[cacheID] = make(map[stmtID]*sql.Stmt)
	cacheMutex.Unlock()
	tx, err := db.db.BeginTx(ctx, opts.plainTXOptions())
	return &TX{tx: tx, db: db, cacheID: cacheID}, err
}

// Commit commits the transaction.
func (tx *TX) Commit() error {
	cacheMutex.Lock()
	// There is no need to close the sql.Stmts here, the resources are freed
	// when the transaction is closed.
	delete(stmtCache, tx.cacheID)
	cacheMutex.Unlock()
	return tx.tx.Commit()
}

// Rollback aborts the transaction.
func (tx *TX) Rollback() error {
	cacheMutex.Lock()
	// There is no need to close the sql.Stmts here, the resources are freed
	// when the transaction is closed.
	delete(stmtCache, tx.cacheID)
	cacheMutex.Unlock()
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
	if err != nil {
		return &Query{ctx: ctx, err: err}
	}

	cacheMutex.RLock()
	txCache, ok := stmtCache[tx.cacheID]
	if !ok {
		cacheMutex.RUnlock()
		return &Query{ctx: ctx, err: sql.ErrTxDone}
	}
	ps, ok := txCache[s.cacheID]
	cacheMutex.RUnlock()
	if !ok {
		// If we cannot find the prepared statement in the transaction cache,
		// try the db cache and use tx.Stmt to prepare it on the tx.
		cacheMutex.RLock()
		dbCache, ok := stmtCache[tx.db.cacheID]
		if !ok {
			cacheMutex.RUnlock()
			return &Query{ctx: ctx, err: fmt.Errorf("sql: database is closed")}
		}
		ps, ok = dbCache[s.cacheID]
		cacheMutex.RUnlock()
		if ok {
			ps = tx.tx.Stmt(ps)
		} else {
			ps, err = tx.tx.PrepareContext(ctx, qe.QuerySQL())
			if err != nil {
				return &Query{ctx: ctx, err: err}
			}
		}
		cacheMutex.Lock()
		dbStmts[s.cacheID] = append(dbStmts[s.cacheID], tx.cacheID)
		txCache[s.cacheID] = ps
		cacheMutex.Unlock()
	}
	return &Query{stmt: ps, qe: qe, ctx: ctx, err: nil}
}
