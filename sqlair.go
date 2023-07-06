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

type dbID = int64
type stmtID = int64

var dbStmtCache = make(map[dbID]map[stmtID]bool)
var stmtDBCache = make(map[stmtID]map[dbID]*sql.Stmt)
var cacheMutex sync.RWMutex

// Statement represents a SQL statement with valid SQLair expressions.
// It is ready to be run on a SQLair DB.
type Statement struct {
	cacheID stmtID
	pe      *expr.PreparedExpr
}

func stmtFinalizer(s *Statement) {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()
	dbCache := stmtDBCache[s.cacheID]
	for dbCacheID, ps := range dbCache {
		ps.Close()
		delete(dbStmtCache[dbCacheID], s.cacheID)
	}
	delete(stmtDBCache, s.cacheID)
}

func dbFinalizer(db *DB) {
	// There is no need to close the sql.Stmts here, the resources in the
	// database are freed on db.Close.
	cacheMutex.Lock()
	defer cacheMutex.Unlock()
	db.Close()
	stmtCache := dbStmtCache[db.cacheID]
	for stmtCacheID, _ := range stmtCache {
		delete(stmtDBCache[stmtCacheID], db.cacheID)
	}
	delete(dbStmtCache, db.cacheID)
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

	cacheID := atomic.AddInt64(&stmtIDCount, 1)
	cacheMutex.Lock()
	defer cacheMutex.Unlock()
	stmtDBCache[cacheID] = make(map[dbID]*sql.Stmt)
	var s = Statement{pe: preparedExpr, cacheID: cacheID}
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
	cacheID dbID
	db      *sql.DB
}

func NewDB(sqldb *sql.DB) *DB {
	cacheID := atomic.AddInt64(&dbIDCount, 1)
	cacheMutex.Lock()
	dbStmtCache[cacheID] = make(map[stmtID]bool)
	cacheMutex.Unlock()
	var db = DB{db: sqldb, cacheID: cacheID}
	runtime.SetFinalizer(&db, dbFinalizer)
	return &db
}

// PlainDB returns the underlying database object.
func (db *DB) PlainDB() *sql.DB {
	return db.db
}

func (db *DB) Close() error {
	return db.db.Close()
}

// Query holds the results of a database query.
type Query struct {
	qe   *expr.QueryExpr
	stmt *sql.Stmt
	ctx  context.Context
	err  error
	isTX bool
}

// Iterator is used to iterate over the results of the query.
type Iterator struct {
	qe      *expr.QueryExpr
	rows    *sql.Rows
	cols    []string
	err     error
	result  sql.Result
	started bool
	closer  func() error
}

// Query takes a context, prepared SQLair Statement and the structs mentioned in the query arguments.
// It returns a Query object for iterating over the results.
func (db *DB) Query(ctx context.Context, s *Statement, inputArgs ...any) *Query {
	if ctx == nil {
		ctx = context.Background()
	}

	ps, err := db.prepareStmt(ctx, s)
	if err != nil {
		return &Query{ctx: ctx, err: err}
	}

	qe, err := s.pe.Query(inputArgs...)
	if err != nil {
		return &Query{ctx: ctx, err: err}
	}

	return &Query{stmt: ps, qe: qe, ctx: ctx, err: nil}
}

// Prepares a Statement on a database. prepareStmt first checks in the cache
// if it has already been preapred.
func (db *DB) prepareStmt(ctx context.Context, s *Statement) (*sql.Stmt, error) {
	var err error
	cacheMutex.RLock()
	// The statement is only removed from the cache when the finalizer is run
	// so it should always be in the cache.
	ps, ok := stmtDBCache[s.cacheID][db.cacheID]
	cacheMutex.RUnlock()
	if !ok {
		ps, err = db.db.PrepareContext(ctx, s.pe.SQL())
		if err != nil {
			return nil, err
		}
		cacheMutex.Lock()
		stmtDBCache[s.cacheID][db.cacheID] = ps
		dbStmtCache[db.cacheID][s.cacheID] = true
		cacheMutex.Unlock()
	}
	return ps, nil
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
	if err != nil {
		return &Iterator{qe: q.qe, err: err}
	}
	var closer func() error
	if q.isTX {
		closer = func() error {
			return q.stmt.Close()
		}
	}
	return &Iterator{qe: q.qe, rows: rows, cols: cols, err: err, result: result, closer: closer}
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
	var cerr error
	if iter.closer != nil {
		cerr = iter.closer()
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
	db *DB
}

// Begin starts a transaction.
func (db *DB) Begin(ctx context.Context, opts *TXOptions) (*TX, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	tx, err := db.db.BeginTx(ctx, opts.plainTXOptions())
	return &TX{tx: tx, db: db}, err
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
	if err != nil {
		return &Query{ctx: ctx, err: err}
	}

	var ps *sql.Stmt
	dbStats := tx.db.db.Stats()
	if dbStats.MaxOpenConnections > 0 && dbStats.MaxOpenConnections == dbStats.InUse {
		ps, err = tx.tx.PrepareContext(ctx, s.pe.SQL())
	} else {
		ps, err = tx.db.prepareStmt(ctx, s)
		ps = tx.tx.Stmt(ps)
	}
	if err != nil {
		return &Query{ctx: ctx, err: err}
	}
	return &Query{stmt: ps, qe: qe, isTX: true, ctx: ctx, err: nil}
}
