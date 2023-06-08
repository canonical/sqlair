package sqlair

import (
	"container/list"
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sync"

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

// Statement represents a SQL statement with valid SQLair expressions.
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
	prepedCache      *lruCache
	prepedCacheMutex sync.RWMutex
	db               *sql.DB
}

type DBOptions struct {
	PreparedStmtCacheSize int
}

func NewDB(db *sql.DB, dbopts *DBOptions) *DB {
	size := 500
	if dbopts != nil && dbopts.PreparedStmtCacheSize != 0 {
		size = dbopts.PreparedStmtCacheSize
	}
	return &DB{db: db, prepedCache: newLRUCache(size)}
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

// A Least Recently Used Cache using a doubly linked list.
type lruCache struct {
	ll   *list.List
	c    map[index]*list.Element
	size int
}

// The cache key.
type index struct {
	tx *TX
	s  *Statement
}

// The cache value.
type entry struct {
	key   index
	value *sql.Stmt
}

func newLRUCache(size int) *lruCache {
	c := &lruCache{}
	c.ll = list.New()
	c.c = make(map[index]*list.Element)
	c.size = size
	return c
}

func (c *lruCache) lookup(s *Statement, tx *TX) (*sql.Stmt, bool) {
	e, ok := c.c[index{tx: tx, s: s}]
	if !ok {
		return nil, ok
	}
	c.ll.MoveToFront(e)
	return e.Value.(*entry).value, ok
}

func (c *lruCache) add(s *Statement, tx *TX, ps *sql.Stmt) error {
	k := index{tx: tx, s: s}
	if e, ok := c.c[k]; ok {
		c.ll.MoveToFront(e)
		return nil
	}
	var err error
	e := c.ll.PushFront(&entry{key: k, value: ps})
	c.c[k] = e
	if c.ll.Len() > c.size {
		b := c.ll.Back()
		delete(c.c, b.Value.(*entry).key)
		c.ll.Remove(b)
		// Close the prepared statement on removal from the cache.
		err = b.Value.(*entry).value.Close()
	}
	return err
}

// Remove all statements prepared on tx from the cache.
// Note that this does not close the sql.Stmt.
func (c *lruCache) removeTX(tx *TX) {
	for k, _ := range c.c {
		if k.tx == tx {
			delete(c.c, k)
		}
	}
}

// Query takes a context, prepared SQLair Statement and the structs mentioned in the query arguments.
// It returns a Query object for iterating over the results.
func (db *DB) Query(ctx context.Context, s *Statement, inputArgs ...any) *Query {
	if ctx == nil {
		ctx = context.Background()
	}

	// Query only actually prepares the query arguemnt, the sql already
	// exists in pe. This and the prepared stmt lookup could be swapped.
	var err error
	var qe *expr.QueryExpr
	qe, err = s.pe.Query(inputArgs...)
	if err != nil {
		return &Query{ctx: ctx, err: err}
	}

	db.prepedCacheMutex.Lock()
	defer db.prepedCacheMutex.Unlock()
	ps, ok := db.prepedCache.lookup(s, nil)
	if !ok {
		ps, err = db.db.PrepareContext(ctx, qe.QuerySQL())
		if err == nil {
			err = db.prepedCache.add(s, nil, ps)
		}
	}

	return &Query{stmt: ps, qe: qe, ctx: ctx, err: err}
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
	tx.db.prepedCache.removeTX(tx)
	return tx.tx.Commit()
}

// Rollback aborts the transaction.
func (tx *TX) Rollback() error {
	tx.db.prepedCache.removeTX(tx)
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

	tx.db.prepedCacheMutex.Lock()
	defer tx.db.prepedCacheMutex.Unlock()

	var ok bool
	var ps *sql.Stmt
	ps, ok = tx.db.prepedCache.lookup(s, tx)
	if !ok {
		ps, ok = tx.db.prepedCache.lookup(s, nil)
		if !ok {
			ps, err = tx.tx.PrepareContext(ctx, qe.QuerySQL())
		} else {
			// If the statement is alredy prepared on the db, prepare it on the tx.
			ps = tx.tx.Stmt(ps)
		}
		if err != nil {
			err = tx.db.prepedCache.add(s, tx, ps)
		}
	}

	return &Query{stmt: ps, qe: qe, ctx: ctx, err: err}
}
