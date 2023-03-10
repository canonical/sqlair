package expr

import "database/sql"

func PreparedSQL(pe *PreparedExpr) string {
	return pe.sql
}

func CompletedArgs(ce *CompletedExpr) []any {
	return ce.args
}

// TestDB allows for tests that include the whole SQLair pipeline.
type TestDB struct {
	db *sql.DB
}

func NewTestDB(db *sql.DB) *TestDB {
	return &TestDB{db: db}
}

func (db *TestDB) Query(ce *CompletedExpr) (*ResultExpr, error) {
	rows, err := db.db.Query(ce.sql, ce.args...)
	if err != nil {
		return nil, err
	}
	return &ResultExpr{outputs: ce.outputs, rows: rows}, nil
}

func (db *TestDB) Exec(ce *CompletedExpr) (sql.Result, error) {
	res, err := db.db.Exec(ce.sql, ce.args...)
	if err != nil {
		return nil, err
	}
	return res, nil
}
