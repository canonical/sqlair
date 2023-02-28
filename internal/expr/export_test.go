package expr

func PreparedSQL(pe *PreparedExpr) string {
	return pe.sql
}

func CompletedArgs(ce *CompletedExpr) []any {
	return ce.args
}
