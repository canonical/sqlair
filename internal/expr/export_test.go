package expr

func PreparedSQL(pe *PreparedExpr) string {
	return pe.SQL(&StmtCriterion{enabled: false})
}
