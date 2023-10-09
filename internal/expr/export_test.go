package expr

// Generate the SQL for a query with no slices in.
func PreparedSQL(pe *PreparedExpr) string {
	return pe.sql(&stmtCriterion{sliceLens: []int{}})
}
