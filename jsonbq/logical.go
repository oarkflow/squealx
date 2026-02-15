package jsonbq

// And combines conditions with AND
func And(conds ...Condition) Condition {
	return Expr{
		build: func(q *Query, columnName string) {
			if len(conds) == 0 {
				return
			}
			if len(conds) == 1 {
				conds[0].Build(q, columnName)
				return
			}
			q.sql.WriteString("(")
			for i, c := range conds {
				if i > 0 {
					q.sql.WriteString(" AND ")
				}
				c.Build(q, columnName)
			}
			q.sql.WriteString(")")
		},
	}
}

// Or combines conditions with OR
func Or(conds ...Condition) Condition {
	return Expr{
		build: func(q *Query, columnName string) {
			if len(conds) == 0 {
				return
			}
			if len(conds) == 1 {
				conds[0].Build(q, columnName)
				return
			}
			q.sql.WriteString("(")
			for i, c := range conds {
				if i > 0 {
					q.sql.WriteString(" OR ")
				}
				c.Build(q, columnName)
			}
			q.sql.WriteString(")")
		},
	}
}

// Not negates a condition
func Not(cond Condition) Condition {
	return Expr{
		build: func(q *Query, columnName string) {
			q.sql.WriteString("NOT (")
			cond.Build(q, columnName)
			q.sql.WriteString(")")
		},
	}
}
