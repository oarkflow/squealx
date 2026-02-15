package jsonbq

import (
	"context"
	"strings"

	sqlx "github.com/oarkflow/squealx"
)

// SelectQuery builds SELECT statements
type SelectQuery struct {
	db         *sqlx.DB
	tx         *sqlx.Tx
	columnName string

	columns    []string
	table      string
	conditions []Condition
	orderBy    []string
	groupBy    []string
	having     []Condition
	limitVal   *int
	offsetVal  *int
	distinct   bool
	joins      []string
}

// Select specifies columns to select
func (s *SelectQuery) Select(cols ...string) *SelectQuery {
	s.columns = cols
	return s
}

// From specifies the table
func (s *SelectQuery) From(table string) *SelectQuery {
	s.table = table
	return s
}

// Where adds WHERE conditions
func (s *SelectQuery) Where(conds ...Condition) *SelectQuery {
	s.conditions = append(s.conditions, conds...)
	return s
}

// OrderBy adds ORDER BY clause
func (s *SelectQuery) OrderBy(expr string) *SelectQuery {
	s.orderBy = append(s.orderBy, expr)
	return s
}

// OrderByAsc adds ascending ORDER BY
func (s *SelectQuery) OrderByAsc(expr string) *SelectQuery {
	s.orderBy = append(s.orderBy, expr+" ASC")
	return s
}

// OrderByDesc adds descending ORDER BY
func (s *SelectQuery) OrderByDesc(expr string) *SelectQuery {
	s.orderBy = append(s.orderBy, expr+" DESC")
	return s
}

// GroupBy adds GROUP BY clause
func (s *SelectQuery) GroupBy(expr ...string) *SelectQuery {
	s.groupBy = append(s.groupBy, expr...)
	return s
}

// Having adds HAVING clause
func (s *SelectQuery) Having(conds ...Condition) *SelectQuery {
	s.having = append(s.having, conds...)
	return s
}

// Limit sets LIMIT
func (s *SelectQuery) Limit(n int) *SelectQuery {
	s.limitVal = &n
	return s
}

// Offset sets OFFSET
func (s *SelectQuery) Offset(n int) *SelectQuery {
	s.offsetVal = &n
	return s
}

// Distinct adds DISTINCT
func (s *SelectQuery) Distinct() *SelectQuery {
	s.distinct = true
	return s
}

// Join adds a JOIN clause
func (s *SelectQuery) Join(join string) *SelectQuery {
	s.joins = append(s.joins, join)
	return s
}

// Build generates SQL and args
func (s *SelectQuery) Build() (string, []any) {
	q := &Query{}

	// SELECT
	q.sql.WriteString("SELECT ")
	if s.distinct {
		q.sql.WriteString("DISTINCT ")
	}
	if len(s.columns) == 0 {
		q.sql.WriteString("*")
	} else {
		q.sql.WriteString(strings.Join(s.columns, ", "))
	}

	// FROM
	if s.table != "" {
		q.sql.WriteString(" FROM ")
		q.sql.WriteString(s.table)
	}

	// JOINs
	for _, join := range s.joins {
		q.sql.WriteString(" ")
		q.sql.WriteString(join)
	}

	// WHERE
	if len(s.conditions) > 0 {
		q.sql.WriteString(" WHERE ")
		for i, cond := range s.conditions {
			if i > 0 {
				q.sql.WriteString(" AND ")
			}
			cond.Build(q, s.columnName)
		}
	}

	// GROUP BY
	if len(s.groupBy) > 0 {
		q.sql.WriteString(" GROUP BY ")
		q.sql.WriteString(strings.Join(s.groupBy, ", "))
	}

	// HAVING
	if len(s.having) > 0 {
		q.sql.WriteString(" HAVING ")
		for i, cond := range s.having {
			if i > 0 {
				q.sql.WriteString(" AND ")
			}
			cond.Build(q, s.columnName)
		}
	}

	// ORDER BY
	if len(s.orderBy) > 0 {
		q.sql.WriteString(" ORDER BY ")
		q.sql.WriteString(strings.Join(s.orderBy, ", "))
	}

	// LIMIT
	if s.limitVal != nil {
		q.sql.WriteString(" LIMIT ")
		q.sql.WriteString(q.addArg(*s.limitVal))
	}

	// OFFSET
	if s.offsetVal != nil {
		q.sql.WriteString(" OFFSET ")
		q.sql.WriteString(q.addArg(*s.offsetVal))
	}

	return q.String(), q.Args()
}

// Execution methods

// Get scans a single row into dest
func (s *SelectQuery) Get(dest any) error {
	return s.GetContext(context.Background(), dest)
}

// GetContext scans a single row with context
func (s *SelectQuery) GetContext(ctx context.Context, dest any) error {
	sql, args := s.Build()
	if s.tx != nil {
		return s.tx.GetContext(ctx, dest, sql, args...)
	}
	return s.db.GetContext(ctx, dest, sql, args...)
}

// Select executes query and scans into dest slice
func (s *SelectQuery) Exec(dest any) error {
	return s.ExecContext(context.Background(), dest)
}

// SelectContext executes query with context
func (s *SelectQuery) ExecContext(ctx context.Context, dest any) error {
	sql, args := s.Build()
	if s.tx != nil {
		return s.tx.SelectContext(ctx, dest, sql, args...)
	}
	return s.db.SelectContext(ctx, dest, sql, args...)
}

// Query returns sql.Rows
func (s *SelectQuery) Query() (sqlx.SQLRows, error) {
	return s.QueryContext(context.Background())
}

// QueryContext returns sql.Rows with context
func (s *SelectQuery) QueryContext(ctx context.Context) (sqlx.SQLRows, error) {
	sql, args := s.Build()
	if s.tx != nil {
		return s.tx.QueryContext(ctx, sql, args...)
	}
	return s.db.QueryContext(ctx, sql, args...)
}

// QueryRow returns a single row
func (s *SelectQuery) QueryRow() sqlx.SQLRow {
	return s.QueryRowContext(context.Background())
}

// QueryRowContext returns a single row with context
func (s *SelectQuery) QueryRowContext(ctx context.Context) sqlx.SQLRow {
	sql, args := s.Build()
	if s.tx != nil {
		return s.tx.QueryRowContext(ctx, sql, args...)
	}
	return s.db.QueryRowContext(ctx, sql, args...)
}

// Count returns count of matching rows
func (s *SelectQuery) Count() (int64, error) {
	return s.CountContext(context.Background())
}

// CountContext returns count with context
func (s *SelectQuery) CountContext(ctx context.Context) (int64, error) {
	// Build count query
	q := &SelectQuery{
		db:         s.db,
		tx:         s.tx,
		columnName: s.columnName,
		table:      s.table,
		conditions: s.conditions,
		joins:      s.joins,
		groupBy:    s.groupBy,
		having:     s.having,
	}
	q.columns = []string{"COUNT(*)"}

	var count int64
	err := q.GetContext(ctx, &count)
	return count, err
}

// Exists checks if any rows match
func (s *SelectQuery) Exists() (bool, error) {
	return s.ExistsContext(context.Background())
}

// ExistsContext checks existence with context
func (s *SelectQuery) ExistsContext(ctx context.Context) (bool, error) {
	count, err := s.CountContext(ctx)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}
