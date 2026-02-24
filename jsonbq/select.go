package jsonbq

import (
	"context"
	"fmt"
	"math"

	sqlx "github.com/oarkflow/squealx"
)

// SelectQuery builds SELECT statements
type SelectQuery struct {
	db         *sqlx.DB
	tx         *sqlx.Tx
	columnName string
	encrypted  *encryptedModeConfig

	columns    []SelectExpr
	table      string
	conditions []Condition
	orderBy    []SelectExpr
	groupBy    []SelectExpr
	having     []Condition
	limitVal   *int
	offsetVal  *int
	distinct   bool
	joins      []JoinClause
}

type SelectExpr struct {
	raw    string
	expr   Expr
	isExpr bool
}

type JoinClause struct {
	raw    string
	kind   string
	table  string
	conds  []Condition
	custom bool
}

// Select specifies columns to select
func (s *SelectQuery) Select(cols ...string) *SelectQuery {
	items := make([]SelectExpr, 0, len(cols))
	for _, col := range cols {
		items = append(items, SelectExpr{raw: col})
	}
	s.columns = items
	return s
}

// SelectExpr specifies expression columns to select.
func (s *SelectQuery) SelectExpr(exprs ...Expr) *SelectQuery {
	s.columns = make([]SelectExpr, 0, len(exprs))
	for _, expr := range exprs {
		s.columns = append(s.columns, SelectExpr{expr: expr, isExpr: true})
	}
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
	s.orderBy = append(s.orderBy, SelectExpr{raw: expr})
	return s
}

// OrderByAsc adds ascending ORDER BY
func (s *SelectQuery) OrderByAsc(expr string) *SelectQuery {
	s.orderBy = append(s.orderBy, SelectExpr{raw: expr + " ASC"})
	return s
}

// OrderByDesc adds descending ORDER BY
func (s *SelectQuery) OrderByDesc(expr string) *SelectQuery {
	s.orderBy = append(s.orderBy, SelectExpr{raw: expr + " DESC"})
	return s
}

// OrderByExpr adds an expression to ORDER BY.
func (s *SelectQuery) OrderByExpr(expr Expr) *SelectQuery {
	s.orderBy = append(s.orderBy, SelectExpr{expr: expr, isExpr: true})
	return s
}

// OrderByExprAsc adds an ascending expression to ORDER BY.
func (s *SelectQuery) OrderByExprAsc(expr Expr) *SelectQuery {
	return s.OrderByExpr(expr.Asc())
}

// OrderByExprDesc adds a descending expression to ORDER BY.
func (s *SelectQuery) OrderByExprDesc(expr Expr) *SelectQuery {
	return s.OrderByExpr(expr.Desc())
}

// GroupBy adds GROUP BY clause
func (s *SelectQuery) GroupBy(expr ...string) *SelectQuery {
	for _, item := range expr {
		s.groupBy = append(s.groupBy, SelectExpr{raw: item})
	}
	return s
}

// GroupByExpr adds GROUP BY expressions.
func (s *SelectQuery) GroupByExpr(expr ...Expr) *SelectQuery {
	for _, item := range expr {
		s.groupBy = append(s.groupBy, SelectExpr{expr: item, isExpr: true})
	}
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

// Page applies page/limit as OFFSET/LIMIT.
// Page number is 1-based. Invalid values are normalized.
func (s *SelectQuery) Page(page, limit int) *SelectQuery {
	if limit <= 0 {
		limit = 20
	}
	if page <= 0 {
		page = 1
	}
	offset := (page - 1) * limit
	return s.Limit(limit).Offset(offset)
}

// Distinct adds DISTINCT
func (s *SelectQuery) Distinct() *SelectQuery {
	s.distinct = true
	return s
}

// Join adds a JOIN clause
func (s *SelectQuery) Join(join string) *SelectQuery {
	s.joins = append(s.joins, JoinClause{raw: join, custom: true})
	return s
}

// InnerJoin adds an INNER JOIN with ON conditions.
func (s *SelectQuery) InnerJoin(table string, conds ...Condition) *SelectQuery {
	s.joins = append(s.joins, JoinClause{kind: "JOIN", table: table, conds: conds})
	return s
}

// LeftJoin adds a LEFT JOIN with ON conditions.
func (s *SelectQuery) LeftJoin(table string, conds ...Condition) *SelectQuery {
	s.joins = append(s.joins, JoinClause{kind: "LEFT JOIN", table: table, conds: conds})
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
		for i, col := range s.columns {
			if i > 0 {
				q.sql.WriteString(", ")
			}
			if col.isExpr {
				col.expr.Build(q, s.columnName)
				continue
			}
			q.sql.WriteString(col.raw)
		}
	}

	// FROM
	if s.table != "" {
		q.sql.WriteString(" FROM ")
		q.sql.WriteString(s.table)
	}

	// JOINs
	for _, join := range s.joins {
		q.sql.WriteString(" ")
		if join.custom {
			q.sql.WriteString(join.raw)
			continue
		}
		q.sql.WriteString(join.kind)
		q.sql.WriteString(" ")
		q.sql.WriteString(join.table)
		if len(join.conds) > 0 {
			valid := countNonNilConditions(join.conds)
			if valid == 0 {
				continue
			}
			q.sql.WriteString(" ON ")
			writeConditions(q, s.columnName, join.conds)
		}
	}

	// WHERE
	if len(s.conditions) > 0 && countNonNilConditions(s.conditions) > 0 {
		q.sql.WriteString(" WHERE ")
		writeConditions(q, s.columnName, s.conditions)
	}

	// GROUP BY
	if len(s.groupBy) > 0 {
		q.sql.WriteString(" GROUP BY ")
		for i, grp := range s.groupBy {
			if i > 0 {
				q.sql.WriteString(", ")
			}
			if grp.isExpr {
				grp.expr.Build(q, s.columnName)
				continue
			}
			q.sql.WriteString(grp.raw)
		}
	}

	// HAVING
	if len(s.having) > 0 && countNonNilConditions(s.having) > 0 {
		q.sql.WriteString(" HAVING ")
		writeConditions(q, s.columnName, s.having)
	}

	// ORDER BY
	if len(s.orderBy) > 0 {
		q.sql.WriteString(" ORDER BY ")
		for i, ord := range s.orderBy {
			if i > 0 {
				q.sql.WriteString(", ")
			}
			if ord.isExpr {
				ord.expr.Build(q, s.columnName)
				continue
			}
			q.sql.WriteString(ord.raw)
		}
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
		encrypted:  s.encrypted,
		table:      s.table,
		conditions: s.conditions,
		joins:      s.joins,
		groupBy:    s.groupBy,
		having:     s.having,
	}
	q.columns = []SelectExpr{{raw: "COUNT(*)"}}

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

// Paginate executes the query with page/limit and returns pagination metadata.
func (s *SelectQuery) Paginate(page, limit int, dest any) (*sqlx.Pagination, error) {
	return s.PaginateContext(context.Background(), page, limit, dest)
}

// PaginateContext executes the query with page/limit and returns pagination metadata.
func (s *SelectQuery) PaginateContext(ctx context.Context, page, limit int, dest any) (*sqlx.Pagination, error) {
	if limit <= 0 {
		limit = 20
	}
	if page <= 0 {
		page = 1
	}
	offset := (page - 1) * limit

	countSQL, countArgs := s.buildCountSQL()
	var total int64
	var err error
	if s.tx != nil {
		err = s.tx.GetContext(ctx, &total, countSQL, countArgs...)
	} else {
		err = s.db.GetContext(ctx, &total, countSQL, countArgs...)
	}
	if err != nil {
		return nil, err
	}

	paged := s.clone()
	paged.Page(page, limit)
	if err := paged.ExecContext(ctx, dest); err != nil {
		return nil, err
	}

	totalPage := 0
	if total > 0 {
		totalPage = int(math.Ceil(float64(total) / float64(limit)))
	}

	pagination := &sqlx.Pagination{
		TotalRecords: total,
		TotalPage:    totalPage,
		Offset:       offset,
		Limit:        limit,
		Page:         page,
		PrevPage:     page,
		NextPage:     page,
	}

	if page > 1 {
		pagination.PrevPage = page - 1
	}
	if totalPage > 0 && page < totalPage {
		pagination.NextPage = page + 1
	}

	return pagination, nil
}

// PaginateResponse executes pagination and returns a combined data+metadata response.
func (s *SelectQuery) PaginateResponse(page, limit int, dest any) sqlx.PaginatedResponse {
	return s.PaginateResponseContext(context.Background(), page, limit, dest)
}

// PaginateResponseContext executes pagination with context and returns a combined data+metadata response.
func (s *SelectQuery) PaginateResponseContext(ctx context.Context, page, limit int, dest any) sqlx.PaginatedResponse {
	pagination, err := s.PaginateContext(ctx, page, limit, dest)
	if err != nil {
		return sqlx.PaginatedResponse{Error: err}
	}
	return sqlx.PaginatedResponse{
		Items:      dest,
		Pagination: pagination,
	}
}

// TypedPaginatedResponse is a typed paginated response for jsonbq queries.
type TypedPaginatedResponse[T any] struct {
	Items      []T              `json:"data"`
	Pagination *sqlx.Pagination `json:"pagination"`
	Error      error            `json:"error,omitempty"`
}

// PaginateTypedResponse executes pagination and returns typed data with metadata.
func PaginateTypedResponse[T any](s *SelectQuery, page, limit int) TypedPaginatedResponse[T] {
	return PaginateTypedResponseContext[T](context.Background(), s, page, limit)
}

// PaginateTypedResponseContext executes pagination with context and returns typed data with metadata.
func PaginateTypedResponseContext[T any](ctx context.Context, s *SelectQuery, page, limit int) TypedPaginatedResponse[T] {
	var items []T
	pagination, err := s.PaginateContext(ctx, page, limit, &items)
	if err != nil {
		return TypedPaginatedResponse[T]{
			Items: items,
			Error: err,
		}
	}
	return TypedPaginatedResponse[T]{
		Items:      items,
		Pagination: pagination,
	}
}

func (s *SelectQuery) clone() *SelectQuery {
	cloned := &SelectQuery{
		db:         s.db,
		tx:         s.tx,
		columnName: s.columnName,
		encrypted:  s.encrypted,
		table:      s.table,
		distinct:   s.distinct,
	}
	cloned.columns = append([]SelectExpr(nil), s.columns...)
	cloned.conditions = append([]Condition(nil), s.conditions...)
	cloned.orderBy = append([]SelectExpr(nil), s.orderBy...)
	cloned.groupBy = append([]SelectExpr(nil), s.groupBy...)
	cloned.having = append([]Condition(nil), s.having...)
	cloned.joins = append([]JoinClause(nil), s.joins...)

	if s.limitVal != nil {
		limit := *s.limitVal
		cloned.limitVal = &limit
	}
	if s.offsetVal != nil {
		offset := *s.offsetVal
		cloned.offsetVal = &offset
	}

	return cloned
}

func (s *SelectQuery) buildCountSQL() (string, []any) {
	countSource := s.clone()
	countSource.orderBy = nil
	countSource.limitVal = nil
	countSource.offsetVal = nil

	querySQL, args := countSource.Build()
	return fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS count_query", querySQL), args
}

func countNonNilConditions(conds []Condition) int {
	count := 0
	for _, cond := range conds {
		if cond != nil {
			count++
		}
	}
	return count
}

func writeConditions(q *Query, columnName string, conds []Condition) {
	written := 0
	for _, cond := range conds {
		if cond == nil {
			continue
		}
		if written > 0 {
			q.sql.WriteString(" AND ")
		}
		cond.Build(q, columnName)
		written++
	}
}
