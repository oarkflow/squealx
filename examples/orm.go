package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/drivers/postgres"
)

func main() {
	db, err := postgres.Open("host=localhost user=postgres password=postgres dbname=clear_dev sslmode=disable", "index")
	if err != nil {
		log.Fatalln(err)
	}
	query := NewSQLXQuery(db)
	var data []map[string]any
	query.Table("charge_master").Limit(1).Find(&data)
	fmt.Println(data)
}

type Query interface {
	WithContext(ctx context.Context) Query
	Driver() string
	Instance() *squealx.DB
	Count(count *int64) Query
	Create(values map[string]any) Query
	Delete(value any, conds ...any) Query
	Distinct(args ...any) Query
	Exec(sql string, values ...any) Query
	Find(dest any, conds ...any) Query
	First(dest any) Query
	FirstOrCreate(dest any, conds ...any) Query
	ForceDelete(value any, conds ...any) Query
	Get(dest any) Query
	Group(name string) Query
	Having(query any, args ...any) Query
	Join(query string, args ...any) Query
	Limit(limit int) Query
	Model(value any) Query
	Offset(offset int) Query
	Order(value any) Query
	OrWhere(query any, args ...any) Query
	Pluck(column string, dest any) Query
	Raw(sql string, values ...any) Query
	Save(value any) Query
	Scan(dest any) Query
	Scopes(funcs ...func(Query) Query) Query
	Select(query any, args ...any) Query
	Table(name string, args ...any) Query
	Update(column string, value any) Query
	Updates(values map[string]any) Query
	Where(query any, args ...any) Query
	With(query string, args ...any) Query
	WithTrashed() Query
	Fields(schema, name string) ([]string, error)
}

type SQLXQuery struct {
	db      *squealx.DB
	ctx     context.Context
	table   string
	query   string
	args    []any
	clauses []string
	limit   int
	offset  int
	group   string
	order   string
}

// NewSQLXQuery initializes a new SQLXQuery
func NewSQLXQuery(db *squealx.DB) *SQLXQuery {
	return &SQLXQuery{
		db:    db,
		query: "",
		args:  []any{},
		ctx:   context.Background(),
	}
}

func (q *SQLXQuery) WithContext(ctx context.Context) Query {
	q.ctx = ctx
	return q
}

func (q *SQLXQuery) Driver() string {
	return q.db.DriverName()
}

func (q *SQLXQuery) Instance() *squealx.DB {
	return q.db
}

func (q *SQLXQuery) Count(count *int64) Query {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s %s", q.table, q.buildClauses())
	err := q.db.GetContext(q.ctx, count, query, q.args...)
	if err != nil {
		// handle error
	}
	return q
}

func (q *SQLXQuery) Create(values map[string]any) Query {
	columns := []string{}
	placeholders := []string{}
	args := []any{}

	for col, val := range values {
		columns = append(columns, col)
		placeholders = append(placeholders, "?")
		args = append(args, val)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", q.table, strings.Join(columns, ", "), strings.Join(placeholders, ", "))
	_, err := q.db.ExecContext(q.ctx, query, args...)
	if err != nil {
		// handle error
	}
	return q
}

func (q *SQLXQuery) Delete(value any, conds ...any) Query {
	_, err := q.db.ExecContext(q.ctx, fmt.Sprintf("DELETE FROM %s WHERE ...", q.table), conds...)
	if err != nil {
		// handle error
	}
	return q
}

func (q *SQLXQuery) Distinct(args ...any) Query {
	var strArg []string
	for _, arg := range args {
		switch arg := arg.(type) {
		case string:
			strArg = append(strArg, arg)
		}
	}
	q.query = fmt.Sprintf("SELECT DISTINCT %s FROM %s", strings.Join(strArg, ", "), q.table)
	return q
}

func (q *SQLXQuery) Exec(sql string, values ...any) Query {
	_, err := q.db.ExecContext(q.ctx, sql, values...)
	if err != nil {
		// handle error
	}
	return q
}

func (q *SQLXQuery) Find(dest any, conds ...any) Query {
	query := fmt.Sprintf("SELECT * FROM %s %s", q.table, q.buildClauses())
	err := q.db.SelectContext(q.ctx, dest, query, q.args...)
	if err != nil {
		// handle error
	}
	return q
}

func (q *SQLXQuery) First(dest any) Query {
	query := fmt.Sprintf("SELECT * FROM %s %s LIMIT 1", q.table, q.buildClauses())
	err := q.db.GetContext(q.ctx, dest, query, q.args...)
	if err != nil {
		// handle error
	}
	return q
}

func (q *SQLXQuery) FirstOrCreate(dest any, conds ...any) Query {
	err := q.First(dest).(*SQLXQuery).Find(dest).(*SQLXQuery).Get(dest)
	if err != nil {
		q.Create(dest.(map[string]any))
	}
	return q
}

func (q *SQLXQuery) ForceDelete(value any, conds ...any) Query {
	return q.Delete(value, conds...)
}

func (q *SQLXQuery) Get(dest any) Query {
	query := fmt.Sprintf("SELECT * FROM %s %s", q.table, q.buildClauses())
	err := q.db.GetContext(q.ctx, dest, query, q.args...)
	if err != nil {
		// handle error
	}
	return q
}

func (q *SQLXQuery) Group(name string) Query {
	q.group = name
	return q
}

func (q *SQLXQuery) Having(query any, args ...any) Query {
	q.clauses = append(q.clauses, fmt.Sprintf("HAVING %s", query))
	q.args = append(q.args, args...)
	return q
}

func (q *SQLXQuery) Join(query string, args ...any) Query {
	q.clauses = append(q.clauses, fmt.Sprintf("JOIN %s", query))
	q.args = append(q.args, args...)
	return q
}

func (q *SQLXQuery) Limit(limit int) Query {
	q.limit = limit
	return q
}

func (q *SQLXQuery) Model(value any) Query {
	q.table = value.(string)
	return q
}

func (q *SQLXQuery) Offset(offset int) Query {
	q.offset = offset
	return q
}

func (q *SQLXQuery) Order(value any) Query {
	q.order = value.(string)
	return q
}

func (q *SQLXQuery) OrWhere(query any, args ...any) Query {
	q.clauses = append(q.clauses, fmt.Sprintf("OR %s", query))
	q.args = append(q.args, args...)
	return q
}

func (q *SQLXQuery) Pluck(column string, dest any) Query {
	query := fmt.Sprintf("SELECT %s FROM %s %s", column, q.table, q.buildClauses())
	err := q.db.SelectContext(q.ctx, dest, query, q.args...)
	if err != nil {
		// handle error
	}
	return q
}

func (q *SQLXQuery) Raw(sql string, values ...any) Query {
	q.query = sql
	q.args = values
	return q
}

func (q *SQLXQuery) Save(value any) Query {
	// Assuming Save means Insert or Update depending on existence
	return q.Create(value.(map[string]any))
}

func (q *SQLXQuery) Scan(dest any) Query {
	err := q.db.SelectContext(q.ctx, dest, q.query, q.args...)
	if err != nil {
		// handle error
	}
	return q
}

func (q *SQLXQuery) Scopes(funcs ...func(Query) Query) Query {
	for _, fn := range funcs {
		fn(q)
	}
	return q
}

func (q *SQLXQuery) Select(query any, args ...any) Query {
	q.query = fmt.Sprintf("SELECT %s FROM %s", query, q.table)
	q.args = append(q.args, args...)
	return q
}

func (q *SQLXQuery) Table(name string, args ...any) Query {
	q.table = name
	return q
}

func (q *SQLXQuery) Update(column string, value any) Query {
	query := fmt.Sprintf("UPDATE %s SET %s = ? %s", q.table, column, q.buildClauses())
	_, err := q.db.ExecContext(q.ctx, query, append([]any{value}, q.args...)...)
	if err != nil {
		// handle error
	}
	return q
}

func (q *SQLXQuery) Updates(values map[string]any) Query {
	sets := []string{}
	args := []any{}
	for col, val := range values {
		sets = append(sets, fmt.Sprintf("%s = ?", col))
		args = append(args, val)
	}
	query := fmt.Sprintf("UPDATE %s SET %s %s", q.table, strings.Join(sets, ", "), q.buildClauses())
	_, err := q.db.ExecContext(q.ctx, query, append(args, q.args...)...)
	if err != nil {
		// handle error
	}
	return q
}

func (q *SQLXQuery) Where(query any, args ...any) Query {
	q.clauses = append(q.clauses, fmt.Sprintf("WHERE %s", query))
	q.args = append(q.args, args...)
	return q
}

func (q *SQLXQuery) With(query string, args ...any) Query {
	q.clauses = append(q.clauses, fmt.Sprintf("WITH %s", query))
	q.args = append(q.args, args...)
	return q
}

func (q *SQLXQuery) WithTrashed() Query {
	// Handle soft deletes
	return q
}

func (q *SQLXQuery) Fields(schema, name string) ([]string, error) {
	var fields []string
	query := fmt.Sprintf("SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ?", schema, name)
	err := q.db.SelectContext(q.ctx, &fields, query, schema, name)
	if err != nil {
		return nil, err
	}
	return fields, nil
}

func (q *SQLXQuery) buildClauses() string {
	var parts []string
	if len(q.clauses) > 0 {
		parts = append(parts, strings.Join(q.clauses, " "))
	}
	if q.group != "" {
		parts = append(parts, fmt.Sprintf("GROUP BY %s", q.group))
	}
	if q.order != "" {
		parts = append(parts, fmt.Sprintf("ORDER BY %s", q.order))
	}
	if q.limit > 0 {
		parts = append(parts, fmt.Sprintf("LIMIT %d", q.limit))
	}
	if q.offset > 0 {
		parts = append(parts, fmt.Sprintf("OFFSET %d", q.offset))
	}
	return strings.Join(parts, " ")
}
