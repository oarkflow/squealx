package jsonbq

import (
	"context"
	"database/sql"
	"encoding/json"

	sqlx "github.com/oarkflow/squealx"
)

// UpdateQuery builds UPDATE statements
type UpdateQuery struct {
	db         *sqlx.DB
	tx         *sqlx.Tx
	columnName string

	table      string
	sets       []*SetExpr
	conditions []Condition
	returning  []string
}

// SetExpr represents a jsonb_set operation
type SetExpr struct {
	path []string
	val  any
}

// Set creates a new SetExpr for a path
func Set(path ...string) *SetExpr {
	return &SetExpr{path: path}
}

// To sets the value for this path
func (s *SetExpr) To(val any) *SetExpr {
	s.val = val
	return s
}

// Set adds jsonb_set operations
func (u *UpdateQuery) Set(exprs ...*SetExpr) *UpdateQuery {
	u.sets = append(u.sets, exprs...)
	return u
}

// SetData replaces entire JSONB column with new data
func (u *UpdateQuery) SetData(data any) *UpdateQuery {
	// Special marker for full replacement
	u.sets = append(u.sets, &SetExpr{path: nil, val: data})
	return u
}

// Where adds WHERE conditions
func (u *UpdateQuery) Where(conds ...Condition) *UpdateQuery {
	u.conditions = append(u.conditions, conds...)
	return u
}

// Returning adds RETURNING clause
func (u *UpdateQuery) Returning(cols ...string) *UpdateQuery {
	u.returning = cols
	return u
}

// Build generates SQL and args
func (u *UpdateQuery) Build() (string, []any, error) {
	q := &Query{}

	q.sql.WriteString("UPDATE ")
	q.sql.WriteString(u.table)
	q.sql.WriteString(" SET ")

	if len(u.sets) == 0 {
		return "", nil, nil
	}

	// Check if this is a full data replacement
	if len(u.sets) == 1 && u.sets[0].path == nil {
		jsonData, err := json.Marshal(u.sets[0].val)
		if err != nil {
			return "", nil, err
		}
		q.sql.WriteString(u.columnName)
		q.sql.WriteString(" = ")
		q.sql.WriteString(q.addArg(string(jsonData)))
		q.sql.WriteString("::jsonb")
	} else {
		// Build nested jsonb_set calls into a single valid expression.
		expr := u.columnName
		for _, s := range u.sets {
			jsonVal, err := json.Marshal(s.val)
			if err != nil {
				return "", nil, err
			}
			pathQ := &Query{}
			pathQ.writeTextArrayLiteral(s.path)
			expr = "jsonb_set(" + expr + ", " + pathQ.String() + ", " + q.addArg(string(jsonVal)) + "::jsonb, true)"
		}

		q.sql.WriteString(u.columnName)
		q.sql.WriteString(" = ")
		q.sql.WriteString(expr)
	}

	// WHERE
	if len(u.conditions) > 0 {
		q.sql.WriteString(" WHERE ")
		for i, cond := range u.conditions {
			if i > 0 {
				q.sql.WriteString(" AND ")
			}
			cond.Build(q, u.columnName)
		}
	}

	// RETURNING
	if len(u.returning) > 0 {
		q.sql.WriteString(" RETURNING ")
		for idx, col := range u.returning {
			if idx > 0 {
				q.sql.WriteString(", ")
			}
			q.sql.WriteString(col)
		}
	}

	return q.String(), q.Args(), nil
}

// Exec executes the update
func (u *UpdateQuery) Exec() (sql.Result, error) {
	return u.ExecContext(context.Background())
}

// ExecContext executes with context
func (u *UpdateQuery) ExecContext(ctx context.Context) (sql.Result, error) {
	sql, args, err := u.Build()
	if err != nil {
		return nil, err
	}
	if u.tx != nil {
		return u.tx.ExecContext(ctx, sql, args...)
	}
	return u.db.ExecContext(ctx, sql, args...)
}

// Get executes and scans RETURNING into dest
func (u *UpdateQuery) Get(dest any) error {
	return u.GetContext(context.Background(), dest)
}

// GetContext executes with context and scans
func (u *UpdateQuery) GetContext(ctx context.Context, dest any) error {
	sql, args, err := u.Build()
	if err != nil {
		return err
	}
	if u.tx != nil {
		return u.tx.GetContext(ctx, dest, sql, args...)
	}
	return u.db.GetContext(ctx, dest, sql, args...)
}

// Select executes and scans RETURNING into dest slice
func (u *UpdateQuery) Select(dest any) error {
	return u.SelectContext(context.Background(), dest)
}

// SelectContext executes with context and scans into slice
func (u *UpdateQuery) SelectContext(ctx context.Context, dest any) error {
	sql, args, err := u.Build()
	if err != nil {
		return err
	}
	if u.tx != nil {
		return u.tx.SelectContext(ctx, dest, sql, args...)
	}
	return u.db.SelectContext(ctx, dest, sql, args...)
}

// Remove operations (using - and #- operators)

// RemoveQuery handles JSONB field removal
type RemoveQuery struct {
	db         *sqlx.DB
	tx         *sqlx.Tx
	columnName string

	table      string
	removes    []*RemoveExpr
	conditions []Condition
	returning  []string
}

// RemoveExpr represents a removal operation
type RemoveExpr struct {
	isPath bool
	key    string
	path   []string
}

// RemoveKey removes a top-level key
func RemoveKey(key string) *RemoveExpr {
	return &RemoveExpr{isPath: false, key: key}
}

// RemovePath removes a nested path
func RemovePath(path ...string) *RemoveExpr {
	return &RemoveExpr{isPath: true, path: path}
}

// Remove creates a remove query
func (db *DB) Remove(table string) *RemoveQuery {
	return &RemoveQuery{
		db:         db.DB,
		columnName: db.columnName,
		table:      table,
	}
}

// Remove for transactions
func (tx *Tx) Remove(table string) *RemoveQuery {
	return &RemoveQuery{
		tx:         tx.Tx,
		columnName: tx.columnName,
		table:      table,
	}
}

// Fields adds removal expressions
func (r *RemoveQuery) Fields(exprs ...*RemoveExpr) *RemoveQuery {
	r.removes = append(r.removes, exprs...)
	return r
}

// Where adds WHERE conditions
func (r *RemoveQuery) Where(conds ...Condition) *RemoveQuery {
	r.conditions = append(r.conditions, conds...)
	return r
}

// Returning adds RETURNING clause
func (r *RemoveQuery) Returning(cols ...string) *RemoveQuery {
	r.returning = cols
	return r
}

// Build generates SQL
func (r *RemoveQuery) Build() (string, []any) {
	q := &Query{}

	q.sql.WriteString("UPDATE ")
	q.sql.WriteString(r.table)
	q.sql.WriteString(" SET ")
	q.sql.WriteString(r.columnName)
	q.sql.WriteString(" = ")

	expr := r.columnName
	for _, rm := range r.removes {
		if rm.isPath {
			pathQ := &Query{}
			pathQ.writeTextArrayLiteral(rm.path)
			expr = expr + " #- " + pathQ.String()
		} else {
			expr = expr + " - " + q.addArg(rm.key)
		}
	}
	q.sql.WriteString(expr)

	// WHERE
	if len(r.conditions) > 0 {
		q.sql.WriteString(" WHERE ")
		for i, cond := range r.conditions {
			if i > 0 {
				q.sql.WriteString(" AND ")
			}
			cond.Build(q, r.columnName)
		}
	}

	// RETURNING
	if len(r.returning) > 0 {
		q.sql.WriteString(" RETURNING ")
		for idx, col := range r.returning {
			if idx > 0 {
				q.sql.WriteString(", ")
			}
			q.sql.WriteString(col)
		}
	}

	return q.String(), q.Args()
}

// Exec executes the removal
func (r *RemoveQuery) Exec() (sql.Result, error) {
	return r.ExecContext(context.Background())
}

// ExecContext executes with context
func (r *RemoveQuery) ExecContext(ctx context.Context) (sql.Result, error) {
	sql, args := r.Build()
	if r.tx != nil {
		return r.tx.ExecContext(ctx, sql, args...)
	}
	return r.db.ExecContext(ctx, sql, args...)
}
