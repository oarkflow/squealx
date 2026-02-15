package jsonbq

import (
	"context"
	"database/sql"

	sqlx "github.com/oarkflow/squealx"
)

// DeleteQuery builds DELETE statements
type DeleteQuery struct {
	db         *sqlx.DB
	tx         *sqlx.Tx
	columnName string
	encrypted  *encryptedModeConfig

	table      string
	conditions []Condition
	returning  []string
}

// Where adds WHERE conditions
func (d *DeleteQuery) Where(conds ...Condition) *DeleteQuery {
	d.conditions = append(d.conditions, conds...)
	return d
}

// Returning adds RETURNING clause
func (d *DeleteQuery) Returning(cols ...string) *DeleteQuery {
	d.returning = cols
	return d
}

// Build generates SQL and args
func (d *DeleteQuery) Build() (string, []any) {
	q := &Query{}

	q.sql.WriteString("DELETE FROM ")
	q.sql.WriteString(d.table)

	// WHERE
	if len(d.conditions) > 0 {
		q.sql.WriteString(" WHERE ")
		for i, cond := range d.conditions {
			if i > 0 {
				q.sql.WriteString(" AND ")
			}
			cond.Build(q, d.columnName)
		}
	}

	// RETURNING
	if len(d.returning) > 0 {
		q.sql.WriteString(" RETURNING ")
		for idx, col := range d.returning {
			if idx > 0 {
				q.sql.WriteString(", ")
			}
			q.sql.WriteString(col)
		}
	}

	return q.String(), q.Args()
}

// Exec executes the delete
func (d *DeleteQuery) Exec() (sql.Result, error) {
	return d.ExecContext(context.Background())
}

// ExecContext executes with context
func (d *DeleteQuery) ExecContext(ctx context.Context) (sql.Result, error) {
	if d.tx != nil {
		if err := ensureEncryptedIntegrityBeforeWrite(ctx, d.tx, d.table, d.columnName, d.encrypted); err != nil {
			return nil, err
		}
	} else {
		if err := ensureEncryptedIntegrityBeforeWrite(ctx, d.db, d.table, d.columnName, d.encrypted); err != nil {
			return nil, err
		}
	}

	sql, args := d.Build()
	if d.tx != nil {
		return d.tx.ExecContext(ctx, sql, args...)
	}
	return d.db.ExecContext(ctx, sql, args...)
}

// Get executes and scans RETURNING into dest
func (d *DeleteQuery) Get(dest any) error {
	return d.GetContext(context.Background(), dest)
}

// GetContext executes with context and scans
func (d *DeleteQuery) GetContext(ctx context.Context, dest any) error {
	if d.tx != nil {
		if err := ensureEncryptedIntegrityBeforeWrite(ctx, d.tx, d.table, d.columnName, d.encrypted); err != nil {
			return err
		}
	} else {
		if err := ensureEncryptedIntegrityBeforeWrite(ctx, d.db, d.table, d.columnName, d.encrypted); err != nil {
			return err
		}
	}

	sql, args := d.Build()
	if d.tx != nil {
		return d.tx.GetContext(ctx, dest, sql, args...)
	}
	return d.db.GetContext(ctx, dest, sql, args...)
}

// Select executes and scans RETURNING into dest slice
func (d *DeleteQuery) Select(dest any) error {
	return d.SelectContext(context.Background(), dest)
}

// SelectContext executes with context and scans into slice
func (d *DeleteQuery) SelectContext(ctx context.Context, dest any) error {
	if d.tx != nil {
		if err := ensureEncryptedIntegrityBeforeWrite(ctx, d.tx, d.table, d.columnName, d.encrypted); err != nil {
			return err
		}
	} else {
		if err := ensureEncryptedIntegrityBeforeWrite(ctx, d.db, d.table, d.columnName, d.encrypted); err != nil {
			return err
		}
	}

	sql, args := d.Build()
	if d.tx != nil {
		return d.tx.SelectContext(ctx, dest, sql, args...)
	}
	return d.db.SelectContext(ctx, dest, sql, args...)
}
