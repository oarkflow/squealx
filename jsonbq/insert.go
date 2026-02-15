package jsonbq

import (
	"context"
	"database/sql"
	"encoding/json"

	sqlx "github.com/oarkflow/squealx"
)

// InsertQuery builds INSERT statements
type InsertQuery struct {
	db         *sqlx.DB
	tx         *sqlx.Tx
	columnName string

	table     string
	data      any
	returning []string
}

// Data sets the data to insert (will be marshaled to JSON)
func (i *InsertQuery) Data(data any) *InsertQuery {
	i.data = data
	return i
}

// Returning adds RETURNING clause
func (i *InsertQuery) Returning(cols ...string) *InsertQuery {
	i.returning = cols
	return i
}

// Build generates SQL and args
func (i *InsertQuery) Build() (string, []any, error) {
	q := &Query{}

	// Marshal data to JSON
	jsonData, err := json.Marshal(i.data)
	if err != nil {
		return "", nil, err
	}

	// INSERT INTO
	q.sql.WriteString("INSERT INTO ")
	q.sql.WriteString(i.table)
	q.sql.WriteString(" (")
	q.sql.WriteString(i.columnName)
	q.sql.WriteString(") VALUES (")
	q.sql.WriteString(q.addArg(string(jsonData)))
	q.sql.WriteString(")")

	// RETURNING
	if len(i.returning) > 0 {
		q.sql.WriteString(" RETURNING ")
		for idx, col := range i.returning {
			if idx > 0 {
				q.sql.WriteString(", ")
			}
			q.sql.WriteString(col)
		}
	}

	return q.String(), q.Args(), nil
}

// Exec executes the insert
func (i *InsertQuery) Exec() (sql.Result, error) {
	return i.ExecContext(context.Background())
}

// ExecContext executes with context
func (i *InsertQuery) ExecContext(ctx context.Context) (sql.Result, error) {
	sql, args, err := i.Build()
	if err != nil {
		return nil, err
	}
	if i.tx != nil {
		return i.tx.ExecContext(ctx, sql, args...)
	}
	return i.db.ExecContext(ctx, sql, args...)
}

// Get executes and scans RETURNING into dest
func (i *InsertQuery) Get(dest any) error {
	return i.GetContext(context.Background(), dest)
}

// GetContext executes with context and scans
func (i *InsertQuery) GetContext(ctx context.Context, dest any) error {
	sql, args, err := i.Build()
	if err != nil {
		return err
	}
	if i.tx != nil {
		return i.tx.GetContext(ctx, dest, sql, args...)
	}
	return i.db.GetContext(ctx, dest, sql, args...)
}

// QueryRow returns a single row
func (i *InsertQuery) QueryRow() sqlx.SQLRow {
	return i.QueryRowContext(context.Background())
}

// QueryRowContext returns row with context
func (i *InsertQuery) QueryRowContext(ctx context.Context) sqlx.SQLRow {
	sql, args, err := i.Build()
	if err != nil {
		panic(err) // or handle differently
	}
	if i.tx != nil {
		return i.tx.QueryRowContext(ctx, sql, args...)
	}
	return i.db.QueryRowContext(ctx, sql, args...)
}

// BatchInsert helper for inserting multiple records
type BatchInsertQuery struct {
	db         *sqlx.DB
	tx         *sqlx.Tx
	columnName string

	table     string
	dataSlice []any
	returning []string
}

// BatchInsert creates a batch insert query
func (db *DB) BatchInsert(table string) *BatchInsertQuery {
	return &BatchInsertQuery{
		db:         db.DB,
		columnName: db.columnName,
		table:      table,
	}
}

// BatchInsert for transactions
func (tx *Tx) BatchInsert(table string) *BatchInsertQuery {
	return &BatchInsertQuery{
		tx:         tx.Tx,
		columnName: tx.columnName,
		table:      table,
	}
}

// Data sets the slice of data to insert
func (b *BatchInsertQuery) Data(dataSlice []any) *BatchInsertQuery {
	b.dataSlice = dataSlice
	return b
}

// Returning adds RETURNING clause
func (b *BatchInsertQuery) Returning(cols ...string) *BatchInsertQuery {
	b.returning = cols
	return b
}

// Build generates SQL and args
func (b *BatchInsertQuery) Build() (string, []any, error) {
	if len(b.dataSlice) == 0 {
		return "", nil, nil
	}

	q := &Query{}
	q.sql.WriteString("INSERT INTO ")
	q.sql.WriteString(b.table)
	q.sql.WriteString(" (")
	q.sql.WriteString(b.columnName)
	q.sql.WriteString(") VALUES ")

	for i, data := range b.dataSlice {
		if i > 0 {
			q.sql.WriteString(", ")
		}
		jsonData, err := json.Marshal(data)
		if err != nil {
			return "", nil, err
		}
		q.sql.WriteString("(")
		q.sql.WriteString(q.addArg(string(jsonData)))
		q.sql.WriteString(")")
	}

	// RETURNING
	if len(b.returning) > 0 {
		q.sql.WriteString(" RETURNING ")
		for idx, col := range b.returning {
			if idx > 0 {
				q.sql.WriteString(", ")
			}
			q.sql.WriteString(col)
		}
	}

	return q.String(), q.Args(), nil
}

// Exec executes the batch insert
func (b *BatchInsertQuery) Exec() (sql.Result, error) {
	return b.ExecContext(context.Background())
}

// ExecContext executes with context
func (b *BatchInsertQuery) ExecContext(ctx context.Context) (sql.Result, error) {
	sql, args, err := b.Build()
	if err != nil {
		return nil, err
	}
	if b.tx != nil {
		return b.tx.ExecContext(ctx, sql, args...)
	}
	return b.db.ExecContext(ctx, sql, args...)
}

// Select executes and scans RETURNING into dest
func (b *BatchInsertQuery) Select(dest any) error {
	return b.SelectContext(context.Background(), dest)
}

// SelectContext executes with context and scans
func (b *BatchInsertQuery) SelectContext(ctx context.Context, dest any) error {
	sql, args, err := b.Build()
	if err != nil {
		return err
	}
	if b.tx != nil {
		return b.tx.SelectContext(ctx, dest, sql, args...)
	}
	return b.db.SelectContext(ctx, dest, sql, args...)
}
