package jsonbq

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	sqlx "github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/drivers/postgres"
)

// DB wraps sqlx.DB with JSONB-specific helpers
type DB struct {
	*sqlx.DB
	columnName string // default JSONB column name
	encrypted  *encryptedModeConfig
}

// Tx wraps sqlx.Tx with JSONB-specific helpers
type Tx struct {
	*sqlx.Tx
	columnName string
	encrypted  *encryptedModeConfig
}

// NewDB creates a new DB wrapper
func NewDB(db *sqlx.DB, columnName string) *DB {
	if columnName == "" {
		columnName = "data" // default column name
	}
	return &DB{
		DB:         db,
		columnName: columnName,
	}
}

// MustOpen creates a DB connection or panics
func MustOpen(dataSourceName, columnName, id string) *DB {
	db := postgres.MustOpen(dataSourceName, id)
	return NewDB(db, columnName)
}

// Open creates a DB connection
func Open(dataSourceName, columnName, id string) (*DB, error) {
	db, err := postgres.Open(dataSourceName, id)
	if err != nil {
		return nil, err
	}
	return NewDB(db, columnName), nil
}

// BeginTxx starts a transaction with context
func (db *DB) BeginTxx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := db.DB.BeginTxx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Tx{Tx: tx, columnName: db.columnName, encrypted: db.encrypted}, nil
}

// Query creates a new query builder
func (db *DB) Query() *SelectQuery {
	return &SelectQuery{
		db:         db.DB,
		columnName: db.columnName,
		encrypted:  db.encrypted,
	}
}

// Insert creates a new insert builder
func (db *DB) Insert(table string) *InsertQuery {
	return &InsertQuery{
		db:         db.DB,
		table:      table,
		columnName: db.columnName,
		encrypted:  db.encrypted,
	}
}

// Update creates a new update builder
func (db *DB) Update(table string) *UpdateQuery {
	return &UpdateQuery{
		db:         db.DB,
		table:      table,
		columnName: db.columnName,
		encrypted:  db.encrypted,
	}
}

// Delete creates a new delete builder
func (db *DB) Delete(table string) *DeleteQuery {
	return &DeleteQuery{
		db:         db.DB,
		table:      table,
		columnName: db.columnName,
		encrypted:  db.encrypted,
	}
}

// Transaction methods
func (tx *Tx) Query() *SelectQuery {
	return &SelectQuery{
		tx:         tx.Tx,
		columnName: tx.columnName,
		encrypted:  tx.encrypted,
	}
}

func (tx *Tx) Insert(table string) *InsertQuery {
	return &InsertQuery{
		tx:         tx.Tx,
		table:      table,
		columnName: tx.columnName,
		encrypted:  tx.encrypted,
	}
}

func (tx *Tx) Update(table string) *UpdateQuery {
	return &UpdateQuery{
		tx:         tx.Tx,
		table:      table,
		columnName: tx.columnName,
		encrypted:  tx.encrypted,
	}
}

func (tx *Tx) Delete(table string) *DeleteQuery {
	return &DeleteQuery{
		tx:         tx.Tx,
		table:      table,
		columnName: tx.columnName,
		encrypted:  tx.encrypted,
	}
}

// Query represents a SQL query with args
type Query struct {
	sql  strings.Builder
	args []any
}

func (q *Query) String() string {
	return q.sql.String()
}

func (q *Query) Args() []any {
	return q.args
}

// addArg adds an argument and returns the placeholder
func (q *Query) addArg(val any) string {
	q.args = append(q.args, val)
	return fmt.Sprintf("$%d", len(q.args))
}

// writeColumn writes the JSONB column name
func (q *Query) writeColumn(columnName string) {
	q.sql.WriteString(columnName)
}

func (q *Query) writeStringLiteral(value string) {
	q.sql.WriteString("'")
	q.sql.WriteString(strings.ReplaceAll(value, "'", "''"))
	q.sql.WriteString("'")
}

func (q *Query) writeTextArrayLiteral(values []string) {
	q.sql.WriteString("ARRAY[")
	for i, value := range values {
		if i > 0 {
			q.sql.WriteString(", ")
		}
		q.writeStringLiteral(value)
	}
	q.sql.WriteString("]::text[]")
}

// Condition is anything that can be used in WHERE clause
type Condition interface {
	Build(*Query, string) // columnName passed for context
}

// Expr represents a JSONB expression
type Expr struct {
	build func(*Query, string)
}

func (e Expr) Build(q *Query, columnName string) {
	e.build(q, columnName)
}

// Common helper to marshal JSON values
func marshalJSON(val any) (any, error) {
	// If already a string or basic type, pass through
	switch val.(type) {
	case string, int, int64, float64, bool, nil:
		return val, nil
	}

	// Marshal complex types to JSON
	b, err := json.Marshal(val)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}
