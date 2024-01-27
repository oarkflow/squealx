package squealx

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"
	"time"
)

type SQLDB interface {
	Query(query string, args ...any) (SQLRows, error)
	QueryContext(ctx context.Context, query string, args ...any) (SQLRows, error)
	QueryRow(query string, args ...any) SQLRow
	Driver() driver.Driver
	SetConnMaxLifetime(d time.Duration)
	SetConnMaxIdleTime(d time.Duration)
	SetMaxIdleConns(n int)
	SetMaxOpenConns(n int)
	Stats() sql.DBStats
	QueryRowContext(ctx context.Context, query string, args ...any) SQLRow
	Exec(query string, args ...any) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Prepare(query string) (SQLStmt, error)
	PrepareContext(ctx context.Context, query string) (SQLStmt, error)
	Ping() error
	PingContext(ctx context.Context) error
	Begin() (SQLTx, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (SQLTx, error)
	Conn(ctx context.Context) (SQLConn, error)
	Close() error
}

type SQLColumnType interface {
	Name() string
	Length() (length int64, ok bool)
	DecimalSize() (precision, scale int64, ok bool)
	ScanType() reflect.Type
	Nullable() (nullable, ok bool)
	DatabaseTypeName() string
}

type SQLRow interface {
	Err() error
	Scan(dest ...any) error
}

type SQLRows interface {
	SQLRow
	ColumnTypes() ([]*sql.ColumnType, error)
	Columns() ([]string, error)
	Close() error
	Next() bool
}

type SQLStmt interface {
	Close() error
	Query(args ...any) (SQLRows, error)
	QueryRow(args ...any) SQLRow
	QueryContext(ctx context.Context, args ...any) (SQLRows, error)
	QueryRowContext(ctx context.Context, args ...any) SQLRow
	Exec(args ...any) (sql.Result, error)
	ExecContext(ctx context.Context, args ...any) (sql.Result, error)
}

type SQLTx interface {
	Commit() error
	Rollback() error
	Stmt(stmt SQLStmt) SQLStmt
	StmtContext(ctx context.Context, stmt SQLStmt) SQLStmt
	Query(query string, args ...any) (SQLRows, error)
	QueryContext(ctx context.Context, query string, args ...any) (SQLRows, error)
	QueryRow(query string, args ...any) SQLRow
	QueryRowContext(ctx context.Context, query string, args ...any) SQLRow
	Exec(query string, args ...any) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Prepare(query string) (SQLStmt, error)
	PrepareContext(ctx context.Context, query string) (SQLStmt, error)
}

type SQLConn interface {
	Close() error
	BeginTx(ctx context.Context, opts *sql.TxOptions) (SQLTx, error)
	QueryContext(ctx context.Context, query string, args ...any) (SQLRows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) SQLRow
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	PrepareContext(ctx context.Context, query string) (SQLStmt, error)
}
