package squealx

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"time"
)

type sqlDBWrapper struct {
	db *sql.DB
}

func WrapSQLDB(db *sql.DB) SQLDB {
	return &sqlDBWrapper{db: db}
}

func (s *sqlDBWrapper) Driver() driver.Driver {
	return s.db.Driver()
}

func (s *sqlDBWrapper) Stats() sql.DBStats {
	return s.db.Stats()
}

func (s *sqlDBWrapper) SetConnMaxLifetime(d time.Duration) {
	s.db.SetConnMaxLifetime(d)
}

func (s *sqlDBWrapper) SetConnMaxIdleTime(d time.Duration) {
	s.db.SetConnMaxIdleTime(d)
}

func (s *sqlDBWrapper) SetMaxIdleConns(n int) {
	s.db.SetMaxIdleConns(n)
}

func (s *sqlDBWrapper) SetMaxOpenConns(n int) {
	s.db.SetMaxOpenConns(n)
}

func (s *sqlDBWrapper) Query(query string, args ...any) (SQLRows, error) {
	return s.db.Query(query, args...)
}

func (s *sqlDBWrapper) QueryContext(ctx context.Context, query string, args ...any) (SQLRows, error) {
	return s.db.QueryContext(ctx, query, args...)
}

func (s *sqlDBWrapper) QueryRow(query string, args ...any) SQLRow {
	return s.db.QueryRow(query, args...)
}

func (s *sqlDBWrapper) QueryRowContext(ctx context.Context, query string, args ...any) SQLRow {
	return s.db.QueryRowContext(ctx, query, args...)
}

func (s *sqlDBWrapper) Exec(query string, args ...any) (sql.Result, error) {
	return s.db.Exec(query, args...)
}

func (s *sqlDBWrapper) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return s.db.ExecContext(ctx, query, args...)
}

func (s *sqlDBWrapper) Prepare(query string) (SQLStmt, error) {
	stmt, err := s.db.Prepare(query)
	if err != nil {
		return nil, err
	}

	return &sqlStmtWrapper{stmt: stmt}, nil
}

func (s *sqlDBWrapper) PrepareContext(ctx context.Context, query string) (SQLStmt, error) {
	stmt, err := s.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	return &sqlStmtWrapper{stmt: stmt}, nil
}

func (s *sqlDBWrapper) Ping() error {
	return s.db.Ping()
}

func (s *sqlDBWrapper) PingContext(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func (s *sqlDBWrapper) Begin() (SQLTx, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}

	return &sqlTxWrapper{tx: tx}, nil
}

func (s *sqlDBWrapper) BeginTx(ctx context.Context, opts *sql.TxOptions) (SQLTx, error) {
	tx, err := s.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &sqlTxWrapper{tx: tx}, nil
}

func (s *sqlDBWrapper) Conn(ctx context.Context) (SQLConn, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, err
	}

	return &sqlConnWrapper{conn: conn}, nil
}

func (s *sqlDBWrapper) Close() error {
	return s.db.Close()
}

type sqlStmtWrapper struct {
	stmt *sql.Stmt
}

func (s *sqlStmtWrapper) Close() error {
	return s.stmt.Close()
}

func (s *sqlStmtWrapper) Query(args ...any) (SQLRows, error) {
	return s.stmt.Query(args...)
}

func (s *sqlStmtWrapper) QueryContext(ctx context.Context, args ...any) (SQLRows, error) {
	return s.stmt.QueryContext(ctx, args...)
}

func (s *sqlStmtWrapper) QueryRow(args ...any) SQLRow {
	return s.stmt.QueryRow(args...)
}

func (s *sqlStmtWrapper) QueryRowContext(ctx context.Context, args ...any) SQLRow {
	return s.stmt.QueryRowContext(ctx, args...)
}

func (s *sqlStmtWrapper) Exec(args ...any) (sql.Result, error) {
	return s.stmt.Exec(args...)
}

func (s *sqlStmtWrapper) ExecContext(ctx context.Context, args ...any) (sql.Result, error) {
	return s.stmt.ExecContext(ctx, args...)
}

type sqlTxWrapper struct {
	tx *sql.Tx
}

func (s *sqlTxWrapper) Commit() error {
	return s.tx.Commit()
}

func (s sqlTxWrapper) Rollback() error {
	return s.tx.Rollback()
}

func (s *sqlTxWrapper) Stmt(stmt SQLStmt) SQLStmt {
	if sqls, ok := stmt.(*sqlStmtWrapper); ok {
		return &sqlStmtWrapper{stmt: s.tx.Stmt(sqls.stmt)}
	}
	return stmt
}

func (s *sqlTxWrapper) StmtContext(ctx context.Context, stmt SQLStmt) SQLStmt {
	if sqls, ok := stmt.(*sqlStmtWrapper); ok {
		return &sqlStmtWrapper{stmt: s.tx.StmtContext(ctx, sqls.stmt)}
	}
	return stmt
}

func (s *sqlTxWrapper) Query(query string, args ...any) (SQLRows, error) {
	return s.tx.Query(query, args...)
}

func (s *sqlTxWrapper) QueryContext(ctx context.Context, query string, args ...any) (SQLRows, error) {
	return s.tx.QueryContext(ctx, query, args...)
}

func (s *sqlTxWrapper) QueryRow(query string, args ...any) SQLRow {
	return s.tx.QueryRow(query, args...)
}

func (s *sqlTxWrapper) QueryRowContext(ctx context.Context, query string, args ...any) SQLRow {
	return s.tx.QueryRowContext(ctx, query, args...)
}

func (s *sqlTxWrapper) Exec(query string, args ...any) (sql.Result, error) {
	return s.tx.Exec(query, args...)
}

func (s *sqlTxWrapper) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return s.tx.ExecContext(ctx, query, args...)
}

func (s *sqlTxWrapper) Prepare(query string) (SQLStmt, error) {
	stmt, err := s.tx.Prepare(query)
	if err != nil {
		return nil, err
	}

	return &sqlStmtWrapper{stmt: stmt}, nil
}

func (s *sqlTxWrapper) PrepareContext(ctx context.Context, query string) (SQLStmt, error) {
	stmt, err := s.tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	return &sqlStmtWrapper{stmt: stmt}, nil
}

type sqlConnWrapper struct {
	conn *sql.Conn
}

func (s *sqlConnWrapper) Close() error {
	return s.conn.Close()
}

func (s *sqlConnWrapper) BeginTx(ctx context.Context, opts *sql.TxOptions) (SQLTx, error) {
	tx, err := s.conn.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &sqlTxWrapper{tx: tx}, nil
}

func (s *sqlConnWrapper) QueryContext(ctx context.Context, query string, args ...any) (SQLRows, error) {
	return s.conn.QueryContext(ctx, query, args...)
}

func (s *sqlConnWrapper) QueryRowContext(ctx context.Context, query string, args ...any) SQLRow {
	return s.conn.QueryRowContext(ctx, query, args...)
}

func (s *sqlConnWrapper) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return s.conn.ExecContext(ctx, query, args...)
}

func (s *sqlConnWrapper) PrepareContext(ctx context.Context, query string) (SQLStmt, error) {
	stmt, err := s.conn.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	return &sqlStmtWrapper{stmt: stmt}, nil
}
