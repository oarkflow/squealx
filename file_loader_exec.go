package squealx

import (
	"context"
	"database/sql"
	"time"
)

// QueryxContext runs a loaded query (or raw SQL) and returns rows.
func (f *FileLoader) QueryxContext(db *DB, ctx context.Context, query string, args ...any) (*Rows, error) {
	q, err := f.sqlFor(db, query)
	if err != nil {
		return nil, err
	}
	q, args, err = f.compileArgs(db, q, args)
	if err != nil {
		return nil, err
	}
	return db.QueryxContext(ctx, q, args...)
}

// QueryRowxContext runs a loaded query (or raw SQL) and returns a single row.
func (f *FileLoader) QueryRowxContext(db *DB, ctx context.Context, query string, args ...any) *Row {
	q, err := f.sqlFor(db, query)
	if err != nil {
		return NewErrorRow(err)
	}
	q, args, err = f.compileArgs(db, q, args)
	if err != nil {
		return NewErrorRow(err)
	}
	return db.QueryRowxContext(ctx, q, args...)
}

// GetContext runs a loaded query (or raw SQL) and scans one row into dest.
func (f *FileLoader) GetContext(db *DB, ctx context.Context, dest any, sql string, args ...any) error {
	q, err := f.sqlFor(db, sql)
	if err != nil {
		return err
	}
	q, args, err = f.compileArgs(db, q, args)
	if err != nil {
		return err
	}
	return db.GetContext(ctx, dest, q, args...)
}

// SelectContext runs a loaded query (or raw SQL) and scans rows into dest.
func (f *FileLoader) SelectContext(db *DB, ctx context.Context, dest any, sql string, args ...any) error {
	q, err := f.sqlFor(db, sql)
	if err != nil {
		return err
	}
	q, args, err = f.compileArgs(db, q, args)
	if err != nil {
		return err
	}
	return db.SelectContext(ctx, dest, q, args...)
}

// Get runs a loaded query (or raw SQL) and scans one row into dest.
func (f *FileLoader) Get(db *DB, dest any, sql string, args ...any) error {
	q, err := f.sqlFor(db, sql)
	if err != nil {
		return err
	}
	q, args, err = f.compileArgs(db, q, args)
	if err != nil {
		return err
	}
	return db.Get(dest, q, args...)
}

// Select runs a loaded query (or raw SQL) and scans rows into dest.
func (f *FileLoader) Select(db *DB, dest any, sql string, args ...any) error {
	q, err := f.sqlFor(db, sql)
	if err != nil {
		return err
	}
	q, args, err = f.compileArgs(db, q, args)
	if err != nil {
		return err
	}
	return db.Select(dest, q, args...)
}

// MustExecContext executes a loaded query (or raw SQL). It panics in strict
// mode when the query name is unknown.
func (f *FileLoader) MustExecContext(db *DB, ctx context.Context, sql string, args ...any) sql.Result {
	q, err := f.sqlFor(db, sql)
	if err != nil {
		panic(err)
	}
	q, args, err = f.compileArgs(db, q, args)
	if err != nil {
		panic(err)
	}
	return db.MustExecContext(ctx, q, args...)
}

// MustExec executes a loaded query (or raw SQL). It panics in strict mode
// when the query name is unknown.
func (f *FileLoader) MustExec(db *DB, sql string, args ...any) sql.Result {
	q, err := f.sqlFor(db, sql)
	if err != nil {
		panic(err)
	}
	q, args, err = f.compileArgs(db, q, args)
	if err != nil {
		panic(err)
	}
	return db.MustExec(q, args...)
}

// Exec executes a loaded query (or raw SQL).
func (f *FileLoader) Exec(db *DB, query string, args ...any) (sql.Result, error) {
	q, err := f.sqlFor(db, query)
	if err != nil {
		return nil, err
	}
	q, args, err = f.compileArgs(db, q, args)
	if err != nil {
		return nil, err
	}
	return db.Exec(q, args...)
}

// ExecContext executes a loaded query (or raw SQL).
func (f *FileLoader) ExecContext(db *DB, ctx context.Context, query string, args ...any) (sql.Result, error) {
	q, err := f.sqlFor(db, query)
	if err != nil {
		return nil, err
	}
	q, args, err = f.compileArgs(db, q, args)
	if err != nil {
		return nil, err
	}
	return db.ExecContext(ctx, q, args...)
}

// Preparex prepares a loaded query (or raw SQL). With WithStmtCache enabled,
// prepared handles for named queries are cached and reused until the
// underlying SQL changes.
func (f *FileLoader) Preparex(db *DB, sql string) (*Stmt, error) {
	q, err := f.resolveQuery(sql)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return db.Preparex(sql)
	}
	if f.opts.stmtCache {
		stmt, _, err := f.cachedPrepare(db, q, false)
		return stmt, err
	}
	return db.Preparex(q.Query)
}

// PreparexContext prepares a loaded query (or raw SQL), reusing cached
// statement handles when WithStmtCache is enabled.
func (f *FileLoader) PreparexContext(db *DB, ctx context.Context, sql string) (*Stmt, error) {
	q, err := f.resolveQuery(sql)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return db.PreparexContext(ctx, sql)
	}
	if f.opts.stmtCache {
		stmt, _, err := f.cachedPrepare(db, q, false)
		return stmt, err
	}
	return db.PreparexContext(ctx, q.Query)
}

// Prepare prepares a loaded query (or raw SQL).
func (f *FileLoader) Prepare(db *DB, query string) (SQLStmt, error) {
	q, err := f.sqlFor(db, query)
	if err != nil {
		return nil, err
	}
	return db.Prepare(q)
}

// PrepareContext prepares a loaded query (or raw SQL).
func (f *FileLoader) PrepareContext(db *DB, ctx context.Context, query string) (SQLStmt, error) {
	q, err := f.sqlFor(db, query)
	if err != nil {
		return nil, err
	}
	return db.PrepareContext(ctx, q)
}

// PrepareNamed prepares a named statement for a loaded query (or raw SQL),
// reusing cached statement handles when WithStmtCache is enabled.
func (f *FileLoader) PrepareNamed(db *DB, sql string) (*NamedStmt, error) {
	q, err := f.resolveQuery(sql)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return db.PrepareNamed(sql)
	}
	if f.opts.stmtCache {
		_, named, err := f.cachedPrepare(db, q, true)
		return named, err
	}
	return db.PrepareNamed(q.Query)
}

// PrepareNamedContext prepares a named statement for a loaded query (or raw
// SQL), reusing cached statement handles when WithStmtCache is enabled.
func (f *FileLoader) PrepareNamedContext(db *DB, ctx context.Context, sql string) (*NamedStmt, error) {
	q, err := f.resolveQuery(sql)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return db.PrepareNamedContext(ctx, sql)
	}
	if f.opts.stmtCache {
		_, named, err := f.cachedPrepare(db, q, true)
		return named, err
	}
	return db.PrepareNamedContext(ctx, q.Query)
}

// NamedExec executes a loaded query (or raw SQL) with named arguments.
func (f *FileLoader) NamedExec(db *DB, sql string, args any) (sql.Result, error) {
	q, err := f.sqlFor(db, sql)
	if err != nil {
		return nil, err
	}
	return db.NamedExec(q, args)
}

// NamedExecContext executes a loaded query (or raw SQL) with named arguments.
func (f *FileLoader) NamedExecContext(db *DB, ctx context.Context, sql string, args any) (sql.Result, error) {
	q, err := f.sqlFor(db, sql)
	if err != nil {
		return nil, err
	}
	return db.NamedExecContext(ctx, q, args)
}

// NamedQuery runs a loaded query (or raw SQL) with named arguments. When the
// query declares a `-- connection:` key it must match the database ID.
func (f *FileLoader) NamedQuery(db *DB, sql string, args any) (*Rows, error) {
	q, err := f.resolveQuery(sql)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return db.NamedQuery(sql, args)
	}
	if err := connectionError(db, q); err != nil {
		return nil, err
	}
	return db.NamedQuery(q.Query, args)
}

// NamedQueryContext runs a loaded query (or raw SQL) with named arguments.
// When the query declares a `-- connection:` key it must match the database ID.
func (f *FileLoader) NamedQueryContext(db *DB, ctx context.Context, sql string, args any) (*Rows, error) {
	q, err := f.resolveQuery(sql)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return db.NamedQueryContext(ctx, sql, args)
	}
	if err := connectionError(db, q); err != nil {
		return nil, err
	}
	return db.NamedQueryContext(ctx, q.Query, args)
}

// NamedSelect runs a loaded query (or raw SQL) with named arguments and
// scans rows into dest.
func (f *FileLoader) NamedSelect(db *DB, dest any, sql string, args any) error {
	q, err := f.sqlFor(db, sql)
	if err != nil {
		return err
	}
	return db.NamedSelect(dest, q, args)
}

// NamedGet runs a loaded query (or raw SQL) with named arguments and scans
// one row into dest.
func (f *FileLoader) NamedGet(db *DB, dest any, sql string, args any) error {
	q, err := f.sqlFor(db, sql)
	if err != nil {
		return err
	}
	return db.NamedGet(dest, q, args)
}

// InGet runs a loaded query (or raw SQL) with slice arguments and scans one
// row into dest.
func (f *FileLoader) InGet(db *DB, dest any, sql string, args ...any) error {
	q, err := f.sqlFor(db, sql)
	if err != nil {
		return err
	}
	return db.InGet(dest, q, args...)
}

// InSelect runs a loaded query (or raw SQL) with slice arguments and scans
// rows into dest.
func (f *FileLoader) InSelect(db *DB, dest any, sql string, args ...any) error {
	q, err := f.sqlFor(db, sql)
	if err != nil {
		return err
	}
	return db.InSelect(dest, q, args...)
}

// InExec executes a loaded query (or raw SQL) with slice arguments.
func (f *FileLoader) InExec(db *DB, sql string, args ...any) (sql.Result, error) {
	q, err := f.sqlFor(db, sql)
	if err != nil {
		return nil, err
	}
	return db.InExec(q, args...)
}

// MustInExec executes a loaded query (or raw SQL) with slice arguments. It
// panics in strict mode when the query name is unknown.
func (f *FileLoader) MustInExec(db *DB, sql string, args ...any) sql.Result {
	q, err := f.sqlFor(db, sql)
	if err != nil {
		panic(err)
	}
	return db.MustInExec(q, args...)
}

// Queryx runs a loaded query (or raw SQL) and returns rows.
func (f *FileLoader) Queryx(db *DB, query string, args ...any) (*Rows, error) {
	q, err := f.sqlFor(db, query)
	if err != nil {
		return nil, err
	}
	q, args, err = f.compileArgs(db, q, args)
	if err != nil {
		return nil, err
	}
	return db.Queryx(q, args...)
}

// QueryRowx runs a loaded query (or raw SQL) and returns a single row.
func (f *FileLoader) QueryRowx(db *DB, query string, args ...any) *Row {
	q, err := f.sqlFor(db, query)
	if err != nil {
		return NewErrorRow(err)
	}
	q, args, err = f.compileArgs(db, q, args)
	if err != nil {
		return NewErrorRow(err)
	}
	return db.QueryRowx(q, args...)
}

// Query runs a loaded query (or raw SQL) and returns rows.
func (f *FileLoader) Query(db *DB, query string, args ...any) (SQLRows, error) {
	q, err := f.sqlFor(db, query)
	if err != nil {
		return nil, err
	}
	q, args, err = f.compileArgs(db, q, args)
	if err != nil {
		return nil, err
	}
	return db.Query(q, args...)
}

// QueryContext runs a loaded query (or raw SQL) and returns rows.
func (f *FileLoader) QueryContext(db *DB, ctx context.Context, query string, args ...any) (SQLRows, error) {
	q, err := f.sqlFor(db, query)
	if err != nil {
		return nil, err
	}
	q, args, err = f.compileArgs(db, q, args)
	if err != nil {
		return nil, err
	}
	return db.QueryContext(ctx, q, args...)
}

// QueryRow runs a loaded query (or raw SQL) and returns a single row.
func (f *FileLoader) QueryRow(db *DB, query string, args ...any) SQLRow {
	q, err := f.sqlFor(db, query)
	if err != nil {
		return NewErrorRow(err)
	}
	q, args, err = f.compileArgs(db, q, args)
	if err != nil {
		return NewErrorRow(err)
	}
	return db.QueryRow(q, args...)
}

// QueryRowContext runs a loaded query (or raw SQL) and returns a single row.
func (f *FileLoader) QueryRowContext(db *DB, ctx context.Context, query string, args ...any) SQLRow {
	q, err := f.sqlFor(db, query)
	if err != nil {
		return NewErrorRow(err)
	}
	q, args, err = f.compileArgs(db, q, args)
	if err != nil {
		return NewErrorRow(err)
	}
	return db.QueryRowContext(ctx, q, args...)
}

// SetConnMaxLifetime forwards to the given database.
func (f *FileLoader) SetConnMaxLifetime(db *DB, duration time.Duration) {
	db.SetConnMaxLifetime(duration)
}

// SetConnMaxIdleTime forwards to the given database.
func (f *FileLoader) SetConnMaxIdleTime(db *DB, duration time.Duration) {
	db.SetConnMaxIdleTime(duration)
}

// SetMaxIdleConns forwards to the given database.
func (f *FileLoader) SetMaxIdleConns(db *DB, maxIdleConns int) {
	db.SetMaxIdleConns(maxIdleConns)
}

// SetMaxOpenConns forwards to the given database.
func (f *FileLoader) SetMaxOpenConns(db *DB, maxOpenConns int) {
	db.SetMaxOpenConns(maxOpenConns)
}

// Stats forwards to the given database.
func (f *FileLoader) Stats(db *DB) sql.DBStats {
	return db.Stats()
}

// Ping forwards to the given database.
func (f *FileLoader) Ping(db *DB) error {
	return db.Ping()
}

// PingContext forwards to the given database.
func (f *FileLoader) PingContext(db *DB, ctx context.Context) error {
	return db.PingContext(ctx)
}

// Begin forwards to the given database.
func (f *FileLoader) Begin(db *DB) (SQLTx, error) {
	return db.Begin()
}

// BeginTx forwards to the given database.
func (f *FileLoader) BeginTx(db *DB, ctx context.Context, opts *sql.TxOptions) (SQLTx, error) {
	return db.BeginTx(ctx, opts)
}

// Conn forwards to the given database.
func (f *FileLoader) Conn(db *DB, ctx context.Context) (SQLConn, error) {
	return db.Conn(ctx)
}

// Close closes the given database.
func (f *FileLoader) Close(db *DB) error {
	return db.Close()
}
