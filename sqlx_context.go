package squealx

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
)

// ConnectContext to a database and verify with a ping.
func ConnectContext(ctx context.Context, driverName, dataSourceName, id string) (*DB, error) {
	db, err := Open(driverName, dataSourceName, id)
	if err != nil {
		return db, err
	}
	err = db.PingContext(ctx)
	return db, err
}

// QueryerContext is an interface used by GetContext and SelectContext
type QueryerContext interface {
	QueryContext(ctx context.Context, query string, args ...any) (SQLRows, error)
	QueryxContext(ctx context.Context, query string, args ...any) (*Rows, error)
	QueryRowxContext(ctx context.Context, query string, args ...any) *Row
}

// PreparerContext is an interface used by PreparexContext.
type PreparerContext interface {
	PrepareContext(ctx context.Context, query string) (SQLStmt, error)
}

// ExecerContext is an interface used by MustExecContext and LoadFileContext
type ExecerContext interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// ExtContext is a union interface which can bind, query, and exec, with Context
// used by NamedQueryContext and NamedExecContext.
type ExtContext interface {
	binder
	QueryerContext
	ExecerContext
}

// SelectContext executes a query using the provided Queryer, and StructScans
// each row into dest, which must be a slice.  If the slice elements are
// scannable, then the result set must have only one column.  Otherwise,
// StructScan is used. The *sql.Rows are closed automatically.
// Any placeholder parameters are replaced with supplied args.
func SelectContext(ctx context.Context, q QueryerContext, dest any, query string, args ...any) error {
	rows, err := q.QueryxContext(ctx, query, args...)
	if err != nil {
		return err
	}
	// if something happens here, we want to make sure the rows are Closed
	defer rows.Close()
	return ScannAll(rows, dest, false)
}

// PreparexContext prepares a statement.
//
// The provided context is used for the preparation of the statement, not for
// the execution of the statement.
func PreparexContext(ctx context.Context, p PreparerContext, query string) (*Stmt, error) {
	s, err := p.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return &Stmt{SQLStmt: s, unsafe: isUnsafe(p), Mapper: mapperFor(p)}, err
}

// GetContext does a QueryRow using the provided Queryer, and scans the
// resulting row to dest.  If dest is scannable, the result must only have one
// column. Otherwise, StructScan is used.  Get will return sql.ErrNoRows like
// row.Scan would. Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func GetContext(ctx context.Context, q QueryerContext, dest any, query string, args ...any) error {
	r := q.QueryRowxContext(ctx, query, args...)
	return r.scanAny(dest, false)
}

// LoadFileContext exec's every statement in a file (as a single call to Exec).
// LoadFileContext may return a nil *sql.Result if errors are encountered
// locating or reading the file at path.  LoadFile reads the entire file into
// memory, so it is not suitable for loading large data dumps, but can be useful
// for initializing schemas or loading indexes.
//
// FIXME: this does not really work with multi-statement files for mattn/go-sqlite3
// or the go-mysql-driver/mysql drivers;  pq seems to be an exception here.  Detecting
// this by requiring something with DriverName() and then attempting to split the
// queries will be difficult to get right, and its current driver-specific behavior
// is deemed at least not complex in its incorrectness.
func LoadFileContext(ctx context.Context, e ExecerContext, path string) (*sql.Result, error) {
	realpath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	contents, err := os.ReadFile(realpath)
	if err != nil {
		return nil, err
	}
	res, err := e.ExecContext(ctx, string(contents))
	return &res, err
}

// MustExecContext execs the query using e and panics if there was an error.
// Any placeholder parameters are replaced with supplied args.
func MustExecContext(ctx context.Context, e ExecerContext, query string, args ...any) sql.Result {
	res, err := e.ExecContext(ctx, query, args...)
	if err != nil {
		panic(err)
	}
	return res
}

// PrepareNamedContext returns an sqlx.NamedStmt
func (db *DB) PrepareNamedContext(ctx context.Context, query string) (*NamedStmt, error) {
	return prepareNamedContext(ctx, db, query)
}

// NamedQueryContext using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (db *DB) NamedQueryContext(ctx context.Context, query string, arg any) (*Rows, error) {
	return NamedQueryContext(ctx, db, query, arg)
}

// NamedExecContext using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (db *DB) NamedExecContext(ctx context.Context, query string, arg any) (sql.Result, error) {
	fn := func() (sql.Result, error) {
		return NamedExecContext(ctx, db, query, arg)
	}
	return handleTwo[sql.Result](fn, db, context.Background(), query, arg)
}

// SelectContext using this DB.
// Any placeholder parameters are replaced with supplied args.
func (db *DB) SelectContext(ctx context.Context, dest any, query string, args ...any) error {
	return SelectContext(ctx, db, dest, query, args...)
}

// GetContext using this DB.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (db *DB) GetContext(ctx context.Context, dest any, query string, args ...any) error {
	return GetContext(ctx, db, dest, query, args...)
}

// PreparexContext returns an sqlx.Stmt instead of a sql.Stmt.
//
// The provided context is used for the preparation of the statement, not for
// the execution of the statement.
func (db *DB) PreparexContext(ctx context.Context, query string) (*Stmt, error) {
	return PreparexContext(ctx, db, query)
}

// QueryxContext queries the database and returns an *sqlx.Rows.
// Any placeholder parameters are replaced with supplied args.
func (db *DB) QueryxContext(ctx context.Context, query string, args ...any) (*Rows, error) {
	fn := func() (*Rows, error) {
		r, err := db.SQLDB.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		return &Rows{SQLRows: r, unsafe: db.unsafe, Mapper: db.Mapper}, err
	}
	return handleTwo[*Rows](fn, db, context.Background(), query, args...)
}

// QueryRowxContext queries the database and returns an *sqlx.Row.
// Any placeholder parameters are replaced with supplied args.
func (db *DB) QueryRowxContext(ctx context.Context, query string, args ...any) *Row {
	fn := func() (*Row, error) {
		rows, err := db.SQLDB.QueryContext(ctx, query, args...)
		return &Row{rows: rows, err: err, unsafe: db.unsafe, Mapper: db.Mapper}, err
	}
	rows, _ := handleTwo[*Row](fn, db, context.Background(), query, args...)
	return rows
}

// TransactionTx txWrapper use sql.Tx
func (db *DB) TransactionTx(ctx context.Context, opts *sql.TxOptions, fn func(tx SQLTx) error) error {
	tx, err := db.SQLDB.BeginTx(ctx, opts)
	if err != nil {
		return err
	}
	if err = fn(tx); err == nil {
		return tx.Commit()
	}
	if _err := tx.Rollback(); _err == nil {
		return err
	} else {
		return fmt.Errorf("rollBack err: %w original err:%w", _err, err)
	}
}

// TransactionTxx txWrapper use sqlx.Tx
func (db *DB) TransactionTxx(ctx context.Context, opts *sql.TxOptions, fn func(tx *Tx) error) error {
	txx, err := db.BeginTxx(ctx, opts)
	if err != nil {
		return err
	}
	if err = fn(txx); err == nil {
		return txx.Commit()
	}
	if _err := txx.Rollback(); _err == nil {
		return err
	} else {
		return fmt.Errorf("rollBack err: %w original err:%w", _err, err)
	}
}

// MustBeginTx starts a transaction, and panics on error.  Returns an *sqlx.Tx instead
// of an *sql.Tx.
//
// The provided context is used until the transaction is committed or rolled
// back. If the context is canceled, the sql package will roll back the
// transaction. Tx.Commit will return an error if the context provided to
// MustBeginContext is canceled.
func (db *DB) MustBeginTx(ctx context.Context, opts *sql.TxOptions) *Tx {
	tx, err := db.BeginTxx(ctx, opts)
	if err != nil {
		panic(err)
	}
	return tx
}

// MustExecContext (panic) runs MustExec using this database.
// Any placeholder parameters are replaced with supplied args.
func (db *DB) MustExecContext(ctx context.Context, query string, args ...any) sql.Result {
	fn := func() (sql.Result, error) {
		return MustExecContext(ctx, db, query, args...), nil
	}
	rows, _ := handleTwo[sql.Result](fn, db, context.Background(), query, args...)
	return rows
}

// BeginTxx begins a transaction and returns an *sqlx.Tx instead of an
// *sql.Tx.
//
// The provided context is used until the transaction is committed or rolled
// back. If the context is canceled, the sql package will roll back the
// transaction. Tx.Commit will return an error if the context provided to
// BeginxContext is canceled.
func (db *DB) BeginTxx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := db.SQLDB.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Tx{SQLTx: tx, driverName: db.driverName, unsafe: db.unsafe, Mapper: db.Mapper}, err
}

// Connx returns an *sqlx.Conn instead of an *sql.Conn.
func (db *DB) Connx(ctx context.Context) (*Conn, error) {
	conn, err := db.SQLDB.Conn(ctx)
	if err != nil {
		return nil, err
	}

	return &Conn{SQLConn: conn, driverName: db.driverName, unsafe: db.unsafe, Mapper: db.Mapper}, nil
}

// BeginTxx begins a transaction and returns an *sqlx.Tx instead of an
// *sql.Tx.
//
// The provided context is used until the transaction is committed or rolled
// back. If the context is canceled, the sql package will roll back the
// transaction. Tx.Commit will return an error if the context provided to
// BeginxContext is canceled.
func (c *Conn) BeginTxx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := c.SQLConn.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Tx{SQLTx: tx, driverName: c.driverName, unsafe: c.unsafe, Mapper: c.Mapper}, err
}

// With starts a transaction and do the give handle.
//
// The provided context is used until the transaction is committed or rolled back.
// If the context is canceled, the sql package will roll back
// the transaction. Tx.Commit will return an error if the context provided to
// BeginTx is canceled.
//
// The provided TxOptions is optional and may be nil if defaults should be used.
// If a non-default isolation level is used that the driver doesn't support,
// an error will be returned.
func (c *Conn) With(handle func(tx SQLTx) error) error {
	tx, err := c.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = handle(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// WithTx starts a transaction and do the give handle.
//
// The provided context is used until the transaction is committed or rolled back.
// If the context is canceled, the sql package will roll back
// the transaction. Tx.Commit will return an error if the context provided to
// BeginTx is canceled.
//
// The provided TxOptions is optional and may be nil if defaults should be used.
// If a non-default isolation level is used that the driver doesn't support,
// an error will be returned.
func (c *Conn) WithTx(ctx context.Context, opts *sql.TxOptions, handle func(tx SQLTx) error) error {
	tx, err := c.BeginTx(ctx, opts)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = handle(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// Withx begins a transaction and returns an *sqlx.Tx instead of an
// *sql.Tx and do the give handle.
//
// The provided context is used until the transaction is committed or rolled
// back. If the context is canceled, the sql package will roll back the
// transaction. Tx.Commit will return an error if the context provided to
// BeginxContext is canceled.
func (c *Conn) Withx(handle func(tx *Tx) error) error {
	tx, err := c.BeginTxx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = handle(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// WithTxx begins a transaction and returns an *sqlx.Tx instead of an
// *sql.Tx and do the give handle.
//
// The provided context is used until the transaction is committed or rolled
// back. If the context is canceled, the sql package will roll back the
// transaction. Tx.Commit will return an error if the context provided to
// BeginxContext is canceled.
func (c *Conn) WithTxx(ctx context.Context, opts *sql.TxOptions, handle func(tx *Tx) error) error {
	tx, err := c.BeginTxx(ctx, opts)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = handle(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// SelectContext using this Conn.
// Any placeholder parameters are replaced with supplied args.
func (c *Conn) SelectContext(ctx context.Context, dest any, query string, args ...any) error {
	return SelectContext(ctx, c, dest, query, args...)
}

func (tx *Tx) NamedQueryContext(ctx context.Context, query string, arg any) (*Rows, error) {
	return NamedQueryContext(ctx, tx, query, arg)
}

// GetContext using this Conn.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (c *Conn) GetContext(ctx context.Context, dest any, query string, args ...any) error {
	return GetContext(ctx, c, dest, query, args...)
}

// PreparexContext returns an sqlx.Stmt instead of a sql.Stmt.
//
// The provided context is used for the preparation of the statement, not for
// the execution of the statement.
func (c *Conn) PreparexContext(ctx context.Context, query string) (*Stmt, error) {
	return PreparexContext(ctx, c, query)
}

// QueryxContext queries the database and returns an *sqlx.Rows.
// Any placeholder parameters are replaced with supplied args.
func (c *Conn) QueryxContext(ctx context.Context, query string, args ...any) (*Rows, error) {
	r, err := c.SQLConn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &Rows{SQLRows: r, unsafe: c.unsafe, Mapper: c.Mapper}, err
}

// QueryRowxContext queries the database and returns an *sqlx.Row.
// Any placeholder parameters are replaced with supplied args.
func (c *Conn) QueryRowxContext(ctx context.Context, query string, args ...any) *Row {
	rows, err := c.SQLConn.QueryContext(ctx, query, args...)
	return &Row{rows: rows, err: err, unsafe: c.unsafe, Mapper: c.Mapper}
}

// Rebind a query within a Conn's bindvar type.
func (c *Conn) Rebind(query string) string {
	return Rebind(BindType(c.driverName), query)
}

// StmtxContext returns a version of the prepared statement which runs within a
// transaction. Provided stmt can be either *sql.Stmt or *sqlx.Stmt.
func (tx *Tx) StmtxContext(ctx context.Context, stmt any) *Stmt {
	var s SQLStmt
	switch v := stmt.(type) {
	case Stmt:
		s = v.SQLStmt
	case *Stmt:
		s = v.SQLStmt
	case *sql.Stmt:
		s = &sqlStmtWrapper{stmt: v}
	default:
		panic(fmt.Sprintf("non-statement type %v passed to Stmtx", reflect.ValueOf(stmt).Type()))
	}
	return &Stmt{SQLStmt: tx.StmtContext(ctx, s), Mapper: tx.Mapper}
}

// NamedStmtContext returns a version of the prepared statement which runs
// within a transaction.
func (tx *Tx) NamedStmtContext(ctx context.Context, stmt *NamedStmt) *NamedStmt {
	return &NamedStmt{
		QueryString: stmt.QueryString,
		Params:      stmt.Params,
		Stmt:        tx.StmtxContext(ctx, stmt.Stmt),
	}
}

// PreparexContext returns an sqlx.Stmt instead of a sql.Stmt.
//
// The provided context is used for the preparation of the statement, not for
// the execution of the statement.
func (tx *Tx) PreparexContext(ctx context.Context, query string) (*Stmt, error) {
	return PreparexContext(ctx, tx, query)
}

// PrepareNamedContext returns an sqlx.NamedStmt
func (tx *Tx) PrepareNamedContext(ctx context.Context, query string) (*NamedStmt, error) {
	return prepareNamedContext(ctx, tx, query)
}

// MustExecContext runs MustExecContext within a transaction.
// Any placeholder parameters are replaced with supplied args.
func (tx *Tx) MustExecContext(ctx context.Context, query string, args ...any) sql.Result {
	return MustExecContext(ctx, tx, query, args...)
}

// QueryxContext within a transaction and context.
// Any placeholder parameters are replaced with supplied args.
func (tx *Tx) QueryxContext(ctx context.Context, query string, args ...any) (*Rows, error) {
	r, err := tx.SQLTx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &Rows{SQLRows: r, unsafe: tx.unsafe, Mapper: tx.Mapper}, err
}

// SelectContext within a transaction and context.
// Any placeholder parameters are replaced with supplied args.
func (tx *Tx) SelectContext(ctx context.Context, dest any, query string, args ...any) error {
	return SelectContext(ctx, tx, dest, query, args...)
}

// GetContext within a transaction and context.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (tx *Tx) GetContext(ctx context.Context, dest any, query string, args ...any) error {
	return GetContext(ctx, tx, dest, query, args...)
}

// QueryRowxContext within a transaction and context.
// Any placeholder parameters are replaced with supplied args.
func (tx *Tx) QueryRowxContext(ctx context.Context, query string, args ...any) *Row {
	rows, err := tx.SQLTx.QueryContext(ctx, query, args...)
	return &Row{rows: rows, err: err, unsafe: tx.unsafe, Mapper: tx.Mapper}
}

// NamedExecContext using this Tx.
// Any named placeholder parameters are replaced with fields from arg.
func (tx *Tx) NamedExecContext(ctx context.Context, query string, arg any) (sql.Result, error) {
	return NamedExecContext(ctx, tx, query, arg)
}

// SelectContext using the prepared statement.
// Any placeholder parameters are replaced with supplied args.
func (s *Stmt) SelectContext(ctx context.Context, dest any, args ...any) error {
	return SelectContext(ctx, &qStmt{s}, dest, "", args...)
}

// GetContext using the prepared statement.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (s *Stmt) GetContext(ctx context.Context, dest any, args ...any) error {
	return GetContext(ctx, &qStmt{s}, dest, "", args...)
}

// MustExecContext (panic) using this statement.  Note that the query portion of
// the error output will be blank, as Stmt does not expose its query.
// Any placeholder parameters are replaced with supplied args.
func (s *Stmt) MustExecContext(ctx context.Context, args ...any) sql.Result {
	return MustExecContext(ctx, &qStmt{s}, "", args...)
}

// QueryRowxContext using this statement.
// Any placeholder parameters are replaced with supplied args.
func (s *Stmt) QueryRowxContext(ctx context.Context, args ...any) *Row {
	qs := &qStmt{s}
	return qs.QueryRowxContext(ctx, "", args...)
}

// QueryxContext using this statement.
// Any placeholder parameters are replaced with supplied args.
func (s *Stmt) QueryxContext(ctx context.Context, args ...any) (*Rows, error) {
	qs := &qStmt{s}
	return qs.QueryxContext(ctx, "", args...)
}

func (q *qStmt) QueryContext(ctx context.Context, query string, args ...any) (SQLRows, error) {
	return q.SQLStmt.QueryContext(ctx, args...)
}

func (q *qStmt) QueryxContext(ctx context.Context, query string, args ...any) (*Rows, error) {
	r, err := q.Stmt.QueryContext(ctx, args...)
	if err != nil {
		return nil, err
	}
	return &Rows{SQLRows: r, unsafe: q.Stmt.unsafe, Mapper: q.Stmt.Mapper}, err
}

func (q *qStmt) QueryRowxContext(ctx context.Context, query string, args ...any) *Row {
	rows, err := q.Stmt.QueryContext(ctx, args...)
	return &Row{rows: rows, err: err, unsafe: q.Stmt.unsafe, Mapper: q.Stmt.Mapper}
}

func (q *qStmt) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return q.Stmt.ExecContext(ctx, args...)
}
