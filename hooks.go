package squealx

import (
	"context"
	"database/sql/driver"
	"errors"
)

// Hook is the hook callback signature
type Hook func(ctx context.Context, query string, args ...interface{}) (context.Context, error)

// ErrorHook is the error handling callback signature
type ErrorHook func(ctx context.Context, err error, query string, args ...interface{}) error

// Hooks instances may be passed to Wrap() to define an instrumented driver
type Hooks interface {
	Before(ctx context.Context, query string, args ...interface{}) (context.Context, error)
	After(ctx context.Context, query string, args ...interface{}) (context.Context, error)
}

// OnErrorer instances will be called if any error happens
type OnErrorer interface {
	OnError(ctx context.Context, err error, query string, args ...interface{}) error
}

func handlerErr(ctx context.Context, hooks Hooks, err error, query string, args ...interface{}) error {
	h, ok := hooks.(OnErrorer)
	if !ok {
		return err
	}

	if err := h.OnError(ctx, err, query, args...); err != nil {
		return err
	}

	return err
}

// HookDriver implements a database/sql/driver.Driver
type HookDriver struct {
	driver.Driver
	hooks Hooks
}

// Open opens a connection
func (drv *HookDriver) Open(name string) (driver.Conn, error) {
	conn, err := drv.Driver.Open(name)
	if err != nil {
		return conn, err
	}

	// Drivers that don't implement driver.ConnBeginTx are not supported.
	if _, ok := conn.(driver.ConnBeginTx); !ok {
		return nil, errors.New("driver must implement driver.ConnBeginTx")
	}

	wrapped := &HookConn{conn, drv.hooks}
	if isExecer(conn) && isQueryer(conn) && isSessionResetter(conn) {
		return &ExecerQueryerContextWithSessionResetter{wrapped,
			&HookExecerContext{wrapped}, &HookQueryerContext{wrapped},
			&SessionResetter{wrapped}}, nil
	} else if isExecer(conn) && isQueryer(conn) {
		return &HookExecerQueryerContext{wrapped, &HookExecerContext{wrapped},
			&HookQueryerContext{wrapped}}, nil
	} else if isExecer(conn) {
		// If conn implements an Execer interface, return a driver.HookConn which
		// also implements Execer
		return &HookExecerContext{wrapped}, nil
	} else if isQueryer(conn) {
		// If conn implements an Queryer interface, return a driver.HookConn which
		// also implements Queryer
		return &HookQueryerContext{wrapped}, nil
	}
	return wrapped, nil
}

// HookConn implements a database/sql.driver.HookConn
type HookConn struct {
	Conn  driver.Conn
	hooks Hooks
}

func (conn *HookConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	var (
		stmt driver.Stmt
		err  error
	)

	if c, ok := conn.Conn.(driver.ConnPrepareContext); ok {
		stmt, err = c.PrepareContext(ctx, query)
	} else {
		stmt, err = conn.Prepare(query)
	}

	if err != nil {
		return stmt, err
	}

	return &HookStmt{stmt, conn.hooks, query}, nil
}

func (conn *HookConn) Prepare(query string) (driver.Stmt, error) { return conn.Conn.Prepare(query) }
func (conn *HookConn) Close() error                              { return conn.Conn.Close() }
func (conn *HookConn) Begin() (driver.Tx, error)                 { return conn.Conn.Begin() }
func (conn *HookConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return conn.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
}

// HookExecerContext implements a database/sql.driver.HookExecerContext
type HookExecerContext struct {
	*HookConn
}

func isExecer(conn driver.Conn) bool {
	switch conn.(type) {
	case driver.ExecerContext:
		return true
	case driver.Execer:
		return true
	default:
		return false
	}
}

func (conn *HookExecerContext) execContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	switch c := conn.HookConn.Conn.(type) {
	case driver.ExecerContext:
		return c.ExecContext(ctx, query, args)
	case driver.Execer:
		dargs, err := namedValueToValue(args)
		if err != nil {
			return nil, err
		}
		return c.Exec(query, dargs)
	default:
		// This should not happen
		return nil, errors.New("HookExecerContext created for a non Execer driver.HookConn")
	}
}

func (conn *HookExecerContext) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	var err error

	list := namedToInterface(args)

	// Exec `Before` Hooks
	if ctx, err = conn.hooks.Before(ctx, query, list...); err != nil {
		return nil, err
	}

	results, err := conn.execContext(ctx, query, args)
	if err != nil {
		return results, handlerErr(ctx, conn.hooks, err, query, list...)
	}

	if _, err := conn.hooks.After(ctx, query, list...); err != nil {
		return nil, err
	}

	return results, err
}

func (conn *HookExecerContext) Exec(query string, args []driver.Value) (driver.Result, error) {
	// We have to implement Exec since it is required in the current version of
	// Go for it to run ExecContext. From Go 10 it will be optional. However,
	// this code should never run since database/sql always prefers to run
	// ExecContext.
	return nil, errors.New("Exec was called when ExecContext was implemented")
}

// HookQueryerContext implements a database/sql.driver.HookQueryerContext
type HookQueryerContext struct {
	*HookConn
}

func isQueryer(conn driver.Conn) bool {
	switch conn.(type) {
	case driver.QueryerContext:
		return true
	case driver.Queryer:
		return true
	default:
		return false
	}
}

func (conn *HookQueryerContext) queryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	switch c := conn.HookConn.Conn.(type) {
	case driver.QueryerContext:
		return c.QueryContext(ctx, query, args)
	case driver.Queryer:
		dargs, err := namedValueToValue(args)
		if err != nil {
			return nil, err
		}
		return c.Query(query, dargs)
	default:
		// This should not happen
		return nil, errors.New("HookQueryerContext created for a non Queryer driver.HookConn")
	}
}

func (conn *HookQueryerContext) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	var err error

	list := namedToInterface(args)

	// Query `Before` Hooks
	if ctx, err = conn.hooks.Before(ctx, query, list...); err != nil {
		return nil, err
	}

	results, err := conn.queryContext(ctx, query, args)
	if err != nil {
		return results, handlerErr(ctx, conn.hooks, err, query, list...)
	}

	if _, err := conn.hooks.After(ctx, query, list...); err != nil {
		return nil, err
	}

	return results, err
}

// HookExecerQueryerContext implements database/sql.driver.HookExecerContext and
// database/sql.driver.HookQueryerContext
type HookExecerQueryerContext struct {
	*HookConn
	*HookExecerContext
	*HookQueryerContext
}

// ExecerQueryerContextWithSessionResetter implements database/sql.driver.HookExecerContext and
// database/sql.driver.HookQueryerContext
type ExecerQueryerContextWithSessionResetter struct {
	*HookConn
	*HookExecerContext
	*HookQueryerContext
	*SessionResetter
}

type SessionResetter struct {
	*HookConn
}

// HookStmt implements a database/sql/driver.Stmt
type HookStmt struct {
	Stmt  driver.Stmt
	hooks Hooks
	query string
}

func (stmt *HookStmt) execContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if s, ok := stmt.Stmt.(driver.StmtExecContext); ok {
		return s.ExecContext(ctx, args)
	}

	values := make([]driver.Value, len(args))
	for _, arg := range args {
		values[arg.Ordinal-1] = arg.Value
	}

	return stmt.Exec(values)
}

func (stmt *HookStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	var err error

	list := namedToInterface(args)

	// Exec `Before` Hooks
	if ctx, err = stmt.hooks.Before(ctx, stmt.query, list...); err != nil {
		return nil, err
	}

	results, err := stmt.execContext(ctx, args)
	if err != nil {
		return results, handlerErr(ctx, stmt.hooks, err, stmt.query, list...)
	}

	if _, err := stmt.hooks.After(ctx, stmt.query, list...); err != nil {
		return nil, err
	}

	return results, err
}

func (stmt *HookStmt) queryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if s, ok := stmt.Stmt.(driver.StmtQueryContext); ok {
		return s.QueryContext(ctx, args)
	}

	values := make([]driver.Value, len(args))
	for _, arg := range args {
		values[arg.Ordinal-1] = arg.Value
	}
	return stmt.Query(values)
}

func (stmt *HookStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	var err error

	list := namedToInterface(args)

	// Exec Before Hooks
	if ctx, err = stmt.hooks.Before(ctx, stmt.query, list...); err != nil {
		return nil, err
	}

	rows, err := stmt.queryContext(ctx, args)
	if err != nil {
		return rows, handlerErr(ctx, stmt.hooks, err, stmt.query, list...)
	}

	if _, err := stmt.hooks.After(ctx, stmt.query, list...); err != nil {
		return nil, err
	}

	return rows, err
}

func (stmt *HookStmt) Close() error                                    { return stmt.Stmt.Close() }
func (stmt *HookStmt) NumInput() int                                   { return stmt.Stmt.NumInput() }
func (stmt *HookStmt) Exec(args []driver.Value) (driver.Result, error) { return stmt.Stmt.Exec(args) }
func (stmt *HookStmt) Query(args []driver.Value) (driver.Rows, error)  { return stmt.Stmt.Query(args) }

// Wrap is used to create a new instrumented driver, it takes a vendor specific driver, and a Hooks instance to produce a new driver instance.
// It's usually used inside a sql.Register() statement
func Wrap(driver driver.Driver, hooks Hooks) driver.Driver {
	return &HookDriver{driver, hooks}
}

func namedToInterface(args []driver.NamedValue) []interface{} {
	list := make([]interface{}, len(args))
	for i, a := range args {
		list[i] = a.Value
	}
	return list
}

// namedValueToValue copied from database/sql
func namedValueToValue(named []driver.NamedValue) ([]driver.Value, error) {
	dargs := make([]driver.Value, len(named))
	for n, param := range named {
		if len(param.Name) > 0 {
			return nil, errors.New("sql: driver does not support the use of Named Parameters")
		}
		dargs[n] = param.Value
	}
	return dargs, nil
}

func isSessionResetter(conn driver.Conn) bool {
	_, ok := conn.(driver.SessionResetter)
	return ok
}

func (s *SessionResetter) ResetSession(ctx context.Context) error {
	c := s.HookConn.Conn.(driver.SessionResetter)
	return c.ResetSession(ctx)
}

/*
type hooks struct {
}

func (h *hooks) Before(ctx context.Context, query string, args ...interface{}) error {
	log.Printf("before> ctx = %+v, q=%s, args = %+v\n", ctx, query, args)
	return nil
}

func (h *hooks) After(ctx context.Context, query string, args ...interface{}) error {
	log.Printf("after>  ctx = %+v, q=%s, args = %+v\n", ctx, query, args)
	return nil
}

func main() {
	sql.Register("sqlite3-proxy", Wrap(&sqlite3.SQLiteDriver{}, &hooks{}))
	db, err := sql.Open("sqlite3-proxy", ":memory:")
	if err != nil {
		log.Fatalln(err)
	}

	if _, ok := driver.HookStmt(&HookStmt{}).(driver.StmtExecContext); !ok {
		panic("NOPE")
	}

	if _, err := db.Exec("CREATE table users(id int)"); err != nil {
		log.Printf("|err| = %+v\n", err)
	}

	if _, err := db.QueryContext(context.Background(), "SELECT * FROM users WHERE id = ?", 1); err != nil {
		log.Printf("err = %+v\n", err)
	}

}
*/
