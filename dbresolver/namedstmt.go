package dbresolver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/oarkflow/squealx"
)

// errors.
var (
	errSelectedNamedStmtNotFound = errors.New("dbresolver: selected named stmt not found")
)

// NamedStmt is a wrapper around sqlx.NamedStmt.
type NamedStmt interface {
	Close() error
	Exec(arg any) (sql.Result, error)
	ExecContext(ctx context.Context, arg any) (sql.Result, error)
	Get(dest any, arg any) error
	GetContext(ctx context.Context, dest any, arg any) error
	MustExec(arg any) sql.Result
	MustExecContext(ctx context.Context, arg any) sql.Result
	Query(arg any) (squealx.SQLRow, error)
	QueryContext(ctx context.Context, arg any) (squealx.SQLRow, error)
	QueryRow(arg any) *squealx.Row
	QueryRowContext(ctx context.Context, arg any) *squealx.Row
	QueryRowx(arg any) *squealx.Row
	QueryRowxContext(ctx context.Context, arg any) *squealx.Row
	Queryx(arg any) (*squealx.Rows, error)
	QueryxContext(ctx context.Context, arg any) (*squealx.Rows, error)
	Select(dest any, arg any) error
	SelectContext(ctx context.Context, dest any, arg any) error
	Unsafe() *squealx.NamedStmt
}

type namedStmt struct {
	masters      []string
	readReplicas []string

	masterStmts  map[*squealx.DB]*squealx.NamedStmt
	replicaStmts map[*squealx.DB]*squealx.NamedStmt

	db *dbResolver
}

// Close closes all primary database's named statements and readable database's named statements.
// Close wraps sqlx.NamedStmt.Close.
func (s *namedStmt) Close() error {
	var errs []error
	for _, pStmt := range s.masterStmts {
		err := pStmt.Close()
		if err != nil {
			errs = append(errs, err)
		}
	}
	for _, rStmt := range s.replicaStmts {
		err := rStmt.Close()
		if err != nil {
			errs = append(errs, err)
		}
	}
	if errs != nil {
		return errors.Join(errs...)
	}

	return nil
}

// Exec chooses a primary database's named statement and executes a named statement given argument.
// Exec wraps sqlx.NamedStmt.Exec.
func (s *namedStmt) Exec(arg any) (sql.Result, error) {
	db := s.db.GetDB(context.Background(), s.masters)
	stmt, ok := s.masterStmts[db]
	if !ok {
		// Should not happen.
		return nil, errors.Join(errSelectedNamedStmtNotFound, fmt.Errorf("primary db: %v", db))
	}
	return stmt.Exec(arg)
}

// ExecContext chooses a primary database's named statement and executes a named statement given argument.
// ExecContext wraps sqlx.NamedStmt.ExecContext.
func (s *namedStmt) ExecContext(ctx context.Context, arg any) (sql.Result, error) {
	db := s.db.GetDB(ctx, s.masters)
	stmt, ok := s.masterStmts[db]
	if !ok {
		// Should not happen.
		return nil, errors.Join(errSelectedNamedStmtNotFound, fmt.Errorf("primary db: %v", db))
	}
	return stmt.ExecContext(ctx, arg)
}

// Get chooses a readable database's named statement and Get using chosen statement.
// Get wraps sqlx.NamedStmt.Get.
func (s *namedStmt) Get(dest any, arg any) error {
	db := s.db.GetDB(context.Background(), s.readReplicas)
	stmt, ok := s.replicaStmts[db]
	if !ok {
		// Should not happen.
		return errors.Join(errSelectedNamedStmtNotFound, fmt.Errorf("readable db: %v", db))
	}
	err := stmt.Get(dest, arg)

	if isDBConnectionError(err) {
		dbPrimary := s.db.GetDB(context.Background(), s.masters)
		stmtPrimary, ok := s.replicaStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return errors.Join(errSelectedNamedStmtNotFound, fmt.Errorf("readable db: %v", db))
		}
		err = stmtPrimary.Get(dest, arg)
	}
	return err
}

// GetContext chooses a readable database's named statement and Get using chosen statement.
// GetContext wraps sqlx.NamedStmt.GetContext.
func (s *namedStmt) GetContext(ctx context.Context, dest any, arg any) error {
	db := s.db.GetDB(ctx, s.readReplicas)
	stmt, ok := s.replicaStmts[db]
	if !ok {
		// Should not happen.
		return errors.Join(errSelectedNamedStmtNotFound, fmt.Errorf("readable db: %v", db))
	}
	err := stmt.GetContext(ctx, dest, arg)

	if isDBConnectionError(err) {
		dbPrimary := s.db.GetDB(ctx, s.masters)
		stmtPrimary, ok := s.replicaStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return errors.Join(errSelectedNamedStmtNotFound, fmt.Errorf("readable db: %v", db))
		}
		err = stmtPrimary.GetContext(ctx, dest, arg)
	}
	return err
}

// MustExec chooses a primary database's named statement
// and executes chosen statement with given argument.
// MustExec wraps sqlx.NamedStmt.MustExec.
func (s *namedStmt) MustExec(arg any) sql.Result {
	db := s.db.GetDB(context.Background(), s.masters)
	stmt, ok := s.masterStmts[db]
	if !ok {
		// Should not happen.
		panic(errors.Join(errSelectedNamedStmtNotFound, fmt.Errorf("primary db: %v", db)))
	}
	return stmt.MustExec(arg)
}

// MustExecContext chooses a primary database's named statement
// and executes chosen statement with given argument.
// MustExecContext wraps sqlx.NamedStmt.MustExecContext.
func (s *namedStmt) MustExecContext(ctx context.Context, arg any) sql.Result {
	db := s.db.GetDB(ctx, s.masters)
	stmt, ok := s.masterStmts[db]
	if !ok {
		// Should not happen.
		panic(errors.Join(errSelectedNamedStmtNotFound, fmt.Errorf("primary db: %v", db)))
	}
	return stmt.MustExecContext(ctx, arg)
}

// Query chooses a readable database's named statement, executes chosen statement with given argument
// and returns sql.Rows.
// Query wraps sqlx.NamedStmt.Query.
func (s *namedStmt) Query(arg any) (squealx.SQLRow, error) {
	db := s.db.GetDB(context.Background(), s.readReplicas)
	stmt, ok := s.replicaStmts[db]
	if !ok {
		// Should not happen.
		return nil, errors.Join(errSelectedNamedStmtNotFound, fmt.Errorf("readable db: %v", db))
	}
	rows, err := stmt.Query(arg)

	if isDBConnectionError(err) {
		dbPrimary := s.db.GetDB(context.Background(), s.masters)
		stmtPrimary, ok := s.replicaStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return nil, errors.Join(errSelectedNamedStmtNotFound, fmt.Errorf("readable db: %v", db))
		}
		rows, err = stmtPrimary.Query(arg)
	}
	return rows, err
}

// QueryContext chooses a readable database's named statement, executes chosen statement with given argument
// and returns sql.Rows.
// QueryContext wraps sqlx.NamedStmt.QueryContext.
func (s *namedStmt) QueryContext(ctx context.Context, arg any) (squealx.SQLRow, error) {
	db := s.db.GetDB(ctx, s.readReplicas)
	stmt, ok := s.replicaStmts[db]
	if !ok {
		// Should not happen.
		return nil, errors.Join(errSelectedNamedStmtNotFound, fmt.Errorf("readable db: %v", db))
	}
	rows, err := stmt.QueryContext(ctx, arg)

	if isDBConnectionError(err) {
		dbPrimary := s.db.GetDB(ctx, s.masters)
		stmtPrimary, ok := s.replicaStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return nil, errors.Join(errSelectedNamedStmtNotFound, fmt.Errorf("readable db: %v", db))
		}
		rows, err = stmtPrimary.QueryContext(ctx, arg)
	}
	return rows, err
}

// QueryRow chooses a readable database's named statement, executes chosen statement with given argument
// and returns a *squealx.Row
// If selected statement is not found, returns nil.
// QueryRow wraps sqlx.NamedStmt.QueryRow.
func (s *namedStmt) QueryRow(arg any) *squealx.Row {
	db := s.db.GetDB(context.Background(), s.readReplicas)
	stmt, ok := s.replicaStmts[db]
	if !ok {
		// Should not happen.
		return nil
	}
	row := stmt.QueryRow(arg)

	if isDBConnectionError(row.Err()) {
		dbPrimary := s.db.GetDB(context.Background(), s.masters)
		stmtPrimary, ok := s.replicaStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return nil
		}
		row = stmtPrimary.QueryRow(arg)
	}
	return row
}

// QueryRowContext chooses a readable database's named statement, executes chosen statement with given argument
// and returns a *squealx.Row
// If selected statement is not found, returns nil.
// QueryRowContext wraps sqlx.NamedStmt.QueryRowContext.
func (s *namedStmt) QueryRowContext(ctx context.Context, arg any) *squealx.Row {
	db := s.db.GetDB(ctx, s.readReplicas)
	stmt, ok := s.replicaStmts[db]
	if !ok {
		// Should not happen.
		return nil
	}
	row := stmt.QueryRowContext(ctx, arg)

	if isDBConnectionError(row.Err()) {
		dbPrimary := s.db.GetDB(ctx, s.masters)
		stmtPrimary, ok := s.replicaStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return nil
		}
		row = stmtPrimary.QueryRowContext(ctx, arg)
	}
	return row
}

// QueryRowx chooses a readable database's named statement, executes chosen statement with given argument
// and returns a *squealx.Row
// If selected statement is not found, returns nil.
// QueryRowx wraps sqlx.NamedStmt.QueryRowx.
func (s *namedStmt) QueryRowx(arg any) *squealx.Row {
	db := s.db.GetDB(context.Background(), s.readReplicas)
	stmt, ok := s.replicaStmts[db]
	if !ok {
		// Should not happen.
		return nil
	}
	row := stmt.QueryRowx(arg)

	if isDBConnectionError(row.Err()) {
		dbPrimary := s.db.GetDB(context.Background(), s.masters)
		stmtPrimary, ok := s.replicaStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return nil
		}
		row = stmtPrimary.QueryRowx(arg)
	}
	return row
}

// QueryRowxContext chooses a readable database's named statement, executes chosen statement with given argument
// and returns a *squealx.Row
// If selected statement is not found, returns nil.
// QueryRowxContext wraps sqlx.NamedStmt.QueryRowxContext.
func (s *namedStmt) QueryRowxContext(ctx context.Context, arg any) *squealx.Row {
	db := s.db.GetDB(ctx, s.readReplicas)
	stmt, ok := s.replicaStmts[db]
	if !ok {
		// Should not happen.
		return nil
	}
	row := stmt.QueryRowxContext(ctx, arg)

	if isDBConnectionError(row.Err()) {
		dbPrimary := s.db.GetDB(ctx, s.masters)
		stmtPrimary, ok := s.replicaStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return nil
		}
		row = stmtPrimary.QueryRowxContext(ctx, arg)
	}
	return row
}

// Queryx chooses a readable database's named statement, executes chosen statement with given argument
// and returns sqlx.Rows.
// Queryx wraps sqlx.NamedStmt.Queryx.
func (s *namedStmt) Queryx(arg any) (*squealx.Rows, error) {
	db := s.db.GetDB(context.Background(), s.readReplicas)
	stmt, ok := s.replicaStmts[db]
	if !ok {
		// Should not happen.
		return nil, errors.Join(errSelectedNamedStmtNotFound, fmt.Errorf("readable db: %v", db))
	}
	rows, err := stmt.Queryx(arg)

	if isDBConnectionError(err) {
		dbPrimary := s.db.GetDB(context.Background(), s.masters)
		stmtPrimary, ok := s.replicaStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return nil, errors.Join(errSelectedNamedStmtNotFound, fmt.Errorf("readable db: %v", db))
		}
		rows, err = stmtPrimary.Queryx(arg)
	}
	return rows, err
}

// QueryxContext chooses a readable database's named statement, executes chosen statement with given argument
// and returns sqlx.Rows.
// QueryxContext wraps sqlx.NamedStmt.QueryxContext.
func (s *namedStmt) QueryxContext(ctx context.Context, arg any) (*squealx.Rows, error) {
	db := s.db.GetDB(ctx, s.readReplicas)
	stmt, ok := s.replicaStmts[db]
	if !ok {
		// Should not happen.
		return nil, errors.Join(errSelectedNamedStmtNotFound, fmt.Errorf("readable db: %v", db))
	}
	rows, err := stmt.QueryxContext(ctx, arg)

	if isDBConnectionError(err) {
		dbPrimary := s.db.GetDB(ctx, s.masters)
		stmtPrimary, ok := s.replicaStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return nil, errors.Join(errSelectedNamedStmtNotFound, fmt.Errorf("readable db: %v", db))
		}
		rows, err = stmtPrimary.QueryxContext(ctx, arg)
	}
	return rows, err
}

// Select chooses a readable database's named statement, executes chosen statement with given argument
// Select wraps sqlx.NamedStmt.Select.
func (s *namedStmt) Select(dest any, arg any) error {
	db := s.db.GetDB(context.Background(), s.readReplicas)
	stmt, ok := s.replicaStmts[db]
	if !ok {
		// Should not happen.
		return errors.Join(errSelectedNamedStmtNotFound, fmt.Errorf("readable db: %v", db))
	}
	err := stmt.Select(dest, arg)

	if isDBConnectionError(err) {
		dbPrimary := s.db.GetDB(context.Background(), s.masters)
		stmtPrimary, ok := s.replicaStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return errors.Join(errSelectedNamedStmtNotFound, fmt.Errorf("readable db: %v", db))
		}
		err = stmtPrimary.Select(dest, arg)
	}
	return err
}

// SelectContext chooses a readable database's named statement, executes chosen statement with given argument
// SelectContext wraps sqlx.NamedStmt.SelectContext.
func (s *namedStmt) SelectContext(ctx context.Context, dest any, arg any) error {
	db := s.db.GetDB(ctx, s.readReplicas)
	stmt, ok := s.replicaStmts[db]
	if !ok {
		// Should not happen.
		return errors.Join(errSelectedNamedStmtNotFound, fmt.Errorf("readable db: %v", db))
	}
	err := stmt.SelectContext(ctx, dest, arg)

	if isDBConnectionError(err) {
		dbPrimary := s.db.GetDB(ctx, s.masters)
		stmtPrimary, ok := s.replicaStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return errors.Join(errSelectedNamedStmtNotFound, fmt.Errorf("readable db: %v", db))
		}
		err = stmtPrimary.SelectContext(ctx, dest, arg)
	}
	return err
}

// Unsafe chooses a primary database's named statement and returns the underlying sqlx.NamedStmt.
// If selected statement is not found, returns nil.
// Unsafe wraps sqlx.NamedStmt.Unsafe.
func (s *namedStmt) Unsafe() *squealx.NamedStmt {
	db := s.db.GetDB(context.Background(), s.masters)
	stmt, ok := s.masterStmts[db]
	if !ok {
		// Should not happen.
		return nil
	}
	return stmt.Unsafe()
}
