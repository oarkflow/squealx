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
	errSelectedStmtNotFound = errors.New("dbresolver: selected stmt not found")
)

// Stmt is a wrapper around sqlx.Stmt.
type Stmt interface {
	Close() error
	Exec(args ...any) (sql.Result, error)
	ExecContext(ctx context.Context, args ...any) (sql.Result, error)
	Get(dest any, args ...any) error
	GetContext(ctx context.Context, dest any, args ...any) error
	MustExec(args ...any) sql.Result
	MustExecContext(ctx context.Context, args ...any) sql.Result
	Query(args ...any) (squealx.SQLRows, error)
	QueryContext(ctx context.Context, args ...any) (squealx.SQLRows, error)
	QueryRow(args ...any) squealx.SQLRow
	QueryRowContext(ctx context.Context, args ...any) squealx.SQLRow
	QueryRowx(args ...any) *squealx.Row
	QueryRowxContext(ctx context.Context, args ...any) *squealx.Row
	Queryx(args ...any) (*squealx.Rows, error)
	QueryxContext(ctx context.Context, args ...any) (*squealx.Rows, error)
	Select(dest any, args ...any) error
	SelectContext(ctx context.Context, dest any, args ...any) error
	Unsafe() *squealx.Stmt
}

type stmt struct {
	masters      []string
	readReplicas []string
	masterStmts  map[*squealx.DB]*squealx.Stmt
	replicaStmts map[*squealx.DB]*squealx.Stmt
	db           *dbResolver
}

var _ Stmt = (*stmt)(nil)

// Close closes all statements.
// Close is a wrapper around sqlx.Stmt.Close.
func (s *stmt) Close() error {
	var errs []error
	for _, stmt := range s.masterStmts {
		if err := stmt.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	for _, stmt := range s.replicaStmts {
		if err := stmt.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Exec chooses a primary database's statement and executes using chosen statement.
// Exec is a wrapper around sqlx.Stmt.Exec.
func (s *stmt) Exec(args ...any) (sql.Result, error) {
	db := s.db.getDB(s.db.loadBalancer.Select(context.Background(), s.masters))
	stmt, ok := s.masterStmts[db]
	if !ok {
		// Should not happen.
		return nil, errors.Join(errSelectedStmtNotFound, fmt.Errorf("primary db: %v", db))
	}
	return stmt.Exec(args...)
}

// ExecContext chooses a primary database's statement and executes using chosen statement.
// ExecContext is a wrapper around sqlx.Stmt.ExecContext.
func (s *stmt) ExecContext(ctx context.Context, args ...any) (sql.Result, error) {
	db := s.db.getDB(s.db.loadBalancer.Select(ctx, s.masters))
	stmt, ok := s.masterStmts[db]
	if !ok {
		// Should not happen.
		return nil, errors.Join(errSelectedStmtNotFound, fmt.Errorf("primary db: %v", db))
	}
	return stmt.ExecContext(ctx, args...)
}

// Get chooses a readable database's statement and Get using chosen statement.
// Get is a wrapper around sqlx.Stmt.Get.
func (s *stmt) Get(dest any, args ...any) error {
	db := s.db.getDB(s.db.loadBalancer.Select(context.Background(), s.readReplicas))
	stmt, ok := s.replicaStmts[db]
	if !ok {
		// Should not happen.
		return errors.Join(errSelectedStmtNotFound, fmt.Errorf("readable db: %v", db))
	}
	err := stmt.Get(dest, args...)

	if isDBConnectionError(err) {
		dbPrimary := s.db.getDB(s.db.loadBalancer.Select(context.Background(), s.masters))
		stmtPrimary, ok := s.replicaStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return errors.Join(errSelectedNamedStmtNotFound, fmt.Errorf("readable db: %v", db))
		}
		err = stmtPrimary.Get(dest, args...)
	}
	return err
}

// GetContext chooses a readable database's statement and Get using chosen statement.
// GetContext is a wrapper around sqlx.Stmt.GetContext.
func (s *stmt) GetContext(ctx context.Context, dest any, args ...any) error {
	db := s.db.getDB(s.db.loadBalancer.Select(ctx, s.readReplicas))
	stmt, ok := s.replicaStmts[db]
	if !ok {
		// Should not happen.
		return errors.Join(errSelectedStmtNotFound, fmt.Errorf("readable db: %v", db))
	}
	err := stmt.GetContext(ctx, dest, args...)

	if isDBConnectionError(err) {
		dbPrimary := s.db.getDB(s.db.loadBalancer.Select(ctx, s.masters))
		stmtPrimary, ok := s.replicaStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return errors.Join(errSelectedNamedStmtNotFound, fmt.Errorf("readable db: %v", db))
		}
		err = stmtPrimary.GetContext(ctx, dest, args...)
	}
	return err
}

// MustExec chooses a primary database's statement and executes using chosen statement or panic.
// MustExec is a wrapper around sqlx.Stmt.MustExec.
func (s *stmt) MustExec(args ...any) sql.Result {
	db := s.db.getDB(s.db.loadBalancer.Select(context.Background(), s.masters))
	stmt, ok := s.masterStmts[db]
	if !ok {
		// Should not happen.
		panic(errors.Join(errSelectedStmtNotFound, fmt.Errorf("primary db: %v", db)))
	}
	return stmt.MustExec(args...)
}

// MustExecContext chooses a primary database's statement and executes using chosen statement or panic.
// MustExecContext is a wrapper around sqlx.Stmt.MustExecContext.
func (s *stmt) MustExecContext(ctx context.Context, args ...any) sql.Result {
	db := s.db.getDB(s.db.loadBalancer.Select(ctx, s.masters))
	stmt, ok := s.masterStmts[db]
	if !ok {
		// Should not happen.
		panic(errors.Join(errSelectedStmtNotFound, fmt.Errorf("primary db: %v", db)))
	}
	return stmt.MustExecContext(ctx, args...)
}

// Query chooses a readable database's statement and executes using chosen statement.
// Query is a wrapper around sqlx.Stmt.Query.
func (s *stmt) Query(args ...any) (squealx.SQLRows, error) {
	db := s.db.getDB(s.db.loadBalancer.Select(context.Background(), s.readReplicas))
	stmt, ok := s.replicaStmts[db]
	if !ok {
		// Should not happen.
		return nil, errors.Join(errSelectedStmtNotFound, fmt.Errorf("readable db: %v", db))
	}
	rows, err := stmt.Query(args...)

	if isDBConnectionError(err) {
		dbPrimary := s.db.getDB(s.db.loadBalancer.Select(context.Background(), s.masters))
		stmtPrimary, ok := s.replicaStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return nil, errors.Join(errSelectedNamedStmtNotFound, fmt.Errorf("readable db: %v", db))
		}
		rows, err = stmtPrimary.Query(args...)
	}
	return rows, err
}

// QueryContext chooses a readable database's statement and executes using chosen statement.
// QueryContext is a wrapper around sqlx.Stmt.QueryContext.
func (s *stmt) QueryContext(ctx context.Context, args ...any) (squealx.SQLRows, error) {
	db := s.db.getDB(s.db.loadBalancer.Select(ctx, s.readReplicas))
	stmt, ok := s.replicaStmts[db]
	if !ok {
		// Should not happen.
		return nil, errors.Join(errSelectedStmtNotFound, fmt.Errorf("readable db: %v", db))
	}
	rows, err := stmt.QueryContext(ctx, args...)

	if isDBConnectionError(err) {
		dbPrimary := s.db.getDB(s.db.loadBalancer.Select(ctx, s.masters))
		stmtPrimary, ok := s.replicaStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return nil, errors.Join(errSelectedNamedStmtNotFound, fmt.Errorf("readable db: %v", db))
		}
		rows, err = stmtPrimary.QueryContext(ctx, args...)
	}
	return rows, err
}

// QueryRow chooses a readable database's statement, executes using chosen statement and returns *squealx.Row.
// If selected statement is not found, returns nil.
// QueryRow is a wrapper around sqlx.Stmt.QueryRow.
func (s *stmt) QueryRow(args ...any) squealx.SQLRow {
	db := s.db.getDB(s.db.loadBalancer.Select(context.Background(), s.readReplicas))
	stmt, ok := s.replicaStmts[db]
	if !ok {
		// Should not happen.
		return nil
	}
	row := stmt.QueryRow(args...)

	if isDBConnectionError(row.Err()) {
		dbPrimary := s.db.getDB(s.db.loadBalancer.Select(context.Background(), s.masters))
		stmtPrimary, ok := s.replicaStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return nil
		}
		row = stmtPrimary.QueryRow(args...)
	}
	return row
}

// QueryRowContext chooses a readable database's statement, executes using chosen statement and returns *squealx.Row.
// If selected statement is not found, returns nil.
// QueryRowContext is a wrapper around sqlx.Stmt.QueryRowContext.
func (s *stmt) QueryRowContext(ctx context.Context, args ...any) squealx.SQLRow {
	db := s.db.getDB(s.db.loadBalancer.Select(ctx, s.readReplicas))
	stmt, ok := s.replicaStmts[db]
	if !ok {
		// Should not happen.
		return nil
	}
	row := stmt.QueryRowContext(ctx, args...)

	if isDBConnectionError(row.Err()) {
		dbPrimary := s.db.getDB(s.db.loadBalancer.Select(ctx, s.masters))
		stmtPrimary, ok := s.replicaStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return nil
		}
		row = stmtPrimary.QueryRowContext(ctx, args...)
	}
	return row
}

// QueryRowx chooses a readable database's statement, executes using chosen statement and returns *squealx.Row.
// If selected statement is not found, returns nil.
// QueryRowx is a wrapper around sqlx.Stmt.QueryRowx.
func (s *stmt) QueryRowx(args ...any) *squealx.Row {
	db := s.db.getDB(s.db.loadBalancer.Select(context.Background(), s.readReplicas))
	stmt, ok := s.replicaStmts[db]
	if !ok {
		// Should not happen.
		return nil
	}
	row := stmt.QueryRowx(args...)

	if isDBConnectionError(row.Err()) {
		dbPrimary := s.db.getDB(s.db.loadBalancer.Select(context.Background(), s.masters))
		stmtPrimary, ok := s.replicaStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return nil
		}
		row = stmtPrimary.QueryRowx(args...)
	}
	return row
}

// QueryRowxContext chooses a readable database's statement, executes using chosen statement and returns *squealx.Row.
// If selected statement is not found, returns nil.
// QueryRowxContext is a wrapper around sqlx.Stmt.QueryRowxContext.
func (s *stmt) QueryRowxContext(ctx context.Context, args ...any) *squealx.Row {
	db := s.db.getDB(s.db.loadBalancer.Select(ctx, s.readReplicas))
	stmt, ok := s.replicaStmts[db]
	if !ok {
		// Should not happen.
		return nil
	}
	row := stmt.QueryRowxContext(ctx, args...)

	if isDBConnectionError(row.Err()) {
		dbPrimary := s.db.getDB(s.db.loadBalancer.Select(ctx, s.masters))
		stmtPrimary, ok := s.replicaStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return nil
		}
		row = stmtPrimary.QueryRowxContext(ctx, args...)
	}
	return row
}

// Queryx chooses a readable database's statement, executes using chosen statement and returns *squealx.Rows.
// Queryx is a wrapper around sqlx.Stmt.Queryx.
func (s *stmt) Queryx(args ...any) (*squealx.Rows, error) {
	db := s.db.getDB(s.db.loadBalancer.Select(context.Background(), s.readReplicas))
	stmt, ok := s.replicaStmts[db]
	if !ok {
		// Should not happen.
		return nil, errors.Join(errSelectedStmtNotFound, fmt.Errorf("readable db: %v", db))
	}
	rows, err := stmt.Queryx(args...)

	if isDBConnectionError(err) {
		dbPrimary := s.db.getDB(s.db.loadBalancer.Select(context.Background(), s.masters))
		stmtPrimary, ok := s.replicaStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return nil, errors.Join(errSelectedNamedStmtNotFound, fmt.Errorf("readable db: %v", db))
		}
		rows, err = stmtPrimary.Queryx(args...)
	}
	return rows, err
}

// QueryxContext chooses a readable database's statement, executes using chosen statement and returns *squealx.Rows.
// QueryxContext is a wrapper around sqlx.Stmt.QueryxContext.
func (s *stmt) QueryxContext(ctx context.Context, args ...any) (*squealx.Rows, error) {
	db := s.db.getDB(s.db.loadBalancer.Select(ctx, s.readReplicas))
	stmt, ok := s.replicaStmts[db]
	if !ok {
		// Should not happen.
		return nil, errors.Join(errSelectedStmtNotFound, fmt.Errorf("readable db: %v", db))
	}
	rows, err := stmt.QueryxContext(ctx, args...)

	if isDBConnectionError(err) {
		dbPrimary := s.db.getDB(s.db.loadBalancer.Select(ctx, s.masters))
		stmtPrimary, ok := s.replicaStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return nil, errors.Join(errSelectedNamedStmtNotFound, fmt.Errorf("readable db: %v", db))
		}
		rows, err = stmtPrimary.QueryxContext(ctx, args...)
	}
	return rows, err
}

// Select chooses a readable database's statement, executes using chosen statement.
// Select is a wrapper around sqlx.Stmt.Select.
func (s *stmt) Select(dest any, args ...any) error {
	db := s.db.getDB(s.db.loadBalancer.Select(context.Background(), s.readReplicas))
	stmt, ok := s.replicaStmts[db]
	if !ok {
		// Should not happen.
		return errors.Join(errSelectedStmtNotFound, fmt.Errorf("readable db: %v", db))
	}
	err := stmt.Select(dest, args...)

	if isDBConnectionError(err) {
		dbPrimary := s.db.getDB(s.db.loadBalancer.Select(context.Background(), s.masters))
		stmtPrimary, ok := s.replicaStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return errors.Join(errSelectedNamedStmtNotFound, fmt.Errorf("readable db: %v", db))
		}
		err = stmtPrimary.Select(dest, args...)
	}
	return err
}

// SelectContext chooses a readable database's statement, executes using chosen statement.
// SelectContext is a wrapper around sqlx.Stmt.SelectContext.
func (s *stmt) SelectContext(ctx context.Context, dest any, args ...any) error {
	db := s.db.getDB(s.db.loadBalancer.Select(ctx, s.readReplicas))
	stmt, ok := s.replicaStmts[db]
	if !ok {
		// Should not happen.
		return errors.Join(errSelectedStmtNotFound, fmt.Errorf("readable db: %v", db))
	}
	err := stmt.SelectContext(ctx, dest, args...)

	if isDBConnectionError(err) {
		dbPrimary := s.db.getDB(s.db.loadBalancer.Select(ctx, s.masters))
		stmtPrimary, ok := s.replicaStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return errors.Join(errSelectedNamedStmtNotFound, fmt.Errorf("readable db: %v", db))
		}
		err = stmtPrimary.SelectContext(ctx, dest, args...)
	}
	return err
}

// Unsafe chooses a primary database's statement and returns underlying sql.Stmt.
// If selected statement is not found, returns nil.
// Unsafe wraps sqlx.Stmt.Unsafe.
func (s *stmt) Unsafe() *squealx.Stmt {
	db := s.db.getDB(s.db.loadBalancer.Select(context.Background(), s.masters))
	stmt, ok := s.masterStmts[db]
	if !ok {
		// Should not happen.
		return nil
	}
	return stmt.Unsafe()
}
