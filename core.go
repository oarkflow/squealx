package squealx

import (
	"context"
	"database/sql"
)

var (
	_ Queryable = (*DB)(nil)
	_ Queryable = (*Tx)(nil)
)

// Queryable includes all methods shared by sqlx.DB and sqlx.Tx, allowing
// either type to be used interchangeably.
type Queryable interface {
	Ext
	ExecIn
	QueryIn
	ExecerContext
	PreparerContext
	QueryerContext
	Preparer

	GetContext(context.Context, any, string, ...any) error
	SelectContext(context.Context, any, string, ...any) error
	Get(any, string, ...any) error
	MustExecContext(context.Context, string, ...any) sql.Result
	PreparexContext(context.Context, string) (*Stmt, error)
	Select(any, string, ...any) error
	QueryRow(string, ...any) SQLRow
	PrepareNamedContext(context.Context, string) (*NamedStmt, error)
	PrepareNamed(string) (*NamedStmt, error)
	Preparex(string) (*Stmt, error)
	NamedExec(string, any) (sql.Result, error)
	NamedExecContext(context.Context, string, any) (sql.Result, error)
	MustExec(string, ...any) sql.Result
	NamedQuery(string, any) (*Rows, error)
	NamedSelect(dest any, query string, arg any) error
	NamedGet(dest any, query string, arg any) error
	InGet(any, string, ...any) error
	InSelect(any, string, ...any) error
	InExec(string, ...any) (sql.Result, error)
	MustInExec(string, ...any) sql.Result
}
