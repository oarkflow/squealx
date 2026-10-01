package squealx

import (
	"context"
	"errors"
	"iter"
)

// All returns a range-over-func iterator over the remaining rows. The cursor
// is closed when the loop ends (including early break). Errors are yielded as
// the second value, after which iteration stops.
//
// The yielded value is the cursor's reused storage: for pointer and map T it
// is overwritten by the next iteration, so call Copy on the cursor to retain
// a row beyond the loop body.
func (c *Cursor[T]) All() iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		if c == nil {
			var zero T
			yield(zero, ErrCursorClosed)
			return
		}
		defer c.Close()
		for c.Next() {
			if !yield(c.value, nil) {
				return
			}
		}
		if err := c.Err(); err != nil {
			var zero T
			yield(zero, err)
		}
	}
}

func openCursor[T any](rows *Rows, err error) (*Cursor[T], error) {
	if err != nil {
		return nil, err
	}
	cursor, err := CursorFromRows[T](rows)
	if err != nil {
		_ = rows.Close()
		return nil, err
	}
	return cursor, nil
}

// TxQueryCursor opens a typed streaming cursor inside a transaction.
func TxQueryCursor[T any](ctx context.Context, tx *Tx, query string, args ...any) (*Cursor[T], error) {
	if tx == nil {
		return nil, errors.New("squealx: nil Tx")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return openCursor[T](tx.QueryxContext(ctx, query, args...))
}

// ConnQueryCursor opens a typed streaming cursor on a dedicated connection.
func ConnQueryCursor[T any](ctx context.Context, conn *Conn, query string, args ...any) (*Cursor[T], error) {
	if conn == nil {
		return nil, errors.New("squealx: nil Conn")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return openCursor[T](conn.QueryxContext(ctx, query, args...))
}

// StmtQueryCursor opens a typed streaming cursor from a prepared statement.
func StmtQueryCursor[T any](ctx context.Context, stmt *Stmt, args ...any) (*Cursor[T], error) {
	if stmt == nil {
		return nil, errors.New("squealx: nil Stmt")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return openCursor[T](stmt.QueryxContext(ctx, args...))
}

// QueryIter runs a query and returns a range-over-func iterator of typed rows.
// See Cursor.All for reuse semantics of the yielded value.
func QueryIter[T any](ctx context.Context, db *DB, query string, args ...any) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		cursor, err := QueryCursor[T](ctx, db, query, args...)
		if err != nil {
			var zero T
			yield(zero, err)
			return
		}
		for v, err := range cursor.All() {
			if !yield(v, err) {
				return
			}
		}
	}
}
