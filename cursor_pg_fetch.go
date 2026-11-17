package squealx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
)

// isPostgresDriver reports whether driverName names a PostgreSQL driver as
// commonly registered with database/sql (lib/pq, pgx's stdlib wrapper, and
// common cloud variants).
func isPostgresDriver(driverName string) bool {
	switch strings.ToLower(driverName) {
	case "postgres", "postgresql", "pgx", "pq", "cloudsqlpostgres":
		return true
	default:
		return false
	}
}

var pgFetchCursorSeq atomic.Uint64

// postgresFetchRows is an SQLRows backed by a named server-side PostgreSQL
// cursor (DECLARE ... CURSOR FOR, paged with FETCH FORWARD), used when a
// CursorConfig.FetchSize is set. Unlike a plain Query — which lib/pq and pgx's
// simple/extended query protocol fetch into memory on the server and hand
// back in one piece before the first row is available — this bounds both
// server- and client-side memory to roughly FetchSize rows at a time, the way
// JDBC's fetchSize/useCursorFetch does for Slick. It owns the transaction the
// cursor lives in and always rolls it back on Close (safe for a read-only
// cursor; nothing written needs to persist).
type postgresFetchRows struct {
	tx        SQLTx
	ctx       context.Context
	name      string
	fetchSize int
	batch     SQLRows
	batchRows int
	columns   []string
	colTypes  []*sql.ColumnType
	done      bool
	finished  bool
	err       error
}

func openPostgresFetchCursorRows(ctx context.Context, db *DB, query string, args []any, fetchSize int) (SQLRows, error) {
	sanitized, err := SanitizeQuery(query, args...)
	if err != nil {
		return nil, err
	}
	tx, err := db.SQLDB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	name := fmt.Sprintf("squealx_cur_%d", pgFetchCursorSeq.Add(1))
	declare := fmt.Sprintf("DECLARE %s NO SCROLL CURSOR FOR %s", name, sanitized)
	if _, err := tx.ExecContext(ctx, declare, args...); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	r := &postgresFetchRows{tx: tx, ctx: ctx, name: name, fetchSize: fetchSize}
	if err := r.fetchNextBatch(); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return r, nil
}

func (r *postgresFetchRows) fetchNextBatch() error {
	if r.batch != nil {
		_ = r.batch.Close()
		r.batch = nil
	}
	r.batchRows = 0
	rows, err := r.tx.QueryContext(r.ctx, fmt.Sprintf("FETCH FORWARD %d FROM %s", r.fetchSize, r.name))
	if err != nil {
		return err
	}
	if r.columns == nil {
		cols, err := rows.Columns()
		if err != nil {
			_ = rows.Close()
			return err
		}
		r.columns = cols
		if colTypes, err := rows.ColumnTypes(); err == nil {
			r.colTypes = colTypes
		}
	}
	r.batch = rows
	return nil
}

func (r *postgresFetchRows) Next() bool {
	if r.done {
		return false
	}
	for {
		if r.batch.Next() {
			r.batchRows++
			return true
		}
		if err := r.batch.Err(); err != nil {
			r.err = err
			r.finish()
			return false
		}
		if r.batchRows < r.fetchSize {
			// A short batch means the cursor is exhausted; no need to pay for
			// one more round trip just to confirm it.
			r.finish()
			return false
		}
		if err := r.fetchNextBatch(); err != nil {
			r.err = err
			r.finish()
			return false
		}
	}
}

func (r *postgresFetchRows) Scan(dest ...any) error {
	if r.batch == nil {
		return errors.New("squealx: postgres fetch cursor: Scan called with no current row")
	}
	return r.batch.Scan(dest...)
}

func (r *postgresFetchRows) Columns() ([]string, error) { return r.columns, nil }

func (r *postgresFetchRows) ColumnTypes() ([]*sql.ColumnType, error) { return r.colTypes, nil }

func (r *postgresFetchRows) Err() error { return r.err }

func (r *postgresFetchRows) Close() error {
	r.finish()
	return r.err
}

func (r *postgresFetchRows) finish() {
	if r.finished {
		return
	}
	r.finished = true
	r.done = true
	if r.batch != nil {
		_ = r.batch.Close()
		r.batch = nil
	}
	_, _ = r.tx.ExecContext(r.ctx, fmt.Sprintf("CLOSE %s", r.name))
	if err := r.tx.Rollback(); err != nil && r.err == nil && !errors.Is(err, sql.ErrTxDone) {
		r.err = err
	}
}
