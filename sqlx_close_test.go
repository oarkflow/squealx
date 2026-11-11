package squealx

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
)

type partialRowsQueryer struct {
	rows *Rows
	err  error
}

func (q partialRowsQueryer) Query(string, ...any) (SQLRows, error) {
	return q.rows.SQLRows, q.err
}
func (q partialRowsQueryer) Queryx(string, ...any) (*Rows, error) { return q.rows, q.err }
func (q partialRowsQueryer) QueryRowx(string, ...any) *Row {
	return &Row{rows: q.rows.SQLRows, err: q.err}
}
func (q partialRowsQueryer) QueryContext(context.Context, string, ...any) (SQLRows, error) {
	return q.rows.SQLRows, q.err
}
func (q partialRowsQueryer) QueryxContext(context.Context, string, ...any) (*Rows, error) {
	return q.rows, q.err
}
func (q partialRowsQueryer) QueryRowxContext(context.Context, string, ...any) *Row {
	return &Row{rows: q.rows.SQLRows, err: q.err}
}

func TestScanAllAndCloseReportsCloseError(t *testing.T) {
	closeErr := errors.New("driver close failed")
	rows := &sinkCloseErrorRows{closeErr: closeErr}
	var values []int
	if err := scanAllAndClose(&Rows{SQLRows: rows, Mapper: mapper()}, &values, false); !errors.Is(err, closeErr) {
		t.Fatalf("scanAllAndClose error = %v, want close error", err)
	}
	if rows.closeCalls != 1 {
		t.Fatalf("rows closed %d times, want 1", rows.closeCalls)
	}
}

func TestScanAllAndCloseJoinsScanAndCloseErrors(t *testing.T) {
	closeErr := errors.New("driver close failed")
	rows := &sinkCloseErrorRows{closeErr: closeErr}
	var values []struct {
		Other int `db:"other"`
	}
	err := scanAllAndClose(&Rows{SQLRows: rows, Mapper: mapper()}, &values, false)
	if !errors.Is(err, closeErr) || !strings.Contains(err.Error(), "missing destination name value") {
		t.Fatalf("scanAllAndClose error = %v, want scan and close errors", err)
	}
	if rows.closeCalls != 1 {
		t.Fatalf("rows closed %d times, want 1", rows.closeCalls)
	}
}

func TestScanEachAndCloseReportsCloseError(t *testing.T) {
	closeErr := errors.New("driver close failed")
	rows := &sinkCloseErrorRows{closeErr: closeErr}
	err := scanEachAndClose(&Rows{SQLRows: rows, Mapper: mapper()}, func(int) error { return nil })
	if !errors.Is(err, closeErr) {
		t.Fatalf("scanEachAndClose error = %v, want close error", err)
	}
	if rows.closeCalls != 1 {
		t.Fatalf("rows closed %d times, want 1", rows.closeCalls)
	}
}

func TestRowScanReportsCloseError(t *testing.T) {
	closeErr := errors.New("driver close failed")
	rows := &sinkCloseErrorRows{closeErr: closeErr}
	err := (&Row{rows: rows}).Scan(new(int))
	if !errors.Is(err, sql.ErrNoRows) || !errors.Is(err, closeErr) {
		t.Fatalf("Row.Scan error = %v, want no rows and close errors", err)
	}
	if rows.closeCalls != 1 {
		t.Fatalf("rows closed %d times, want 1", rows.closeCalls)
	}
}

func TestRowScanAnyJoinsCloseError(t *testing.T) {
	closeErr := errors.New("driver close failed")
	rows := &sinkCloseErrorRows{closeErr: closeErr}
	var dest struct {
		Other int `db:"other"`
	}
	err := (&Row{rows: rows, Mapper: mapper()}).scanAny(&dest, false)
	if !errors.Is(err, closeErr) || !strings.Contains(err.Error(), "missing destination name value") {
		t.Fatalf("Row.scanAny error = %v, want scan and close errors", err)
	}
	if rows.closeCalls != 1 {
		t.Fatalf("rows closed %d times, want 1", rows.closeCalls)
	}
}

func TestRowQueryErrorClosesPartialRows(t *testing.T) {
	queryErr := errors.New("query failed")
	closeErr := errors.New("driver close failed")
	rows := &sinkCloseErrorRows{closeErr: closeErr}
	err := (&Row{rows: rows, err: queryErr}).Scan(new(int))
	if !errors.Is(err, queryErr) || !errors.Is(err, closeErr) {
		t.Fatalf("Row.Scan error = %v, want query and close errors", err)
	}
	if rows.closeCalls != 1 {
		t.Fatalf("rows closed %d times, want 1", rows.closeCalls)
	}
}

func TestRowScanAnyQueryErrorClosesPartialRows(t *testing.T) {
	queryErr := errors.New("query failed")
	closeErr := errors.New("driver close failed")
	rows := &sinkCloseErrorRows{closeErr: closeErr}
	err := (&Row{rows: rows, err: queryErr}).scanAny(new(int), false)
	if !errors.Is(err, queryErr) || !errors.Is(err, closeErr) {
		t.Fatalf("Row.scanAny error = %v, want query and close errors", err)
	}
	if rows.closeCalls != 1 {
		t.Fatalf("rows closed %d times, want 1", rows.closeCalls)
	}
}

func TestAfterHookJoinsRowsCloseError(t *testing.T) {
	hookErr := errors.New("after hook failed")
	closeErr := errors.New("driver close failed")
	raw := &cursorCloseRows{closeErr: closeErr}
	db := &DB{hooks: newHookStore()}
	db.UseAfter(func(ctx context.Context, query string, args ...any) (context.Context, string, []any, error) {
		return ctx, query, args, hookErr
	})
	rows, err := handleTwo(func(string, []any) (*Rows, error) {
		return &Rows{SQLRows: raw}, nil
	}, db, context.Background(), "SELECT 1")
	if rows == nil || !errors.Is(err, hookErr) || !errors.Is(err, closeErr) {
		t.Fatalf("rows=%v error=%v, want hook and close errors", rows, err)
	}
	if raw.closed != 1 {
		t.Fatalf("rows closed %d times, want 1", raw.closed)
	}
}

func TestSelectHelpersClosePartialRowsOnQueryError(t *testing.T) {
	queryErr := errors.New("query failed")
	closeErr := errors.New("driver close failed")
	for _, test := range []struct {
		name string
		call func(partialRowsQueryer) error
	}{
		{name: "select", call: func(q partialRowsQueryer) error {
			var values []int
			return Select(q, &values, "SELECT 1")
		}},
		{name: "select context", call: func(q partialRowsQueryer) error {
			var values []int
			return SelectContext(context.Background(), q, &values, "SELECT 1")
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw := &sinkCloseErrorRows{closeErr: closeErr}
			queryer := partialRowsQueryer{rows: &Rows{SQLRows: raw}, err: queryErr}
			err := test.call(queryer)
			if !errors.Is(err, queryErr) || !errors.Is(err, closeErr) {
				t.Fatalf("select error = %v, want query and close errors", err)
			}
			if raw.closeCalls != 1 {
				t.Fatalf("rows closed %d times, want 1", raw.closeCalls)
			}
		})
	}
}
