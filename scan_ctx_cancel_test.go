package squealx

import (
	"context"
	"database/sql"
	"testing"
)

// scanCancelRows yields scannable integer rows up to limit, used to test
// ScannAllContext's periodic cancellation check without racing on shared
// state (everything here runs on the single calling goroutine).
type scanCancelRows struct {
	n      int
	limit  int
	cancel func()
	// cancelAt, if non-zero, calls cancel() the moment Next() is about to
	// return the cancelAt-th row, synchronously on the scanning goroutine.
	cancelAt int
}

func (r *scanCancelRows) Close() error                            { return nil }
func (r *scanCancelRows) Columns() ([]string, error)              { return []string{"n"}, nil }
func (r *scanCancelRows) ColumnTypes() ([]*sql.ColumnType, error) { return nil, nil }
func (r *scanCancelRows) Err() error                              { return nil }
func (r *scanCancelRows) Next() bool {
	if r.n >= r.limit {
		return false
	}
	r.n++
	if r.cancelAt != 0 && r.n == r.cancelAt {
		r.cancel()
	}
	return true
}
func (r *scanCancelRows) Scan(dest ...any) error {
	*(dest[0].(*int)) = r.n
	return nil
}

func TestScannAllContextStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	// Cancel partway into the second check interval, well before the full
	// 10x-interval result set would otherwise be scanned.
	rows := &scanCancelRows{limit: scanCtxCheckInterval * 10, cancelAt: scanCtxCheckInterval + 1}
	rows.cancel = cancel

	var dest []int
	err := ScannAllContext(ctx, rows, &dest, false)
	if err == nil {
		t.Fatal("expected context cancellation error")
	}
	if ctxErr := ctx.Err(); err != ctxErr {
		t.Fatalf("error = %v, want %v", err, ctxErr)
	}
	// The scan must stop at the next periodic check after cancellation, not
	// drain the full result set.
	if len(dest) > scanCtxCheckInterval*3 {
		t.Fatalf("scanned %d rows after cancellation, expected it to stop promptly", len(dest))
	}
}

func TestScannAllWithoutContextIgnoresCancellation(t *testing.T) {
	// ScannAll (no context) must keep its existing behavior: it drains the
	// full result set regardless of any ambient cancellation elsewhere.
	const want = 1000
	_, cancel := context.WithCancel(context.Background())
	cancel() // already canceled before scanning even starts
	rows := &scanCancelRows{limit: want}

	var dest []int
	if err := ScannAll(rows, &dest, false); err != nil {
		t.Fatal(err)
	}
	if len(dest) != want {
		t.Fatalf("scanned %d rows, want %d", len(dest), want)
	}
}
