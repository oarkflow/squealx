package squealx

import (
	"database/sql"
	"errors"
	"testing"
)

type cursorCloseRows struct {
	closeErr error
	scanErr  error
	rowCount int
	next     int
	closed   int
}

func (r *cursorCloseRows) Columns() ([]string, error)            { return []string{"value"}, nil }
func (*cursorCloseRows) ColumnTypes() ([]*sql.ColumnType, error) { return nil, nil }
func (r *cursorCloseRows) Close() error {
	r.closed++
	return r.closeErr
}
func (r *cursorCloseRows) Err() error { return nil }
func (r *cursorCloseRows) Next() bool {
	if r.next >= r.rowCount {
		return false
	}
	r.next++
	return true
}
func (r *cursorCloseRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	switch target := dest[0].(type) {
	case *int:
		*target = r.next
		return nil
	case sql.Scanner:
		return target.Scan(int64(r.next))
	default:
		return errors.New("unexpected scan destination")
	}
}

func TestCursorJoinsEOFAndCloseErrors(t *testing.T) {
	closeErr := errors.New("rows close failed")
	rows := &cursorCloseRows{closeErr: closeErr}
	cursor, err := CursorFromRows[int](&Rows{SQLRows: rows})
	if err != nil {
		t.Fatal(err)
	}
	if cursor.Next() {
		t.Fatal("unexpected row")
	}
	if !errors.Is(cursor.Err(), closeErr) {
		t.Fatalf("cursor error = %v, want close error", cursor.Err())
	}
	if rows.closed != 1 {
		t.Fatalf("rows closed %d times, want 1", rows.closed)
	}
}

func TestCursorEachValidationErrorClosesRows(t *testing.T) {
	closeErr := errors.New("rows close failed")
	rows := &cursorCloseRows{closeErr: closeErr}
	cursor, err := CursorFromRows[int](&Rows{SQLRows: rows})
	if err != nil {
		t.Fatal(err)
	}
	if err := cursor.Each(nil); err == nil || !errors.Is(err, closeErr) {
		t.Fatalf("Each error = %v, want callback and close errors", err)
	}
	if rows.closed != 1 {
		t.Fatalf("rows closed %d times, want 1", rows.closed)
	}
}

func TestCursorCollectInvalidLimitClosesRows(t *testing.T) {
	closeErr := errors.New("rows close failed")
	rows := &cursorCloseRows{closeErr: closeErr}
	cursor, err := CursorFromRows[int](&Rows{SQLRows: rows})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cursor.Collect(-1); err == nil || !errors.Is(err, closeErr) {
		t.Fatalf("Collect error = %v, want limit and close errors", err)
	}
	if rows.closed != 1 {
		t.Fatalf("rows closed %d times, want 1", rows.closed)
	}
}

func TestPreloadedCursorEachValidationErrorClosesSource(t *testing.T) {
	closeErr := errors.New("rows close failed")
	rows := &cursorCloseRows{closeErr: closeErr}
	cursor, err := CursorFromRows[int](&Rows{SQLRows: rows})
	if err != nil {
		t.Fatal(err)
	}
	preloaded := &PreloadedCursor[int]{source: cursor}
	if err := preloaded.Each(nil); err == nil || !errors.Is(err, closeErr) {
		t.Fatalf("Each error = %v, want callback and close errors", err)
	}
	if rows.closed != 1 {
		t.Fatalf("rows closed %d times, want 1", rows.closed)
	}
}

func TestPreloadedCursorAllYieldsSourceCloseError(t *testing.T) {
	closeErr := errors.New("rows close failed")
	rows := &cursorCloseRows{rowCount: 1, closeErr: closeErr}
	type row struct {
		Value int `db:"value"`
	}
	source, err := CursorFromRows[row](&Rows{SQLRows: rows})
	if err != nil {
		t.Fatal(err)
	}
	preloaded := &PreloadedCursor[row]{
		repo:      &repository[row]{},
		source:    source,
		batchSize: 1,
	}
	var values []row
	var yieldedErr error
	for value, err := range preloaded.All() {
		if err != nil {
			yieldedErr = err
			continue
		}
		values = append(values, value)
	}
	if len(values) != 1 || values[0].Value != 1 {
		t.Fatalf("iterated values = %v, want one row with value 1", values)
	}
	if !errors.Is(yieldedErr, closeErr) {
		t.Fatalf("iterator error = %v, want source close error", yieldedErr)
	}
	if rows.closed != 1 {
		t.Fatalf("rows closed %d times, want 1", rows.closed)
	}
}

func TestOpenCursorClosesPartialRowsOnQueryError(t *testing.T) {
	queryErr := errors.New("query failed")
	closeErr := errors.New("rows close failed")
	rows := &cursorCloseRows{closeErr: closeErr}
	_, err := openCursorConfig[int](&Rows{SQLRows: rows}, queryErr, CursorConfig{})
	if !errors.Is(err, queryErr) || !errors.Is(err, closeErr) {
		t.Fatalf("openCursorConfig error = %v, want query and close errors", err)
	}
	if rows.closed != 1 {
		t.Fatalf("rows closed %d times, want 1", rows.closed)
	}
}

func TestWrapRowsClosesPartialResultOnQueryError(t *testing.T) {
	queryErr := errors.New("query failed")
	closeErr := errors.New("rows close failed")
	raw := &cursorCloseRows{closeErr: closeErr}
	rows, err := wrapRows(raw, queryErr, nil, false)
	if rows != nil {
		t.Fatal("wrapRows returned rows after a query error")
	}
	if !errors.Is(err, queryErr) || !errors.Is(err, closeErr) {
		t.Fatalf("wrapRows error = %v, want query and close errors", err)
	}
	if raw.closed != 1 {
		t.Fatalf("raw rows closed %d times, want 1", raw.closed)
	}
}

func TestWrapRowsRejectsNilSuccessResult(t *testing.T) {
	if _, err := wrapRows(nil, nil, nil, false); err == nil {
		t.Fatal("wrapRows accepted a nil result without an error")
	}
}

func TestRowScanClosesRowsOnce(t *testing.T) {
	raw := &cursorCloseRows{rowCount: 1}
	row := &Row{rows: raw}
	var value int
	if err := row.Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != 1 || raw.closed != 1 {
		t.Fatalf("value=%d rows closed=%d, want value=1 and one close", value, raw.closed)
	}
}

func TestRowScanAnyClosesRowsOnce(t *testing.T) {
	raw := &cursorCloseRows{rowCount: 1}
	row := &Row{rows: raw}
	var value int
	if err := row.scanAny(&value, false); err != nil {
		t.Fatal(err)
	}
	if value != 1 || raw.closed != 1 {
		t.Fatalf("value=%d rows closed=%d, want value=1 and one close", value, raw.closed)
	}
}

func TestRowHandlesTypedNilRows(t *testing.T) {
	var raw *cursorCloseRows
	row := &Row{rows: raw}
	if err := row.Scan(new(int)); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("Scan error = %v, want sql.ErrNoRows", err)
	}
	if _, err := (&Row{rows: raw}).Columns(); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("Columns error = %v, want sql.ErrNoRows", err)
	}
	if err := (&Row{rows: raw}).scanAny(new(int), false); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("scanAny error = %v, want sql.ErrNoRows", err)
	}
}

func TestRowsCloseReleasesScanCacheAndIsIdempotent(t *testing.T) {
	closeErr := errors.New("rows close failed")
	raw := &cursorCloseRows{closeErr: closeErr}
	rows := &Rows{
		SQLRows: raw,
		started: true,
		fields:  [][]int{{1, 2}},
		values:  []any{new(int)},
	}
	if err := rows.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("Close error = %v, want close error", err)
	}
	if rows.started || rows.fields != nil || rows.values != nil || rows.octx != nil {
		t.Fatal("Rows retained scan cache after Close")
	}
	if err := rows.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("second Close error = %v, want cached close error", err)
	}
	if raw.closed != 1 {
		t.Fatalf("raw rows closed %d times, want 1", raw.closed)
	}
}

func TestRowsNextReleasesScanCacheAtEOF(t *testing.T) {
	raw := &cursorCloseRows{rowCount: 1}
	rows := &Rows{SQLRows: raw, Mapper: mapper()}
	type row struct {
		Value int `db:"value"`
	}
	if !rows.Next() {
		t.Fatal("Next returned false before the first row")
	}
	if err := rows.StructScan(&row{}); err != nil {
		t.Fatal(err)
	}
	if rows.octx == nil || rows.values == nil {
		t.Fatal("StructScan did not initialize its scan cache")
	}
	if rows.Next() {
		t.Fatal("Next returned true after the final row")
	}
	if rows.started || rows.fields != nil || rows.values != nil || rows.octx != nil {
		t.Fatal("Rows retained its StructScan cache after EOF")
	}
	// database/sql closes automatically at EOF. This fake leaves that detail to
	// its caller, and the wrapper must not add a second close of its own.
	if raw.closed != 0 {
		t.Fatalf("underlying rows closed %d times at EOF, want 0 explicit closes", raw.closed)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if raw.closed != 1 {
		t.Fatalf("underlying rows closed %d times, want 1 after explicit Close", raw.closed)
	}
}

func TestCursorJoinsScanAndCloseErrors(t *testing.T) {
	scanErr := errors.New("scan failed")
	closeErr := errors.New("rows close failed")
	rows := &cursorCloseRows{rowCount: 1, scanErr: scanErr, closeErr: closeErr}
	cursor, err := CursorFromRows[int](&Rows{SQLRows: rows})
	if err != nil {
		t.Fatal(err)
	}
	if cursor.Next() {
		t.Fatal("unexpected successful scan")
	}
	if !errors.Is(cursor.Err(), scanErr) || !errors.Is(cursor.Err(), closeErr) {
		t.Fatalf("cursor error = %v, want scan and close errors", cursor.Err())
	}
	if rows.closed != 1 {
		t.Fatalf("rows closed %d times, want 1", rows.closed)
	}
}

func TestCursorJoinsLimitAndCloseErrors(t *testing.T) {
	closeErr := errors.New("rows close failed")
	rows := &cursorCloseRows{rowCount: 2, closeErr: closeErr}
	cursor, err := CursorFromRowsConfig[int](&Rows{SQLRows: rows}, CursorConfig{MaxRows: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() || cursor.Value() != 1 {
		t.Fatal("expected first row")
	}
	if cursor.Next() {
		t.Fatal("expected row limit")
	}
	if !errors.Is(cursor.Err(), ErrCursorLimit) || !errors.Is(cursor.Err(), closeErr) {
		t.Fatalf("cursor error = %v, want limit and close errors", cursor.Err())
	}
	if rows.closed != 1 {
		t.Fatalf("rows closed %d times, want 1", rows.closed)
	}
}

func TestPreloadedCursorPreservesSourceCloseError(t *testing.T) {
	closeErr := errors.New("rows close failed")
	rows := &cursorCloseRows{closeErr: closeErr}
	source, err := CursorFromRows[int](&Rows{SQLRows: rows})
	if err != nil {
		t.Fatal(err)
	}
	preloadErr := errors.New("preload failed")
	cursor := &PreloadedCursor[int]{source: source, err: preloadErr}
	if err := cursor.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("Close error = %v, want source close error", err)
	}
	if !errors.Is(cursor.Err(), closeErr) || !errors.Is(cursor.Err(), preloadErr) {
		t.Fatalf("cursor error = %v, want preload and close errors", cursor.Err())
	}
	if rows.closed != 1 {
		t.Fatalf("rows closed %d times, want 1", rows.closed)
	}
}

func TestConsumeCursorJoinsCallbackAndCloseErrors(t *testing.T) {
	callbackErr := errors.New("callback failed")
	closeErr := errors.New("rows close failed")
	rows := &cursorCloseRows{rowCount: 1, closeErr: closeErr}
	cursor, err := CursorFromRows[int](&Rows{SQLRows: rows})
	if err != nil {
		t.Fatal(err)
	}
	err = consumeCursor(cursor, func(int) error { return callbackErr })
	if !errors.Is(err, callbackErr) || !errors.Is(err, closeErr) {
		t.Fatalf("consumeCursor error = %v, want callback and close errors", err)
	}
	if rows.closed != 1 {
		t.Fatalf("rows closed %d times, want 1", rows.closed)
	}
}

func TestCursorEachReturnsCloseError(t *testing.T) {
	closeErr := errors.New("rows close failed")
	rows := &cursorCloseRows{rowCount: 1, closeErr: closeErr}
	cursor, err := CursorFromRowsConfig[int](&Rows{SQLRows: rows}, CursorConfig{KeepOpen: true})
	if err != nil {
		t.Fatal(err)
	}
	err = cursor.Each(func(int) error { return nil })
	if !errors.Is(err, closeErr) {
		t.Fatalf("Each error = %v, want close error", err)
	}
	if rows.closed != 1 {
		t.Fatalf("rows closed %d times, want 1", rows.closed)
	}
}

func TestCursorEachJoinsCallbackAndCloseErrors(t *testing.T) {
	callbackErr := errors.New("callback failed")
	closeErr := errors.New("rows close failed")
	rows := &cursorCloseRows{rowCount: 1, closeErr: closeErr}
	cursor, err := CursorFromRowsConfig[int](&Rows{SQLRows: rows}, CursorConfig{KeepOpen: true})
	if err != nil {
		t.Fatal(err)
	}
	err = cursor.Each(func(int) error { return callbackErr })
	if !errors.Is(err, callbackErr) || !errors.Is(err, closeErr) {
		t.Fatalf("Each error = %v, want callback and close errors", err)
	}
}

func TestCursorCollectReturnsCloseError(t *testing.T) {
	closeErr := errors.New("rows close failed")
	rows := &cursorCloseRows{rowCount: 1, closeErr: closeErr}
	cursor, err := CursorFromRowsConfig[int](&Rows{SQLRows: rows}, CursorConfig{KeepOpen: true})
	if err != nil {
		t.Fatal(err)
	}
	values, err := cursor.Collect(0)
	if !errors.Is(err, closeErr) {
		t.Fatalf("Collect error = %v, want close error", err)
	}
	if len(values) != 1 || values[0] != 1 {
		t.Fatalf("Collect values = %v, want [1]", values)
	}
}
