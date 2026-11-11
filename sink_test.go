package squealx

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

type sinkCloseErrorRows struct {
	closeErr   error
	closeCalls int
}

type rawColumnTestRows struct {
	values  [][]byte
	scanErr error
}

func (*rawColumnTestRows) Err() error                              { return nil }
func (*rawColumnTestRows) ColumnTypes() ([]*sql.ColumnType, error) { return nil, nil }
func (*rawColumnTestRows) Columns() ([]string, error)              { return []string{"a", "b", "c"}, nil }
func (*rawColumnTestRows) Close() error                            { return nil }
func (*rawColumnTestRows) Next() bool                              { return false }
func (r *rawColumnTestRows) Scan(dest ...any) error {
	for i, value := range r.values {
		*dest[i].(*sql.RawBytes) = value
	}
	return r.scanErr
}

type sinkWriteError struct{ err error }

func (w sinkWriteError) Write([]byte) (int, error) { return 0, w.err }

func (*sinkCloseErrorRows) Err() error                              { return nil }
func (*sinkCloseErrorRows) Scan(...any) error                       { return errors.New("unexpected scan") }
func (*sinkCloseErrorRows) ColumnTypes() ([]*sql.ColumnType, error) { return nil, nil }
func (*sinkCloseErrorRows) Columns() ([]string, error)              { return []string{"value"}, nil }
func (r *sinkCloseErrorRows) Close() error                          { r.closeCalls++; return r.closeErr }
func (*sinkCloseErrorRows) Next() bool                              { return false }

var sinkJSONBenchmarkBytes []byte

func TestAppendJSONStringMatchesEncodingJSON(t *testing.T) {
	cases := []string{
		"", `quote: \" slash: \\`, "line\nfeed\tand\x00control", "<tag>&value>",
		"separators: \u2028\u2029", string([]byte{'a', 0xff, 'b'}),
	}
	for _, input := range cases {
		got, err := appendJSONString(nil, input)
		want, wantErr := json.Marshal(input)
		if err != nil || wantErr != nil || !bytes.Equal(got, want) {
			t.Errorf("input %q: got %s (err %v), want %s (err %v)", input, got, err, want, wantErr)
		}
		if utf8.ValidString(input) {
			gotBytes, err := appendJSONValue(nil, []byte(input))
			if err != nil || !bytes.Equal(gotBytes, want) {
				t.Errorf("byte input %q: got %s (err %v), want %s", input, gotBytes, err, want)
			}
		}
	}
}

func TestRawColumnScannerDiscardsUnselectedValues(t *testing.T) {
	rawRows := &rawColumnTestRows{values: [][]byte{
		bytes.Repeat([]byte("a"), 1024),
		[]byte("selected"),
		bytes.Repeat([]byte("c"), 1024),
	}}
	rows := &Rows{SQLRows: rawRows}
	scanner, err := NewRawColumnScanner(rows, 1)
	if err != nil {
		t.Fatal(err)
	}
	data, ok, err := scanner.Scan(rows)
	if err != nil || !ok || string(data) != "selected" {
		t.Fatalf("Scan = (%q, %t, %v), want selected bytes", data, ok, err)
	}
	if scanner.discard != nil {
		t.Fatal("scanner retained the final unselected column")
	}
	if len(scanner.targets) != 3 || scanner.targets[0] != scanner.targets[2] {
		t.Fatal("unselected columns do not share the discard target")
	}
	if err := scanner.Close(); err != nil {
		t.Fatal(err)
	}
	if scanner.selected != nil || scanner.discard != nil || scanner.targets != nil {
		t.Fatal("Close retained scan references")
	}
}

func TestRawColumnScannerClearsReferencesAfterScanError(t *testing.T) {
	scanErr := errors.New("scan failed")
	rawRows := &rawColumnTestRows{
		values:  [][]byte{[]byte("discard"), []byte("selected"), []byte("discard")},
		scanErr: scanErr,
	}
	rows := &Rows{SQLRows: rawRows}
	scanner, err := NewRawColumnScanner(rows, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := scanner.Scan(rows); !errors.Is(err, scanErr) {
		t.Fatalf("Scan error = %v, want %v", err, scanErr)
	}
	if scanner.selected != nil || scanner.discard != nil {
		t.Fatal("failed scan retained borrowed byte references")
	}
}

func BenchmarkAppendJSONString(b *testing.B) {
	value := "value with \"quotes\", <html>, and unicode \u2028"
	dst := make([]byte, 0, 128)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = dst[:0]
		dst, _ = appendJSONString(dst, value)
	}
}

func BenchmarkJSONMarshalString(b *testing.B) {
	value := "value with \"quotes\", <html>, and unicode \u2028"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := json.Marshal(value); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendJSONValueTime(b *testing.B) {
	value := time.Date(2026, 11, 10, 21, 30, 15, 123456789, time.FixedZone("test", 5*60*60+45*60))
	var arg any = value
	dst := make([]byte, 0, 64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst, _ = appendJSONValue(dst[:0], arg)
	}
	sinkJSONBenchmarkBytes = dst
}

func BenchmarkJSONMarshalTime(b *testing.B) {
	value := time.Date(2026, 11, 10, 21, 30, 15, 123456789, time.FixedZone("test", 5*60*60+45*60))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		sinkJSONBenchmarkBytes, err = json.Marshal(value.Format(time.RFC3339Nano))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendJSONValueBinary(b *testing.B) {
	value := []byte{0, 1, 2, 0xff, 0xfe, 0x80}
	var arg any = value
	dst := make([]byte, 0, 32)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst, _ = appendJSONValue(dst[:0], arg)
	}
	sinkJSONBenchmarkBytes = dst
}

func BenchmarkAppendJSONValueValidBytes(b *testing.B) {
	var arg any = []byte("a valid UTF-8 payload with \"quotes\"")
	dst := make([]byte, 0, 64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst, _ = appendJSONValue(dst[:0], arg)
	}
	sinkJSONBenchmarkBytes = dst
}

func BenchmarkAppendJSONValueString(b *testing.B) {
	var arg any = "a valid UTF-8 payload with \"quotes\""
	dst := make([]byte, 0, 64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst, _ = appendJSONValue(dst[:0], arg)
	}
	sinkJSONBenchmarkBytes = dst
}

func BenchmarkJSONMarshalBinary(b *testing.B) {
	value := []byte{0, 1, 2, 0xff, 0xfe, 0x80}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoded := base64.StdEncoding.EncodeToString(value)
		sinkJSONBenchmarkBytes, _ = json.Marshal(encoded)
	}
}

func BenchmarkAppendJSONValueFloat(b *testing.B) {
	var arg any = 123456.789
	dst := make([]byte, 0, 32)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst, _ = appendJSONValue(dst[:0], arg)
	}
	sinkJSONBenchmarkBytes = dst
}

func BenchmarkJSONMarshalFloat(b *testing.B) {
	value := 123456.789
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sinkJSONBenchmarkBytes, _ = json.Marshal(value)
	}
}

func TestAppendJSONValue(t *testing.T) {
	ts := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	cases := []struct {
		in   any
		want string
	}{
		{nil, "null"},
		{[]byte("hi"), `"hi"`},
		{[]byte{0xff, 0xfe}, `"//4="`},
		{int64(7), "7"},
		{1.5, "1.5"},
		{true, "true"},
		{ts, `"2024-01-02T03:04:05Z"`},
		{`a"b`, `"a\"b"`},
	}
	for _, c := range cases {
		b, err := appendJSONValue(nil, c.in)
		if err != nil || string(b) != c.want {
			t.Errorf("%v: got %s err %v want %s", c.in, b, err, c.want)
		}
	}
}

func TestAppendJSONFloatMatchesEncodingJSON(t *testing.T) {
	values := []float64{
		0, math.Copysign(0, -1), 1.5, -123.456, 1e-7, 1e-6, 1e20, 1e21,
		math.SmallestNonzeroFloat64, math.MaxFloat64,
	}
	for _, value := range values {
		got, err := appendJSONValue(nil, value)
		want, wantErr := json.Marshal(value)
		if err != nil || wantErr != nil || !bytes.Equal(got, want) {
			t.Errorf("value %g: got %s (err %v), want %s (err %v)", value, got, err, want, wantErr)
		}
	}
	for _, value := range []float64{math.Inf(1), math.Inf(-1), math.NaN()} {
		if _, err := appendJSONValue(nil, value); err == nil {
			t.Errorf("value %g: expected unsupported value error", value)
		}
	}
}

func TestAppendJSONTimeInvalidMatchesEncodingJSON(t *testing.T) {
	values := []time.Time{
		time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 1, 1, 0, 0, 0, 0, time.FixedZone("outside-rfc3339", 24*60*60)),
	}
	for _, value := range values {
		_, gotErr := appendJSONValue(nil, value)
		_, wantErr := json.Marshal(value)
		if gotErr == nil || wantErr == nil || gotErr.Error() != wantErr.Error() {
			t.Errorf("value %v: err=%v, want json.Marshal error %v", value, gotErr, wantErr)
		}
	}
}

func TestCSVField(t *testing.T) {
	if s, _ := csvField(nil); s != "" {
		t.Fatal("nil should be empty")
	}
	if s, _ := csvField([]byte{0xff}); s != "/w==" {
		t.Fatalf("got %q", s)
	}
	if s, _ := csvField(int64(3)); s != "3" {
		t.Fatalf("got %q", s)
	}
}

func TestLimitedBytes(t *testing.T) {
	l := LimitedBytes{Max: 3}
	if err := l.Scan([]byte("abc")); err != nil || string(l.Data) != "abc" {
		t.Fatal(err)
	}
	if err := l.Scan([]byte("abcd")); !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("got %v", err)
	}
	if l.Data != nil {
		t.Fatalf("oversized scan retained previous data: %q", l.Data)
	}
	if err := l.Scan([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := l.Scan("abcd"); !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("got %v", err)
	}
	if l.Data != nil {
		t.Fatalf("oversized string scan retained previous data: %q", l.Data)
	}
	if err := l.Scan(struct{}{}); err == nil || l.Data != nil {
		t.Fatalf("unsupported scan error=%v data=%q", err, l.Data)
	}
	if err := l.Scan(nil); err != nil || l.Data != nil {
		t.Fatal("nil")
	}
	u := LimitedBytes{}
	if err := u.Scan([]byte("abcdef")); err != nil {
		t.Fatal(err)
	}
}

func TestLimitedBytesReuseAndRetentionCap(t *testing.T) {
	l := LimitedBytes{Max: 1024, Reuse: true}
	if err := l.Scan([]byte("first")); err != nil {
		t.Fatal(err)
	}
	first := l.Data
	if err := l.Scan("second"); err != nil {
		t.Fatal(err)
	}
	if string(l.Data) != "second" || len(first) == 0 || &first[0] != &l.Data[0] {
		t.Fatalf("buffer was not reused: first=%q current=%q", first, l.Data)
	}

	l.Data = make([]byte, 4, maxRetainedLimitedBytes*2)
	if err := l.Scan("small"); err != nil {
		t.Fatal(err)
	}
	if cap(l.Data) > maxRetainedLimitedBytes {
		t.Fatalf("retained buffer capacity = %d, want at most %d", cap(l.Data), maxRetainedLimitedBytes)
	}
}

func BenchmarkLimitedBytesScan(b *testing.B) {
	source := bytes.Repeat([]byte("x"), 1024)
	arg := any(source)
	b.Run("allocate", func(b *testing.B) {
		var scanner LimitedBytes
		b.ReportAllocs()
		for b.Loop() {
			if err := scanner.Scan(arg); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("reuse", func(b *testing.B) {
		scanner := LimitedBytes{Reuse: true}
		b.ReportAllocs()
		for b.Loop() {
			if err := scanner.Scan(arg); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestStreamingWritersReportCloseErrors(t *testing.T) {
	closeErr := errors.New("close failed")
	var output bytes.Buffer
	jsonRows := &sinkCloseErrorRows{closeErr: closeErr}
	if _, err := WriteJSONLines(context.Background(), &output, &Rows{SQLRows: jsonRows}); !errors.Is(err, closeErr) {
		t.Fatalf("JSON writer error = %v", err)
	}
	if jsonRows.closeCalls != 1 {
		t.Fatalf("JSON rows closed %d times", jsonRows.closeCalls)
	}
	csvRows := &sinkCloseErrorRows{closeErr: closeErr}
	if _, err := WriteCSV(context.Background(), &output, &Rows{SQLRows: csvRows}, false); !errors.Is(err, closeErr) {
		t.Fatalf("CSV writer error = %v", err)
	}
	if csvRows.closeCalls != 1 {
		t.Fatalf("CSV rows closed %d times", csvRows.closeCalls)
	}
	cursorRows := &sinkCloseErrorRows{closeErr: closeErr}
	cursor, err := CursorFromRows[map[string]any](&Rows{SQLRows: cursorRows})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := WriteMapCursorJSONLines(context.Background(), &output, cursor); !errors.Is(err, closeErr) {
		t.Fatalf("map cursor writer error = %v", err)
	}
	if cursorRows.closeCalls != 1 {
		t.Fatalf("cursor rows closed %d times", cursorRows.closeCalls)
	}
}

func TestStreamingWritersJoinCloseErrorsAfterCancellation(t *testing.T) {
	closeErr := errors.New("close failed")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var output bytes.Buffer

	jsonRows := &sinkCloseErrorRows{closeErr: closeErr}
	_, err := WriteJSONLines(ctx, &output, &Rows{SQLRows: jsonRows})
	if !errors.Is(err, context.Canceled) || !errors.Is(err, closeErr) {
		t.Fatalf("JSON writer error = %v", err)
	}
	if jsonRows.closeCalls != 1 {
		t.Fatalf("JSON rows closed %d times", jsonRows.closeCalls)
	}

	csvRows := &sinkCloseErrorRows{closeErr: closeErr}
	_, err = WriteCSV(ctx, &output, &Rows{SQLRows: csvRows}, false)
	if !errors.Is(err, context.Canceled) || !errors.Is(err, closeErr) {
		t.Fatalf("CSV writer error = %v", err)
	}
	if csvRows.closeCalls != 1 {
		t.Fatalf("CSV rows closed %d times", csvRows.closeCalls)
	}

	cursorRows := &sinkCloseErrorRows{closeErr: closeErr}
	cursor, err := CursorFromRows[map[string]any](&Rows{SQLRows: cursorRows})
	if err != nil {
		t.Fatal(err)
	}
	_, err = WriteMapCursorJSONLines(ctx, &output, cursor)
	if !errors.Is(err, context.Canceled) || !errors.Is(err, closeErr) {
		t.Fatalf("map cursor writer error = %v", err)
	}
	if cursorRows.closeCalls != 1 {
		t.Fatalf("cursor rows closed %d times", cursorRows.closeCalls)
	}
}

func TestCSVWriterJoinsFlushAndCloseErrors(t *testing.T) {
	flushErr := errors.New("write failed")
	closeErr := errors.New("close failed")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rows := &sinkCloseErrorRows{closeErr: closeErr}
	_, err := WriteCSV(ctx, sinkWriteError{err: flushErr}, &Rows{SQLRows: rows}, true)
	for _, want := range []error{context.Canceled, flushErr, closeErr} {
		if !errors.Is(err, want) {
			t.Errorf("error %v does not include %v", err, want)
		}
	}
	if rows.closeCalls != 1 {
		t.Fatalf("rows closed %d times", rows.closeCalls)
	}
}

func TestStreamingWritersCloseOnNilWriter(t *testing.T) {
	closeErr := errors.New("close failed")
	check := func(name string, err error, calls int) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), "nil writer") || !errors.Is(err, closeErr) {
			t.Errorf("%s error = %v", name, err)
		}
		if calls != 1 {
			t.Errorf("%s close calls = %d", name, calls)
		}
	}

	jsonRows := &sinkCloseErrorRows{closeErr: closeErr}
	_, err := WriteJSONLines(context.Background(), nil, &Rows{SQLRows: jsonRows})
	check("JSON", err, jsonRows.closeCalls)

	csvRows := &sinkCloseErrorRows{closeErr: closeErr}
	_, err = WriteCSV(context.Background(), nil, &Rows{SQLRows: csvRows}, false)
	check("CSV", err, csvRows.closeCalls)

	cursorRows := &sinkCloseErrorRows{closeErr: closeErr}
	cursor, err := CursorFromRows[map[string]any](&Rows{SQLRows: cursorRows})
	if err != nil {
		t.Fatal(err)
	}
	_, err = WriteMapCursorJSONLines(context.Background(), nil, cursor)
	check("map cursor", err, cursorRows.closeCalls)
}

func TestCursorEOFReleasesRowsWrapper(t *testing.T) {
	rows := &sinkCloseErrorRows{}
	cursor, err := CursorFromRows[map[string]any](&Rows{SQLRows: rows})
	if err != nil {
		t.Fatal(err)
	}
	if cursor.Next() {
		t.Fatal("unexpected row")
	}
	if cursor.rows != nil {
		t.Fatal("closed cursor retained its rows wrapper")
	}
	if cursor.value != nil || cursor.Copy() != nil {
		t.Fatalf("closed cursor retained map storage: value=%v copy=%v", cursor.value, cursor.Copy())
	}
	if rows.closeCalls != 1 {
		t.Fatalf("rows closed %d times", rows.closeCalls)
	}
}

func TestCursorConstructionFailureClosesRowsOnceAndJoinsError(t *testing.T) {
	closeErr := errors.New("close failed")
	rows := &sinkCloseErrorRows{closeErr: closeErr}
	_, err := CursorFromRowsConfig[struct{ Other int }](&Rows{SQLRows: rows}, CursorConfig{})
	if err == nil || !strings.Contains(err.Error(), "missing destination name") || !errors.Is(err, closeErr) {
		t.Fatalf("construction error = %v", err)
	}
	if rows.closeCalls != 1 {
		t.Fatalf("rows closed %d times", rows.closeCalls)
	}
}

func TestPreloadedCursorCloseReleasesReferences(t *testing.T) {
	rows := &sinkCloseErrorRows{}
	source, err := CursorFromRows[int](&Rows{SQLRows: rows})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cursor := &PreloadedCursor[int]{
		ctx: ctx, repo: &repository[int]{db: &DB{}, preloadRelations: []Relation{{With: "related"}}},
		source: source, batchSize: 2, batch: []int{1, 2}, pendingErr: errors.New("pending"),
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	if cursor.ctx != nil || cursor.repo != nil || cursor.source != nil || cursor.pendingErr != nil || cursor.batch != nil {
		t.Fatalf("preloaded cursor retained references after close: %+v", cursor)
	}
	if source.rows != nil || source.mapper != nil || source.baseType != nil || source.valueValue.IsValid() {
		t.Fatal("source cursor retained scan references after close")
	}
	if rows.closeCalls != 1 {
		t.Fatalf("source rows closed %d times", rows.closeCalls)
	}
	if err := cursor.Close(); err != nil || rows.closeCalls != 1 {
		t.Fatalf("second close: err=%v close calls=%d", err, rows.closeCalls)
	}
}
