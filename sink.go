package squealx

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"
	"unicode/utf8"
)

// sinkFlushEvery is the number of rows between explicit flushes of sink writers.
const sinkFlushEvery = 512

// sinkScanner holds a reusable set of scan targets so that streaming a result
// set allocates no per-row target slices and never holds more than one row.
type sinkScanner struct {
	cols    []string
	vals    []any
	targets []any
}

func newSinkScanner(rows *Rows) (*sinkScanner, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	s := &sinkScanner{cols: cols, vals: make([]any, len(cols)), targets: make([]any, len(cols))}
	for i := range s.vals {
		s.targets[i] = &s.vals[i]
	}
	return s, nil
}

func (s *sinkScanner) scan(rows *Rows) error {
	for i := range s.vals {
		s.vals[i] = nil
	}
	return rows.Scan(s.targets...)
}

// appendJSONValue appends v encoded as JSON. []byte values that are valid
// UTF-8 are emitted as strings, otherwise as base64 strings. time.Time values
// are RFC3339Nano strings.
func appendJSONValue(dst []byte, v any) ([]byte, error) {
	switch x := v.(type) {
	case nil:
		return append(dst, "null"...), nil
	case []byte:
		if utf8.Valid(x) {
			return appendJSONString(dst, string(x))
		}
		return appendJSONString(dst, base64.StdEncoding.EncodeToString(x))
	case string:
		return appendJSONString(dst, x)
	case time.Time:
		return appendJSONString(dst, x.Format(time.RFC3339Nano))
	case bool:
		return strconv.AppendBool(dst, x), nil
	case int64:
		return strconv.AppendInt(dst, x, 10), nil
	case float64:
		b, err := json.Marshal(x)
		if err != nil {
			return dst, err
		}
		return append(dst, b...), nil
	default:
		b, err := json.Marshal(x)
		if err != nil {
			return dst, err
		}
		return append(dst, b...), nil
	}
}

func appendJSONString(dst []byte, s string) ([]byte, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return dst, err
	}
	return append(dst, b...), nil
}

// WriteJSONLines streams every row of rows to w as one JSON object per line.
// Only one row is held in memory at a time; output is buffered and flushed
// periodically. It honors ctx cancellation between rows, closes rows when
// done, and returns the number of rows written.
func WriteJSONLines(ctx context.Context, w io.Writer, rows *Rows) (int64, error) {
	defer rows.Close()
	sc, err := newSinkScanner(rows)
	if err != nil {
		return 0, err
	}
	keys := make([][]byte, len(sc.cols))
	for i, c := range sc.cols {
		k, err := appendJSONString(nil, c)
		if err != nil {
			return 0, err
		}
		keys[i] = append(k, ':')
	}
	bw := bufio.NewWriterSize(w, 64*1024)
	var n int64
	var line []byte
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			_ = bw.Flush()
			return n, err
		}
		if err := sc.scan(rows); err != nil {
			_ = bw.Flush()
			return n, err
		}
		line = append(line[:0], '{')
		for i, v := range sc.vals {
			if i > 0 {
				line = append(line, ',')
			}
			line = append(line, keys[i]...)
			if line, err = appendJSONValue(line, v); err != nil {
				_ = bw.Flush()
				return n, fmt.Errorf("column %q: %w", sc.cols[i], err)
			}
		}
		line = append(line, '}', '\n')
		if _, err := bw.Write(line); err != nil {
			return n, err
		}
		n++
		if n%sinkFlushEvery == 0 {
			if err := bw.Flush(); err != nil {
				return n, err
			}
		}
	}
	if err := rows.Err(); err != nil {
		_ = bw.Flush()
		return n, err
	}
	return n, bw.Flush()
}

func csvField(v any) (string, error) {
	switch x := v.(type) {
	case nil:
		return "", nil
	case []byte:
		if utf8.Valid(x) {
			return string(x), nil
		}
		return base64.StdEncoding.EncodeToString(x), nil
	case string:
		return x, nil
	case time.Time:
		return x.Format(time.RFC3339Nano), nil
	case bool:
		return strconv.FormatBool(x), nil
	case int64:
		return strconv.FormatInt(x, 10), nil
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64), nil
	default:
		return fmt.Sprint(x), nil
	}
}

// WriteCSV streams every row of rows to w as CSV, optionally preceded by a
// header line of column names. NULL is written as an empty field. Only one
// row is held in memory; ctx is checked between rows. rows is closed when
// done. It returns the number of data rows written.
func WriteCSV(ctx context.Context, w io.Writer, rows *Rows, header bool) (int64, error) {
	defer rows.Close()
	sc, err := newSinkScanner(rows)
	if err != nil {
		return 0, err
	}
	cw := csv.NewWriter(w)
	if header {
		if err := cw.Write(sc.cols); err != nil {
			return 0, err
		}
	}
	rec := make([]string, len(sc.cols))
	var n int64
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			cw.Flush()
			return n, err
		}
		if err := sc.scan(rows); err != nil {
			cw.Flush()
			return n, err
		}
		for i, v := range sc.vals {
			if rec[i], err = csvField(v); err != nil {
				cw.Flush()
				return n, err
			}
		}
		if err := cw.Write(rec); err != nil {
			return n, err
		}
		n++
		if n%sinkFlushEvery == 0 {
			cw.Flush()
			if err := cw.Error(); err != nil {
				return n, err
			}
		}
	}
	if err := rows.Err(); err != nil {
		cw.Flush()
		return n, err
	}
	cw.Flush()
	return n, cw.Error()
}

// WriteMapCursorJSONLines streams a map cursor to w as JSON lines. Keys are
// emitted in encoding/json (sorted) order. The cursor is closed when done.
func WriteMapCursorJSONLines(ctx context.Context, w io.Writer, c *Cursor[map[string]any]) (int64, error) {
	defer c.Close()
	bw := bufio.NewWriterSize(w, 64*1024)
	enc := json.NewEncoder(bw)
	var n int64
	for c.Next() {
		if err := ctx.Err(); err != nil {
			_ = bw.Flush()
			return n, err
		}
		m := c.Value()
		for k, v := range m {
			if b, ok := v.([]byte); ok {
				if utf8.Valid(b) {
					m[k] = string(b)
				} else {
					m[k] = base64.StdEncoding.EncodeToString(b)
				}
			}
		}
		if err := enc.Encode(m); err != nil {
			_ = bw.Flush()
			return n, err
		}
		n++
		if n%sinkFlushEvery == 0 {
			if err := bw.Flush(); err != nil {
				return n, err
			}
		}
	}
	if err := c.Err(); err != nil {
		_ = bw.Flush()
		return n, err
	}
	return n, bw.Flush()
}

// ErrValueTooLarge is returned when a scanned value exceeds its size cap.
var ErrValueTooLarge = errors.New("squealx: value exceeds maximum allowed size")

// LimitedBytes is a sql.Scanner that copies a column value once, failing with
// ErrValueTooLarge if it is longer than Max bytes (Max <= 0 means unlimited).
//
// Note: database/sql has no true column streaming; the driver has already
// materialized the value by the time Scan is called. LimitedBytes protects
// the application from retaining or processing huge values, not the driver
// from reading them. For real streaming use driver features such as pgx
// large objects.
type LimitedBytes struct {
	Max  int64
	Data []byte
}

// Scan implements sql.Scanner.
func (l *LimitedBytes) Scan(src any) error {
	var b []byte
	switch x := src.(type) {
	case nil:
		l.Data = nil
		return nil
	case []byte:
		b = x
	case string:
		if l.Max > 0 && int64(len(x)) > l.Max {
			return ErrValueTooLarge
		}
		l.Data = []byte(x)
		return nil
	default:
		return fmt.Errorf("squealx: LimitedBytes cannot scan %T", src)
	}
	if l.Max > 0 && int64(len(b)) > l.Max {
		return ErrValueTooLarge
	}
	l.Data = append(l.Data[:0:0], b...)
	return nil
}

// RawColumn scans the current row's column col (0-based) of rows into a
// sql.RawBytes and returns an io.Reader over it, without copying. The other
// columns are scanned into throwaway targets. The reader is valid only until
// the next call to rows.Next or rows.Close; a NULL value yields an empty
// reader and ok=false. This is a convenience, not true streaming: see
// LimitedBytes.
func RawColumn(rows *Rows, col int) (r io.Reader, ok bool, err error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, false, err
	}
	if col < 0 || col >= len(cols) {
		return nil, false, fmt.Errorf("squealx: column index %d out of range", col)
	}
	targets := make([]any, len(cols))
	raws := make([]sql.RawBytes, len(cols))
	for i := range targets {
		targets[i] = &raws[i]
	}
	if err := rows.Scan(targets...); err != nil {
		return nil, false, err
	}
	b := raws[col]
	if b == nil {
		return bytes.NewReader(nil), false, nil
	}
	return bytes.NewReader(b), true, nil
}
