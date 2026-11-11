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
	"math"
	"reflect"
	"sort"
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
	if rows == nil || rows.SQLRows == nil {
		return nil, errors.New("squealx: nil rows")
	}
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
			return appendJSONBytes(dst, x), nil
		}
		dst = append(dst, '"')
		dst = base64.StdEncoding.AppendEncode(dst, x)
		return append(dst, '"'), nil
	case string:
		return appendJSONString(dst, x)
	case time.Time:
		if year := x.Year(); year < 0 || year > 9999 {
			_, err := json.Marshal(x)
			return dst, err
		}
		_, offset := x.Zone()
		zoneHours := offset / 60 / 60
		if zoneHours <= -24 || zoneHours >= 24 {
			_, err := json.Marshal(x)
			return dst, err
		}
		dst = append(dst, '"')
		dst = x.AppendFormat(dst, time.RFC3339Nano)
		return append(dst, '"'), nil
	case bool:
		return strconv.AppendBool(dst, x), nil
	case int64:
		return strconv.AppendInt(dst, x, 10), nil
	case float64:
		if math.IsInf(x, 0) || math.IsNaN(x) {
			return dst, &json.UnsupportedValueError{
				Value: reflect.ValueOf(x),
				Str:   strconv.FormatFloat(x, 'g', -1, 64),
			}
		}
		format := byte('f')
		abs := math.Abs(x)
		if abs != 0 && (abs < 1e-6 || abs >= 1e21) {
			format = 'e'
		}
		dst = strconv.AppendFloat(dst, x, format, -1, 64)
		if format == 'e' {
			n := len(dst)
			if n >= 4 && dst[n-4] == 'e' && dst[n-3] == '-' && dst[n-2] == '0' {
				dst[n-2] = dst[n-1]
				dst = dst[:n-1]
			}
		}
		return dst, nil
	default:
		b, err := json.Marshal(x)
		if err != nil {
			return dst, err
		}
		return append(dst, b...), nil
	}
}

func appendJSONString(dst []byte, s string) ([]byte, error) {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			if c == '"' || c == '\\' || c < 0x20 || c == '<' || c == '>' || c == '&' {
				dst = append(dst, s[start:i]...)
				dst = appendJSONASCIIEscape(dst, c)
				i++
				start = i
				continue
			}
			i++
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			dst = append(dst, s[start:i]...)
			dst = append(dst, '\xef', '\xbf', '\xbd')
			i++
			start = i
			continue
		}
		if r == '\u2028' || r == '\u2029' {
			dst = append(dst, s[start:i]...)
			if r == '\u2028' {
				dst = append(dst, `\u2028`...)
			} else {
				dst = append(dst, `\u2029`...)
			}
			i += size
			start = i
			continue
		}
		i += size
	}
	dst = append(dst, s[start:]...)
	dst = append(dst, '"')
	return dst, nil
}

func appendJSONBytes(dst, data []byte) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(data); {
		c := data[i]
		if c < utf8.RuneSelf {
			if c == '"' || c == '\\' || c < 0x20 || c == '<' || c == '>' || c == '&' {
				dst = append(dst, data[start:i]...)
				dst = appendJSONASCIIEscape(dst, c)
				i++
				start = i
				continue
			}
			i++
			continue
		}
		r, size := utf8.DecodeRune(data[i:])
		if r == '\u2028' || r == '\u2029' {
			dst = append(dst, data[start:i]...)
			if r == '\u2028' {
				dst = append(dst, `\u2028`...)
			} else {
				dst = append(dst, `\u2029`...)
			}
			i += size
			start = i
			continue
		}
		i += size
	}
	dst = append(dst, data[start:]...)
	return append(dst, '"')
}

func appendJSONASCIIEscape(dst []byte, c byte) []byte {
	switch c {
	case '"', '\\':
		return append(dst, '\\', c)
	case '\b':
		return append(dst, `\b`...)
	case '\f':
		return append(dst, `\f`...)
	case '\n':
		return append(dst, `\n`...)
	case '\r':
		return append(dst, `\r`...)
	case '\t':
		return append(dst, `\t`...)
	case '<':
		return append(dst, `\u003c`...)
	case '>':
		return append(dst, `\u003e`...)
	case '&':
		return append(dst, `\u0026`...)
	default:
		dst = append(dst, `\u00`...)
		return append(dst, "0123456789abcdef"[c>>4], "0123456789abcdef"[c&0xf])
	}
}

// WriteJSONLines streams every row of rows to w as one JSON object per line.
// Only one row is held in memory at a time; output is buffered and flushed
// periodically. It honors ctx cancellation between rows, closes rows when
// done, and returns the number of rows written.
func WriteJSONLines(ctx context.Context, w io.Writer, rows *Rows) (written int64, returnErr error) {
	if rows == nil || rows.SQLRows == nil {
		return 0, errors.New("squealx: nil rows")
	}
	if w == nil {
		return 0, errors.Join(errors.New("squealx: nil writer"), rows.Close())
	}
	if ctx == nil {
		ctx = context.Background()
	}
	rowsClosed := false
	defer func() {
		if !rowsClosed {
			if closeErr := rows.Close(); closeErr != nil {
				returnErr = errors.Join(returnErr, closeErr)
			}
		}
	}()
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
	for {
		if err := ctx.Err(); err != nil {
			return n, errors.Join(err, bw.Flush())
		}
		if !rows.Next() {
			break
		}
		if err := sc.scan(rows); err != nil {
			return n, errors.Join(err, bw.Flush())
		}
		line = append(line[:0], '{')
		for i, v := range sc.vals {
			if i > 0 {
				line = append(line, ',')
			}
			line = append(line, keys[i]...)
			if line, err = appendJSONValue(line, v); err != nil {
				return n, errors.Join(fmt.Errorf("column %q: %w", sc.cols[i], err), bw.Flush())
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
		return n, errors.Join(err, bw.Flush())
	}
	flushErr := bw.Flush()
	closeErr := rows.Close()
	rowsClosed = true
	return n, errors.Join(flushErr, closeErr)
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
func WriteCSV(ctx context.Context, w io.Writer, rows *Rows, header bool) (written int64, returnErr error) {
	if rows == nil || rows.SQLRows == nil {
		return 0, errors.New("squealx: nil rows")
	}
	if w == nil {
		return 0, errors.Join(errors.New("squealx: nil writer"), rows.Close())
	}
	if ctx == nil {
		ctx = context.Background()
	}
	rowsClosed := false
	defer func() {
		if !rowsClosed {
			if closeErr := rows.Close(); closeErr != nil {
				returnErr = errors.Join(returnErr, closeErr)
			}
		}
	}()
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
	for {
		if err := ctx.Err(); err != nil {
			cw.Flush()
			return n, errors.Join(err, cw.Error())
		}
		if !rows.Next() {
			break
		}
		if err := sc.scan(rows); err != nil {
			cw.Flush()
			return n, errors.Join(err, cw.Error())
		}
		for i, v := range sc.vals {
			if rec[i], err = csvField(v); err != nil {
				cw.Flush()
				return n, errors.Join(err, cw.Error())
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
		return n, errors.Join(err, cw.Error())
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		closeErr := rows.Close()
		rowsClosed = true
		return n, errors.Join(err, closeErr)
	}
	closeErr := rows.Close()
	rowsClosed = true
	return n, errors.Join(cw.Error(), closeErr)
}

// WriteMapCursorJSONLines streams a map cursor to w as JSON lines. Keys are
// emitted in encoding/json (sorted) order. The cursor is closed when done.
func WriteMapCursorJSONLines(ctx context.Context, w io.Writer, c *Cursor[map[string]any]) (written int64, returnErr error) {
	if w == nil {
		if c == nil {
			return 0, errors.New("squealx: nil writer")
		}
		return 0, errors.Join(errors.New("squealx: nil writer"), c.Close())
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if c == nil {
		return 0, ErrCursorClosed
	}
	cursorClosed := false
	defer func() {
		if !cursorClosed {
			if closeErr := c.Close(); closeErr != nil {
				returnErr = errors.Join(returnErr, closeErr)
			}
		}
	}()
	bw := bufio.NewWriterSize(w, 64*1024)
	keys := make([]string, 0, len(c.columns))
	seen := make(map[string]struct{}, len(c.columns))
	for _, column := range c.columns {
		if _, exists := seen[column]; exists {
			continue
		}
		seen[column] = struct{}{}
		keys = append(keys, column)
	}
	sort.Strings(keys)
	var n int64
	var line []byte
	for {
		if err := ctx.Err(); err != nil {
			return n, errors.Join(err, bw.Flush())
		}
		if !c.Next() {
			break
		}
		m := c.Value()
		line = append(line[:0], '{')
		for i, key := range keys {
			if i > 0 {
				line = append(line, ',')
			}
			line, _ = appendJSONString(line, key)
			line = append(line, ':')
			var err error
			line, err = appendJSONValue(line, m[key])
			if err != nil {
				return n, errors.Join(fmt.Errorf("column %q: %w", key, err), bw.Flush())
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
	if err := c.Err(); err != nil {
		return n, errors.Join(err, bw.Flush())
	}
	flushErr := bw.Flush()
	closeErr := c.Close()
	cursorClosed = true
	return n, errors.Join(flushErr, closeErr)
}

// ErrValueTooLarge is returned when a scanned value exceeds its size cap.
var (
	ErrValueTooLarge          = errors.New("squealx: value exceeds maximum allowed size")
	ErrRawColumnScannerClosed = errors.New("squealx: raw column scanner is closed")
)

// LimitedBytes is a sql.Scanner that copies a column value, failing with
// ErrValueTooLarge if it is longer than Max bytes (Max <= 0 means unlimited).
// Set Reuse to reuse a destination buffer across scans; then Data is valid only
// until the next Scan call and must be copied if it needs to be retained.
//
// Note: database/sql has no true column streaming; the driver has already
// materialized the value by the time Scan is called. LimitedBytes protects
// the application from retaining or processing huge values, not the driver
// from reading them. For real streaming use driver features such as pgx
// large objects.
type LimitedBytes struct {
	Max   int64
	Reuse bool
	Data  []byte
}

const maxRetainedLimitedBytes = 64 << 10

// Scan implements sql.Scanner.
func (l *LimitedBytes) Scan(src any) error {
	if l == nil {
		return errors.New("squealx: nil LimitedBytes")
	}
	// A failed scan must never leave the prior row's value visible or retained.
	buffer := l.Data
	l.Data = nil
	if l.Reuse && cap(buffer) <= maxRetainedLimitedBytes {
		clear(buffer)
		buffer = buffer[:0]
	} else {
		buffer = nil
	}
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
		if l.Reuse {
			l.Data = append(buffer, x...)
		} else {
			l.Data = []byte(x)
		}
		return nil
	default:
		return fmt.Errorf("squealx: LimitedBytes cannot scan %T", src)
	}
	if l.Max > 0 && int64(len(b)) > l.Max {
		return ErrValueTooLarge
	}
	if l.Reuse {
		l.Data = append(buffer, b...)
	} else {
		l.Data = append([]byte(nil), b...)
	}
	return nil
}

// RawColumnScanner scans one column without copying and reuses its scan
// targets across rows. The returned bytes are valid only until the next call
// to rows.Next or rows.Close. A NULL column returns nil bytes and ok=false.
// This is not true driver-level streaming; see LimitedBytes.
type RawColumnScanner struct {
	targets  []any
	selected sql.RawBytes
	discard  sql.RawBytes
	closed   bool
}

// NewRawColumnScanner validates col and prepares reusable scan targets.
func NewRawColumnScanner(rows *Rows, col int) (*RawColumnScanner, error) {
	if rows == nil || rows.SQLRows == nil {
		return nil, errors.New("squealx: nil rows")
	}
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if col < 0 || col >= len(cols) {
		return nil, fmt.Errorf("squealx: column index %d out of range", col)
	}
	s := &RawColumnScanner{
		targets: make([]any, len(cols)),
	}
	for i := range s.targets {
		if i == col {
			s.targets[i] = &s.selected
		} else {
			s.targets[i] = &s.discard
		}
	}
	return s, nil
}

// Scan scans the current row and returns the selected column's borrowed bytes.
func (s *RawColumnScanner) Scan(rows *Rows) ([]byte, bool, error) {
	if s == nil || rows == nil || rows.SQLRows == nil {
		return nil, false, errors.New("squealx: nil raw column scanner or rows")
	}
	if s.closed {
		return nil, false, ErrRawColumnScannerClosed
	}
	if err := rows.Scan(s.targets...); err != nil {
		s.selected = nil
		s.discard = nil
		return nil, false, err
	}
	// All unselected columns share one discard target. Drop its final value so
	// the scanner retains only the selected column without a per-row clear loop.
	s.discard = nil
	b := s.selected
	if b == nil {
		return nil, false, nil
	}
	return b, true, nil
}

// Close releases the scanner's target and borrowed-byte references. It does
// not close rows, which remain owned by the caller.
func (s *RawColumnScanner) Close() error {
	if s == nil || s.closed {
		return nil
	}
	s.closed = true
	s.selected = nil
	s.discard = nil
	clear(s.targets)
	s.targets = nil
	return nil
}

// RawColumn scans the current row's column col (0-based) into a sql.RawBytes
// and returns an io.Reader over it, without copying. For repeated row scans,
// use RawColumnScanner to reuse the scan target allocations.
func RawColumn(rows *Rows, col int) (r io.Reader, ok bool, err error) {
	scanner, err := NewRawColumnScanner(rows, col)
	if err != nil {
		return nil, false, err
	}
	b, ok, err := scanner.Scan(rows)
	if err != nil {
		return nil, false, err
	}
	return bytes.NewReader(b), ok, nil
}
