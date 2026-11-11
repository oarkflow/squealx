package squealx

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	defaultKeysetLimit         = 20
	maxKeysetLimit             = 10000
	maxKeysetCursorEncodedSize = 64 << 10
)

// KeyColumn names a column participating in the keyset ordering. The
// combination of key columns must be unique per row and non-NULL for
// traversal to be exact.
type KeyColumn struct {
	Name string
	Desc bool
}

// KeysetPage is one page of a keyset (seek) paginated result.
type KeysetPage[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"next_cursor,omitempty"`
	HasMore    bool   `json:"has_more"`
}

// ErrInvalidKeysetCursor is returned when a cursor cannot be decoded.
var ErrInvalidKeysetCursor = errors.New("squealx: invalid keyset cursor")

// ErrInvalidKeysetRow indicates that a row cannot form a seek cursor, usually
// because one of its configured key columns is NULL.
var ErrInvalidKeysetRow = errors.New("squealx: invalid keyset row")

// EncodeKeysetCursor encodes scalar key values as base64url JSON. Composite
// values and custom JSON marshalers are rejected so oversized payloads can be
// bounded before full encoding.
func EncodeKeysetCursor(values []any) (string, error) {
	if err := validateKeysetCursorValues(values); err != nil {
		return "", err
	}
	b, err := json.Marshal(values)
	if err != nil {
		return "", err
	}
	if base64.RawURLEncoding.EncodedLen(len(b)) > maxKeysetCursorEncodedSize {
		return "", fmt.Errorf("%w: encoded cursor exceeds %d bytes", ErrInvalidKeysetCursor, maxKeysetCursorEncodedSize)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

const maxKeysetCursorJSONSize = maxKeysetCursorEncodedSize * 3 / 4

// validateKeysetCursorValues bounds temporary encoding memory and keeps the
// encoder's output within the scalar format accepted by DecodeKeysetCursor.
func validateKeysetCursorValues(values []any) error {
	if len(values) > maxKeysetCursorJSONSize {
		return fmt.Errorf("%w: too many key values", ErrInvalidKeysetCursor)
	}
	size := 2 // JSON array brackets.
	for i, value := range values {
		if i > 0 {
			size++ // comma
		}
		var encodedSize int
		switch x := value.(type) {
		case nil:
			encodedSize = 4
		case []byte:
			if x == nil {
				encodedSize = 4 // null
			} else if len(x) > maxKeysetCursorJSONSize {
				return fmt.Errorf("%w: encoded cursor exceeds %d bytes", ErrInvalidKeysetCursor, maxKeysetCursorEncodedSize)
			} else {
				encodedSize = 2 + base64.StdEncoding.EncodedLen(len(x))
			}
		case time.Time:
			encoded, err := json.Marshal(x)
			if err != nil {
				return err
			}
			encodedSize = len(encoded)
		case json.Number:
			if len(x) > maxKeysetCursorJSONSize {
				return fmt.Errorf("%w: encoded cursor exceeds %d bytes", ErrInvalidKeysetCursor, maxKeysetCursorEncodedSize)
			}
			encoded, err := json.Marshal(x)
			if err != nil {
				return err
			}
			encodedSize = len(encoded)
		default:
			if value == nil {
				encodedSize = 4
				break
			}
			if _, ok := value.(json.Marshaler); ok {
				return fmt.Errorf("%w: custom JSON marshalers are not supported for key values", ErrInvalidKeysetCursor)
			}
			rv := reflect.ValueOf(value)
			switch rv.Kind() {
			case reflect.String:
				encodedSize = keysetJSONStringSize(rv.String())
			case reflect.Bool:
				if rv.Bool() {
					encodedSize = 4
				} else {
					encodedSize = 5
				}
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				var scratch [24]byte
				encodedSize = len(strconv.AppendInt(scratch[:0], rv.Int(), 10))
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				var scratch [24]byte
				encodedSize = len(strconv.AppendUint(scratch[:0], rv.Uint(), 10))
			case reflect.Float32:
				encoded, err := json.Marshal(float32(rv.Float()))
				if err != nil {
					return err
				}
				encodedSize = len(encoded)
			case reflect.Float64:
				encoded, err := json.Marshal(rv.Float())
				if err != nil {
					return err
				}
				encodedSize = len(encoded)
			default:
				return fmt.Errorf("%w: unsupported key value type %T", ErrInvalidKeysetCursor, value)
			}
		}
		if encodedSize > maxKeysetCursorJSONSize-size {
			return fmt.Errorf("%w: encoded cursor exceeds %d bytes", ErrInvalidKeysetCursor, maxKeysetCursorEncodedSize)
		}
		size += encodedSize
	}
	return nil
}

// keysetJSONStringSize computes encoding/json's escaped size and exits as soon
// as the cursor's raw payload limit is exceeded.
func keysetJSONStringSize(value string) int {
	size := 2 // quotes
	for i := 0; i < len(value); {
		c := value[i]
		added := 1
		if c < utf8.RuneSelf {
			switch {
			case c == '"' || c == '\\' || c == '\b' || c == '\f' || c == '\n' || c == '\r' || c == '\t':
				added = 2
			case c < 0x20 || c == '<' || c == '>' || c == '&':
				added = 6
			}
			i++
		} else {
			r, width := utf8.DecodeRuneInString(value[i:])
			if r == utf8.RuneError && width == 1 {
				added = 3 // encoding/json replaces invalid UTF-8 with U+FFFD
				i++
			} else {
				if r == '\u2028' || r == '\u2029' {
					added = 6
				} else {
					added = width
				}
				i += width
			}
		}
		size += added
		if size > maxKeysetCursorJSONSize {
			return size
		}
	}
	return size
}

// DecodeKeysetCursor decodes a cursor produced by EncodeKeysetCursor. Numbers
// become int64 when integral, otherwise float64.
func DecodeKeysetCursor(cursor string) ([]any, error) {
	if len(cursor) > maxKeysetCursorEncodedSize {
		return nil, fmt.Errorf("%w: encoded cursor exceeds %d bytes", ErrInvalidKeysetCursor, maxKeysetCursorEncodedSize)
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidKeysetCursor, err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var values []any
	if err := dec.Decode(&values); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidKeysetCursor, err)
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("%w: trailing data", ErrInvalidKeysetCursor)
		}
		return nil, fmt.Errorf("%w: %v", ErrInvalidKeysetCursor, err)
	}
	for i, v := range values {
		switch x := v.(type) {
		case json.Number:
			if n, err := x.Int64(); err == nil {
				values[i] = n
			} else if f, err := x.Float64(); err == nil {
				values[i] = f
			} else {
				return nil, fmt.Errorf("%w: bad number", ErrInvalidKeysetCursor)
			}
		case nil, string, bool:
		default:
			return nil, fmt.Errorf("%w: unsupported value type", ErrInvalidKeysetCursor)
		}
	}
	return values, nil
}

func keysetColumn(name string) (string, error) {
	if err := validateIdentifier(name); err != nil {
		return "", err
	}
	// The base query is wrapped as a derived table, so only the output column
	// name is addressable.
	if i := strings.LastIndexByte(name, '.'); i >= 0 {
		name = name[i+1:]
	}
	return name, nil
}

func keysetRowValueSupported(driver string) bool {
	switch driver {
	case "mysql", "nrmysql", "mariadb",
		"sqlite", "sqlite3", "nrsqlite3",
		"postgres", "pgx", "pgx/v4", "pgx/v5", "pq-timeouts", "cloudsqlpostgres", "nrpostgres", "cockroach":
		return true
	}
	return false
}

// buildKeysetSQL builds the keyset query for the driver. When hasCursor is
// true it adds a seek predicate using :ks_after_N parameters.
func buildKeysetSQL(driver, baseQuery string, keys []KeyColumn, limit int, hasCursor bool) (string, error) {
	if len(keys) == 0 {
		return "", errors.New("squealx: keyset pagination requires at least one key column")
	}
	cols := make([]string, len(keys))
	order := make([]string, len(keys))
	sameDir := true
	for i, k := range keys {
		c, err := keysetColumn(k.Name)
		if err != nil {
			return "", err
		}
		cols[i] = c
		order[i] = c
		if k.Desc {
			order[i] += " DESC"
		} else {
			order[i] += " ASC"
		}
		if k.Desc != keys[0].Desc {
			sameDir = false
		}
	}
	base := stripPaging(baseQuery)
	if i := topLevelOrderByIndex(base); i >= 0 {
		base = strings.TrimSpace(base[:i])
	}
	if base == "" {
		return "", errors.New("squealx: empty keyset base query")
	}

	var where string
	if hasCursor {
		op := func(k KeyColumn) string {
			if k.Desc {
				return "<"
			}
			return ">"
		}
		if len(keys) == 1 {
			where = fmt.Sprintf("%s %s :ks_after_0", cols[0], op(keys[0]))
		} else if sameDir && keysetRowValueSupported(driver) {
			ph := make([]string, len(keys))
			for i := range keys {
				ph[i] = fmt.Sprintf(":ks_after_%d", i)
			}
			where = fmt.Sprintf("(%s) %s (%s)", strings.Join(cols, ", "), op(keys[0]), strings.Join(ph, ", "))
		} else {
			terms := make([]string, len(keys))
			for i := range keys {
				parts := make([]string, 0, i+1)
				for j := 0; j < i; j++ {
					parts = append(parts, fmt.Sprintf("%s = :ks_after_%d", cols[j], j))
				}
				parts = append(parts, fmt.Sprintf("%s %s :ks_after_%d", cols[i], op(keys[i]), i))
				terms[i] = "(" + strings.Join(parts, " AND ") + ")"
			}
			where = "(" + strings.Join(terms, " OR ") + ")"
		}
		where = " WHERE " + where
	}

	n := limit + 1
	orderBy := " ORDER BY " + strings.Join(order, ", ")
	switch driver {
	case "sql-server", "sqlserver", "mssql", "ms-sql":
		return fmt.Sprintf("SELECT TOP (%d) * FROM (%s) AS keyset_q%s%s", n, base, where, orderBy), nil
	case "oracle", "godror", "ora", "oci8":
		return fmt.Sprintf("SELECT * FROM (%s) keyset_q%s%s FETCH FIRST %d ROWS ONLY", base, where, orderBy, n), nil
	}
	return fmt.Sprintf("SELECT * FROM (%s) AS keyset_q%s%s LIMIT %d", base, where, orderBy, n), nil
}

// keysetValues extracts the key column values from a row of type T (struct,
// pointer to struct, or map with string keys).
func keysetValues(db *DB, row any, keys []KeyColumn) ([]any, error) {
	v := reflect.ValueOf(row)
	for v.IsValid() && (v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface) {
		if v.IsNil() {
			return nil, errors.New("squealx: nil keyset row")
		}
		v = v.Elem()
	}
	out := make([]any, len(keys))
	switch v.Kind() {
	case reflect.Map:
		if v.Type().Key().Kind() != reflect.String {
			return nil, ErrCursorUnsupported
		}
		for i, k := range keys {
			col, _ := keysetColumn(k.Name)
			mv := v.MapIndex(reflect.ValueOf(col).Convert(v.Type().Key()))
			if !mv.IsValid() {
				return nil, fmt.Errorf("squealx: key column %q missing from row", col)
			}
			out[i] = normalizeKeyValue(mv.Interface())
		}
	case reflect.Struct:
		m := db.Mapper
		if m == nil {
			m = mapper()
		}
		tm := m.TypeMap(v.Type())
		for i, k := range keys {
			col, _ := keysetColumn(k.Name)
			fi, ok := tm.Names[col]
			if !ok {
				return nil, fmt.Errorf("squealx: key column %q has no matching field in %s", col, v.Type())
			}
			out[i] = normalizeKeyValue(reflectxField(v, fi.Index))
		}
	default:
		return nil, ErrCursorUnsupported
	}
	return out, nil
}

func reflectxField(v reflect.Value, index []int) any {
	for _, i := range index {
		for v.Kind() == reflect.Pointer {
			if v.IsNil() {
				return nil
			}
			v = v.Elem()
		}
		v = v.Field(i)
	}
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}
	return v.Interface()
}

func normalizeKeyValue(v any) any {
	switch x := v.(type) {
	case []byte:
		return string(x)
	case nil:
		return nil
	}
	if vv := reflect.ValueOf(v); vv.IsValid() {
		switch vv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return vv.Int()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			u := vv.Uint()
			if u <= 1<<63-1 {
				return int64(u)
			}
			return u
		case reflect.Float32, reflect.Float64:
			return vv.Float()
		case reflect.String:
			return vv.String()
		case reflect.Bool:
			return vv.Bool()
		}
	}
	return v // e.g. time.Time; marshals as RFC3339 string
}

// KeysetPaginate returns up to limit rows of baseQuery ordered by keys,
// starting after the position encoded in after (empty for the first page).
// baseQuery must expose every key column in its output and should not contain
// its own ORDER BY/LIMIT (a top-level one is stripped). Key columns must be
// unique together and non-NULL. Rows are streamed through a Cursor.
func KeysetPaginate[T any](ctx context.Context, db *DB, baseQuery string, keys []KeyColumn, limit int, after string, params map[string]any) (page KeysetPage[T], returnErr error) {
	if db == nil {
		return page, errors.New("squealx: nil DB")
	}
	if limit < 0 {
		return page, fmt.Errorf("invalid keyset limit %d", limit)
	}
	if limit == 0 {
		limit = defaultKeysetLimit
	}
	if limit > maxKeysetLimit {
		limit = maxKeysetLimit
	}
	var cursorVals []any
	if after != "" {
		var err error
		cursorVals, err = DecodeKeysetCursor(after)
		if err != nil {
			return page, err
		}
		if len(cursorVals) != len(keys) {
			return page, fmt.Errorf("%w: expected %d values, got %d", ErrInvalidKeysetCursor, len(keys), len(cursorVals))
		}
		for _, v := range cursorVals {
			if v == nil {
				return page, fmt.Errorf("%w: null key value", ErrInvalidKeysetCursor)
			}
		}
	}
	query, err := buildKeysetSQL(db.driverName, baseQuery, keys, limit, after != "")
	if err != nil {
		return page, err
	}
	args := cloneMap(params)
	if args == nil {
		args = make(map[string]any, len(cursorVals))
	}
	for i, v := range cursorVals {
		args[fmt.Sprintf("ks_after_%d", i)] = v
	}
	cur, err := NamedQueryCursor[T](ctx, db, query, args)
	if err != nil {
		return page, err
	}
	defer func() {
		returnErr = joinCloseError(returnErr, cur.Close())
	}()

	page.Items = make([]T, 0, min(limit, 128))
	for cur.Next() {
		if len(page.Items) == limit {
			page.HasMore = true
			break
		}
		page.Items = append(page.Items, cur.Copy())
	}
	if err := cur.Err(); err != nil {
		return KeysetPage[T]{}, err
	}
	if page.HasMore {
		last := page.Items[len(page.Items)-1]
		vals, err := keysetValues(db, last, keys)
		if err != nil {
			return KeysetPage[T]{}, err
		}
		for i, value := range vals {
			if value == nil {
				return KeysetPage[T]{}, fmt.Errorf("%w: key column %q is NULL", ErrInvalidKeysetRow, keys[i].Name)
			}
		}
		if page.NextCursor, err = EncodeKeysetCursor(vals); err != nil {
			return KeysetPage[T]{}, err
		}
	}
	return page, nil
}
