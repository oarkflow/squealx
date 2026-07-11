package squealx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/oarkflow/squealx/reflectx"
)

var (
	ErrCursorClosed      = errors.New("squealx: cursor is closed")
	ErrCursorLimit       = errors.New("squealx: cursor row limit reached")
	ErrCursorUnsupported = errors.New("squealx: unsupported cursor result type")
)

// CursorConfig controls streaming cursor behavior. A zero value is ready to use.
// MaxRows is a defensive upper bound; zero means unlimited. Cursors close
// automatically at EOF unless KeepOpen is explicitly set.
type CursorConfig struct {
	MaxRows  int64
	KeepOpen bool
}

// CursorStats is a cheap snapshot of cursor progress.
type CursorStats struct {
	Rows      int64
	StartedAt time.Time
	Elapsed   time.Duration
	Closed    bool
}

type cursorMode uint8

const (
	cursorScalar cursorMode = iota
	cursorStruct
	cursorMap
)

// Cursor is a pull-based, O(1)-memory row iterator. It prepares the scan plan
// once and reuses both the destination value and scan argument slice for every
// row. That avoids the per-row reflection plan and slice allocations incurred
// by callback- or channel-based iterators.
//
// Cursor is intentionally not safe for concurrent use. For pointer and map T,
// Value returns storage reused by the next call to Next. Process it before
// advancing, or call Copy when retaining the row.
type Cursor[T any] struct {
	rows       *Rows
	config     CursorConfig
	mode       cursorMode
	baseType   reflect.Type
	isPointer  bool
	value      T
	valueValue reflect.Value
	scanArgs   []any
	columns    []string
	colTypes   []*sql.ColumnType
	rawValues  []any
	mapper     *reflectx.Mapper
	err        error
	closed     bool
	startedAt  time.Time
	rowsRead   int64
}

// QueryCursor opens a typed streaming cursor for a positional query.
func QueryCursor[T any](ctx context.Context, db *DB, query string, args ...any) (*Cursor[T], error) {
	return QueryCursorConfig[T](ctx, db, CursorConfig{}, query, args...)
}

// QueryCursorConfig opens a typed streaming cursor with explicit limits.
func QueryCursorConfig[T any](ctx context.Context, db *DB, config CursorConfig, query string, args ...any) (*Cursor[T], error) {
	if db == nil {
		return nil, errors.New("squealx: nil DB")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	rows, err := db.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	cursor, err := CursorFromRowsConfig[T](rows, config)
	if err != nil {
		_ = rows.Close()
		return nil, err
	}
	return cursor, nil
}

// NamedQueryCursor opens a cursor for a named query, including named slice/IN
// expansion supported by Squealx's named binder.
func NamedQueryCursor[T any](ctx context.Context, db *DB, query string, arg any) (*Cursor[T], error) {
	if db == nil {
		return nil, errors.New("squealx: nil DB")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	rows, err := db.NamedQueryContext(ctx, query, arg)
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

// InQueryCursor expands positional slice arguments and opens a cursor.
func InQueryCursor[T any](ctx context.Context, db *DB, query string, args ...any) (*Cursor[T], error) {
	if db == nil {
		return nil, errors.New("squealx: nil DB")
	}
	expanded, params, err := db.In(query, args...)
	if err != nil {
		return nil, err
	}
	return QueryCursor[T](ctx, db, expanded, params...)
}

// CursorFromRows creates a cursor from rows already opened by DB, Tx, Conn, or
// a prepared statement.
func CursorFromRows[T any](rows *Rows) (*Cursor[T], error) {
	return CursorFromRowsConfig[T](rows, CursorConfig{})
}

func CursorFromRowsConfig[T any](rows *Rows, config CursorConfig) (*Cursor[T], error) {
	if rows == nil || rows.SQLRows == nil {
		return nil, errors.New("squealx: nil rows")
	}
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	c := &Cursor[T]{
		rows:      rows,
		config:    config,
		columns:   columns,
		colTypes:  colTypes,
		mapper:    rows.Mapper,
		startedAt: time.Now(),
	}
	if c.mapper == nil {
		c.mapper = mapper()
	}
	if err := c.prepare(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Cursor[T]) prepare() error {
	typ := reflect.TypeOf((*T)(nil)).Elem()
	if typ.Kind() == reflect.Interface {
		return fmt.Errorf("%w: interface type %s is ambiguous", ErrCursorUnsupported, typ)
	}
	c.isPointer = typ.Kind() == reflect.Pointer
	c.baseType = typ
	if c.isPointer {
		c.baseType = typ.Elem()
		ptr := reflect.New(c.baseType)
		c.value = ptr.Interface().(T)
		c.valueValue = ptr.Elem()
	} else {
		c.valueValue = reflect.ValueOf(&c.value).Elem()
	}

	if c.baseType.Kind() == reflect.Map {
		if c.baseType.Key().Kind() != reflect.String {
			return fmt.Errorf("%w: cursor maps must use string keys, got %s", ErrCursorUnsupported, c.baseType)
		}
		c.mode = cursorMap
		if c.valueValue.IsNil() {
			c.valueValue.Set(reflect.MakeMapWithSize(c.baseType, len(c.columns)))
		}
		c.rawValues = make([]any, len(c.columns))
		c.scanArgs = make([]any, len(c.columns))
		for i := range c.rawValues {
			c.scanArgs[i] = &c.rawValues[i]
		}
		return nil
	}

	if isScannable(c.baseType) {
		if len(c.columns) != 1 {
			return fmt.Errorf("squealx: scalar cursor type %s requires exactly one column, got %d", c.baseType, len(c.columns))
		}
		c.mode = cursorScalar
		if c.isPointer {
			c.scanArgs = []any{reflect.ValueOf(c.value).Interface()}
		} else {
			c.scanArgs = []any{c.valueValue.Addr().Interface()}
		}
		return nil
	}

	if c.baseType.Kind() != reflect.Struct {
		return fmt.Errorf("%w: %s", ErrCursorUnsupported, c.baseType)
	}
	c.mode = cursorStruct
	fields := c.mapper.TraversalsByName(c.baseType, c.columns)
	if missing := firstMissingField(fields); missing >= 0 && !c.rows.unsafe {
		return fmt.Errorf("missing destination name %s in cursor type %s", c.columns[missing], c.baseType)
	}
	c.scanArgs = make([]any, len(c.columns))
	if err := fieldsByTraversal(reflectx.NewObjectContext(), c.valueValue, fields, c.scanArgs, true); err != nil {
		return err
	}
	return nil
}

func firstMissingField(fields [][]int) int {
	for i, field := range fields {
		if len(field) == 0 {
			return i
		}
	}
	return -1
}

// Next advances and decodes one row. It returns false on EOF, cancellation,
// decoding error, or after Close. Inspect Err to distinguish EOF from failure.
func (c *Cursor[T]) Next() bool {
	if c == nil {
		return false
	}
	if c.closed {
		if c.err == nil && c.rowsRead == 0 {
			c.err = ErrCursorClosed
		}
		return false
	}
	if c.config.MaxRows > 0 && c.rowsRead >= c.config.MaxRows {
		c.err = ErrCursorLimit
		_ = c.Close()
		return false
	}
	if !c.rows.Next() {
		if err := c.rows.Err(); err != nil {
			c.err = err
		}
		if !c.config.KeepOpen {
			_ = c.Close()
		}
		return false
	}

	if err := c.rows.Scan(c.scanArgs...); err != nil {
		c.err = err
		_ = c.Close()
		return false
	}
	if c.mode == cursorMap {
		if err := c.materializeMap(); err != nil {
			c.err = err
			_ = c.Close()
			return false
		}
	}
	c.rowsRead++
	return true
}

func (c *Cursor[T]) materializeMap() error {
	c.valueValue.Clear()
	valueType := c.baseType.Elem()
	for i, column := range c.columns {
		raw := bytesToAny(c.rawValues[i], columnTypeName(c.colTypes, i))
		value, err := convertMapValue(raw, valueType)
		if err != nil {
			return fmt.Errorf("column %q: %w", column, err)
		}
		c.valueValue.SetMapIndex(reflect.ValueOf(column), value)
	}
	return nil
}

// Value returns the current row. It is valid only after Next returned true.
func (c *Cursor[T]) Value() T {
	if c == nil {
		var zero T
		return zero
	}
	return c.value
}

// NextValue combines Next and Value.
func (c *Cursor[T]) NextValue() (T, bool) {
	if c.Next() {
		return c.value, true
	}
	var zero T
	return zero, false
}

// Copy returns a shallow, independently addressable copy of the current row.
// For pointer T it allocates a new pointed-to value; for map T it allocates a
// new map. Nested pointers/slices remain shallow copies by design.
func (c *Cursor[T]) Copy() T {
	if c == nil {
		var zero T
		return zero
	}
	if c.isPointer {
		copyValue := reflect.New(c.baseType)
		copyValue.Elem().Set(c.valueValue)
		return copyValue.Interface().(T)
	}
	if c.mode == cursorMap {
		copyMap := reflect.MakeMapWithSize(c.baseType, c.valueValue.Len())
		iter := c.valueValue.MapRange()
		for iter.Next() {
			copyMap.SetMapIndex(iter.Key(), iter.Value())
		}
		return copyMap.Interface().(T)
	}
	return c.value
}

// Each consumes the cursor. Pointer and map rows use reusable storage; call
// Copy inside the callback when retaining a row.
func (c *Cursor[T]) Each(callback func(T) error) error {
	if callback == nil {
		return errors.New("squealx: nil cursor callback")
	}
	defer c.Close()
	for c.Next() {
		if err := callback(c.value); err != nil {
			c.err = err
			return err
		}
	}
	return c.Err()
}

// Collect consumes up to limit rows and returns independent row values. A zero
// limit means unlimited; a negative limit is invalid.
func (c *Cursor[T]) Collect(limit int) ([]T, error) {
	if limit < 0 {
		return nil, errors.New("squealx: cursor collect limit must not be negative")
	}
	result := make([]T, 0, maxCursorCapacity(limit))
	defer c.Close()
	for (limit == 0 || len(result) < limit) && c.Next() {
		result = append(result, c.Copy())
	}
	if err := c.Err(); err != nil {
		return result, err
	}
	return result, nil
}

func maxCursorCapacity(limit int) int {
	if limit <= 0 {
		return 0
	}
	if limit > 1024 {
		return 1024
	}
	return limit
}

func (c *Cursor[T]) Columns() []string {
	if c == nil {
		return nil
	}
	return append([]string(nil), c.columns...)
}

func (c *Cursor[T]) Err() error {
	if c == nil {
		return ErrCursorClosed
	}
	return c.err
}

func (c *Cursor[T]) Close() error {
	if c == nil || c.closed {
		return nil
	}
	c.closed = true
	if c.rows == nil {
		return nil
	}
	if err := c.rows.Close(); err != nil {
		if c.err == nil {
			c.err = err
		}
		return err
	}
	return nil
}

func (c *Cursor[T]) Stats() CursorStats {
	if c == nil {
		return CursorStats{Closed: true}
	}
	return CursorStats{
		Rows:      c.rowsRead,
		StartedAt: c.startedAt,
		Elapsed:   time.Since(c.startedAt),
		Closed:    c.closed,
	}
}
