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
	ErrCursorClosed        = errors.New("squealx: cursor is closed")
	ErrCursorLimit         = errors.New("squealx: cursor row limit reached")
	ErrCursorUnsupported   = errors.New("squealx: unsupported cursor result type")
	ErrInvalidCursorConfig = errors.New("squealx: invalid cursor configuration")
)

// CursorConfig controls streaming cursor behavior. A zero value is ready to use.
// MaxRows is a defensive upper bound; zero means unlimited. Cursors close
// automatically at EOF unless KeepOpen is explicitly set.
type CursorConfig struct {
	MaxRows  int64
	KeepOpen bool
	// FetchSize, when > 0 and the DB's driver is PostgreSQL (lib/pq, pgx, or
	// a common cloud variant), opens the cursor as a named server-side
	// cursor (DECLARE ... CURSOR FOR, paged with FETCH FORWARD FetchSize)
	// instead of a plain query. This matters specifically for PostgreSQL:
	// its drivers fetch a query's full result set server-side before the
	// first row is returned, unlike go-sql-driver/mysql, which already
	// streams rows without this problem — FetchSize has no effect for
	// MySQL or any other driver, since there's no equivalent buffering to
	// avoid there.
	FetchSize int
}

func (config CursorConfig) validate() error {
	if config.MaxRows < 0 {
		return fmt.Errorf("%w: MaxRows cannot be negative", ErrInvalidCursorConfig)
	}
	if config.FetchSize < 0 {
		return fmt.Errorf("%w: FetchSize cannot be negative", ErrInvalidCursorConfig)
	}
	return nil
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
	rows           *Rows
	config         CursorConfig
	mode           cursorMode
	baseType       reflect.Type
	isPointer      bool
	value          T
	valueValue     reflect.Value
	scanArgs       []any
	columns        []string
	colTypes       []*sql.ColumnType
	rawValues      []any
	objectContext  reflectx.ObjectContext
	mapper         *reflectx.Mapper
	err            error
	closed         bool
	closedAt       time.Time
	explicitClosed bool
	startedAt      time.Time
	rowsRead       int64
}

// QueryCursor opens a typed streaming cursor for a positional query.
func QueryCursor[T any](ctx context.Context, db *DB, query string, args ...any) (*Cursor[T], error) {
	return QueryCursorConfig[T](ctx, db, CursorConfig{}, query, args...)
}

// QueryCursorConfig opens a typed streaming cursor with explicit limits.
func QueryCursorConfig[T any](ctx context.Context, db *DB, config CursorConfig, query string, args ...any) (*Cursor[T], error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	if db == nil {
		return nil, errors.New("squealx: nil DB")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if config.FetchSize > 0 && isPostgresDriver(db.driverName) {
		raw, err := openPostgresFetchCursorRows(ctx, db, query, args, config.FetchSize)
		rows, err := wrapRows(raw, err, db.Mapper, db.unsafe)
		return openCursorConfig[T](rows, err, config)
	}
	rows, err := db.QueryxContext(ctx, query, args...)
	return openCursorConfig[T](rows, err, config)
}

// NamedQueryCursor opens a cursor for a named query, including named slice/IN
// expansion supported by Squealx's named binder.
func NamedQueryCursor[T any](ctx context.Context, db *DB, query string, arg any) (*Cursor[T], error) {
	return NamedQueryCursorConfig[T](ctx, db, CursorConfig{}, query, arg)
}

// NamedQueryCursorConfig opens a named streaming cursor with explicit limits.
func NamedQueryCursorConfig[T any](ctx context.Context, db *DB, config CursorConfig, query string, arg any) (*Cursor[T], error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	if db == nil {
		return nil, errors.New("squealx: nil DB")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if config.FetchSize > 0 && isPostgresDriver(db.driverName) {
		bound, params, err := bindNamedMapper(BindType(db.driverName), query, arg, mapperFor(db))
		if err != nil {
			return nil, err
		}
		raw, err := openPostgresFetchCursorRows(ctx, db, bound, params, config.FetchSize)
		rows, err := wrapRows(raw, err, db.Mapper, db.unsafe)
		return openCursorConfig[T](rows, err, config)
	}
	rows, err := db.NamedQueryContext(ctx, query, arg)
	return openCursorConfig[T](rows, err, config)
}

// InQueryCursor expands positional slice arguments and opens a cursor.
func InQueryCursor[T any](ctx context.Context, db *DB, query string, args ...any) (*Cursor[T], error) {
	return InQueryCursorConfig[T](ctx, db, CursorConfig{}, query, args...)
}

// InQueryCursorConfig opens a slice/IN streaming cursor with explicit limits.
func InQueryCursorConfig[T any](ctx context.Context, db *DB, config CursorConfig, query string, args ...any) (*Cursor[T], error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	if db == nil {
		return nil, errors.New("squealx: nil DB")
	}
	expanded, params, err := db.In(query, args...)
	if err != nil {
		return nil, err
	}
	return QueryCursorConfig[T](ctx, db, config, expanded, params...)
}

// CursorFromRows creates a cursor from rows already opened by DB, Tx, Conn, or
// a prepared statement. Ownership of rows transfers to the cursor; rows are
// closed if cursor construction fails.
func CursorFromRows[T any](rows *Rows) (*Cursor[T], error) {
	return CursorFromRowsConfig[T](rows, CursorConfig{})
}

func CursorFromRowsConfig[T any](rows *Rows, config CursorConfig) (*Cursor[T], error) {
	if rows == nil || rows.SQLRows == nil {
		return nil, errors.New("squealx: nil rows")
	}
	closeOnError := func(err error) (*Cursor[T], error) {
		return nil, errors.Join(err, rows.Close())
	}
	if err := config.validate(); err != nil {
		return closeOnError(err)
	}
	columns, err := rows.Columns()
	if err != nil {
		return closeOnError(err)
	}
	c := &Cursor[T]{
		rows:      rows,
		config:    config,
		columns:   columns,
		mapper:    rows.Mapper,
		startedAt: time.Now(),
	}
	if c.mapper == nil {
		c.mapper = mapper()
	}
	if err := c.prepare(); err != nil {
		return closeOnError(err)
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
		// Column type metadata is only needed to convert raw map values. Avoid
		// asking the driver for it on the common struct and scalar cursor paths.
		colTypes, err := c.rows.ColumnTypes()
		if err != nil {
			return err
		}
		c.colTypes = colTypes
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
	fields := c.mapper.TraversalsByNameCached(c.baseType, c.columns)
	if missing := firstMissingField(fields); missing >= 0 && !c.rows.unsafe {
		return fmt.Errorf("missing destination name %s in cursor type %s", c.columns[missing], c.baseType)
	}
	c.scanArgs = make([]any, len(c.columns))
	if err := fieldsByTraversal(&c.objectContext, c.valueValue, fields, c.scanArgs, true); err != nil {
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
		if c.err == nil && c.explicitClosed {
			c.err = ErrCursorClosed
		}
		return false
	}
	if c.config.MaxRows > 0 && c.rowsRead >= c.config.MaxRows {
		// Distinguish a result that ended exactly at the limit from one that
		// was truncated. Probe once without scanning to avoid a false limit
		// error on ordinary EOF.
		if c.rows.Next() {
			c.err = ErrCursorLimit
			_ = c.closeInternal()
			return false
		}
		if err := c.rows.Err(); err != nil {
			c.err = err
		}
		if !c.config.KeepOpen {
			_ = c.closeInternal()
		}
		return false
	}
	if !c.rows.Next() {
		if err := c.rows.Err(); err != nil {
			c.err = err
		}
		if !c.config.KeepOpen {
			_ = c.closeInternal()
		}
		return false
	}

	if c.mode == cursorStruct {
		// Clear the reused destination so pointer fields are re-allocated
		// per row and a retained (copied) row never aliases the next one,
		// and no stale values survive from the previous row.
		c.valueValue.SetZero()
	}
	if err := c.rows.Scan(c.scanArgs...); err != nil {
		c.err = err
		_ = c.closeInternal()
		return false
	}
	if c.mode == cursorMap {
		if err := c.materializeMap(); err != nil {
			c.err = err
			_ = c.closeInternal()
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
		raw := mapScanRawValue(c.rawValues[i], columnTypeName(c.colTypes, i), valueType)
		value, err := convertMapValue(raw, valueType)
		if err != nil {
			return fmt.Errorf("column %q: %w", column, err)
		}
		c.valueValue.SetMapIndex(reflect.ValueOf(column), value)
	}
	return nil
}

// Value returns the current row. It is valid after Next returns true and until
// the next Next call or Close. Use Copy to retain pointer or map rows.
func (c *Cursor[T]) Value() T {
	if c == nil {
		var zero T
		return zero
	}
	return c.value
}

// ValueRef returns a pointer to the cursor's current value slot without
// copying a struct row. The reference is valid until the next Next call or
// Close. It refers to reusable cursor storage and must not be retained; use
// Copy when an independent value is needed.
func (c *Cursor[T]) ValueRef() *T {
	if c == nil {
		return nil
	}
	return &c.value
}

// NextRef advances the cursor and returns a reference to the current value
// slot. The reference is valid until the next Next call or Close.
func (c *Cursor[T]) NextRef() (*T, bool) {
	if !c.Next() {
		return nil, false
	}
	return &c.value, true
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
	if c.closed {
		var zero T
		return zero
	}
	if c.mode == cursorMap {
		copyMap := reflect.MakeMapWithSize(c.baseType, c.valueValue.Len())
		iter := c.valueValue.MapRange()
		for iter.Next() {
			copyMap.SetMapIndex(iter.Key(), iter.Value())
		}
		if c.isPointer {
			copyValue := reflect.New(c.baseType)
			copyValue.Elem().Set(copyMap)
			return copyValue.Interface().(T)
		}
		return copyMap.Interface().(T)
	}
	if c.isPointer {
		copyValue := reflect.New(c.baseType)
		copyValue.Elem().Set(c.valueValue)
		return copyValue.Interface().(T)
	}
	return c.value
}

// Each consumes the cursor. Pointer and map rows use reusable storage; call
// Copy inside the callback when retaining a row.
func (c *Cursor[T]) Each(callback func(T) error) (returnErr error) {
	if callback == nil {
		return joinCloseError(errors.New("squealx: nil cursor callback"), c.closeInternal())
	}
	defer func() {
		returnErr = joinCloseError(returnErr, c.closeInternal())
	}()
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
func (c *Cursor[T]) Collect(limit int) (result []T, returnErr error) {
	if limit < 0 {
		return nil, joinCloseError(errors.New("squealx: cursor collect limit must not be negative"), c.closeInternal())
	}
	result = make([]T, 0, maxCursorCapacity(limit))
	defer func() {
		returnErr = joinCloseError(returnErr, c.closeInternal())
	}()
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

// Columns returns a copy of the result column names, or nil after Close.
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
	c.explicitClosed = true
	return c.closeInternal()
}

func (c *Cursor[T]) closeInternal() error {
	if c == nil || c.closed {
		return nil
	}
	c.closed = true
	// A caller may keep the cursor value after closing it. Drop driver scan
	// buffers and scan metadata now so a large last row or result schema does
	// not remain live for the lifetime of that cursor value.
	clear(c.rawValues)
	c.rawValues = nil
	c.scanArgs = nil
	c.colTypes = nil
	c.mapper = nil
	c.baseType = nil
	c.objectContext.NewRow(reflect.Value{})
	clear(c.columns)
	c.columns = nil
	if c.valueValue.IsValid() {
		// Release the last decoded row promptly. Value is documented as valid
		// only until the cursor advances or closes, and retained rows should be
		// copied explicitly through Copy.
		c.valueValue.SetZero()
	}
	if c.isPointer {
		var zero T
		c.value = zero
	}
	c.valueValue = reflect.Value{}
	rows := c.rows
	c.rows = nil
	if rows == nil {
		c.closedAt = time.Now()
		return nil
	}
	err := rows.Close()
	c.closedAt = time.Now()
	if err != nil {
		c.err = joinCloseError(c.err, err)
		return err
	}
	return nil
}

func (c *Cursor[T]) Stats() CursorStats {
	if c == nil {
		return CursorStats{Closed: true}
	}
	elapsed := time.Since(c.startedAt)
	if c.closed && !c.closedAt.IsZero() {
		elapsed = c.closedAt.Sub(c.startedAt)
	}
	return CursorStats{
		Rows:      c.rowsRead,
		StartedAt: c.startedAt,
		Elapsed:   elapsed,
		Closed:    c.closed,
	}
}
