package squealx

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"encoding/json"

	"github.com/oarkflow/date"

	"github.com/oarkflow/squealx/reflectx"
	"github.com/oarkflow/squealx/utils/sqlstr"
	"github.com/oarkflow/squealx/utils/xstrings"
)

// Although the NameMapper is convenient, in practice it should not
// be relied on except for application code.  If you are writing a library
// that uses sqlx, you should be aware that the name mappings you expect
// can be overridden by your user's application.

// NameMapper is used to map column names to struct field names.  By default,
// it uses strings.ToLower to lowercase struct field names.  It can be set
// to whatever you want, but it is encouraged to be set before sqlx is used
// as name-to-field mappings are cached after first use on a type.
var NameMapper = xstrings.ToSnakeCase
var origMapper = reflect.ValueOf(NameMapper)

// Rather than creating on init, this is created when necessary so that
// importers have time to customize the NameMapper.
var mpr *reflectx.Mapper

// mprMu protects mpr.
var mprMu sync.Mutex

// mapper returns a valid mapper using the configured NameMapper func.
func mapper() *reflectx.Mapper {
	mprMu.Lock()
	defer mprMu.Unlock()

	if mpr == nil {
		mpr = reflectx.NewMapperFunc("db", NameMapper)
	} else if origMapper != reflect.ValueOf(NameMapper) {
		// if NameMapper has changed, create a new mapper
		mpr = reflectx.NewMapperFunc("db", NameMapper)
		origMapper = reflect.ValueOf(NameMapper)
	}
	return mpr
}

// isScannable takes the reflect.Type and the actual dest value and returns
// whether or not it's Scannable.  Something is scannable if:
//   - it is not a struct, slice, or map
//   - it implements sql.Scanner
//   - it has no exported fields
func isScannable(t reflect.Type) bool {
	if reflect.PtrTo(t).Implements(_scannerInterface) {
		return true
	}
	if t.Kind() != reflect.Struct && t.Kind() != reflect.Slice && t.Kind() != reflect.Map {
		return true
	}

	// For structs, check exported fields
	if t.Kind() == reflect.Struct {
		return len(mapper().TypeMap(t).Index) == 0
	}

	// For slices and maps, return false to use field traversal with nullSafe
	return false
}

// ColScanner is an interface used by MapScan and SliceScan
type ColScanner interface {
	Columns() ([]string, error)
	ColumnTypes() ([]*sql.ColumnType, error)
	Scan(dest ...any) error
	Err() error
}

// Queryer is an interface used by Get and Select
type Queryer interface {
	Query(query string, args ...any) (SQLRows, error)
	Queryx(query string, args ...any) (*Rows, error)
	QueryRowx(query string, args ...any) *Row
}

// Execer is an interface used by MustExec and LoadFile
type Execer interface {
	Exec(query string, args ...any) (sql.Result, error)
}

// QueryIn is an interface used by InGet and InSelect
type QueryIn interface {
	Queryer
	In(query string, args ...any) (string, []any, error)
}

// ExecIn is an interface used by MustInExec and InExec
type ExecIn interface {
	Execer
	In(query string, args ...any) (string, []any, error)
}

// Binder is an interface for something which can bind queries (Tx, DB)
type binder interface {
	DriverName() string
	Rebind(string) string
	BindNamed(string, any) (string, []any, error)
}

// Ext is a union interface which can bind, query, and exec, used by
// NamedQuery and NamedExec.
type Ext interface {
	binder
	Queryer
	Execer
}

// Preparer is an interface used by Preparex.
type Preparer interface {
	Prepare(query string) (SQLStmt, error)
}

// determine if any of our extensions are unsafe
func isUnsafe(i any) bool {
	switch v := i.(type) {
	case Row:
		return v.unsafe
	case *Row:
		return v.unsafe
	case Rows:
		return v.unsafe
	case *Rows:
		return v.unsafe
	case NamedStmt:
		return v.Stmt.unsafe
	case *NamedStmt:
		return v.Stmt.unsafe
	case Stmt:
		return v.unsafe
	case *Stmt:
		return v.unsafe
	case qStmt:
		return v.unsafe
	case *qStmt:
		return v.unsafe
	case DB:
		return v.unsafe
	case *DB:
		return v.unsafe
	case Tx:
		return v.unsafe
	case *Tx:
		return v.unsafe
	case sql.Rows, *sql.Rows:
		return false
	default:
		return false
	}
}

func mapperFor(i any) *reflectx.Mapper {
	switch i := i.(type) {
	case DB:
		return i.Mapper
	case *DB:
		return i.Mapper
	case Tx:
		return i.Mapper
	case *Tx:
		return i.Mapper
	default:
		return mapper()
	}
}

var _scannerInterface = reflect.TypeOf((*sql.Scanner)(nil)).Elem()

// Row is a reimplementation of sql.Row in order to gain access to the underlying
// sql.Rows.Columns() data, necessary for StructScan.
type Row struct {
	err    error
	unsafe bool
	rows   SQLRows
	Mapper *reflectx.Mapper
}

// Scan is a fixed implementation of sql.Row.Scan, which does not discard the
// underlying error from the internal rows object if it exists.
func (r *Row) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}

	// TODO(bradfitz): for now we need to defensively clone all
	// []byte that the driver returned (not permitting
	// *RawBytes in Rows.Scan), since we're about to close
	// the Rows in our defer, when we return from this function.
	// the contract with the driver.Next(...) interface is that it
	// can return slices into read-only temporary memory that's
	// only valid until the next Scan/Close.  But the TODO is that
	// for a lot of drivers, this copy will be unnecessary.  We
	// should provide an optional interface for drivers to
	// implement to say, "don't worry, the []bytes that I return
	// from Next will not be modified again." (for instance, if
	// they were obtained from the network anyway) But for now we
	// don't care.
	defer r.rows.Close()
	for _, dp := range dest {
		if _, ok := dp.(*sql.RawBytes); ok {
			return errors.New("sql: RawBytes isn't allowed on Row.Scan")
		}
	}

	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	err := r.rows.Scan(dest...)
	if err != nil {
		return err
	}
	// Make sure the query can be processed to completion with no errors.
	if err := r.rows.Close(); err != nil {
		return err
	}
	return nil
}

// Columns returns the underlying sql.Rows.Columns(), or the deferred error usually
// returned by Row.Scan()
func (r *Row) Columns() ([]string, error) {
	if r == nil {
		return nil, errors.New("squealx: nil row")
	}
	if r.err != nil {
		return nil, r.err
	}
	if r.rows == nil {
		return nil, sql.ErrNoRows
	}
	return r.rows.Columns()
}

// ColumnTypes returns the underlying sql.Rows.ColumnTypes(), or the deferred error
func (r *Row) ColumnTypes() ([]*sql.ColumnType, error) {
	if r == nil {
		return nil, errors.New("squealx: nil row")
	}
	if r.err != nil {
		return nil, r.err
	}
	if r.rows == nil {
		return nil, sql.ErrNoRows
	}
	return r.rows.ColumnTypes()
}

// Err returns the error encountered while scanning.
func (r *Row) Err() error {
	if r == nil {
		return errors.New("squealx: nil row")
	}
	return r.err
}

// NewErrorRow creates a Row that returns err from Scan/Err. It is useful for
// wrappers such as database resolvers whose QueryRow-shaped APIs cannot return
// a separate error value.
func NewErrorRow(err error) *Row {
	if err == nil {
		err = errors.New("squealx: unknown row error")
	}
	return &Row{err: err, Mapper: mapper()}
}

// DB is a wrapper around sql.DB which keeps track of the driverName upon Open,
// used mostly to automatically bind named queries using the right bindvars.
type DB struct {
	SQLDB
	ID         string
	driverName string
	dbName     string
	unsafe     bool
	Mapper     *reflectx.Mapper
	hooks      *hookStore
}

// NewDb returns a new sqlx DB wrapper for a pre-existing *sql.DB.  The
// driverName of the original database is required for named query support.
func NewDb(db *sql.DB, driverName, id string) *DB {
	return &DB{SQLDB: WrapSQLDB(db), driverName: driverName, Mapper: mapper(), ID: id, hooks: newHookStore()}
}

// NewSQLDb returns a new sqlx DB wrapper for a pre-existing SQLDB.  The
// driverName of the original database is required for named query support.
func NewSQLDb(db SQLDB, driverName, id string) *DB {
	return &DB{SQLDB: db, driverName: driverName, Mapper: mapper(), ID: id, hooks: newHookStore()}
}

// OpenExist uses already opened connection instead of creating new one.
func OpenExist(driverName string, raw *sql.DB) *DB {
	return &DB{SQLDB: WrapSQLDB(raw), driverName: driverName, Mapper: mapper(), hooks: newHookStore()}
}

func (db *DB) GetDBName() (string, error) {
	if db.dbName != "" {
		return db.dbName, nil
	}
	// Query to get the current database name
	var dbName, query string
	switch strings.ToLower(db.driverName) {
	case "pgx", "postgres", "postgresql":
		query = "SELECT current_database()"
	case "mysql":
		query = "SELECT DATABASE()"
	case "mssql", "sqlserver":
		query = "SELECT DB_NAME()"
	case "sqlite", "sqlite3":
		// SQLite has no database name in the server sense. The second column
		// returned by PRAGMA database_list is the logical schema name (usually
		// "main"), which is the useful value for metadata queries.
		query = "SELECT name FROM pragma_database_list WHERE seq = 0"
	default:
		return "", fmt.Errorf("squealx: database name lookup is unsupported for driver %q", db.driverName)
	}
	err := db.QueryRow(query).Scan(&dbName)
	if err != nil {
		return "", err
	}
	db.dbName = dbName
	return dbName, nil
}

func (db *DB) handleBeforeHooks(ctx context.Context, query string, args ...any) (context.Context, string, []any, error) {
	var err error
	for _, hook := range db.hooks.snapshot().before {
		ctx, query, args, err = invokeHook("before", hook, ctx, query, args...)
		if err != nil {
			return ctx, query, args, err
		}
	}
	return ctx, query, args, nil
}

func (db *DB) handleAfterHooks(ctx context.Context, query string, args ...any) (context.Context, string, []any, error) {
	var err error
	for _, hook := range db.hooks.snapshot().after {
		ctx, query, args, err = invokeHook("after", hook, ctx, query, args...)
		if err != nil {
			return ctx, query, args, err
		}
	}
	return ctx, query, args, nil
}

func (db *DB) handleErrorHooks(ctx context.Context, err error, query string, args ...any) error {
	current := err
	for _, hook := range db.hooks.snapshot().onErr {
		if next := invokeErrorHook(hook, ctx, current, query, args...); next != nil {
			current = next
		}
	}
	return current
}

func (db *DB) Use(hooks ...any) {
	for _, hook := range hooks {
		if h, ok := hook.(BeforeHook); ok {
			db.UseBefore(h.Before)
		}

		if h, ok := hook.(AfterHook); ok {
			db.UseAfter(h.After)
		}

		if h, ok := hook.(ErrorerHook); ok {
			db.UseOnError(h.OnError)
		}
	}
}

func (db *DB) UseBefore(hooks ...Hook) {
	if db.hooks == nil {
		db.hooks = newHookStore()
	}
	db.hooks.addBefore(hooks...)
}

func (db *DB) UseAfter(hooks ...Hook) {
	if db.hooks == nil {
		db.hooks = newHookStore()
	}
	db.hooks.addAfter(hooks...)
}

func (db *DB) UseOnError(onError ...ErrorHook) {
	if db.hooks == nil {
		db.hooks = newHookStore()
	}
	db.hooks.addError(onError...)
}

func handleTwo[T any](fn func(query string, args []any) (T, error), db *DB, ctx context.Context, query string, args ...any) (T, error) {
	var t T
	if db.hooks.empty() {
		return fn(query, args)
	}
	ctx = withDriverName(ctx, db.driverName)
	ctx2, query, args, err := db.handleBeforeHooks(ctx, query, args...)
	if err != nil {
		return t, err
	}
	data, err := fn(query, args)
	if err != nil {
		return data, db.handleErrorHooks(ctx2, err, query, args...)
	}
	_, _, _, err = db.handleAfterHooks(ctx2, query, args...)
	if err != nil {
		closeIfPossible(data)
		return data, err
	}
	return data, nil
}

func (db *DB) Query(query string, args ...any) (SQLRows, error) {
	query, err := SanitizeQuery(query, args...)
	if err != nil {
		return nil, err
	}
	fn := func(query string, args []any) (SQLRows, error) {
		return db.SQLDB.Query(query, args...)
	}
	return handleTwo(fn, db, context.Background(), query, args...)
}

func (db *DB) QueryRow(query string, args ...any) SQLRow {
	query, err := SanitizeQuery(query, args...)
	if err != nil {
		return &Row{rows: nil, err: err, unsafe: db.unsafe, Mapper: db.Mapper}
	}
	fn := func(query string, args []any) (*Row, error) {
		rows, err := db.SQLDB.Query(query, args...)
		return &Row{rows: rows, err: err, unsafe: db.unsafe, Mapper: db.Mapper}, err
	}
	row, hookErr := handleTwo(fn, db, context.Background(), query, args...)
	if hookErr != nil {
		if row != nil {
			closeIfPossible(row.rows)
		}
		return &Row{err: hookErr, unsafe: db.unsafe, Mapper: db.Mapper}
	}
	if row == nil {
		return &Row{err: errors.New("squealx: query row returned no row wrapper"), unsafe: db.unsafe, Mapper: db.Mapper}
	}
	return row
}

func (db *DB) Exec(query string, args ...any) (sql.Result, error) {
	query, err := SanitizeQuery(query, args...)
	if err != nil {
		return nil, err
	}
	fn := func(query string, args []any) (sql.Result, error) {
		return db.SQLDB.Exec(query, args...)
	}
	return handleTwo(fn, db, context.Background(), query, args...)
}

// DriverName returns the driverName passed to the Open function for this DB.
func (db *DB) DriverName() string {
	return db.driverName
}

// Driver returns the driverName passed to the Open function for this DB.
func (db *DB) Driver() driver.Driver {
	return db.SQLDB.Driver()
}

func (db *DB) SetConnMaxLifetime(d time.Duration) {
	db.SQLDB.SetConnMaxLifetime(d)
}

func (db *DB) SetConnMaxIdleTime(d time.Duration) {
	db.SQLDB.SetConnMaxIdleTime(d)
}

func (db *DB) SetMaxIdleConns(n int) {
	db.SQLDB.SetMaxIdleConns(n)
}

func (db *DB) SetMaxOpenConns(n int) {
	db.SQLDB.SetMaxOpenConns(n)
}

func (db *DB) Stats() sql.DBStats {
	return db.SQLDB.Stats()
}

// Open is the same as sql.Open, but returns an *sqlx.DB instead.
func Open(driverName, dataSourceName, id string) (*DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	return &DB{SQLDB: WrapSQLDB(db), driverName: driverName, Mapper: mapper(), ID: id, hooks: newHookStore()}, err
}

// MustOpen is the same as sql.Open, but returns an *sqlx.DB instead and panics on error.
func MustOpen(driverName, dataSourceName, id string) *DB {
	db, err := Open(driverName, dataSourceName, id)
	if err != nil {
		panic(err)
	}
	return db
}

// MapperFunc sets a new mapper for this db using the default sqlx struct tag
// and the provided mapper function.
func (db *DB) MapperFunc(mf func(string) string) {
	db.Mapper = reflectx.NewMapperFunc("db", mf)
}

// Rebind transforms a query from QUESTION to the DB driver's bindvar type.
func (db *DB) Rebind(query string) string {
	return Rebind(BindType(db.driverName), query)
}

// Unsafe returns a version of DB which will silently succeed to scan when
// columns in the SQL result have no fields in the destination struct.
// sqlx.Stmt and sqlx.Tx which are created from this DB will inherit its
// safety behavior.
func (db *DB) Unsafe() *DB {
	return &DB{
		SQLDB:      db.SQLDB,
		ID:         db.ID,
		driverName: db.driverName,
		dbName:     db.dbName,
		unsafe:     true,
		Mapper:     db.Mapper,
		hooks:      db.hooks,
	}
}

// BindNamed binds a query using the DB driver's bindvar type.
func (db *DB) BindNamed(query string, arg any) (string, []any, error) {
	return bindNamedMapper(BindType(db.driverName), query, arg, db.Mapper)
}

// NamedQuery using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (db *DB) NamedQuery(query string, arg any) (*Rows, error) {
	query, err := SanitizeQuery(query, arg)
	if err != nil {
		return nil, err
	}
	return NamedQuery(db, query, arg)
}

// NamedSelect using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (db *DB) NamedSelect(dest any, query string, arg any) error {
	query, err := SanitizeQuery(query, arg)
	if err != nil {
		return err
	}
	if !IsNamedQuery(query) {
		return db.Select(dest, query, arg)
	}
	rows, err := NamedQuery(db, query, arg)
	if err != nil {
		return err
	}
	// if something happens here, we want to make sure the rows are Closed
	defer rows.Close()
	return ScannAll(rows, dest, false)
}

// NamedExec using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (db *DB) NamedExec(query string, arg any) (sql.Result, error) {
	query, err := SanitizeQuery(query, arg)
	if err != nil {
		return nil, err
	}
	fn := func(query string, args []any) (sql.Result, error) {
		if len(args) == 0 {
			return NamedExec(db, query, arg)
		}
		return NamedExec(db, query, args[0])
	}
	return handleTwo(fn, db, context.Background(), query, arg)
}

func (db *DB) NamedGet(dest any, query string, arg any) error {
	query, err := SanitizeQuery(query, arg)
	if err != nil {
		return err
	}
	if InReg.MatchString(query) {
		query, arg = prepareNamedInQuery(query, arg)
		q, p, err := bindNamedMapper(BindType(db.DriverName()), query, arg, mapperFor(db))
		if err != nil {
			return err
		}
		r := db.QueryRowx(q, p...)
		return r.scanAny(dest, false)
	}
	q, p, err := bindNamedMapper(BindType(db.DriverName()), query, arg, mapperFor(db))
	if err != nil {
		return err
	}
	r := db.QueryRowx(q, p...)
	return r.scanAny(dest, false)
}

// Select using this DB.
// Any placeholder parameters are replaced with supplied args.
func (db *DB) Select(dest any, query string, arguments ...any) error {
	if dest == nil {
		return errors.New("Select: destination must be a non-nil pointer")
	}
	var args []any
	if len(arguments) > 0 && arguments[0] != nil {
		switch ag := arguments[0].(type) {
		case map[string]any:
			if len(ag) > 0 {
				args = arguments
			}
		case map[string]string:
			if len(ag) > 0 {
				args = arguments
			}
		default:
			args = arguments
		}
	}
	sanitized, err := SanitizeQuery(query, args...)
	if err != nil {
		return err
	}
	t := reflect.TypeOf(dest)
	if t == nil || t.Kind() != reflect.Ptr {
		return errors.New("Select: must pass a pointer to slice or struct")
	}
	if reflect.ValueOf(dest).IsNil() {
		return errors.New("Select: destination must be a non-nil pointer")
	}

	if t.Elem().Kind() != reflect.Slice {
		if IsNamedQuery(sanitized) && len(args) > 0 {
			return db.NamedGet(dest, sanitized, args[0])
		}
		if InReg.MatchString(sanitized) {
			return db.InGet(dest, sanitized, args...)
		}
		return Get(db, dest, sanitized, args...)
	}

	if IsNamedQuery(sanitized) && len(args) > 0 {
		return db.NamedSelect(dest, sanitized, args[0])
	}
	if InReg.MatchString(sanitized) {
		return db.InSelect(dest, sanitized, args...)
	}
	return Select(db, dest, sanitized, args...)
}

// ExecWithReturn executes INSERT/UPDATE/DELETE, returning the full row via args.
// - If driver supports RETURNING, uses one statement with RETURNING *.
// - Otherwise, falls back:
//   - DELETE: SELECT before deleting.
//   - INSERT: NamedExec + LastInsertId + SELECT by PK.
//   - UPDATE: NamedExec + SELECT by PK.
//
// query: SQL with named (":field") or "?" placeholders.
// args: pointer to a struct or map[string]any for binding inputs and receiving outputs.
func (db *DB) ExecWithReturn(query string, args any) error {
	return db.ExecWithReturnContext(context.Background(), query, args)
}

// ExecWithReturnContext is the context-aware write-and-fetch variant. Drivers
// with native RETURNING support perform one atomic round trip. Other drivers
// execute the write and then fetch by the discovered primary key.
func (db *DB) ExecWithReturnContext(ctx context.Context, query string, args any) error {
	if db == nil {
		return errors.New("squealx: nil DB")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	sanitizedSQL, err := SanitizeQuery(query, args)
	if err != nil {
		return err
	}
	v := reflect.ValueOf(args)
	if !v.IsValid() || v.Kind() != reflect.Ptr || v.IsNil() {
		return fmt.Errorf("args must be a non-nil pointer to struct or map, got %T", args)
	}
	value := v.Elem().Interface()
	upper := strings.ToUpper(strings.TrimSpace(sanitizedSQL))
	isInsert := strings.HasPrefix(upper, "INSERT")
	isUpdate := strings.HasPrefix(upper, "UPDATE")
	isDelete := strings.HasPrefix(upper, "DELETE")
	if driverSupportsReturning(db.driverName) && (isInsert || isUpdate || isDelete) {
		return db.SmartSelectContext(ctx, args, WithReturning(sanitizedSQL), value)
	}

	tables := sqlstr.TableNames(sanitizedSQL)
	if len(tables) == 0 {
		_, err := db.NamedExecContext(ctx, sanitizedSQL, args)
		return err
	}
	table := tables[0]
	dbName, nameErr := db.GetDBName()
	if nameErr != nil {
		return fmt.Errorf("resolve database metadata: %w", nameErr)
	}
	fields, err := db.GetTableFields(table, dbName)
	if err != nil {
		return fmt.Errorf("resolve table metadata for %s: %w", table, err)
	}
	primaryKey := ""
	for _, field := range fields {
		if field.Key == "PRI" {
			primaryKey = field.Name
			break
		}
	}

	// DELETE fallback must capture the row before it is removed. Native
	// RETURNING-capable drivers never enter this path.
	if isDelete && primaryKey != "" {
		pkValue, err := db.extractPK(primaryKey, args)
		if err != nil {
			return err
		}
		if err := db.fetchByPKContext(ctx, table, primaryKey, pkValue, args); err != nil {
			return fmt.Errorf("delete fallback select: %w", err)
		}
	}

	result, err := db.NamedExecContext(ctx, sanitizedSQL, args)
	if err != nil {
		return err
	}
	if primaryKey == "" || isDelete {
		return nil
	}

	var pkValue any
	if isInsert {
		pkValue, err = db.extractPK(primaryKey, args)
		if err != nil {
			pkValue, err = result.LastInsertId()
		}
	} else if isUpdate {
		pkValue, err = db.extractPK(primaryKey, args)
	}
	if err != nil {
		return fmt.Errorf("write succeeded but primary key %q could not be resolved: %w", primaryKey, err)
	}
	return db.fetchByPKContext(ctx, table, primaryKey, pkValue, args)
}

func driverSupportsReturning(driverName string) bool {
	switch strings.ToLower(driverName) {
	case "postgres", "pgx", "cockroach", "sqlite", "sqlite3":
		return true
	default:
		return false
	}
}

func (db *DB) fetchByPKContext(ctx context.Context, table, primaryKey string, pkValue any, dest any) error {
	if err := validateIdentifier(table); err != nil {
		return err
	}
	if err := validateIdentifier(primaryKey); err != nil {
		return err
	}
	query := fmt.Sprintf("SELECT * FROM %s WHERE %s = :squealx_primary_key", table, primaryKey)
	return db.SmartSelectContext(ctx, dest, query, map[string]any{"squealx_primary_key": pkValue})
}

// extractPK retrieves a primary-key value without coercing UUID/string keys to
// int64. Struct db tags are honored and map key matching is case-insensitive.
func (db *DB) extractPK(primaryKey string, args any) (any, error) {
	value := reflect.ValueOf(args)
	if !value.IsValid() || value.Kind() != reflect.Ptr || value.IsNil() {
		return nil, fmt.Errorf("extractPK: args must be a non-nil pointer, got %T", args)
	}
	value = value.Elem()
	for value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface {
		if value.IsNil() {
			return nil, fmt.Errorf("extractPK: nil value")
		}
		value = value.Elem()
	}
	switch value.Kind() {
	case reflect.Map:
		for _, key := range value.MapKeys() {
			if key.Kind() == reflect.String && strings.EqualFold(key.String(), primaryKey) {
				item := value.MapIndex(key)
				if item.IsValid() {
					return item.Interface(), nil
				}
			}
		}
	case reflect.Struct:
		typeOf := value.Type()
		for i := 0; i < typeOf.NumField(); i++ {
			fieldType := typeOf.Field(i)
			column := strings.Split(fieldType.Tag.Get("db"), ",")[0]
			if column == "" {
				column = fieldType.Name
			}
			if strings.EqualFold(column, primaryKey) || strings.EqualFold(fieldType.Name, primaryKey) {
				field := value.Field(i)
				if field.CanInterface() {
					return field.Interface(), nil
				}
			}
		}
	default:
		return nil, fmt.Errorf("extractPK: unsupported kind %s", value.Kind())
	}
	return nil, fmt.Errorf("extractPK: primary key %q not found", primaryKey)
}

func (db *DB) LazyExec(query string) func(args ...any) (sql.Result, error) {
	return func(args ...any) (sql.Result, error) {
		query, err := SanitizeQuery(query, args...)
		if err != nil {
			return nil, err
		}
		return db.Exec(query, args...)
	}
}

func (db *DB) LazyExecWithReturn(query string) func(args any) error {
	return func(args any) error {
		query, err := SanitizeQuery(query, args)
		if err != nil {
			return err
		}
		return db.ExecWithReturn(query, args)
	}
}

func (db *DB) LazySelect(query string) func(dest any, args ...any) error {
	return func(dest any, args ...any) error {
		query, err := SanitizeQuery(query, args...)
		if err != nil {
			return err
		}
		return db.Select(dest, query, args...)
	}
}

func LazySelect[T any](db *DB, query string) func(args ...any) (T, error) {
	return func(args ...any) (T, error) {
		var t T
		query, err := SanitizeQuery(query, args...)
		if err != nil {
			return t, err
		}
		return SelectTyped[T](db, query, args...)
	}
}

func SelectTyped[T any](db *DB, query string, args ...any) (T, error) {
	var zero T
	typ := reflect.TypeOf((*T)(nil)).Elem()
	if typ.Kind() == reflect.Interface {
		return zero, fmt.Errorf("SelectTyped: interface result type %s is ambiguous; use a concrete scalar, struct, map, pointer, or slice", typ)
	}
	if typ.Kind() != reflect.Slice {
		query = LimitQueryForDriver(db.DriverName(), query)
	}
	if typ.Kind() == reflect.Ptr {
		value := reflect.New(typ.Elem()).Interface().(T)
		err := db.Select(value, query, args...)
		return value, err
	}
	var value T
	err := db.Select(&value, query, args...)
	return value, err
}

func LazySelectEach[T any](db *DB, callback func(row T) error, query string) func(args ...any) error {
	return func(args ...any) error {
		return SelectEach(db, callback, query, args...)
	}
}

func SelectEach[T any](db *DB, callback func(row T) error, query string, args ...any) error {
	if IsNamedQuery(query) && len(args) > 0 {
		rows, err := NamedQuery(db, query, args[0])
		if err != nil {
			return err
		}
		defer rows.Close()
		return ScanEach(rows, false, callback)
	}
	if InReg.MatchString(query) {
		newQuery, params, err := db.In(query, args...)
		if err != nil {
			return err
		}
		rows, err := db.Queryx(newQuery, params...)
		if err != nil {
			return err
		}
		// if something happens here, we want to make sure the rows are Closed
		defer rows.Close()
		return ScanEach(rows, false, callback)
	}
	rows, err := db.Queryx(query, args...)
	if err != nil {
		return err
	}
	// if something happens here, we want to make sure the rows are Closed
	defer rows.Close()
	return ScanEach(rows, false, callback)
}

// Get using this DB.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (db *DB) Get(dest any, query string, args ...any) error {
	if InReg.MatchString(query) {
		return InGet(db, dest, query, args...)
	}
	return Get(db, dest, query, args...)
}

// MustBegin starts a transaction, and panics on error.  Returns an *sqlx.Tx instead
// of an *sql.Tx.
func (db *DB) MustBegin() *Tx {
	tx, err := db.Beginx()
	if err != nil {
		panic(err)
	}
	return tx
}

// Beginx begins a transaction and returns an *sqlx.Tx instead of an *sql.Tx.
func (db *DB) Beginx() (*Tx, error) {
	tx, err := db.SQLDB.Begin()
	if err != nil {
		return nil, err
	}
	hooks := db.hooks.snapshot()
	return &Tx{SQLTx: tx, driverName: db.driverName, unsafe: db.unsafe, Mapper: db.Mapper, beforeHooks: hooks.before, afterHooks: hooks.after, onError: hooks.onErr, hookCtx: context.Background()}, nil
}

// Begin starts a transaction and do the given handle. The default isolation level
// is dependent on the driver.
//
// With uses context.Background internally; to specify the context, use
// With Tx.
func (db *DB) With(handle func(tx SQLTx) error) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = handle(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// BeginTx starts a transaction and do the given handle.
//
// The provided context is used until the transaction is committed or rolled back.
// If the context is canceled, the sql package will roll back
// the transaction. Tx.Commit will return an error if the context provided to
// BeginTx is canceled.
//
// The provided TxOptions is optional and may be nil if defaults should be used.
// If a non-default isolation level is used that the driver doesn't support,
// an error will be returned.
func (db *DB) WithTx(ctx context.Context, opts *sql.TxOptions, handle func(tx SQLTx) error) error {
	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = handle(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// Withx begins a transaction and returns an *sqlx.Tx instead of an *sql.Tx and do the given handle.
func (db *DB) Withx(handle func(tx *Tx) error) error {
	tx, err := db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = handle(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// WithTxx begins a transaction and returns an *sqlx.Tx instead of an
// *sql.Tx and do the give handle.
//
// The provided context is used until the transaction is committed or rolled
// back. If the context is canceled, the sql package will roll back the
// transaction. Tx.Commit will return an error if the context provided to
// BeginxContext is canceled.
func (db *DB) WithTxx(ctx context.Context, opts *sql.TxOptions, handle func(tx *Tx) error) error {
	tx, err := db.BeginTxx(ctx, opts)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = handle(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// In expands slice values in args, returning the modified query string
// and a new arg list that can be executed by a database. The `query` should
// use the `?` bindVar.  The return value uses had rebinded bindvar type.
func (db *DB) In(query string, args ...any) (string, []any, error) {
	query, err := SanitizeQuery(query, args...)
	if err != nil {
		return "", nil, err
	}
	q, params, err := In(query, args...)
	if err != nil {
		return "", nil, err
	}
	return db.Rebind(q), params, nil
}

// InExec executes a query without returning any rows for in.
// The args are for any placeholder parameters in the query.
//
// InExec uses context.Background internally; to specify the context, use
// ExecContext.
func (db *DB) InExec(query string, args ...any) (sql.Result, error) {
	query, err := SanitizeQuery(query, args...)
	if err != nil {
		return nil, err
	}
	fn := func(query string, args []any) (sql.Result, error) {
		return InExec(db, query, args...)
	}
	return handleTwo(fn, db, context.Background(), query, args...)
}

// InSelect using this DB but for in.
// Any placeholder parameters are replaced with supplied args.
func (db *DB) InSelect(dest any, query string, args ...any) error {
	query, err := SanitizeQuery(query, args...)
	if err != nil {
		return err
	}
	return InSelect(db, dest, query, args...)
}

// InGet using this DB but for in.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (db *DB) InGet(dest any, query string, args ...any) error {
	query, err := SanitizeQuery(query, args...)
	if err != nil {
		return err
	}
	return InGet(db, dest, query, args...)
}

// Queryx queries the database and returns an *sqlx.Rows.
// Any placeholder parameters are replaced with supplied args.
func (db *DB) Queryx(query string, args ...any) (*Rows, error) {
	query, err := SanitizeQuery(query, args...)
	if err != nil {
		return nil, err
	}
	fn := func(query string, args []any) (*Rows, error) {
		r, err := db.SQLDB.Query(query, args...)
		if err != nil {
			return nil, err
		}
		return &Rows{SQLRows: r, unsafe: db.unsafe, Mapper: db.Mapper}, err
	}
	return handleTwo(fn, db, context.Background(), query, args...)
}

// QueryRowx queries the database and returns an *sqlx.Row.
// Any placeholder parameters are replaced with supplied args.
func (db *DB) QueryRowx(query string, args ...any) *Row {
	query, err := SanitizeQuery(query, args...)
	if err != nil {
		return &Row{rows: nil, err: err, unsafe: db.unsafe, Mapper: db.Mapper}
	}
	fn := func(query string, args []any) (*Row, error) {
		rows, err := db.SQLDB.Query(query, args...)
		return &Row{rows: rows, err: err, unsafe: db.unsafe, Mapper: db.Mapper}, err
	}
	row, hookErr := handleTwo(fn, db, context.Background(), query, args...)
	if hookErr != nil {
		if row != nil {
			closeIfPossible(row.rows)
		}
		return &Row{err: hookErr, unsafe: db.unsafe, Mapper: db.Mapper}
	}
	if row == nil {
		return &Row{err: errors.New("squealx: query row returned no row wrapper"), unsafe: db.unsafe, Mapper: db.Mapper}
	}
	return row
}

// MustExec (panic) runs MustExec using this database.
// Any placeholder parameters are replaced with supplied args.
func (db *DB) MustExec(query string, args ...any) sql.Result {
	query, err := SanitizeQuery(query, args...)
	if err != nil {
		return nil
	}
	fn := func(query string, args []any) (sql.Result, error) {
		return MustExec(db, query, args...), nil
	}
	row, _ := handleTwo(fn, db, context.Background(), query, args...)
	return row
}

// MustInExec (panic) runs MustExec using this database for in.
// Any placeholder parameters are replaced with supplied args.
func (db *DB) MustInExec(query string, args ...any) sql.Result {
	query, err := SanitizeQuery(query, args...)
	if err != nil {
		return nil
	}
	fn := func(query string, args []any) (sql.Result, error) {
		return MustInExec(db, query, args...), nil
	}
	row, _ := handleTwo(fn, db, context.Background(), query, args...)
	return row

}

// Preparex returns an sqlx.Stmt instead of a sql.Stmt
func (db *DB) Preparex(query string) (*Stmt, error) {
	return Preparex(db, query)
}

// PrepareNamed returns an sqlx.NamedStmt
func (db *DB) PrepareNamed(query string) (*NamedStmt, error) {
	return prepareNamed(db, query)
}

// Conn is a wrapper around sql.Conn with extra functionality
type Conn struct {
	SQLConn
	driverName string
	unsafe     bool
	Mapper     *reflectx.Mapper
}

// Tx is an sqlx wrapper around sql.Tx with extra functionality
type Tx struct {
	SQLTx
	driverName  string
	unsafe      bool
	Mapper      *reflectx.Mapper
	beforeHooks []Hook
	afterHooks  []Hook
	onError     []ErrorHook
	hookCtx     context.Context
}

// DriverName returns the driverName used by the DB which began this transaction.
func (tx *Tx) DriverName() string {
	return tx.driverName
}

// Rebind a query within a transaction's bindvar type.
func (tx *Tx) Rebind(query string) string {
	return Rebind(BindType(tx.driverName), query)
}

// Unsafe returns a version of Tx which will silently succeed to scan when
// columns in the SQL result have no fields in the destination struct.
func (tx *Tx) Unsafe() *Tx {
	return &Tx{SQLTx: tx.SQLTx, driverName: tx.driverName, unsafe: true, Mapper: tx.Mapper, beforeHooks: tx.beforeHooks, afterHooks: tx.afterHooks, onError: tx.onError, hookCtx: tx.hookCtx}
}

func (tx *Tx) baseContext() context.Context {
	if tx.hookCtx != nil {
		return tx.hookCtx
	}
	return context.Background()
}

func (tx *Tx) handleBeforeHooks(ctx context.Context, query string, args ...any) (context.Context, string, []any, error) {
	var err error
	for _, hook := range tx.beforeHooks {
		ctx, query, args, err = invokeHook("before", hook, ctx, query, args...)
		if err != nil {
			return ctx, query, args, err
		}
	}
	return ctx, query, args, nil
}

func (tx *Tx) handleAfterHooks(ctx context.Context, query string, args ...any) (context.Context, string, []any, error) {
	var err error
	for _, hook := range tx.afterHooks {
		ctx, query, args, err = invokeHook("after", hook, ctx, query, args...)
		if err != nil {
			return ctx, query, args, err
		}
	}
	return ctx, query, args, nil
}

func (tx *Tx) handleErrorHooks(ctx context.Context, err error, query string, args ...any) error {
	current := err
	for _, hook := range tx.onError {
		if next := invokeErrorHook(hook, ctx, current, query, args...); next != nil {
			current = next
		}
	}
	return current
}

func handleTwoTx[T any](fn func(query string, args []any) (T, error), tx *Tx, ctx context.Context, query string, args ...any) (T, error) {
	var t T
	if len(tx.beforeHooks) == 0 && len(tx.afterHooks) == 0 && len(tx.onError) == 0 {
		return fn(query, args)
	}
	ctx = withDriverName(ctx, tx.driverName)
	ctx2, query, args, err := tx.handleBeforeHooks(ctx, query, args...)
	if err != nil {
		return t, err
	}
	data, err := fn(query, args)
	if err != nil {
		return data, tx.handleErrorHooks(ctx2, err, query, args...)
	}
	_, _, _, err = tx.handleAfterHooks(ctx2, query, args...)
	if err != nil {
		closeIfPossible(data)
		return data, err
	}
	return data, nil
}

func (tx *Tx) Query(query string, args ...any) (SQLRows, error) {
	query, err := SanitizeQuery(query, args...)
	if err != nil {
		return nil, err
	}
	fn := func(query string, args []any) (SQLRows, error) {
		return tx.SQLTx.Query(query, args...)
	}
	return handleTwoTx(fn, tx, tx.baseContext(), query, args...)
}

func (tx *Tx) QueryRow(query string, args ...any) SQLRow {
	query, err := SanitizeQuery(query, args...)
	if err != nil {
		return &Row{rows: nil, err: err, unsafe: tx.unsafe, Mapper: tx.Mapper}
	}
	fn := func(query string, args []any) (*Row, error) {
		rows, err := tx.SQLTx.Query(query, args...)
		return &Row{rows: rows, err: err, unsafe: tx.unsafe, Mapper: tx.Mapper}, err
	}
	row, hookErr := handleTwoTx(fn, tx, tx.baseContext(), query, args...)
	if hookErr != nil {
		if row != nil {
			closeIfPossible(row.rows)
		}
		return &Row{err: hookErr, unsafe: tx.unsafe, Mapper: tx.Mapper}
	}
	if row == nil {
		return &Row{err: errors.New("squealx: transaction query row returned no row wrapper"), unsafe: tx.unsafe, Mapper: tx.Mapper}
	}
	return row
}

func (tx *Tx) Exec(query string, args ...any) (sql.Result, error) {
	query, err := SanitizeQuery(query, args...)
	if err != nil {
		return nil, err
	}
	fn := func(query string, args []any) (sql.Result, error) {
		return tx.SQLTx.Exec(query, args...)
	}
	return handleTwoTx(fn, tx, tx.baseContext(), query, args...)
}

// BindNamed binds a query within a transaction's bindvar type.
func (tx *Tx) BindNamed(query string, arg any) (string, []any, error) {
	return bindNamedMapper(BindType(tx.driverName), query, arg, tx.Mapper)
}

// NamedQuery within a transaction.
// Any named placeholder parameters are replaced with fields from arg.
func (tx *Tx) NamedQuery(query string, arg any) (*Rows, error) {
	return NamedQuery(tx, query, arg)
}

// NamedGet within a transaction.
// Any named placeholder parameters are replaced with fields from arg.
func (tx *Tx) NamedGet(dest any, query string, arg any) error {
	if InReg.MatchString(query) {
		query, arg = prepareNamedInQuery(query, arg)
		q, p, err := bindNamedMapper(BindType(tx.DriverName()), query, arg, mapperFor(tx))
		if err != nil {
			return err
		}
		r := tx.QueryRowx(q, p...)
		return r.scanAny(dest, false)
	}
	q, p, err := bindNamedMapper(BindType(tx.DriverName()), query, arg, mapperFor(tx))
	if err != nil {
		return err
	}
	r := tx.QueryRowx(q, p...)
	return r.scanAny(dest, false)
}

func (tx *Tx) NamedSelect(dest any, query string, arg any) error {
	if !IsNamedQuery(query) {
		return tx.Select(dest, query, arg)
	}
	rows, err := NamedQuery(tx, query, arg)
	if err != nil {
		return err
	}
	// if something happens here, we want to make sure the rows are Closed
	defer rows.Close()
	return ScannAll(rows, dest, false)
}

// NamedExec a named query within a transaction.
// Any named placeholder parameters are replaced with fields from arg.
func (tx *Tx) NamedExec(query string, arg any) (sql.Result, error) {
	return NamedExec(tx, query, arg)
}

// In expands slice values in args, returning the modified query string
// and a new arg list that can be executed by a database. The `query` should
// use the `?` bindVar.  The return value uses had rebinded bindvar type.
func (tx *Tx) In(query string, args ...any) (string, []any, error) {
	q, params, err := In(query, args...)
	if err != nil {
		return "", nil, err
	}
	return tx.Rebind(q), params, nil
}

// InExec executes a query that doesn't return rows for in.
// For example: an INSERT and UPDATE.
//
// Exec uses context.Background internally; to specify the context, use
// ExecContext.
func (tx *Tx) InExec(query string, args ...any) (sql.Result, error) {
	return InExec(tx, query, args...)
}

// InSelect within a transaction for in.
// Any placeholder parameters are replaced with supplied args.
func (tx *Tx) InSelect(dest any, query string, args ...any) error {
	return InSelect(tx, dest, query, args...)
}

// Get within a transaction for in.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (tx *Tx) InGet(dest any, query string, args ...any) error {
	return InGet(tx, dest, query, args...)
}

// Select within a transaction.
// Any placeholder parameters are replaced with supplied args.
func (tx *Tx) Select(dest any, query string, args ...any) error {
	return Select(tx, dest, query, args...)
}

// Queryx within a transaction.
// Any placeholder parameters are replaced with supplied args.
func (tx *Tx) Queryx(query string, args ...any) (*Rows, error) {
	r, err := tx.Query(query, args...)
	if err != nil {
		return nil, err
	}
	return &Rows{SQLRows: r, unsafe: tx.unsafe, Mapper: tx.Mapper}, err
}

// QueryRowx within a transaction.
// Any placeholder parameters are replaced with supplied args.
func (tx *Tx) QueryRowx(query string, args ...any) *Row {
	row, _ := tx.QueryRow(query, args...).(*Row)
	return row
}

// Get within a transaction.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (tx *Tx) Get(dest any, query string, args ...any) error {
	return Get(tx, dest, query, args...)
}

// MustExec runs MustExec within a transaction.
// Any placeholder parameters are replaced with supplied args.
func (tx *Tx) MustExec(query string, args ...any) sql.Result {
	return MustExec(tx, query, args...)
}

// MustInExec runs MustExec within a transaction for in.
// Any placeholder parameters are replaced with supplied args.
func (tx *Tx) MustInExec(query string, args ...any) sql.Result {
	return MustInExec(tx, query, args...)
}

// Preparex  a statement within a transaction.
func (tx *Tx) Preparex(query string) (*Stmt, error) {
	return Preparex(tx, query)
}

// Stmtx returns a version of the prepared statement which runs within a transaction.  Provided
// stmt can be either *sql.Stmt or *sqlx.Stmt.
func (tx *Tx) Stmtx(stmt any) *Stmt {
	var s SQLStmt
	switch v := stmt.(type) {
	case SQLStmt:
		s = v
	case Stmt:
		s = v.SQLStmt
	case *Stmt:
		s = v.SQLStmt
	case *sql.Stmt:
		s = &sqlStmtWrapper{stmt: v}
	default:
		panic(fmt.Sprintf("non-statement type %v passed to Stmtx", reflect.ValueOf(stmt).Type()))
	}
	return &Stmt{SQLStmt: tx.Stmt(s), Mapper: tx.Mapper}
}

// NamedStmt returns a version of the prepared statement which runs within a transaction.
func (tx *Tx) NamedStmt(stmt *NamedStmt) *NamedStmt {
	return &NamedStmt{
		QueryString: stmt.QueryString,
		Params:      stmt.Params,
		Stmt:        tx.Stmtx(stmt.Stmt),
	}
}

// PrepareNamed returns an sqlx.NamedStmt
func (tx *Tx) PrepareNamed(query string) (*NamedStmt, error) {
	return prepareNamed(tx, query)
}

// Stmt is an sqlx wrapper around sql.Stmt with extra functionality
type Stmt struct {
	SQLStmt
	unsafe bool
	Mapper *reflectx.Mapper
}

// Unsafe returns a version of Stmt which will silently succeed to scan when
// columns in the SQL result have no fields in the destination struct.
func (s *Stmt) Unsafe() *Stmt {
	return &Stmt{SQLStmt: s.SQLStmt, unsafe: true, Mapper: s.Mapper}
}

// Select using the prepared statement.
// Any placeholder parameters are replaced with supplied args.
func (s *Stmt) Select(dest any, args ...any) error {
	return Select(&qStmt{s}, dest, "", args...)
}

// Get using the prepared statement.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (s *Stmt) Get(dest any, args ...any) error {
	return Get(&qStmt{s}, dest, "", args...)
}

// MustExec (panic) using this statement.  Note that the query portion of the error
// output will be blank, as Stmt does not expose its query.
// Any placeholder parameters are replaced with supplied args.
func (s *Stmt) MustExec(args ...any) sql.Result {
	return MustExec(&qStmt{s}, "", args...)
}

// QueryRowx using this statement.
// Any placeholder parameters are replaced with supplied args.
func (s *Stmt) QueryRowx(args ...any) *Row {
	qs := &qStmt{s}
	return qs.QueryRowx("", args...)
}

// Queryx using this statement.
// Any placeholder parameters are replaced with supplied args.
func (s *Stmt) Queryx(args ...any) (*Rows, error) {
	qs := &qStmt{s}
	return qs.Queryx("", args...)
}

// qStmt is an unexposed wrapper which lets you use a Stmt as a Queryer & Execer by
// implementing those interfaces and ignoring the `query` argument.
type qStmt struct{ *Stmt }

func (q *qStmt) Query(query string, args ...any) (SQLRows, error) {
	return q.Stmt.Query(args...)
}

func (q *qStmt) Queryx(query string, args ...any) (*Rows, error) {
	r, err := q.Stmt.Query(args...)
	if err != nil {
		return nil, err
	}
	return &Rows{SQLRows: r, unsafe: q.Stmt.unsafe, Mapper: q.Stmt.Mapper}, err
}

func (q *qStmt) QueryRowx(query string, args ...any) *Row {
	rows, err := q.Stmt.Query(args...)
	return &Row{rows: rows, err: err, unsafe: q.Stmt.unsafe, Mapper: q.Stmt.Mapper}
}

func (q *qStmt) Exec(query string, args ...any) (sql.Result, error) {
	return q.Stmt.Exec(args...)
}

// Rows is a wrapper around sql.Rows which caches costly reflect operations
// during a looped StructScan
type Rows struct {
	SQLRows
	unsafe bool
	Mapper *reflectx.Mapper
	// these fields cache memory use for a rows during iteration w/ structScan
	started bool
	fields  [][]int
	values  []any
	octx    *reflectx.ObjectContext
}

// SliceScan using this Rows.
func (r *Rows) SliceScan() ([]any, error) {
	return SliceScan(r)
}

// MapScan using this Rows.
func (r *Rows) MapScan(dest map[string]any) error {
	return MapScan(r, dest)
}

// prepareValues prepare values slice
func prepareValues(values []any, columnTypes []*sql.ColumnType, columns []string) {
	if len(columnTypes) == 0 {
		backing := make([]any, len(columns))
		for idx := range columns {
			values[idx] = &backing[idx]
		}
		return
	}
	if len(columnTypes) > 0 {
		for idx, columnType := range columnTypes {
			if columnType != nil {
				if scanType := columnType.ScanType(); scanType != nil {
					values[idx] = reflect.New(scanType).Interface()
					continue
				}
			}
			values[idx] = new(any)
		}
	} else {
		for idx := range columns {
			values[idx] = new(any)
		}
	}
}

// StructScan is like sql.Rows.Scan, but scans a single Row into a single Struct.
// Use this and iterate over Rows manually when the memory load of Select() might be
// prohibitive.  *Rows.StructScan caches reflect work of matching up column
// positions to fields to avoid that overhead per scan, which means it is not safe
// to run StructScan on the same Rows instance with different struct types.
func (r *Rows) StructScan(dest any) error {
	v := reflect.ValueOf(dest)

	if v.Kind() != reflect.Ptr {
		return errors.New("must pass a pointer, not a value, to StructScan destination")
	}
	if v.IsNil() {
		return errors.New("nil pointer passed to StructScan destination")
	}

	v = v.Elem()
	if v.Kind() != reflect.Struct {
		return structOnlyError(v.Type())
	}

	if !r.started {
		columns, err := r.Columns()
		if err != nil {
			return err
		}
		m := r.Mapper

		r.fields = m.TraversalsByNameCached(v.Type(), columns)
		// if we are not unsafe and are missing fields, return an error
		if missing := firstMissingField(r.fields); missing >= 0 && !r.unsafe {
			return fmt.Errorf("missing destination name %s in %T", columns[missing], dest)
		}
		r.values = make([]any, len(columns))
		r.started = true
	}

	if r.octx == nil {
		r.octx = reflectx.NewObjectContext()
	}
	err := fieldsByTraversal(r.octx, v, r.fields, r.values, true)
	if err != nil {
		return err
	}
	// scan into the struct field pointers and append to our results
	err = r.Scan(r.values...)
	if err != nil {
		return err
	}
	return r.Err()
}

// ConnectExist is the same as Connect, but using already opened connection.
func ConnectExist(driverName string, raw *sql.DB) (*DB, error) {
	db := OpenExist(driverName, raw)
	err := db.Ping()
	if err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// Connect to a database and verify with a ping.
func Connect(driverName, dataSourceName, id string) (*DB, error) {
	db, err := Open(driverName, dataSourceName, id)
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// MustConnect connects to a database and panics on error.
func MustConnect(driverName, dataSourceName, id string) *DB {
	db, err := Connect(driverName, dataSourceName, id)
	if err != nil {
		panic(err)
	}
	return db
}

// Preparex prepares a statement.
func Preparex(p Preparer, query string) (*Stmt, error) {
	s, err := p.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &Stmt{SQLStmt: s, unsafe: isUnsafe(p), Mapper: mapperFor(p)}, err
}

// Select executes a query using the provided Queryer, and StructScans each row
// into dest, which must be a slice.  If the slice elements are scannable, then
// the result set must have only one column.  Otherwise, StructScan is used.
// The *sql.Rows are closed automatically.
// Any placeholder parameters are replaced with supplied args.
func Select(q Queryer, dest any, query string, args ...any) error {
	query, err := SanitizeQuery(query, args...)
	if err != nil {
		return err
	}
	rows, err := q.Queryx(query, args...)
	if err != nil {
		return err
	}
	// if something happens here, we want to make sure the rows are Closed
	defer rows.Close()
	return ScannAll(rows, dest, false)
}

// Get does a QueryRow using the provided Queryer, and scans the resulting row
// to dest.  If dest is scannable, the result must only have one column.  Otherwise,
// StructScan is used.  Get will return sql.ErrNoRows like row.Scan would.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func Get(q Queryer, dest any, query string, args ...any) error {
	query, err := SanitizeQuery(query, args...)
	if err != nil {
		return err
	}
	r := q.QueryRowx(query, args...)
	return r.scanAny(dest, false)
}

// InSelect for in scene executes a query using the provided Queryer, and StructScans each row
// into dest, which must be a slice.  If the slice elements are scannable, then
// the result set must have only one column.  Otherwise, StructScan is used.
// The *sql.Rows are closed automatically.
// Any placeholder parameters are replaced with supplied args.
func InSelect(q QueryIn, dest any, query string, args ...any) error {
	newQuery, params, err := q.In(query, args...)
	if err != nil {
		return err
	}
	rows, err := q.Queryx(newQuery, params...)
	if err != nil {
		return err
	}
	// if something happens here, we want to make sure the rows are Closed
	defer rows.Close()
	return ScannAll(rows, dest, false)
}

// InGet for in scene does a QueryRow using the provided Queryer, and scans the resulting row
// to dest.  If dest is scannable, the result must only have one column.  Otherwise,
// StructScan is used.  Get will return sql.ErrNoRows like row.Scan would.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func InGet(q QueryIn, dest any, query string, args ...any) error {
	query, err := SanitizeQuery(query, args...)
	if err != nil {
		return err
	}
	newQuery, params, err := q.In(query, args...)
	if err != nil {
		return err
	}
	r := q.QueryRowx(newQuery, params...)
	return r.scanAny(dest, false)
}

// LoadFile exec's every statement in a file (as a single call to Exec).
// LoadFile may return a nil *sql.Result if errors are encountered locating or
// reading the file at path.  LoadFile reads the entire file into memory, so it
// is not suitable for loading large data dumps, but can be useful for initializing
// schemas or loading indexes.
//
// FIXME: this does not really work with multi-statement files for mattn/go-sqlite3
// or the go-mysql-driver/mysql drivers;  pq seems to be an exception here.  Detecting
// this by requiring something with DriverName() and then attempting to split the
// queries will be difficult to get right, and its current driver-specific behavior
// is deemed at least not complex in its incorrectness.
func LoadFile(e Execer, path string) (*sql.Result, error) {
	realpath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	contents, err := os.ReadFile(realpath)
	if err != nil {
		return nil, err
	}
	res, err := e.Exec(string(contents))
	return &res, err
}

// MustExec execs the query using e and panics if there was an error.
// Any placeholder parameters are replaced with supplied args.
func MustExec(e Execer, query string, args ...any) sql.Result {
	res, err := e.Exec(query, args...)
	if err != nil {
		panic(err)
	}
	return res
}

// MustInExec for in scene execs the query using e and panics if there was an error.
// Any placeholder parameters are replaced with supplied args.
func MustInExec(e ExecIn, query string, args ...any) sql.Result {
	newQuery, params, err := e.In(query, args...)
	if err != nil {
		panic(err)
	}
	res, err := e.Exec(newQuery, params...)
	if err != nil {
		panic(err)
	}
	return res
}

// Exec for in scene executes a query that doesn't return rows.
// For example: an INSERT and UPDATE.
//
// Exec uses context.Background internally; to specify the context, use
// ExecContext.
func InExec(e ExecIn, query string, args ...any) (sql.Result, error) {
	query, err := SanitizeQuery(query, args...)
	if err != nil {
		return nil, err
	}
	newQuery, params, err := e.In(query, args...)
	if err != nil {
		return nil, err
	}
	return e.Exec(newQuery, params...)
}

// SliceScan using this Rows.
func (r *Row) SliceScan() ([]any, error) {
	return SliceScan(r)
}

// MapScan using this Rows.
func (r *Row) MapScan(dest map[string]any) error {
	return MapScan(r, dest)
}

func (r *Row) scanAny(dest any, structOnly bool) error {
	if r.err != nil {
		return r.err
	}
	if r.rows == nil {
		r.err = sql.ErrNoRows
		return r.err
	}
	defer r.rows.Close()

	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr {
		return errors.New("must pass a pointer, not a value, to StructScan destination")
	}
	if v.IsNil() {
		return errors.New("nil pointer passed to StructScan destination")
	}

	base := reflectx.Deref(v.Type())

	scannable := isScannable(base)

	if structOnly && scannable {
		return structOnlyError(base)
	}

	columns, err := r.Columns()
	if err != nil {
		return err
	}
	if base.Kind() == reflect.Map {
		colTypes, err := r.ColumnTypes()
		if err != nil {
			return err
		}
		rowMap, err := scanCurrentRowMap(r, columns, colTypes)
		if err != nil {
			return err
		}

		dv := reflect.ValueOf(dest)
		if dv.Kind() != reflect.Ptr || dv.IsNil() {
			return fmt.Errorf("map scan requires a non-nil pointer destination, got %T", dest)
		}
		mv := dv.Elem()
		if mv.Kind() != reflect.Map || mv.Type().Key().Kind() != reflect.String {
			return fmt.Errorf("map scan requires a map[string]T destination, got %T", dest)
		}

		typedMap, err := convertStringMapToType(rowMap, mv.Type())
		if err != nil {
			return err
		}
		mv.Set(typedMap)
		return nil
	}
	if scannable && len(columns) > 1 {
		return fmt.Errorf("scannable dest type %s with >1 columns (%d) in result", base.Kind(), len(columns))
	}

	if scannable {
		return r.Scan(dest)
	}

	m := r.Mapper

	fields := m.TraversalsByNameCached(v.Type(), columns)
	// if we are not unsafe and are missing fields, return an error
	if missing := firstMissingField(fields); missing >= 0 && !r.unsafe {
		return fmt.Errorf("missing destination name %s in %T", columns[missing], dest)
	}
	sc := getScanScratch(len(columns))
	defer sc.release()

	err = fieldsByTraversalArena(&sc.octx, v, fields, sc.values, true, &sc.ns)
	if err != nil {
		return err
	}
	// scan into the struct field pointers and append to our results
	return r.Scan(sc.values...)
}

// StructScan a single Row into dest.
func (r *Row) StructScan(dest any) error {
	return r.scanAny(dest, true)
}

// SliceScan a row, returning a []any with values similar to MapScan.
// This function is primarily intended for use where the number of columns
// is not known.  Because you can pass an []any directly to Scan,
// it's recommended that you do that as it will not have to allocate new
// slices per row.
func SliceScan(r ColScanner) ([]any, error) {
	columns, err := r.Columns()
	if err != nil {
		return nil, err
	}
	columnTypes, err := r.ColumnTypes()
	if err != nil {
		return nil, err
	}
	values := make([]any, len(columns))
	prepareValues(values, columnTypes, columns)

	err = r.Scan(values...)
	if err != nil {
		return nil, err
	}
	for idx := range columns {
		v, err := normalizeScanned(values[idx])
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", columns[idx], err)
		}
		values[idx] = v
	}
	return values, r.Err()
}

// MapScan scans a single Row into the dest map[string]any.
// Use this to get results for SQL that might not be under your control
// (for instance, if you're building an interface for an SQL server that
// executes SQL from input).  Please do not use this as a primary interface!
// This will modify the map sent to it in place, so reuse the same map with
// care.  Columns which occur more than once in the result will overwrite
// each other!
func MapScan(r ColScanner, dest map[string]any) error {
	// ignore r.started, since we needn't use reflect for anything.
	columns, err := r.Columns()
	if err != nil {
		return err
	}
	columnTypes, err := r.ColumnTypes()
	if err != nil {
		return err
	}
	values := make([]any, len(columns))
	prepareValues(values, columnTypes, columns)

	err = r.Scan(values...)
	if err != nil {
		return err
	}
	for idx, column := range columns {
		v, err := normalizeScanned(values[idx])
		if err != nil {
			return fmt.Errorf("column %q: %w", column, err)
		}
		dest[column] = v
	}
	return r.Err()
}

// normalizeScanned dereferences a scan target and resolves driver.Valuer and
// sql.RawBytes values. Valuer errors are returned instead of being dropped.
func normalizeScanned(target any) (any, error) {
	reflectValue := reflect.Indirect(reflect.Indirect(reflect.ValueOf(target)))
	if !reflectValue.IsValid() {
		return nil, nil
	}
	v := reflectValue.Interface()
	if valuer, ok := v.(driver.Valuer); ok {
		return valuer.Value()
	}
	if b, ok := v.(sql.RawBytes); ok {
		return string(b), nil
	}
	return v, nil
}

type Rowsi interface {
	Close() error
	Columns() ([]string, error)
	ColumnTypes() ([]*sql.ColumnType, error)
	Err() error
	Next() bool
	Scan(...any) error
}

// structOnlyError returns an error appropriate for type when a non-scannable
// struct is expected but something else is given
func structOnlyError(t reflect.Type) error {
	isStruct := t.Kind() == reflect.Struct
	isScanner := reflect.PtrTo(t).Implements(_scannerInterface)
	if !isStruct {
		return fmt.Errorf("expected %s but got %s", reflect.Struct, t.Kind())
	}
	if isScanner {
		return fmt.Errorf("structscan expects a struct dest but the provided struct type %s implements scanner", t.Name())
	}
	return fmt.Errorf("expected a struct, but struct %s has no exported fields", t.Name())
}

func ScannAll(rows Rowsi, dest any, structOnly bool) error {
	value := reflect.ValueOf(dest)
	if value.Kind() != reflect.Ptr || value.IsNil() {
		return errors.New("must pass a non-nil pointer to StructScan destination")
	}
	direct := reflect.Indirect(value)

	slice, err := baseType(value.Type(), reflect.Slice)
	if err != nil {
		return err
	}
	direct.SetLen(0)

	elemType := slice.Elem()
	isPtr := elemType.Kind() == reflect.Ptr
	base := reflectx.Deref(elemType)
	scannable := isScannable(base)

	if structOnly && scannable {
		return structOnlyError(base)
	}
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	if base.Kind() == reflect.Map {
		colTypes, err := rows.ColumnTypes()
		if err != nil {
			return err
		}
		if err := scanMap(rows, columns, colTypes, dest); err != nil {
			return err
		}
		return rows.Err()
	}

	mapper := func() *reflectx.Mapper {
		if r, ok := rows.(*Rows); ok {
			return r.Mapper
		}
		return mapper()
	}()

	if !scannable {
		fields := mapper.TraversalsByNameCached(base, columns)
		if missing := firstMissingField(fields); missing >= 0 && !isUnsafe(rows) {
			return fmt.Errorf("missing destination name %s in %s", columns[missing], base)
		}
		sc := getScanScratch(len(columns))
		defer sc.release()
		values, octx := sc.values, &sc.octx

		// Rows are scanned in place into the slice's backing array: no
		// per-row reflect.New/Append for value elements, and the scan
		// arguments are retargeted rather than rebuilt for every row.
		n := 0
		for rows.Next() {
			growSlice(direct)
			direct.SetLen(n + 1)
			elem := direct.Index(n)
			target := elem
			if isPtr {
				vp := reflect.New(base)
				elem.Set(vp)
				target = vp.Elem()
			} else {
				elem.SetZero()
			}
			if err := fieldsByTraversalArena(octx, target, fields, values, true, &sc.ns); err != nil {
				direct.SetLen(n)
				return err
			}
			if err := rows.Scan(values...); err != nil {
				direct.SetLen(n)
				return err
			}
			n++
		}
		return rows.Err()
	}
	n := 0
	// *T elements of plain scalar types can take NULL as nil; types with
	// their own Scan (sql.Null*, custom Scanners) keep their semantics.
	ptrScannable := isPtr && !reflect.PointerTo(base).Implements(scannerType)
	for rows.Next() {
		growSlice(direct)
		direct.SetLen(n + 1)
		elem := direct.Index(n)
		var dst any
		if isPtr && ptrScannable {
			// Scan into the element itself (**T) so NULL becomes a nil
			// element instead of an error.
			dst = elem.Addr().Interface()
		} else if isPtr {
			vp := reflect.New(base)
			elem.Set(vp)
			dst = vp.Interface()
		} else {
			elem.SetZero()
			dst = elem.Addr().Interface()
		}
		if err := rows.Scan(dst); err != nil {
			direct.SetLen(n)
			return err
		}
		n++
	}

	return rows.Err()
}

// growSlice doubles the capacity of the slice when it is full so repeated
// appends are amortized.
func growSlice(direct reflect.Value) {
	if direct.Len() < direct.Cap() {
		return
	}
	n := direct.Cap() * 2
	if n < 8 {
		n = 8
	}
	grown := reflect.MakeSlice(direct.Type(), direct.Len(), n)
	reflect.Copy(grown, direct)
	direct.Set(grown)
}

func scanMap(rows Rowsi, columns []string, colTypes []*sql.ColumnType, dest any) error {
	switch dest := dest.(type) {
	case *[]map[string]any:
		return scanMapSlices(rows, columns, colTypes, dest)
	case *[]any:
		return scanAnySlices(rows, columns, colTypes, dest)
	}
	return scanTypedMapSlice(rows, columns, colTypes, dest)
}

func scanMapSlices(rows Rowsi, columns []string, colTypes []*sql.ColumnType, dest *[]map[string]any) error {
	ms := newMapRowScanner(columns, colTypes)
	for rows.Next() {
		m, err := ms.scan(rows)
		if err != nil {
			return err
		}
		*dest = append(*dest, m)
	}
	return nil
}

func scanAnySlices(rows Rowsi, columns []string, colTypes []*sql.ColumnType, dest *[]any) error {
	ms := newMapRowScanner(columns, colTypes)
	for rows.Next() {
		m, err := ms.scan(rows)
		if err != nil {
			return err
		}
		*dest = append(*dest, m)
	}
	return nil
}

func scanTypedMapSlice(rows Rowsi, columns []string, colTypes []*sql.ColumnType, dest any) error {
	value := reflect.ValueOf(dest)
	if value.Kind() != reflect.Ptr || value.IsNil() {
		return fmt.Errorf("unsupported dest type for map scanning: %T", dest)
	}
	slice := value.Elem()
	if slice.Kind() != reflect.Slice {
		return fmt.Errorf("unsupported dest type for map scanning: %T", dest)
	}

	elemType := slice.Type().Elem()
	isPtr := elemType.Kind() == reflect.Ptr
	mapType := elemType
	if isPtr {
		mapType = elemType.Elem()
	}
	if mapType.Kind() != reflect.Map || mapType.Key().Kind() != reflect.String {
		return fmt.Errorf("unsupported dest type for map scanning: %T", dest)
	}

	ms := newMapRowScanner(columns, colTypes)
	for rows.Next() {
		rowMap, err := ms.scan(rows)
		if err != nil {
			return err
		}
		typedMap, err := convertStringMapToType(rowMap, mapType)
		if err != nil {
			return err
		}
		if isPtr {
			ptr := reflect.New(mapType)
			ptr.Elem().Set(typedMap)
			slice.Set(reflect.Append(slice, ptr))
		} else {
			slice.Set(reflect.Append(slice, typedMap))
		}
	}
	return rows.Err()
}

// ScanEach is a generic function that processes each row with the provided callback function.
func ScanEach[T any](rows Rowsi, structOnly bool, callback func(row T) error) error {
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	mapper := func() *reflectx.Mapper {
		if r, ok := rows.(*Rows); ok {
			return r.Mapper
		}
		return reflectx.NewMapperFunc("db", NameMapper)
	}()

	plan := &rowPlan[T]{rows: rows, columns: columns, colTypes: colTypes, mapper: mapper, structOnly: structOnly}
	for rows.Next() {
		row, err := plan.scan()
		if err != nil {
			return err
		}
		if err := callback(row); err != nil {
			return err
		}
	}

	return rows.Err()
}

// rowPlan holds the scan plan for ScanEach. It is built lazily on the first
// row and reused for all following rows. For non-pointer struct and scalar T a
// single destination is reused (the callback receives a value copy); pointer
// and map T allocate fresh storage per row.
type rowPlan[T any] struct {
	rows       Rowsi
	columns    []string
	colTypes   []*sql.ColumnType
	mapper     *reflectx.Mapper
	structOnly bool

	ready    bool
	base     reflect.Type
	isPtr    bool
	isMap    bool
	scalar   bool
	fields   [][]int
	scanArgs []any
	holder   *T            // reusable destination for non-pointer T
	holderV  reflect.Value // holder.Elem()
	octx     *reflectx.ObjectContext
	mapScan  *mapRowScanner
}

func (p *rowPlan[T]) init() error {
	var zero T
	resultType := reflect.TypeOf((*T)(nil)).Elem()
	if resultType.Kind() == reflect.Interface {
		return fmt.Errorf("ScanEach: interface row type %s is ambiguous; use a concrete type", resultType)
	}
	p.base = resultType
	if resultType.Kind() == reflect.Ptr {
		p.base = resultType.Elem()
		p.isPtr = true
	}
	p.scalar = isScannable(p.base)
	if p.structOnly && p.scalar {
		return structOnlyError(p.base)
	}
	if p.base.Kind() == reflect.Map {
		p.isMap = true
		return nil
	}
	if !p.isPtr {
		p.holder = &zero
		p.holderV = reflect.ValueOf(p.holder).Elem()
	}
	if p.scalar {
		p.scanArgs = make([]any, 1)
		if p.holder != nil {
			p.scanArgs[0] = p.holder
		}
		return nil
	}
	p.fields = p.mapper.TraversalsByNameCached(p.base, p.columns)
	if missing := firstMissingField(p.fields); missing >= 0 && !isUnsafe(p.rows) {
		return fmt.Errorf("missing destination name %s in %s", p.columns[missing], p.base)
	}
	p.scanArgs = make([]any, len(p.columns))
	p.octx = reflectx.NewObjectContext()
	if p.holder != nil {
		return fieldsByTraversal(p.octx, p.holderV, p.fields, p.scanArgs, true)
	}
	return nil
}

// scan scans the current row.
func (p *rowPlan[T]) scan() (T, error) {
	var result T
	if !p.ready {
		if err := p.init(); err != nil {
			return result, err
		}
		p.ready = true
	}
	if p.isMap {
		if p.mapScan == nil {
			p.mapScan = newMapRowScanner(p.columns, p.colTypes)
		}
		rowMap, err := p.mapScan.scan(p.rows)
		if err != nil {
			return result, err
		}
		typedMap, err := convertStringMapToType(rowMap, p.base)
		if err != nil {
			return result, err
		}
		if p.isPtr {
			ptr := reflect.New(p.base)
			ptr.Elem().Set(typedMap)
			return ptr.Interface().(T), nil
		}
		return typedMap.Interface().(T), nil
	}
	if p.holder != nil {
		if err := p.rows.Scan(p.scanArgs...); err != nil {
			return result, err
		}
		// Hand out the row and clear the shared destination so pointer
		// fields (and embedded pointer structs) are re-allocated for the next
		// row instead of being shared with values the callback retained.
		out := *p.holder
		var z T
		*p.holder = z
		return out, nil
	}
	vp := reflect.New(p.base)
	if p.scalar {
		p.scanArgs[0] = vp.Interface()
	} else if err := fieldsByTraversal(p.octx, vp.Elem(), p.fields, p.scanArgs, true); err != nil {
		return result, err
	}
	if err := p.rows.Scan(p.scanArgs...); err != nil {
		return result, err
	}
	return vp.Interface().(T), nil
}

// DecimalAsFloat makes DECIMAL columns decode to float64 in map/slice scans.
// By default DECIMAL values stay as exact strings.
var DecimalAsFloat bool

var datetimeLayouts = []string{
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05.999999999-07:00:00",
	"2006-01-02T15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05.999999999 -0700 MST",
	"2006-01-02 15:04:05.999999999 -0700",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04:05.999999999",
}

func parseDatetime(value string) (time.Time, error) {
	var firstErr error
	for _, layout := range datetimeLayouts {
		tm, err := time.Parse(layout, value)
		if err == nil {
			return tm, nil
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	return time.Time{}, firstErr
}

// bytesToAny converts raw driver bytes to a typed value. On a parse failure
// the original string is returned rather than a zero value.
func bytesToAny(t any, colType string) any {
	out, err := bytesToAnyErr(t, colType)
	if err != nil {
		if b, ok := t.([]byte); ok {
			return string(b)
		}
		return t
	}
	return out
}

// bytesToAnyErr converts raw driver bytes to a typed value, returning an error
// when the textual value cannot be parsed as the column type.
func bytesToAnyErr(t any, colType string) (any, error) {
	v, ok := t.([]byte)
	if !ok {
		return t, nil
	}
	value := string(v)
	switch colType {
	case "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT", "YEAR":
		return strconv.ParseInt(value, 10, 64)
	case "TINYINT", "BOOL", "BOOLEAN", "BIT":
		return strconv.ParseBool(value)
	case "FLOAT", "DOUBLE":
		return strconv.ParseFloat(value, 64)
	case "DECIMAL":
		if DecimalAsFloat {
			return strconv.ParseFloat(value, 64)
		}
		return value, nil
	case "DATETIME", "TIMESTAMP":
		return parseDatetime(value)
	case "DATE":
		return time.Parse("2006-01-02", value)
	case "TIME":
		return time.Parse("15:04:05.999999999", value)
	case "NULL":
		return nil, nil
	case "ENUM", "SET":
		var s []any
		if err := json.Unmarshal(v, &s); err != nil {
			return value, err
		}
		return s, nil
	}
	return value, nil
}

// mapRowScanner scans rows into map[string]any while reusing its scratch
// slices across rows; only the result map itself is allocated per row.
type mapRowScanner struct {
	columns   []string
	typeNames []string
	values    []any
	ptrs      []any
}

func newMapRowScanner(columns []string, colTypes []*sql.ColumnType) *mapRowScanner {
	s := &mapRowScanner{
		columns:   columns,
		typeNames: make([]string, len(columns)),
		values:    make([]any, len(columns)),
		ptrs:      make([]any, len(columns)),
	}
	for i := range columns {
		s.typeNames[i] = columnTypeName(colTypes, i)
		s.ptrs[i] = &s.values[i]
	}
	return s
}

func (s *mapRowScanner) scan(scanner interface{ Scan(...any) error }) (map[string]any, error) {
	if err := scanner.Scan(s.ptrs...); err != nil {
		return nil, err
	}
	m := make(map[string]any, len(s.columns))
	for i, colName := range s.columns {
		m[colName] = bytesToAny(s.values[i], s.typeNames[i])
	}
	return m, nil
}

func scanCurrentRowMap(scanner interface{ Scan(...any) error }, columns []string, colTypes []*sql.ColumnType) (map[string]any, error) {
	return newMapRowScanner(columns, colTypes).scan(scanner)
}

func columnTypeName(colTypes []*sql.ColumnType, i int) string {
	if i >= 0 && i < len(colTypes) && colTypes[i] != nil {
		return colTypes[i].DatabaseTypeName()
	}
	return ""
}

func convertStringMapToType(src map[string]any, mapType reflect.Type) (reflect.Value, error) {
	if mapType.Kind() != reflect.Map || mapType.Key().Kind() != reflect.String {
		return reflect.Value{}, fmt.Errorf("map destination must use string keys, got %s", mapType.String())
	}

	dest := reflect.MakeMapWithSize(mapType, len(src))
	valueType := mapType.Elem()

	for k, raw := range src {
		v, err := convertMapValue(raw, valueType)
		if err != nil {
			return reflect.Value{}, fmt.Errorf("column %q: %w", k, err)
		}
		dest.SetMapIndex(reflect.ValueOf(k), v)
	}

	return dest, nil
}

func convertMapValue(raw any, target reflect.Type) (reflect.Value, error) {
	if raw == nil {
		return reflect.Zero(target), nil
	}

	if target.Kind() == reflect.Interface && target.NumMethod() == 0 {
		return reflect.ValueOf(raw), nil
	}

	rv := reflect.ValueOf(raw)
	if rv.Type().AssignableTo(target) {
		return rv, nil
	}
	if rv.Type().ConvertibleTo(target) {
		return rv.Convert(target), nil
	}
	if target.Kind() == reflect.Ptr {
		elem, err := convertMapValue(raw, target.Elem())
		if err != nil {
			return reflect.Value{}, err
		}
		ptr := reflect.New(target.Elem())
		ptr.Elem().Set(elem)
		return ptr, nil
	}
	if b, ok := raw.([]byte); ok && target.Kind() == reflect.String {
		return reflect.ValueOf(string(b)), nil
	}
	return reflect.Value{}, fmt.Errorf("cannot convert %T to %s", raw, target.String())
}

// FIXME: StructScan was the very first bit of API in sqlx, and now unfortunately
// it doesn't really feel like it's named properly.  There is an incongruency
// between this and the way that StructScan (which might better be ScanStruct
// anyway) works on a rows object.

// StructScan all rows from an sql.Rows or an sqlx.Rows into the dest slice.
// StructScan will scan in the entire rows result, so if you do not want to
// allocate structs for the entire result, use Queryx and see sqlx.Rows.StructScan.
// If rows is sqlx.Rows, it will use its mapper, otherwise it will use the default.
func StructScan(rows Rowsi, dest any) error {
	return ScannAll(rows, dest, true)

}

// reflect helpers

func baseType(t reflect.Type, expected reflect.Kind) (reflect.Type, error) {
	t = reflectx.Deref(t)
	if t.Kind() != expected {
		return nil, fmt.Errorf("expected %s but got %s", expected, t.Kind())
	}
	return t, nil
}

// Add the nullSafe type to wrap dest pointers for null handling.
type nullSafe struct {
	dest any
}

func (ns *nullSafe) Scan(src any) error {
	// Use custom type's Scan method if implemented.
	if scanner, ok := ns.dest.(sql.Scanner); ok {
		return scanner.Scan(src)
	}
	if nullSafeFast(ns.dest, src) {
		return nil
	}
	rv := reflect.ValueOf(ns.dest)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("destination must be a non-nil pointer")
	}
	destVal := rv.Elem()
	if src == nil {
		destVal.Set(reflect.Zero(destVal.Type()))
		return nil
	}
	// Walk pointer levels and check if any intermediate type implements
	// sql.Scanner. This handles cases like *uuid.UUID where the outer
	// **uuid.UUID does not implement Scanner but the inner *uuid.UUID does
	// (via the pointer receiver method uuid.UUID.Scan).
	for checkVal := destVal; checkVal.Kind() == reflect.Ptr; checkVal = checkVal.Elem() {
		if checkVal.IsNil() {
			checkVal.Set(reflect.New(checkVal.Type().Elem()))
		}
		if scanner, ok := checkVal.Interface().(sql.Scanner); ok {
			return scanner.Scan(src)
		}
	}
	// Also check if the addressable base value implements Scanner via pointer receiver.
	if destVal.CanAddr() {
		if scanner, ok := destVal.Addr().Interface().(sql.Scanner); ok {
			return scanner.Scan(src)
		}
	}
	assignVal := destVal
	for assignVal.Kind() == reflect.Ptr {
		if assignVal.IsNil() {
			assignVal.Set(reflect.New(assignVal.Type().Elem()))
		}
		assignVal = assignVal.Elem()
	}
	if err := assignDirect(assignVal, src); err == nil {
		return nil
	}
	if err := assignFromStringLike(assignVal, src); err == nil {
		return nil
	}
	if err := tryJSONAssign(assignVal, src); err == nil {
		return nil
	}
	assignVal.Set(reflect.Zero(assignVal.Type()))
	return nil
}

// nullSafeFast handles the common native dest/src combinations without
// reflection. It returns false when the slow path must be used. Semantics
// mirror the slow path (NULL -> zero value, Go conversion rules for numerics).
func nullSafeFast(dest, src any) bool {
	switch d := dest.(type) {
	case *string:
		switch v := src.(type) {
		case nil:
			*d = ""
		case string:
			*d = v
		case []byte:
			*d = string(v)
		default:
			return false
		}
	case *int64:
		switch v := src.(type) {
		case nil:
			*d = 0
		case int64:
			*d = v
		default:
			return false
		}
	case *int:
		switch v := src.(type) {
		case nil:
			*d = 0
		case int64:
			*d = int(v)
		default:
			return false
		}
	case *int32:
		switch v := src.(type) {
		case nil:
			*d = 0
		case int64:
			*d = int32(v)
		default:
			return false
		}
	case *int16:
		switch v := src.(type) {
		case nil:
			*d = 0
		case int64:
			*d = int16(v)
		default:
			return false
		}
	case *int8:
		switch v := src.(type) {
		case nil:
			*d = 0
		case int64:
			*d = int8(v)
		default:
			return false
		}
	case *uint64:
		switch v := src.(type) {
		case nil:
			*d = 0
		case int64:
			*d = uint64(v)
		default:
			return false
		}
	case *uint:
		switch v := src.(type) {
		case nil:
			*d = 0
		case int64:
			*d = uint(v)
		default:
			return false
		}
	case *uint32:
		switch v := src.(type) {
		case nil:
			*d = 0
		case int64:
			*d = uint32(v)
		default:
			return false
		}
	case *uint16:
		switch v := src.(type) {
		case nil:
			*d = 0
		case int64:
			*d = uint16(v)
		default:
			return false
		}
	case *uint8:
		switch v := src.(type) {
		case nil:
			*d = 0
		case int64:
			*d = uint8(v)
		default:
			return false
		}
	case *float64:
		switch v := src.(type) {
		case nil:
			*d = 0
		case float64:
			*d = v
		case int64:
			*d = float64(v)
		default:
			return false
		}
	case *float32:
		switch v := src.(type) {
		case nil:
			*d = 0
		case float64:
			*d = float32(v)
		default:
			return false
		}
	case *bool:
		switch v := src.(type) {
		case nil:
			*d = false
		case bool:
			*d = v
		default:
			return false
		}
	case *time.Time:
		switch v := src.(type) {
		case nil:
			*d = time.Time{}
		case time.Time:
			*d = v
		default:
			return false
		}
	case **string:
		return fastPtr(d, src, func(s any) (string, bool) {
			switch v := s.(type) {
			case string:
				return v, true
			case []byte:
				return string(v), true
			}
			return "", false
		})
	case **int64:
		return fastPtr(d, src, func(s any) (int64, bool) { v, ok := s.(int64); return v, ok })
	case **int:
		return fastPtr(d, src, func(s any) (int, bool) { v, ok := s.(int64); return int(v), ok })
	case **int32:
		return fastPtr(d, src, func(s any) (int32, bool) { v, ok := s.(int64); return int32(v), ok })
	case **float64:
		return fastPtr(d, src, func(s any) (float64, bool) {
			switch v := s.(type) {
			case float64:
				return v, true
			case int64:
				return float64(v), true
			}
			return 0, false
		})
	case **bool:
		return fastPtr(d, src, func(s any) (bool, bool) { v, ok := s.(bool); return v, ok })
	case **time.Time:
		return fastPtr(d, src, func(s any) (time.Time, bool) { v, ok := s.(time.Time); return v, ok })
	default:
		return false
	}
	return true
}

// fastPtr handles pointer-to-scalar destinations without reflection. NULL
// leaves the pointer nil; a matching source always gets a fresh allocation so
// a row value retained by the caller never aliases a later row. Mismatches
// fall back to the lenient slow path.
func fastPtr[T any](d **T, src any, conv func(any) (T, bool)) bool {
	if src == nil {
		*d = nil
		return true
	}
	v, ok := conv(src)
	if !ok {
		return false
	}
	p := new(T)
	*p = v
	*d = p
	return true
}

var (
	scannerType  = reflect.TypeOf((*sql.Scanner)(nil)).Elem()
	rawBytesType = reflect.TypeOf(sql.RawBytes(nil))
	bytesType    = reflect.TypeOf([]byte(nil))
	// nullSafeSkipCache maps a field type to whether the destination can be
	// scanned by database/sql directly with identical NULL semantics.
	nullSafeSkipCache sync.Map // reflect.Type -> bool
)

// nativeNullHandling reports whether a pointer to a field of type t can be
// handed straight to database/sql with the same NULL -> zero behaviour.
func nativeNullHandling(t reflect.Type) bool {
	if v, ok := nullSafeSkipCache.Load(t); ok {
		return v.(bool)
	}
	r := computeNativeNullHandling(t)
	nullSafeSkipCache.Store(t, r)
	return r
}

func computeNativeNullHandling(t reflect.Type) bool {
	if reflect.PointerTo(t).Implements(scannerType) {
		return true
	}
	if t == bytesType || t == rawBytesType {
		return true
	}
	if t.Kind() == reflect.Ptr {
		// NULL sets the pointer to nil, matching zero-value semantics.
		e := t.Elem()
		// Pointer-to-scalar (and *time.Time) deliberately stay wrapped: the
		// wrapper keeps lenient semantics (string->number/time parsing, zero
		// on mismatch) and nullSafeFast handles the native cases without
		// reflection, so skipping would only trade robustness for nothing.
		if reflect.PointerTo(e).Implements(scannerType) {
			return true
		}
	}
	return false
}

func (ns *nullSafe) Value() (driver.Value, error) {
	// If ns.dest implements driver.Valuer, delegate.
	if valuer, ok := ns.dest.(driver.Valuer); ok {
		return valuer.Value()
	}
	v := reflect.ValueOf(ns.dest).Elem()
	if !v.IsValid() {
		return nil, nil
	}
	// Optionally, return nil for zero values if desired.
	return v.Interface(), nil
}

var timeType = reflect.TypeOf(time.Time{})

func assignDirect(destVal reflect.Value, src any) error {
	srcVal := reflect.ValueOf(src)
	if !srcVal.IsValid() {
		return fmt.Errorf("invalid source value")
	}
	if srcVal.Type().AssignableTo(destVal.Type()) {
		destVal.Set(srcVal)
		return nil
	}
	if srcVal.Type().ConvertibleTo(destVal.Type()) {
		destVal.Set(srcVal.Convert(destVal.Type()))
		return nil
	}
	return fmt.Errorf("not assignable")
}

func assignFromStringLike(destVal reflect.Value, src any) error {
	switch v := src.(type) {
	case string:
		return assignFromString(destVal, v)
	case []byte:
		if destVal.Kind() == reflect.Slice && destVal.Type().Elem().Kind() == reflect.Uint8 {
			cp := append([]byte(nil), v...)
			destVal.Set(reflect.ValueOf(cp))
			return nil
		}
		return assignFromString(destVal, string(v))
	case fmt.Stringer:
		return assignFromString(destVal, v.String())
	default:
		return fmt.Errorf("not string-like")
	}
}

func assignFromString(destVal reflect.Value, s string) error {
	s = strings.TrimSpace(s)
	if s == "" || strings.EqualFold(s, "null") {
		destVal.Set(reflect.Zero(destVal.Type()))
		return nil
	}
	switch destVal.Kind() {
	case reflect.String:
		destVal.SetString(s)
		return nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i64, err := strconv.ParseInt(s, 10, destVal.Type().Bits())
		if err != nil {
			return err
		}
		destVal.SetInt(i64)
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		u64, err := strconv.ParseUint(s, 10, destVal.Type().Bits())
		if err != nil {
			return err
		}
		destVal.SetUint(u64)
		return nil
	case reflect.Float32, reflect.Float64:
		f64, err := strconv.ParseFloat(s, destVal.Type().Bits())
		if err != nil {
			return err
		}
		destVal.SetFloat(f64)
		return nil
	case reflect.Bool:
		b, err := strconv.ParseBool(s)
		if err != nil {
			return err
		}
		destVal.SetBool(b)
		return nil
	case reflect.Slice:
		if destVal.Type().Elem().Kind() == reflect.Uint8 {
			destVal.Set(reflect.ValueOf([]byte(s)))
			return nil
		}
	case reflect.Struct:
		if destVal.Type() == timeType {
			tm, err := parseFlexibleTime(s)
			if err != nil {
				return err
			}
			destVal.Set(reflect.ValueOf(tm))
			return nil
		}
	case reflect.Interface:
		if destVal.NumMethod() == 0 {
			destVal.Set(reflect.ValueOf(s))
			return nil
		}
	}
	return fmt.Errorf("unhandled string assignment")
}

func parseFlexibleTime(s string) (time.Time, error) {
	if parsed, err := date.Parse(s); err == nil {
		return parsed, nil
	}
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02 15:04:05.999999999 +0700 +0700 m=+0.000000001",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if parsed, err := time.Parse(layout, s); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported time format: %s", s)
}

func tryJSONAssign(destVal reflect.Value, src any) error {
	switch destVal.Kind() {
	case reflect.Struct, reflect.Map, reflect.Slice, reflect.Array, reflect.Interface:
	default:
		return fmt.Errorf("json not applicable")
	}
	var data []byte
	switch v := src.(type) {
	case []byte:
		data = v
	case string:
		data = []byte(v)
	default:
		if s := fmt.Sprintf("%v", src); s != "" {
			data = []byte(s)
		}
	}
	if len(data) == 0 {
		return fmt.Errorf("empty json data")
	}
	if !destVal.CanAddr() {
		return fmt.Errorf("destination not addressable")
	}
	return json.Unmarshal(data, destVal.Addr().Interface())
}

// fieldsByTraversal fills a values interface with fields from the passed value based
// on the traversals in int.  If ptrs is true, return addresses instead of values.
// We write this instead of using FieldsByName to save allocations and map lookups
// when iterating over many rows.  Empty traversals will get an interface pointer.
// Because of the necessity of requesting ptrs or values, it's considered a bit too
// specialized for inclusion in reflectx itself.
func fieldsByTraversal(octx *reflectx.ObjectContext, v reflect.Value, traversals [][]int, values []any, ptrs bool) error {
	return fieldsByTraversalArena(octx, v, traversals, values, ptrs, nil)
}

// scanScratch holds the per-query scan state (scan arguments, object context
// and a nullSafe arena) so one-shot struct scans (Get, Select) allocate none
// of it. It is pooled and cleared on release so it never retains user data.
type scanScratch struct {
	octx   reflectx.ObjectContext
	values []any
	ns     []nullSafe
}

var scanScratchPool = sync.Pool{New: func() any { return new(scanScratch) }}

const maxPooledScanColumns = 256

func getScanScratch(n int) *scanScratch {
	s := scanScratchPool.Get().(*scanScratch)
	if cap(s.values) < n {
		s.values = make([]any, n)
	} else {
		s.values = s.values[:n]
	}
	if cap(s.ns) < n {
		s.ns = make([]nullSafe, 0, n)
	} else {
		s.ns = s.ns[:0]
	}
	return s
}

func (s *scanScratch) release() {
	clear(s.values)
	for i := range s.ns {
		s.ns[i].dest = nil
	}
	s.ns = s.ns[:0]
	s.octx.NewRow(reflect.Value{})
	if cap(s.values) <= maxPooledScanColumns {
		scanScratchPool.Put(s)
	}
}

// fieldsByTraversalArena is fieldsByTraversal with an optional arena for the
// nullSafe wrappers. The arena must have capacity for len(traversals) entries;
// wrappers are appended within capacity so their addresses stay stable.
func fieldsByTraversalArena(octx *reflectx.ObjectContext, v reflect.Value, traversals [][]int, values []any, ptrs bool, arena *[]nullSafe) error {
	v = reflect.Indirect(v)
	if v.Kind() != reflect.Struct {
		return errors.New("argument not a struct")
	}

	octx.NewRow(v)

	// values may be reused across rows with the same traversals and the same
	// octx: previously built wrappers are retargeted instead of reallocated,
	// and row-independent entries (unmapped columns, nested scanners that
	// follow octx) are kept as they are.
	for i, traversal := range traversals {
		if len(traversal) == 0 {
			if values[i] == nil {
				values[i] = new(any)
			}
			continue
		}
		if !ptrs {
			values[i] = octx.FieldForIndexes(traversal).Interface()
			continue
		}
		if len(traversal) > 1 && values[i] != nil {
			continue
		}
		f := octx.FieldForIndexes(traversal)
		dest := f.Addr().Interface()
		if ns, ok := values[i].(*nullSafe); ok && len(traversal) == 1 {
			ns.dest = dest
			continue
		}
		// Types that handle NULL natively skip the wrapper entirely.
		if nativeNullHandling(f.Type()) {
			values[i] = dest
		} else if arena != nil && len(*arena) < cap(*arena) {
			*arena = append(*arena, nullSafe{dest: dest})
			values[i] = &(*arena)[len(*arena)-1]
		} else {
			values[i] = &nullSafe{dest: dest}
		}
	}
	return nil
}
