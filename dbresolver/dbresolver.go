package dbresolver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/oarkflow/squealx"
)

// errors.
var (
	errNoPrimaryDB            = errors.New("dbresolver: no primary database")
	errInvalidReadWritePolicy = errors.New("dbresolver: invalid read/write policy")
	errNoDBToRead             = errors.New("dbresolver: no database to read")
)

// ReadWritePolicy is the read/write policy for the primary databases.
type ReadWritePolicy string

// ReadWritePolicies.
const (
	ReadWrite ReadWritePolicy = "read-write"
	ReadOnly  ReadWritePolicy = "read-only"
	WriteOnly ReadWritePolicy = "write-only"
)

var validReadWritePolicies = map[ReadWritePolicy]struct{}{
	ReadWrite: {},
	WriteOnly: {},
	ReadOnly:  {},
}

// Config is the config of primary databases.
type Config struct {
	DBs             []*squealx.DB
	DefaultDB       *squealx.DB
	ReadWritePolicy ReadWritePolicy
}

// DBResolver chooses one of databases and then executes a query.
// This supposed to be aligned with sqlx.DB.
// Some functions which must select from multiple database are only available for the primary DBResolver
// or the first primary DBResolver (if using multi-primary). For example, `DriverName()`, `Unsafe()`.
type DBResolver interface {
	Begin() (squealx.SQLTx, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (squealx.SQLTx, error)
	BeginTxx(ctx context.Context, opts *sql.TxOptions) (*squealx.Tx, error)
	Beginx() (*squealx.Tx, error)
	BindNamed(query string, arg any) (string, []any, error)
	Close() error
	Use(db string) (*squealx.DB, error)
	Register(db *squealx.DB, useAsDefault bool)
	RegisterMaster(db *squealx.DB, useAsDefault bool)
	RegisterReplica(db *squealx.DB)
	RegisterRead(db *squealx.DB)
	GetDB(ctx context.Context, dbs []string) *squealx.DB
	ResolveDB(ctx context.Context, dbs []string) (*squealx.DB, error)
	Conn(ctx context.Context) (squealx.SQLConn, error)
	Connx(ctx context.Context) (*squealx.Conn, error)
	Driver() driver.Driver
	DriverName() string

	ExecWithReturn(query string, args any) error
	LazyExec(query string) func(args ...any) (sql.Result, error)
	LazyExecWithReturn(query string) func(args any) error
	LazySelect(query string) func(dest any, args ...any) error

	Exec(query string, args ...any) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Get(dest any, query string, args ...any) error
	GetContext(ctx context.Context, dest any, query string, args ...any) error
	MapperFunc(mf func(string) string)
	MustBegin() *squealx.Tx
	MustBeginTx(ctx context.Context, opts *sql.TxOptions) *squealx.Tx
	MustExec(query string, args ...any) sql.Result
	MustExecContext(ctx context.Context, query string, args ...any) sql.Result
	NamedExec(query string, arg any) (sql.Result, error)
	NamedExecContext(ctx context.Context, query string, arg any) (sql.Result, error)
	NamedQuery(query string, arg any) (*squealx.Rows, error)
	NamedQueryContext(ctx context.Context, query string, arg any) (*squealx.Rows, error)
	Ping() error
	PingContext(ctx context.Context) error
	Prepare(query string) (Stmt, error)
	PrepareContext(ctx context.Context, query string) (Stmt, error)
	PrepareNamed(query string) (NamedStmt, error)
	PrepareNamedContext(ctx context.Context, query string) (NamedStmt, error)
	Preparex(query string) (Stmt, error)
	PreparexContext(ctx context.Context, query string) (Stmt, error)
	Query(query string, args ...any) (squealx.SQLRows, error)
	QueryContext(ctx context.Context, query string, args ...any) (squealx.SQLRows, error)
	QueryRow(query string, args ...any) squealx.SQLRow
	QueryRowContext(ctx context.Context, query string, args ...any) squealx.SQLRow
	QueryRowx(query string, args ...any) *squealx.Row
	QueryRowxContext(ctx context.Context, query string, args ...any) *squealx.Row
	Queryx(query string, args ...any) (*squealx.Rows, error)
	QueryxContext(ctx context.Context, query string, args ...any) (*squealx.Rows, error)
	Rebind(query string) string
	Select(dest any, query string, args ...any) error
	NamedSelect(dest any, query string, args any) error
	NamedGet(dest any, query string, args any) error
	SelectContext(ctx context.Context, dest any, query string, args ...any) error
	SetConnMaxIdleTime(d time.Duration)
	SetConnMaxLifetime(d time.Duration)
	SetMaxIdleConns(n int)
	SetMaxOpenConns(n int)
	Stats() sql.DBStats
	Unsafe() *squealx.DB
	MasterDBs() []*squealx.DB
	ReplicaDBs() []*squealx.DB
	ReadDBs() []*squealx.DB
	LoadBalancer() LoadBalancer
	GetQuery(string) *squealx.Query
	Paginate(query string, result any, paging squealx.Paging, params ...map[string]any) squealx.PaginatedResponse
	SetDefaultDB(db string)
	UseDefault() (*squealx.DB, error)
	UseBefore(hooks ...squealx.Hook)
	WithHooks(hooks ...any)
	UseAfter(hooks ...squealx.Hook)
	UseOnError(onError ...squealx.ErrorHook)
}

type dbResolver struct {
	masters      []string
	replicas     []string
	readDBs      []string
	dbs          map[string]*squealx.DB
	defaultDB    string
	policy       ReadWritePolicy
	loadBalancer LoadBalancer
	queryLoader  *squealx.FileLoader
	mu           sync.RWMutex
}

var _ DBResolver = (*dbResolver)(nil)

func appendUnique(ids []string, id string) []string {
	for _, existing := range ids {
		if existing == id {
			return ids
		}
	}
	return append(ids, id)
}

func (r *dbResolver) snapshotDBs() []*squealx.DB {
	r.mu.RLock()
	defer r.mu.RUnlock()
	dbs := make([]*squealx.DB, 0, len(r.dbs))
	for _, db := range r.dbs {
		if db != nil {
			dbs = append(dbs, db)
		}
	}
	return dbs
}

// New creates a new DBResolver and returns it.
// If no primary DBResolver is given, it returns an error.
// If you do not give WriteOnly option, it will use the primary DBResolver as the read DBResolver.
// if you do not give LoadBalancer option, it will use the RandomLoadBalancer.
func New(opts ...OptionFunc) (DBResolver, error) {
	dbs := make(map[string]*squealx.DB)
	var masterDBs, replicaDBs, readDBs []string
	options, err := compileOptions(opts...)
	if err != nil {
		return nil, err
	}
	if len(options.masterDBs) == 0 && options.defaultDB != nil {
		options.masterDBs = append(options.masterDBs, options.defaultDB)
	}
	if len(options.masterDBs) == 0 {
		return nil, errNoPrimaryDB
	}
	if options.readWritePolicy == "" {
		options.readWritePolicy = ReadWrite
	}
	if _, ok := validReadWritePolicies[options.readWritePolicy]; !ok {
		return nil, errInvalidReadWritePolicy
	}

	var readReplicas []*squealx.DB
	readReplicas = append(readReplicas, options.replicaDBs...)
	if options.readWritePolicy == ReadWrite {
		readReplicas = append(readReplicas, options.masterDBs...)
	}
	for _, db := range options.masterDBs {
		masterDBs = append(masterDBs, db.ID)
		if _, exists := dbs[db.ID]; !exists {
			dbs[db.ID] = db
		}
	}
	for _, db := range options.replicaDBs {
		replicaDBs = append(replicaDBs, db.ID)
		if _, exists := dbs[db.ID]; !exists {
			dbs[db.ID] = db
		}
	}
	for _, db := range readReplicas {
		readDBs = append(readDBs, db.ID)
		if _, exists := dbs[db.ID]; !exists {
			dbs[db.ID] = db
		}
	}
	defaultDB := ""
	if options.defaultDB != nil {
		if _, exists := dbs[options.defaultDB.ID]; !exists {
			dbs[options.defaultDB.ID] = options.defaultDB
		}
		defaultDB = options.defaultDB.ID
	}
	return &dbResolver{
		masters:      masterDBs,
		replicas:     replicaDBs,
		readDBs:      readDBs,
		loadBalancer: options.loadBalancer,
		queryLoader:  options.fileLoader,
		defaultDB:    defaultDB,
		dbs:          dbs,
		policy:       options.readWritePolicy,
	}, nil
}

func compileOptions(opts ...OptionFunc) (*Options, error) {
	options := &Options{}
	for _, opt := range opts {
		opt(options)
	}

	if options.loadBalancer == nil {
		options.loadBalancer = NewRandomLoadBalancer()
	}

	return options, nil
}

func MustNew(opts ...OptionFunc) DBResolver {
	db, err := New(opts...)
	if err != nil {
		panic(err)
	}
	return db
}

func (r *dbResolver) MasterDBs() (dbs []*squealx.DB) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, db := range r.masters {
		if val, exists := r.dbs[db]; exists {
			dbs = append(dbs, val)
		}
	}
	return
}

func (r *dbResolver) WithHooks(hooks ...any) {
	for _, db := range r.snapshotDBs() {
		db.Use(hooks...)
	}
}

func (r *dbResolver) UseBefore(hooks ...squealx.Hook) {
	for _, db := range r.snapshotDBs() {
		db.UseBefore(hooks...)
	}
}

func (r *dbResolver) UseAfter(hooks ...squealx.Hook) {
	for _, db := range r.snapshotDBs() {
		db.UseAfter(hooks...)
	}
}

func (r *dbResolver) UseOnError(onError ...squealx.ErrorHook) {
	for _, db := range r.snapshotDBs() {
		db.UseOnError(onError...)
	}
}

func (r *dbResolver) resolveWrite(ctx context.Context) (*squealx.DB, error) {
	r.mu.RLock()
	candidates := append([]string(nil), r.masters...)
	r.mu.RUnlock()
	return r.ResolveDB(ctx, candidates)
}

func (r *dbResolver) resolveRead(ctx context.Context) (*squealx.DB, error) {
	r.mu.RLock()
	candidates := append([]string(nil), r.readDBs...)
	r.mu.RUnlock()
	return r.ResolveDB(ctx, candidates)
}

func readWithFailover[T any](r *dbResolver, ctx context.Context, operation func(*squealx.DB) (T, error)) (T, error) {
	var zero T
	db, err := r.resolveRead(ctx)
	if err != nil {
		return zero, err
	}
	value, err := operation(db)
	if isDBConnectionError(err) {
		if primary, resolveErr := r.resolveWrite(ctx); resolveErr == nil && primary != db {
			return operation(primary)
		}
	}
	return value, err
}

func (r *dbResolver) roleIDs() (masters, reads []string) {
	r.mu.RLock()
	masters = append([]string(nil), r.masters...)
	reads = append([]string(nil), r.readDBs...)
	r.mu.RUnlock()
	return masters, reads
}

func (r *dbResolver) GetDB(ctx context.Context, dbs []string) *squealx.DB {
	db, _ := r.ResolveDB(ctx, dbs)
	return db
}

// ResolveDB selects a valid database without panicking. The default database
// is honored only when it belongs to the candidate set, preventing a default
// read replica from accidentally receiving writes.
func (r *dbResolver) ResolveDB(ctx context.Context, candidates []string) (*squealx.DB, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.RLock()
	valid := make([]string, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for _, id := range candidates {
		if _, duplicate := seen[id]; duplicate {
			continue
		}
		if _, exists := r.dbs[id]; exists {
			valid = append(valid, id)
			seen[id] = struct{}{}
		}
	}
	if len(valid) == 0 {
		r.mu.RUnlock()
		return nil, errNoDBToRead
	}
	selected := ""
	if r.defaultDB != "" {
		if _, ok := seen[r.defaultDB]; ok {
			selected = r.defaultDB
		}
	}
	if selected == "" {
		selected = r.loadBalancer.Select(ctx, valid)
	}
	db := r.dbs[selected]
	r.mu.RUnlock()
	if db == nil {
		return nil, fmt.Errorf("dbresolver: selected database %q is unavailable", selected)
	}
	return db, nil
}

func (r *dbResolver) SetDefaultDB(db string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if db != "" {
		if _, exists := r.dbs[db]; !exists {
			return
		}
		r.defaultDB = db
	}
}

func (r *dbResolver) UseDefault() (*squealx.DB, error) {
	r.mu.RLock()
	defaultDB := r.defaultDB
	r.mu.RUnlock()
	if defaultDB == "" {
		return nil, errors.New("no default database set")
	}
	return r.Use(defaultDB)
}

func (r *dbResolver) ReplicaDBs() (dbs []*squealx.DB) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, db := range r.replicas {
		if val, exists := r.dbs[db]; exists {
			dbs = append(dbs, val)
		}
	}
	return
}

func (r *dbResolver) Use(db string) (*squealx.DB, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if db, exists := r.dbs[db]; exists {
		return db, nil
	}
	return nil, errors.New("no database with the provided id: " + db)
}

func (r *dbResolver) Register(db *squealx.DB, useAsDefault bool) {
	if db == nil || db.ID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.dbs[db.ID]; !exists {
		r.dbs[db.ID] = db
	}
	if useAsDefault {
		r.defaultDB = db.ID
	}
}

func (r *dbResolver) RegisterMaster(db *squealx.DB, useAsDefault bool) {
	if db == nil || db.ID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.masters = appendUnique(r.masters, db.ID)
	if _, exists := r.dbs[db.ID]; !exists {
		r.dbs[db.ID] = db
	}
	if r.policy == ReadWrite {
		r.readDBs = appendUnique(r.readDBs, db.ID)
	}
	if useAsDefault {
		r.defaultDB = db.ID
	}
}

func (r *dbResolver) RegisterReplica(db *squealx.DB) {
	if db == nil || db.ID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.replicas = appendUnique(r.replicas, db.ID)
	if _, exists := r.dbs[db.ID]; !exists {
		r.dbs[db.ID] = db
	}
}

func (r *dbResolver) RegisterRead(db *squealx.DB) {
	if db == nil || db.ID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.readDBs = appendUnique(r.readDBs, db.ID)
	if _, exists := r.dbs[db.ID]; !exists {
		r.dbs[db.ID] = db
	}
}

func (r *dbResolver) ReadDBs() (dbs []*squealx.DB) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, db := range r.readDBs {
		if val, exists := r.dbs[db]; exists {
			dbs = append(dbs, val)
		}
	}
	return
}

func (r *dbResolver) LoadBalancer() LoadBalancer {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.loadBalancer
}

func (r *dbResolver) getDB(id string) (*squealx.DB, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if id == "" {
		return nil, errors.New("id not provided")
	}
	db, exists := r.dbs[id]
	if !exists {
		return nil, errors.New("invalid ID provided")
	}
	return db, nil
}

// Begin chooses a primary database and starts a transaction.
// This supposed to be aligned with sqlx.DB.Begin.
func (r *dbResolver) Begin() (squealx.SQLTx, error) {
	db, err := r.resolveWrite(context.Background())
	if err != nil {
		return nil, err
	}
	return db.Begin()
}

// BeginTx chooses a primary database and starts a transaction.
// This supposed to be aligned with sqlx.DB.BeginTx.
func (r *dbResolver) BeginTx(ctx context.Context, opts *sql.TxOptions) (squealx.SQLTx, error) {
	db, err := r.resolveWrite(ctx)
	if err != nil {
		return nil, err
	}
	return db.BeginTx(ctx, opts)
}

// BeginTxx chooses a primary database, begins a transaction and returns an *squealx.Tx
// This supposed to be aligned with sqlx.DB.BeginTxx.
func (r *dbResolver) BeginTxx(ctx context.Context, opts *sql.TxOptions) (*squealx.Tx, error) {
	db, err := r.resolveWrite(ctx)
	if err != nil {
		return nil, err
	}
	return db.BeginTxx(ctx, opts)
}

// Beginx chooses a primary database, begins a transaction and returns an *squealx.Tx
// This supposed to be aligned with sqlx.DB.Beginx.
func (r *dbResolver) Beginx() (*squealx.Tx, error) {
	db, err := r.resolveWrite(context.Background())
	if err != nil {
		return nil, err
	}
	return db.Beginx()
}

// BindNamed chooses a primary database and binds a query using the DB driver's bindvar type.
// This supposed to be aligned with sqlx.DB.BindNamed.
func (r *dbResolver) BindNamed(query string, arg any) (string, []any, error) {
	db, err := r.resolveWrite(context.Background())
	if err != nil {
		return "", nil, err
	}
	return db.BindNamed(query, arg)
}

func (r *dbResolver) Paginate(query string, result any, paging squealx.Paging, params ...map[string]any) squealx.PaginatedResponse {
	query = r.GetQueryString(query)
	db, err := r.resolveRead(context.Background())
	if err != nil {
		return squealx.PaginatedResponse{Error: err}
	}
	makeParam := func(target *squealx.DB) *squealx.Param {
		p := &squealx.Param{DB: target, Query: query, Paging: &paging}
		if len(params) > 0 {
			p.Param = params[0]
		}
		return p
	}
	pages, err := squealx.Pages(makeParam(db), result)
	if isDBConnectionError(err) {
		if primary, resolveErr := r.resolveWrite(context.Background()); resolveErr == nil && primary != db {
			pages, err = squealx.Pages(makeParam(primary), result)
		}
	}
	if err != nil {
		return squealx.PaginatedResponse{Error: err}
	}
	return squealx.PaginatedResponse{Items: result, Pagination: pages}
}

// Close closes all the databases.
func (r *dbResolver) Close() error {
	var errs []error
	for _, db := range r.snapshotDBs() {
		if err := db.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Conn chooses a primary database and returns a squealx.SQLConn.
// This supposed to be aligned with sqlx.DB.Conn.
func (r *dbResolver) Conn(ctx context.Context) (squealx.SQLConn, error) {
	db, err := r.resolveWrite(ctx)
	if err != nil {
		return nil, err
	}
	return db.Conn(ctx)
}

// Connx chooses a primary database and returns a *squealx.Conn.
// This supposed to be aligned with sqlx.DB.Connx.
func (r *dbResolver) Connx(ctx context.Context) (*squealx.Conn, error) {
	db, err := r.resolveWrite(ctx)
	if err != nil {
		return nil, err
	}
	return db.Connx(ctx)
}

// Driver chooses a primary database and returns a driver.Driver.
// This supposed to be aligned with sqlx.DB.Driver.
func (r *dbResolver) Driver() driver.Driver {
	db, err := r.resolveWrite(context.Background())
	if err != nil {
		return nil
	}
	return db.Driver()
}

// DriverName chooses a primary database and returns the driverName.
// This supposed to be aligned with sqlx.DB.DriverName.
func (r *dbResolver) DriverName() string {
	db, err := r.resolveWrite(context.Background())
	if err != nil {
		return ""
	}
	return db.DriverName()
}

func (r *dbResolver) GetQuery(query string) *squealx.Query {
	if r.queryLoader != nil {
		return r.queryLoader.GetQuery(query)
	}
	return nil
}

func (r *dbResolver) GetQueryString(query string) string {
	q := r.GetQuery(query)
	if q == nil {
		return query
	}
	return q.Query
}

// Exec chooses a primary database and executes a query without returning any rows.
// This supposed to be aligned with sqlx.DB.Exec.
func (r *dbResolver) Exec(query string, args ...any) (sql.Result, error) {
	query = r.GetQueryString(query)
	if squealx.IsNamedQuery(query) && len(args) > 0 {
		return r.NamedExec(query, args[0])
	}
	db, err := r.resolveWrite(context.Background())
	if err != nil {
		return nil, err
	}
	return db.Exec(query, args...)
}

// ExecContext chooses a primary database and executes a query without returning any rows.
// This supposed to be aligned with sqlx.DB.ExecContext.
func (r *dbResolver) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	query = r.GetQueryString(query)
	if squealx.IsNamedQuery(query) && len(args) > 0 {
		return r.NamedExecContext(ctx, query, args[0])
	}
	db, err := r.resolveWrite(ctx)
	if err != nil {
		return nil, err
	}
	return db.ExecContext(ctx, query, args...)
}

// Get chooses a readable database and Get using chosen DB.
// This supposed to be aligned with sqlx.DB.Get.
func (r *dbResolver) Get(dest any, query string, args ...any) error {
	query = r.GetQueryString(query)
	db, err := r.resolveRead(context.Background())
	if err != nil {
		return err
	}
	err = db.Get(dest, query, args...)
	if isDBConnectionError(err) {
		if primary, resolveErr := r.resolveWrite(context.Background()); resolveErr == nil && primary != db {
			err = primary.Get(dest, query, args...)
		}
	}
	return err
}

// GetContext chooses a readable database and Get using chosen DB.
// This supposed to be aligned with sqlx.DB.GetContext.
func (r *dbResolver) GetContext(ctx context.Context, dest any, query string, args ...any) error {
	query = r.GetQueryString(query)
	db, err := r.resolveRead(ctx)
	if err != nil {
		return err
	}
	err = db.GetContext(ctx, dest, query, args...)
	if isDBConnectionError(err) {
		if primary, resolveErr := r.resolveWrite(ctx); resolveErr == nil && primary != db {
			err = primary.GetContext(ctx, dest, query, args...)
		}
	}
	return err
}

// MapperFunc sets the mapper function for the all primary databases and secondary databases.
func (r *dbResolver) MapperFunc(mf func(string) string) {
	for _, db := range r.snapshotDBs() {
		db.MapperFunc(mf)
	}
}

// MustBegin chooses a primary database, starts a transaction and returns an *squealx.Tx or panic.
// This supposed to be aligned with sqlx.DB.MustBegin.
func (r *dbResolver) MustBegin() *squealx.Tx {
	db, err := r.resolveWrite(context.Background())
	if err != nil {
		panic(err)
	}
	return db.MustBegin()
}

// MustBeginTx chooses a primary database, starts a transaction and returns an *squealx.Tx or panic.
// This supposed to be aligned with sqlx.DB.MustBeginTx.
func (r *dbResolver) MustBeginTx(ctx context.Context, opts *sql.TxOptions) *squealx.Tx {
	db, err := r.resolveWrite(ctx)
	if err != nil {
		panic(err)
	}
	return db.MustBeginTx(ctx, opts)
}

// MustExec chooses a primary database and executes a query or panic.
// This supposed to be aligned with sqlx.DB.MustExec.
func (r *dbResolver) MustExec(query string, args ...any) sql.Result {
	query = r.GetQueryString(query)
	db, err := r.resolveWrite(context.Background())
	if err != nil {
		panic(err)
	}
	if squealx.IsNamedQuery(query) && len(args) > 0 {
		rs, err := db.NamedExec(query, args[0])
		if err != nil {
			panic(err)
		}
		return rs
	}
	return db.MustExec(query, args...)
}

// MustExecContext chooses a primary database and executes a query or panic.
// This supposed to be aligned with sqlx.DB.MustExecContext.
func (r *dbResolver) MustExecContext(ctx context.Context, query string, args ...any) sql.Result {
	query = r.GetQueryString(query)
	db, err := r.resolveWrite(ctx)
	if err != nil {
		panic(err)
	}
	if squealx.IsNamedQuery(query) && len(args) > 0 {
		rs, err := db.NamedExecContext(ctx, query, args[0])
		if err != nil {
			panic(err)
		}
		return rs
	}
	return db.MustExecContext(ctx, query, args...)
}

// NamedExec chooses a primary database and then executes a named query.
// This supposed to be aligned with sqlx.DB.NamedExec.
func (r *dbResolver) NamedExec(query string, arg any) (sql.Result, error) {
	query = r.GetQueryString(query)
	db, err := r.resolveWrite(context.Background())
	if err != nil {
		return nil, err
	}
	return db.NamedExec(query, arg)
}

// NamedExecContext chooses a primary database and then executes a named query.
// This supposed to be aligned with sqlx.DB.NamedExecContext.
func (r *dbResolver) NamedExecContext(ctx context.Context, query string, arg any) (sql.Result, error) {
	query = r.GetQueryString(query)
	db, err := r.resolveWrite(ctx)
	if err != nil {
		return nil, err
	}
	return db.NamedExecContext(ctx, query, arg)
}

// NamedQuery chooses a readable database and then executes a named query.
// This supposed to be aligned with sqlx.DB.NamedQuery.
func (r *dbResolver) NamedQuery(query string, arg any) (*squealx.Rows, error) {
	query = r.GetQueryString(query)
	return readWithFailover(r, context.Background(), func(db *squealx.DB) (*squealx.Rows, error) {
		return db.NamedQuery(query, arg)
	})
}

// NamedQueryContext chooses a readable database and then executes a named query.
// This supposed to be aligned with sqlx.DB.NamedQueryContext.
func (r *dbResolver) NamedQueryContext(ctx context.Context, query string, arg any) (*squealx.Rows, error) {
	query = r.GetQueryString(query)
	return readWithFailover(r, ctx, func(db *squealx.DB) (*squealx.Rows, error) {
		return db.NamedQueryContext(ctx, query, arg)
	})
}

// Ping sends a ping to the all databases.
func (r *dbResolver) Ping() error {
	var errs []error
	for _, db := range r.snapshotDBs() {
		if err := db.Ping(); err != nil {
			errs = append(errs, err)
		}
	}
	if errs != nil {
		return errors.Join(errs...)
	}
	return nil
}

// PingContext sends a ping to the all databases.
func (r *dbResolver) PingContext(ctx context.Context) error {
	var errs []error
	for _, db := range r.snapshotDBs() {
		if err := db.PingContext(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if errs != nil {
		return errors.Join(errs...)
	}
	return nil
}

// Prepare returns a Stmt which can be used sql.Stmt instead.
// This supposed to be aligned with sqlx.DB.Prepare.
func (r *dbResolver) Prepare(query string) (Stmt, error) {
	masters, readDBs := r.roleIDs()
	query = r.GetQueryString(query)
	primaryDBStmts := make(map[*squealx.DB]*squealx.Stmt, len(masters))
	readDBStmts := make(map[*squealx.DB]*squealx.Stmt, len(readDBs))

	var errs []error
	for _, id := range masters {
		db, err := r.getDB(id)
		if err != nil {
			return nil, err
		}
		stmt, err := db.Preparex(query)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		primaryDBStmts[db] = stmt
	}
	for _, id := range readDBs {
		db, err := r.getDB(id)
		if err != nil {
			return nil, err
		}
		stmt, err := db.Preparex(query)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		readDBStmts[db] = stmt
	}
	if errs != nil {
		return nil, errors.Join(errs...)
	}

	return &stmt{
		masters:      masters,
		readReplicas: readDBs,
		masterStmts:  primaryDBStmts,
		replicaStmts: readDBStmts,
		db:           r,
	}, nil
}

// PrepareContext returns a Stmt which can be used sql.Stmt instead.
// This supposed to be aligned with sqlx.DB.PrepareContext.
func (r *dbResolver) PrepareContext(ctx context.Context, query string) (Stmt, error) {
	masters, readDBs := r.roleIDs()
	query = r.GetQueryString(query)
	primaryDBStmts := make(map[*squealx.DB]*squealx.Stmt, len(masters))
	readDBStmts := make(map[*squealx.DB]*squealx.Stmt, len(readDBs))

	var errs []error
	for _, id := range masters {
		db, err := r.getDB(id)
		if err != nil {
			return nil, err
		}
		stmt, err := db.PreparexContext(ctx, query)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		primaryDBStmts[db] = stmt
	}
	for _, id := range readDBs {
		db, err := r.getDB(id)
		if err != nil {
			return nil, err
		}
		stmt, err := db.PreparexContext(ctx, query)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		readDBStmts[db] = stmt
	}
	if errs != nil {
		return nil, errors.Join(errs...)
	}

	return &stmt{
		masters:      masters,
		readReplicas: readDBs,
		masterStmts:  primaryDBStmts,
		replicaStmts: readDBStmts,
		db:           r,
	}, nil
}

// PrepareNamed returns an NamedStmt which can be used sqlx.NamedStmt instead.
// This supposed to be aligned with sqlx.DB.PrepareNamed.
func (r *dbResolver) PrepareNamed(query string) (NamedStmt, error) {
	masters, readDBs := r.roleIDs()
	query = r.GetQueryString(query)
	primaryDBStmts := make(map[*squealx.DB]*squealx.NamedStmt, len(masters))
	readDBStmts := make(map[*squealx.DB]*squealx.NamedStmt, len(readDBs))

	var errs []error
	for _, id := range masters {
		db, err := r.getDB(id)
		if err != nil {
			return nil, err
		}
		stmt, err := db.PrepareNamed(query)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		primaryDBStmts[db] = stmt
	}
	for _, id := range readDBs {
		db, err := r.getDB(id)
		if err != nil {
			return nil, err
		}
		stmt, err := db.PrepareNamed(query)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		readDBStmts[db] = stmt
	}
	if errs != nil {
		return nil, errors.Join(errs...)
	}

	return &namedStmt{
		masters:      masters,
		readReplicas: readDBs,
		masterStmts:  primaryDBStmts,
		replicaStmts: readDBStmts,
		db:           r,
	}, nil
}

// PrepareNamedContext returns an NamedStmt which can be used sqlx.NamedStmt instead.
// This supposed to be aligned with sqlx.DB.PrepareNamedContext.
func (r *dbResolver) PrepareNamedContext(ctx context.Context, query string) (NamedStmt, error) {
	masters, readDBs := r.roleIDs()
	query = r.GetQueryString(query)
	primaryDBStmts := make(map[*squealx.DB]*squealx.NamedStmt, len(masters))
	readDBStmts := make(map[*squealx.DB]*squealx.NamedStmt, len(readDBs))

	var errs []error
	for _, id := range masters {
		db, err := r.getDB(id)
		if err != nil {
			return nil, err
		}
		stmt, err := db.PrepareNamedContext(ctx, query)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		primaryDBStmts[db] = stmt
	}
	for _, id := range readDBs {
		db, err := r.getDB(id)
		if err != nil {
			return nil, err
		}
		stmt, err := db.PrepareNamedContext(ctx, query)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		readDBStmts[db] = stmt
	}
	if errs != nil {
		return nil, errors.Join(errs...)
	}

	return &namedStmt{
		masters:      masters,
		readReplicas: readDBs,
		masterStmts:  primaryDBStmts,
		replicaStmts: readDBStmts,
		db:           r,
	}, nil
}

// Preparex returns an Stmt which can be used sqlx.Stmt instead.
// This supposed to be aligned with sqlx.DB.Preparex.
func (r *dbResolver) Preparex(query string) (Stmt, error) {
	masters, readDBs := r.roleIDs()
	query = r.GetQueryString(query)
	primaryDBStmts := make(map[*squealx.DB]*squealx.Stmt, len(masters))
	readDBStmts := make(map[*squealx.DB]*squealx.Stmt, len(readDBs))

	var errs []error
	for _, id := range masters {
		db, err := r.getDB(id)
		if err != nil {
			return nil, err
		}
		stmt, err := db.Preparex(query)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		primaryDBStmts[db] = stmt
	}
	for _, id := range readDBs {
		db, err := r.getDB(id)
		if err != nil {
			return nil, err
		}
		stmt, err := db.Preparex(query)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		readDBStmts[db] = stmt
	}
	if errs != nil {
		return nil, errors.Join(errs...)
	}

	return &stmt{
		masters:      masters,
		readReplicas: readDBs,
		masterStmts:  primaryDBStmts,
		replicaStmts: readDBStmts,
		db:           r,
	}, nil
}

// PreparexContext returns a Stmt which can be used sqlx.Stmt instead.
// This supposed to be aligned with sqlx.DB.PreparexContext.
func (r *dbResolver) PreparexContext(ctx context.Context, query string) (Stmt, error) {
	masters, readDBs := r.roleIDs()
	query = r.GetQueryString(query)
	primaryDBStmts := make(map[*squealx.DB]*squealx.Stmt, len(masters))
	readDBStmts := make(map[*squealx.DB]*squealx.Stmt, len(readDBs))

	var errs []error
	for _, id := range masters {
		db, err := r.getDB(id)
		if err != nil {
			return nil, err
		}
		stmt, err := db.PreparexContext(ctx, query)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		primaryDBStmts[db] = stmt
	}
	for _, id := range readDBs {
		db, err := r.getDB(id)
		if err != nil {
			return nil, err
		}
		stmt, err := db.PreparexContext(ctx, query)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		readDBStmts[db] = stmt
	}
	if errs != nil {
		return nil, errors.Join(errs...)
	}

	return &stmt{
		masters:      masters,
		readReplicas: readDBs,
		masterStmts:  primaryDBStmts,
		replicaStmts: readDBStmts,
		db:           r,
	}, nil
}

// Query chooses a readable database, executes the query and executes a query that returns sql.Rows.
// This supposed to be aligned with sqlx.DB.Query.
func (r *dbResolver) Query(query string, args ...any) (squealx.SQLRows, error) {
	query = r.GetQueryString(query)
	return readWithFailover(r, context.Background(), func(db *squealx.DB) (squealx.SQLRows, error) {
		return db.Query(query, args...)
	})
}

// QueryContext chooses a readable database, executes the query and executes a query that returns sql.Rows.
// This supposed to be aligned with sqlx.DB.QueryContext.
func (r *dbResolver) QueryContext(ctx context.Context, query string, args ...any) (squealx.SQLRows, error) {
	query = r.GetQueryString(query)
	return readWithFailover(r, ctx, func(db *squealx.DB) (squealx.SQLRows, error) {
		return db.QueryContext(ctx, query, args...)
	})
}

// QueryRow chooses a readable database, executes the query and executes a query that returns sql.Row.
// This supposed to be aligned with sqlx.DB.QueryRow.
func (r *dbResolver) QueryRow(query string, args ...any) squealx.SQLRow {
	query = r.GetQueryString(query)
	row, err := readWithFailover(r, context.Background(), func(db *squealx.DB) (squealx.SQLRow, error) {
		row := db.QueryRow(query, args...)
		return row, row.Err()
	})
	if err != nil {
		return squealx.NewErrorRow(err)
	}
	return row
}

// QueryRowContext chooses a readable database, executes the query and executes a query that returns sql.Row.
// This supposed to be aligned with sqlx.DB.QueryRowContext.
func (r *dbResolver) QueryRowContext(ctx context.Context, query string, args ...any) squealx.SQLRow {
	query = r.GetQueryString(query)
	row, err := readWithFailover(r, ctx, func(db *squealx.DB) (squealx.SQLRow, error) {
		row := db.QueryRowContext(ctx, query, args...)
		return row, row.Err()
	})
	if err != nil {
		return squealx.NewErrorRow(err)
	}
	return row
}

// QueryRowx chooses a readable database, queries the database and returns an *squealx.Row.
// This supposed to be aligned with sqlx.DB.QueryRowx.
func (r *dbResolver) QueryRowx(query string, args ...any) *squealx.Row {
	query = r.GetQueryString(query)
	row, err := readWithFailover(r, context.Background(), func(db *squealx.DB) (*squealx.Row, error) {
		row := db.QueryRowx(query, args...)
		return row, row.Err()
	})
	if err != nil {
		return squealx.NewErrorRow(err)
	}
	return row
}

// QueryRowxContext chooses a readable database, queries the database and returns an *squealx.Row.
// This supposed to be aligned with sqlx.DB.QueryRowxContext.
func (r *dbResolver) QueryRowxContext(ctx context.Context, query string, args ...any) *squealx.Row {
	query = r.GetQueryString(query)
	row, err := readWithFailover(r, ctx, func(db *squealx.DB) (*squealx.Row, error) {
		row := db.QueryRowxContext(ctx, query, args...)
		return row, row.Err()
	})
	if err != nil {
		return squealx.NewErrorRow(err)
	}
	return row
}

// Queryx chooses a readable database, queries the database and returns an *squealx.Rows.
// This supposed to be aligned with sqlx.DB.Queryx.
func (r *dbResolver) Queryx(query string, args ...any) (*squealx.Rows, error) {
	query = r.GetQueryString(query)
	return readWithFailover(r, context.Background(), func(db *squealx.DB) (*squealx.Rows, error) {
		return db.Queryx(query, args...)
	})
}

// QueryxContext chooses a readable database, queries the database and returns an *squealx.Rows.
// This supposed to be aligned with sqlx.DB.QueryxContext.
func (r *dbResolver) QueryxContext(ctx context.Context, query string, args ...any) (*squealx.Rows, error) {
	query = r.GetQueryString(query)
	return readWithFailover(r, ctx, func(db *squealx.DB) (*squealx.Rows, error) {
		return db.QueryxContext(ctx, query, args...)
	})
}

// Rebind chooses a primary database and
// transforms a query from QUESTION to the DB driver's bindvar type.
// This supposed to be aligned with sqlx.DB.Rebind.
func (r *dbResolver) Rebind(query string) string {
	query = r.GetQueryString(query)
	db, err := r.resolveWrite(context.Background())
	if err != nil {
		return query
	}
	return db.Rebind(query)
}

// Select chooses a readable database and execute SELECT using chosen DB.
// This supposed to be aligned with sqlx.DB.Select.
func (r *dbResolver) Select(dest any, query string, args ...any) error {
	query = r.GetQueryString(query)
	_, err := readWithFailover(r, context.Background(), func(db *squealx.DB) (struct{}, error) {
		return struct{}{}, db.Select(dest, query, args...)
	})
	return err
}

func (r *dbResolver) ExecWithReturn(query string, args any) error {
	db, err := r.resolveWrite(context.Background())
	if err != nil {
		return err
	}
	return db.ExecWithReturn(query, args)
}
func (r *dbResolver) LazyExec(query string) func(args ...any) (sql.Result, error) {
	return func(args ...any) (sql.Result, error) {
		db, err := r.resolveWrite(context.Background())
		if err != nil {
			return nil, err
		}
		return db.LazyExec(query)(args...)
	}
}
func (r *dbResolver) LazyExecWithReturn(query string) func(args any) error {
	return func(args any) error {
		db, err := r.resolveWrite(context.Background())
		if err != nil {
			return err
		}
		return db.LazyExecWithReturn(query)(args)
	}
}

func (r *dbResolver) LazySelect(query string) func(dest any, args ...any) error {
	return func(dest any, args ...any) error {
		return r.Select(dest, query, args...)
	}
}

// NamedSelect chooses a readable database and execute SELECT using chosen DB.
// This supposed to be aligned with sqlx.DB.Select.
func (r *dbResolver) NamedSelect(dest any, query string, args any) error {
	query = r.GetQueryString(query)
	_, err := readWithFailover(r, context.Background(), func(db *squealx.DB) (struct{}, error) {
		return struct{}{}, db.NamedSelect(dest, query, args)
	})
	return err
}

// NamedGet chooses a readable database and execute SELECT using chosen DB.
// This supposed to be aligned with sqlx.DB.Select.
func (r *dbResolver) NamedGet(dest any, query string, args any) error {
	query = r.GetQueryString(query)
	_, err := readWithFailover(r, context.Background(), func(db *squealx.DB) (struct{}, error) {
		return struct{}{}, db.NamedGet(dest, query, args)
	})
	return err
}

// SelectContext chooses a readable database and execute SELECT using chosen DB.
// This supposed to be aligned with sqlx.DB.SelectContext.
func (r *dbResolver) SelectContext(ctx context.Context, dest any, query string, args ...any) error {
	query = r.GetQueryString(query)
	_, err := readWithFailover(r, ctx, func(db *squealx.DB) (struct{}, error) {
		return struct{}{}, db.SmartSelectContext(ctx, dest, query, args...)
	})
	return err
}

// NamedSelectContext chooses a readable database and execute SELECT using chosen DB.
// This supposed to be aligned with sqlx.DB.SelectContext.
func (r *dbResolver) NamedSelectContext(ctx context.Context, dest any, query string, args ...any) error {
	if len(args) == 0 {
		return errors.New("dbresolver: named select requires arguments")
	}
	query = r.GetQueryString(query)
	_, err := readWithFailover(r, ctx, func(db *squealx.DB) (struct{}, error) {
		return struct{}{}, db.SmartSelectContext(ctx, dest, query, args[0])
	})
	return err
}

// SetConnMaxIdleTime sets the maximum amount of time a connection may be idle to all databases.
func (r *dbResolver) SetConnMaxIdleTime(d time.Duration) {
	for _, db := range r.snapshotDBs() {
		db.SetConnMaxIdleTime(d)
	}
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be reused to all databases.
func (r *dbResolver) SetConnMaxLifetime(d time.Duration) {
	for _, db := range r.snapshotDBs() {
		db.SetConnMaxLifetime(d)
	}
}

// SetMaxIdleConns sets the maximum number of connections in the idle connection pool to all databases.
func (r *dbResolver) SetMaxIdleConns(n int) {
	for _, db := range r.snapshotDBs() {
		db.SetMaxIdleConns(n)
	}
}

// SetMaxOpenConns sets the maximum number of open connections to all databases.
func (r *dbResolver) SetMaxOpenConns(n int) {
	for _, db := range r.snapshotDBs() {
		db.SetMaxOpenConns(n)
	}
}

// Stats returns first primary database statistics.
func (r *dbResolver) Stats() sql.DBStats {
	db, err := r.resolveWrite(context.Background())
	if err != nil || db == nil {
		return sql.DBStats{}
	}
	return db.Stats()
}

// Unsafe chose a primary database and returns a version of DB
// which will silently succeed to scan
// when columns in the SQL result have no fields in the destination struct.
// This supposed to be aligned with sqlx.DB.Unsafe.
func (r *dbResolver) Unsafe() *squealx.DB {
	db, err := r.resolveWrite(context.Background())
	if err != nil {
		return nil
	}
	return db.Unsafe()
}
