package dbresolver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
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

// MasterConfig is the config of primary databases.
type MasterConfig struct {
	DBs             []*squealx.DB
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
	Conn(ctx context.Context) (squealx.SQLConn, error)
	Connx(ctx context.Context) (*squealx.Conn, error)
	Driver() driver.Driver
	DriverName() string
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
	NamedSelect(dest any, query string, args ...any) error
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
}

type dbResolver struct {
	masters      []*squealx.DB
	replicas     []*squealx.DB
	readDBs      []*squealx.DB
	loadBalancer LoadBalancer
}

var _ DBResolver = (*dbResolver)(nil)

// NewDBResolver creates a new DBResolver and returns it.
// If no primary DBResolver is given, it returns an error.
// If you do not give WriteOnly option, it will use the primary DBResolver as the read DBResolver.
// if you do not give LoadBalancer option, it will use the RandomLoadBalancer.
func NewDBResolver(master *MasterConfig, opts ...OptionFunc) (DBResolver, error) {
	if master == nil || len(master.DBs) == 0 {
		return nil, errNoPrimaryDB
	}

	if master.ReadWritePolicy == "" {
		master.ReadWritePolicy = ReadWrite
	}
	if _, ok := validReadWritePolicies[master.ReadWritePolicy]; !ok {
		return nil, errInvalidReadWritePolicy
	}

	options, err := compileOptions(opts...)
	if err != nil {
		return nil, err
	}

	var readReplicas []*squealx.DB
	readReplicas = append(readReplicas, options.ReplicaDBs...)
	if master.ReadWritePolicy == ReadWrite {
		readReplicas = append(readReplicas, master.DBs...)
	}
	if len(readReplicas) == 0 {
		return nil, errNoDBToRead
	}

	return &dbResolver{
		masters:      master.DBs,
		replicas:     options.ReplicaDBs,
		readDBs:      readReplicas,
		loadBalancer: options.LoadBalancer,
	}, nil
}

func compileOptions(opts ...OptionFunc) (*Options, error) {
	options := &Options{}
	for _, opt := range opts {
		opt(options)
	}

	if options.LoadBalancer == nil {
		options.LoadBalancer = NewRandomLoadBalancer()
	}

	return options, nil
}

func MustNewDBResolver(master *MasterConfig, opts ...OptionFunc) DBResolver {
	db, err := NewDBResolver(master, opts...)
	if err != nil {
		panic(err)
	}
	return db
}

func (r *dbResolver) MasterDBs() []*squealx.DB {
	return r.masters
}

func (r *dbResolver) ReplicaDBs() []*squealx.DB {
	return r.replicas
}

func (r *dbResolver) ReadDBs() []*squealx.DB {
	return r.readDBs
}

func (r *dbResolver) LoadBalancer() LoadBalancer {
	return r.loadBalancer
}

// Begin chooses a primary database and starts a transaction.
// This supposed to be aligned with sqlx.DB.Begin.
func (r *dbResolver) Begin() (squealx.SQLTx, error) {
	db := r.loadBalancer.Select(context.Background(), r.masters)
	return db.Begin()
}

// BeginTx chooses a primary database and starts a transaction.
// This supposed to be aligned with sqlx.DB.BeginTx.
func (r *dbResolver) BeginTx(ctx context.Context, opts *sql.TxOptions) (squealx.SQLTx, error) {
	db := r.loadBalancer.Select(ctx, r.masters)
	return db.BeginTx(ctx, opts)
}

// BeginTxx chooses a primary database, begins a transaction and returns an *squealx.Tx
// This supposed to be aligned with sqlx.DB.BeginTxx.
func (r *dbResolver) BeginTxx(ctx context.Context, opts *sql.TxOptions) (*squealx.Tx, error) {
	db := r.loadBalancer.Select(ctx, r.masters)
	return db.BeginTxx(ctx, opts)
}

// Beginx chooses a primary database, begins a transaction and returns an *squealx.Tx
// This supposed to be aligned with sqlx.DB.Beginx.
func (r *dbResolver) Beginx() (*squealx.Tx, error) {
	db := r.loadBalancer.Select(context.Background(), r.masters)
	return db.Beginx()
}

// BindNamed chooses a primary database and binds a query using the DB driver's bindvar type.
// This supposed to be aligned with sqlx.DB.BindNamed.
func (r *dbResolver) BindNamed(query string, arg any) (string, []any, error) {
	db := r.loadBalancer.Select(context.Background(), r.masters)
	return db.BindNamed(query, arg)
}

// Close closes all the databases.
func (r *dbResolver) Close() error {
	var errs []error
	for _, db := range r.masters {
		if err := db.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	for _, db := range r.replicas {
		if err := db.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Conn chooses a primary database and returns a squealx.SQLConn.
// This supposed to be aligned with sqlx.DB.Conn.
func (r *dbResolver) Conn(ctx context.Context) (squealx.SQLConn, error) {
	db := r.loadBalancer.Select(ctx, r.masters)
	return db.Conn(ctx)
}

// Connx chooses a primary database and returns a *squealx.Conn.
// This supposed to be aligned with sqlx.DB.Connx.
func (r *dbResolver) Connx(ctx context.Context) (*squealx.Conn, error) {
	db := r.loadBalancer.Select(ctx, r.masters)
	return db.Connx(ctx)
}

// Driver chooses a primary database and returns a driver.Driver.
// This supposed to be aligned with sqlx.DB.Driver.
func (r *dbResolver) Driver() driver.Driver {
	db := r.loadBalancer.Select(context.Background(), r.masters)
	return db.Driver()
}

// DriverName chooses a primary database and returns the driverName.
// This supposed to be aligned with sqlx.DB.DriverName.
func (r *dbResolver) DriverName() string {
	db := r.loadBalancer.Select(context.Background(), r.masters)
	return db.DriverName()
}

// Exec chooses a primary database and executes a query without returning any rows.
// This supposed to be aligned with sqlx.DB.Exec.
func (r *dbResolver) Exec(query string, args ...any) (sql.Result, error) {
	db := r.loadBalancer.Select(context.Background(), r.masters)
	return db.Exec(query, args...)
}

// ExecContext chooses a primary database and executes a query without returning any rows.
// This supposed to be aligned with sqlx.DB.ExecContext.
func (r *dbResolver) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	db := r.loadBalancer.Select(ctx, r.masters)
	return db.Exec(query, args...)
}

// Get chooses a readable database and Get using chosen DB.
// This supposed to be aligned with sqlx.DB.Get.
func (r *dbResolver) Get(dest any, query string, args ...any) error {
	db := r.loadBalancer.Select(context.Background(), r.readDBs)
	err := db.Get(dest, query, args...)
	if isDBConnectionError(err) {
		dbPrimary := r.loadBalancer.Select(context.Background(), r.masters)
		err = dbPrimary.Get(dest, query, args...)
	}
	return err
}

// GetContext chooses a readable database and Get using chosen DB.
// This supposed to be aligned with sqlx.DB.GetContext.
func (r *dbResolver) GetContext(ctx context.Context, dest any, query string, args ...any) error {
	db := r.loadBalancer.Select(ctx, r.readDBs)
	err := db.GetContext(ctx, dest, query, args...)
	if isDBConnectionError(err) {
		dbPrimary := r.loadBalancer.Select(ctx, r.masters)
		err = dbPrimary.GetContext(ctx, dest, query, args...)
	}
	return err
}

// MapperFunc sets the mapper function for the all primary databases and secondary databases.
func (r *dbResolver) MapperFunc(mf func(string) string) {
	for _, db := range r.masters {
		db.MapperFunc(mf)
	}
	for _, db := range r.replicas {
		db.MapperFunc(mf)
	}
}

// MustBegin chooses a primary database, starts a transaction and returns an *squealx.Tx or panic.
// This supposed to be aligned with sqlx.DB.MustBegin.
func (r *dbResolver) MustBegin() *squealx.Tx {
	db := r.loadBalancer.Select(context.Background(), r.masters)
	return db.MustBegin()
}

// MustBeginTx chooses a primary database, starts a transaction and returns an *squealx.Tx or panic.
// This supposed to be aligned with sqlx.DB.MustBeginTx.
func (r *dbResolver) MustBeginTx(ctx context.Context, opts *sql.TxOptions) *squealx.Tx {
	db := r.loadBalancer.Select(ctx, r.masters)
	return db.MustBeginTx(ctx, opts)
}

// MustExec chooses a primary database and executes a query or panic.
// This supposed to be aligned with sqlx.DB.MustExec.
func (r *dbResolver) MustExec(query string, args ...any) sql.Result {
	db := r.loadBalancer.Select(context.Background(), r.masters)
	if strings.Contains(query, ":") && len(args) > 0 {
		rs, err := db.Exec(query, args[0])
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
	db := r.loadBalancer.Select(ctx, r.masters)
	if strings.Contains(query, ":") && len(args) > 0 {
		rs, err := db.ExecContext(ctx, query, args[0])
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
	db := r.loadBalancer.Select(context.Background(), r.masters)
	return db.NamedExec(query, arg)
}

// NamedExecContext chooses a primary database and then executes a named query.
// This supposed to be aligned with sqlx.DB.NamedExecContext.
func (r *dbResolver) NamedExecContext(ctx context.Context, query string, arg any) (sql.Result, error) {
	db := r.loadBalancer.Select(ctx, r.masters)
	return db.NamedExecContext(ctx, query, arg)
}

// NamedQuery chooses a readable database and then executes a named query.
// This supposed to be aligned with sqlx.DB.NamedQuery.
func (r *dbResolver) NamedQuery(query string, arg any) (*squealx.Rows, error) {
	db := r.loadBalancer.Select(context.Background(), r.readDBs)
	rows, err := db.NamedQuery(query, arg)
	if isDBConnectionError(err) {
		dbPrimary := r.loadBalancer.Select(context.Background(), r.masters)
		rows, err = dbPrimary.NamedQuery(query, arg)
	}
	return rows, err
}

// NamedQueryContext chooses a readable database and then executes a named query.
// This supposed to be aligned with sqlx.DB.NamedQueryContext.
func (r *dbResolver) NamedQueryContext(ctx context.Context, query string, arg any) (*squealx.Rows, error) {
	db := r.loadBalancer.Select(ctx, r.readDBs)
	rows, err := db.NamedQueryContext(ctx, query, arg)
	if isDBConnectionError(err) {
		dbPrimary := r.loadBalancer.Select(ctx, r.masters)
		rows, err = dbPrimary.NamedQueryContext(ctx, query, arg)
	}
	return rows, err
}

// Ping sends a ping to the all databases.
func (r *dbResolver) Ping() error {
	var errs []error
	for _, db := range r.masters {
		if err := db.Ping(); err != nil {
			errs = append(errs, err)
		}
	}
	for _, db := range r.replicas {
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
	for _, db := range r.masters {
		if err := db.PingContext(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	for _, db := range r.replicas {
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
	primaryDBStmts := make(map[*squealx.DB]*squealx.Stmt, len(r.masters))
	readDBStmts := make(map[*squealx.DB]*squealx.Stmt, len(r.readDBs))

	var errs []error
	for _, db := range r.masters {
		stmt, err := db.Preparex(query)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		primaryDBStmts[db] = stmt
	}
	for _, db := range r.readDBs {
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
		masters:      r.masters,
		readReplicas: r.readDBs,
		masterStmts:  primaryDBStmts,
		replicaStmts: readDBStmts,
		loadBalancer: r.loadBalancer,
	}, nil
}

// PrepareContext returns a Stmt which can be used sql.Stmt instead.
// This supposed to be aligned with sqlx.DB.PrepareContext.
func (r *dbResolver) PrepareContext(ctx context.Context, query string) (Stmt, error) {
	primaryDBStmts := make(map[*squealx.DB]*squealx.Stmt, len(r.masters))
	readDBStmts := make(map[*squealx.DB]*squealx.Stmt, len(r.readDBs))

	var errs []error
	for _, db := range r.masters {
		stmt, err := db.PreparexContext(ctx, query)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		primaryDBStmts[db] = stmt
	}
	for _, db := range r.readDBs {
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
		masters:      r.masters,
		readReplicas: r.readDBs,
		masterStmts:  primaryDBStmts,
		replicaStmts: readDBStmts,
		loadBalancer: r.loadBalancer,
	}, nil
}

// PrepareNamed returns an NamedStmt which can be used sqlx.NamedStmt instead.
// This supposed to be aligned with sqlx.DB.PrepareNamed.
func (r *dbResolver) PrepareNamed(query string) (NamedStmt, error) {
	primaryDBStmts := make(map[*squealx.DB]*squealx.NamedStmt, len(r.masters))
	readDBStmts := make(map[*squealx.DB]*squealx.NamedStmt, len(r.readDBs))

	var errs []error
	for _, db := range r.masters {
		stmt, err := db.PrepareNamed(query)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		primaryDBStmts[db] = stmt
	}
	for _, db := range r.readDBs {
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
		masters:      r.masters,
		readReplicas: r.readDBs,
		masterStmts:  primaryDBStmts,
		replicaStmts: readDBStmts,
		loadBalancer: r.loadBalancer,
	}, nil
}

// PrepareNamedContext returns an NamedStmt which can be used sqlx.NamedStmt instead.
// This supposed to be aligned with sqlx.DB.PrepareNamedContext.
func (r *dbResolver) PrepareNamedContext(ctx context.Context, query string) (NamedStmt, error) {
	primaryDBStmts := make(map[*squealx.DB]*squealx.NamedStmt, len(r.masters))
	readDBStmts := make(map[*squealx.DB]*squealx.NamedStmt, len(r.readDBs))

	var errs []error
	for _, db := range r.masters {
		stmt, err := db.PrepareNamedContext(ctx, query)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		primaryDBStmts[db] = stmt
	}
	for _, db := range r.readDBs {
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
		masters:      r.masters,
		readReplicas: r.readDBs,
		masterStmts:  primaryDBStmts,
		replicaStmts: readDBStmts,
		loadBalancer: r.loadBalancer,
	}, nil
}

// Preparex returns an Stmt which can be used sqlx.Stmt instead.
// This supposed to be aligned with sqlx.DB.Preparex.
func (r *dbResolver) Preparex(query string) (Stmt, error) {
	primaryDBStmts := make(map[*squealx.DB]*squealx.Stmt, len(r.masters))
	readDBStmts := make(map[*squealx.DB]*squealx.Stmt, len(r.readDBs))

	var errs []error
	for _, db := range r.masters {
		stmt, err := db.Preparex(query)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		primaryDBStmts[db] = stmt
	}
	for _, db := range r.readDBs {
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
		masters:      r.masters,
		readReplicas: r.readDBs,
		masterStmts:  primaryDBStmts,
		replicaStmts: readDBStmts,
		loadBalancer: r.loadBalancer,
	}, nil
}

// PreparexContext returns a Stmt which can be used sqlx.Stmt instead.
// This supposed to be aligned with sqlx.DB.PreparexContext.
func (r *dbResolver) PreparexContext(ctx context.Context, query string) (Stmt, error) {
	primaryDBStmts := make(map[*squealx.DB]*squealx.Stmt, len(r.masters))
	readDBStmts := make(map[*squealx.DB]*squealx.Stmt, len(r.readDBs))

	var errs []error
	for _, db := range r.masters {
		stmt, err := db.PreparexContext(ctx, query)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		primaryDBStmts[db] = stmt
	}
	for _, db := range r.readDBs {
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
		masters:      r.masters,
		readReplicas: r.readDBs,
		masterStmts:  primaryDBStmts,
		replicaStmts: readDBStmts,
		loadBalancer: r.loadBalancer,
	}, nil
}

// Query chooses a readable database, executes the query and executes a query that returns sql.Rows.
// This supposed to be aligned with sqlx.DB.Query.
func (r *dbResolver) Query(query string, args ...any) (squealx.SQLRows, error) {
	db := r.loadBalancer.Select(context.Background(), r.readDBs)
	rows, err := db.Query(query, args...)
	if isDBConnectionError(err) {
		dbPrimary := r.loadBalancer.Select(context.Background(), r.masters)
		rows, err = dbPrimary.Query(query, args...)
	}
	return rows, err
}

// QueryContext chooses a readable database, executes the query and executes a query that returns sql.Rows.
// This supposed to be aligned with sqlx.DB.QueryContext.
func (r *dbResolver) QueryContext(ctx context.Context, query string, args ...any) (squealx.SQLRows, error) {
	db := r.loadBalancer.Select(ctx, r.readDBs)
	rows, err := db.QueryContext(ctx, query, args...)
	if isDBConnectionError(err) {
		dbPrimary := r.loadBalancer.Select(ctx, r.masters)
		rows, err = dbPrimary.QueryContext(ctx, query, args...)
	}
	return rows, err
}

// QueryRow chooses a readable database, executes the query and executes a query that returns sql.Row.
// This supposed to be aligned with sqlx.DB.QueryRow.
func (r *dbResolver) QueryRow(query string, args ...any) squealx.SQLRow {
	db := r.loadBalancer.Select(context.Background(), r.readDBs)
	row := db.QueryRow(query, args...)
	if isDBConnectionError(row.Err()) {
		dbPrimary := r.loadBalancer.Select(context.Background(), r.masters)
		row = dbPrimary.QueryRow(query, args...)
	}
	return row
}

// QueryRowContext chooses a readable database, executes the query and executes a query that returns sql.Row.
// This supposed to be aligned with sqlx.DB.QueryRowContext.
func (r *dbResolver) QueryRowContext(ctx context.Context, query string, args ...any) squealx.SQLRow {
	db := r.loadBalancer.Select(ctx, r.readDBs)
	row := db.QueryRowContext(ctx, query, args...)
	if isDBConnectionError(row.Err()) {
		dbPrimary := r.loadBalancer.Select(ctx, r.masters)
		row = dbPrimary.QueryRowContext(ctx, query, args...)
	}
	return row
}

// QueryRowx chooses a readable database, queries the database and returns an *squealx.Row.
// This supposed to be aligned with sqlx.DB.QueryRowx.
func (r *dbResolver) QueryRowx(query string, args ...any) *squealx.Row {
	db := r.loadBalancer.Select(context.Background(), r.readDBs)
	row := db.QueryRowx(query, args...)
	if isDBConnectionError(row.Err()) {
		dbPrimary := r.loadBalancer.Select(context.Background(), r.masters)
		row = dbPrimary.QueryRowx(query, args...)
	}
	return row
}

// QueryRowxContext chooses a readable database, queries the database and returns an *squealx.Row.
// This supposed to be aligned with sqlx.DB.QueryRowxContext.
func (r *dbResolver) QueryRowxContext(ctx context.Context, query string, args ...any) *squealx.Row {
	db := r.loadBalancer.Select(ctx, r.readDBs)
	row := db.QueryRowxContext(ctx, query, args...)
	if isDBConnectionError(row.Err()) {
		dbPrimary := r.loadBalancer.Select(ctx, r.masters)
		row = dbPrimary.QueryRowxContext(ctx, query, args...)
	}
	return row
}

// Queryx chooses a readable database, queries the database and returns an *squealx.Rows.
// This supposed to be aligned with sqlx.DB.Queryx.
func (r *dbResolver) Queryx(query string, args ...any) (*squealx.Rows, error) {
	db := r.loadBalancer.Select(context.Background(), r.readDBs)
	rows, err := db.Queryx(query, args...)
	if isDBConnectionError(err) {
		dbPrimary := r.loadBalancer.Select(context.Background(), r.masters)
		rows, err = dbPrimary.Queryx(query, args...)
	}
	return rows, err
}

// QueryxContext chooses a readable database, queries the database and returns an *squealx.Rows.
// This supposed to be aligned with sqlx.DB.QueryxContext.
func (r *dbResolver) QueryxContext(ctx context.Context, query string, args ...any) (*squealx.Rows, error) {
	db := r.loadBalancer.Select(ctx, r.readDBs)
	rows, err := db.QueryxContext(ctx, query, args...)
	if isDBConnectionError(err) {
		dbPrimary := r.loadBalancer.Select(ctx, r.masters)
		rows, err = dbPrimary.QueryxContext(ctx, query, args...)
	}
	return rows, err
}

// Rebind chooses a primary database and
// transforms a query from QUESTION to the DB driver's bindvar type.
// This supposed to be aligned with sqlx.DB.Rebind.
func (r *dbResolver) Rebind(query string) string {
	db := r.loadBalancer.Select(context.Background(), r.masters)
	return db.Rebind(query)
}

// Select chooses a readable database and execute SELECT using chosen DB.
// This supposed to be aligned with sqlx.DB.Select.
func (r *dbResolver) Select(dest any, query string, args ...any) error {
	if strings.Contains(query, ":") {
		return r.NamedSelect(dest, query, args...)
	}
	db := r.loadBalancer.Select(context.Background(), r.readDBs)
	err := db.Select(dest, query, args...)
	if isDBConnectionError(err) {
		dbPrimary := r.loadBalancer.Select(context.Background(), r.masters)
		err = dbPrimary.Select(dest, query, args...)
	}
	return err
}

// NamedSelect chooses a readable database and execute SELECT using chosen DB.
// This supposed to be aligned with sqlx.DB.Select.
func (r *dbResolver) NamedSelect(dest any, query string, args ...any) error {
	db := r.loadBalancer.Select(context.Background(), r.readDBs)
	rows, err := db.NamedQuery(query, args[0])
	if isDBConnectionError(err) {
		dbPrimary := r.loadBalancer.Select(context.Background(), r.masters)
		rows, err := dbPrimary.NamedQuery(query, args[0])
		if err != nil {
			return err
		}
		// if something happens here, we want to make sure the rows are Closed
		defer rows.Close()
		return squealx.ScannAll(rows, dest, false)
	}
	// if something happens here, we want to make sure the rows are Closed
	defer rows.Close()
	return squealx.ScannAll(rows, dest, false)
}

// SelectContext chooses a readable database and execute SELECT using chosen DB.
// This supposed to be aligned with sqlx.DB.SelectContext.
func (r *dbResolver) SelectContext(ctx context.Context, dest any, query string, args ...any) error {
	if strings.Contains(query, ":") {
		return r.NamedSelectContext(ctx, dest, query, args...)
	}
	db := r.loadBalancer.Select(ctx, r.readDBs)
	err := db.SelectContext(ctx, dest, query, args...)
	if isDBConnectionError(err) {
		dbPrimary := r.loadBalancer.Select(ctx, r.masters)
		err = dbPrimary.SelectContext(ctx, dest, query, args...)
	}
	return err
}

// NamedSelectContext chooses a readable database and execute SELECT using chosen DB.
// This supposed to be aligned with sqlx.DB.SelectContext.
func (r *dbResolver) NamedSelectContext(ctx context.Context, dest any, query string, args ...any) error {
	db := r.loadBalancer.Select(ctx, r.readDBs)
	rows, err := db.NamedQueryContext(ctx, query, args[0])
	if err != nil {
		return err
	}
	// if something happens here, we want to make sure the rows are Closed
	defer rows.Close()
	return squealx.ScannAll(rows, dest, false)
}

// SetConnMaxIdleTime sets the maximum amount of time a connection may be idle to all databases.
func (r *dbResolver) SetConnMaxIdleTime(d time.Duration) {
	for _, db := range r.masters {
		db.SetConnMaxIdleTime(d)
	}
	for _, db := range r.readDBs {
		db.SetConnMaxIdleTime(d)
	}
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be reused to all databases.
func (r *dbResolver) SetConnMaxLifetime(d time.Duration) {
	for _, db := range r.masters {
		db.SetConnMaxLifetime(d)
	}
	for _, db := range r.readDBs {
		db.SetConnMaxLifetime(d)
	}
}

// SetMaxIdleConns sets the maximum number of connections in the idle connection pool to all databases.
func (r *dbResolver) SetMaxIdleConns(n int) {
	for _, db := range r.masters {
		db.SetMaxIdleConns(n)
	}
	for _, db := range r.readDBs {
		db.SetMaxIdleConns(n)
	}
}

// SetMaxOpenConns sets the maximum number of open connections to all databases.
func (r *dbResolver) SetMaxOpenConns(n int) {
	for _, db := range r.masters {
		db.SetMaxOpenConns(n)
	}
	for _, db := range r.readDBs {
		db.SetMaxOpenConns(n)
	}
}

// Stats returns first primary database statistics.
func (r *dbResolver) Stats() sql.DBStats {
	return r.masters[0].Stats()
}

// Unsafe chose a primary database and returns a version of DB
// which will silently succeed to scan
// when columns in the SQL result have no fields in the destination struct.
// This supposed to be aligned with sqlx.DB.Unsafe.
func (r *dbResolver) Unsafe() *squealx.DB {
	db := r.loadBalancer.Select(context.Background(), r.masters)
	return db.Unsafe()
}
