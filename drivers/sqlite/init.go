package sqlite

import (
	"context"
	"fmt"

	_ "modernc.org/sqlite"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/internal/sqliteutil"
)

const (
	defaultMaxOpenConns = sqliteutil.DefaultMaxOpenConnections
	defaultMaxIdleConns = sqliteutil.DefaultMaxIdleConnections
)

var ErrPrivateMemoryPool = sqliteutil.ErrPrivateMemoryPool

// Open opens and verifies a SQLite database with a bounded connection pool.
// Private in-memory databases use a single connection because each connection
// would otherwise observe a different database.
func Open(dsn string, id string) (*squealx.DB, error) {
	return OpenContext(context.Background(), dsn, id)
}

// OpenContext opens and verifies SQLite with a cancellable startup ping and
// the same bounded defaults as Open.
func OpenContext(ctx context.Context, dsn string, id string) (*squealx.DB, error) {
	return OpenWithPoolConfigContext(ctx, dsn, id, defaultPoolConfigForDSN(dsn))
}

// DefaultPoolConfig returns SQLite's bounded defaults for file-backed and
// shared-memory databases: four open and two idle connections.
func DefaultPoolConfig() squealx.PoolConfig {
	return squealx.PoolConfig{MaxOpenConns: defaultMaxOpenConns, MaxIdleConns: defaultMaxIdleConns}
}

func defaultPoolConfigForDSN(dsn string) squealx.PoolConfig {
	maxOpen, maxIdle := sqliteutil.DefaultPoolLimits(dsn)
	return squealx.PoolConfig{MaxOpenConns: maxOpen, MaxIdleConns: maxIdle}
}

// OpenWithPoolConfigContext opens SQLite with explicit pool limits applied
// before a cancellable startup ping.
func OpenWithPoolConfigContext(ctx context.Context, dsn string, id string, pool squealx.PoolConfig) (*squealx.DB, error) {
	if err := sqliteutil.ValidatePrivateMemoryPool(dsn, pool.MaxOpenConns, pool.MaxIdleConns,
		pool.ConnMaxLifetime, pool.ConnMaxIdleTime); err != nil {
		return nil, fmt.Errorf("%w; use a shared-memory URI or keep one idle connection without expiry", err)
	}
	dsn = sqliteutil.WithDefaultBusyTimeout(dsn, sqliteutil.DefaultBusyTimeoutMS)
	return squealx.ConnectWithPoolConfig(ctx, "sqlite", dsn, id, pool)
}

// OpenWithPoolConfig opens SQLite with explicit pool limits applied before
// the startup ping. Use it when the workload needs limits different from
// Open's bounded defaults.
func OpenWithPoolConfig(dsn string, id string, pool squealx.PoolConfig) (*squealx.DB, error) {
	return OpenWithPoolConfigContext(context.Background(), dsn, id, pool)
}

func MustOpen(dsn string, id string) *squealx.DB {
	db, err := Open(dsn, id)
	if err != nil {
		panic(err)
	}
	return db
}

// MustOpenWithPoolConfig is OpenWithPoolConfig and panics on error.
func MustOpenWithPoolConfig(dsn string, id string, pool squealx.PoolConfig) *squealx.DB {
	db, err := OpenWithPoolConfig(dsn, id, pool)
	if err != nil {
		panic(err)
	}
	return db
}
