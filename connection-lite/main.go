// Package connectionlite opens configured Squealx databases without importing
// database drivers. Applications register only the drivers they use.
package connectionlite

import (
	"context"
	"fmt"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/internal/sqliteutil"
)

// FromConfig opens the configured database using drivers registered by the
// application. It applies configured pool limits before checking connectivity.
func FromConfig(cfg squealx.Config) (*squealx.DB, string, error) {
	return FromConfigContext(context.Background(), cfg)
}

// FromConfigContext is FromConfig with a cancellable startup ping.
func FromConfigContext(ctx context.Context, cfg squealx.Config) (*squealx.DB, string, error) {
	poolConfig, err := cfg.PoolConfig()
	if err != nil {
		return nil, "", err
	}
	driverName, err := registeredDriverName(cfg.Driver)
	if err != nil {
		return nil, "", err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	dsn := cfg.ToString()
	configuredPool := cfg.MaxOpenCons != 0 || cfg.MaxIdleCons != 0 || cfg.MaxLifetime != 0 || cfg.MaxIdleTime != 0
	if driverName == "sqlite" {
		if !configuredPool {
			maxOpen, maxIdle := sqliteutil.DefaultPoolLimits(dsn)
			poolConfig = squealx.PoolConfig{MaxOpenConns: maxOpen, MaxIdleConns: maxIdle}
		}
		if err := sqliteutil.ValidatePrivateMemoryPool(dsn, poolConfig.MaxOpenConns, poolConfig.MaxIdleConns,
			poolConfig.ConnMaxLifetime, poolConfig.ConnMaxIdleTime); err != nil {
			return nil, "", fmt.Errorf("%w; use a shared-memory URI or keep one idle connection without expiry", err)
		}
		dsn = sqliteutil.WithDefaultBusyTimeout(dsn, sqliteutil.DefaultBusyTimeoutMS)
		db, err := squealx.ConnectWithPoolConfig(ctx, driverName, dsn, cfg.Key, poolConfig)
		return db, cfg.Driver, err
	}
	if configuredPool {
		db, err := squealx.ConnectWithPoolConfig(ctx, driverName, dsn, cfg.Key, poolConfig)
		return db, cfg.Driver, err
	}
	db, err := squealx.ConnectContext(ctx, driverName, dsn, cfg.Key)
	if err != nil {
		return nil, "", err
	}
	return db, cfg.Driver, nil
}

func registeredDriverName(name string) (string, error) {
	switch name {
	case "postgresql", "postgres", "psql", "pgx":
		return "pgx", nil
	case "mysql", "mariadb":
		return "mysql", nil
	case "sqlite", "sqlite3":
		return "sqlite", nil
	case "mssql", "sqlserver", "sql-server":
		return "sqlserver", nil
	default:
		return "", fmt.Errorf("driver not supported %s", name)
	}
}
