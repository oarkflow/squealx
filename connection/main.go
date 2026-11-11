package connection

import (
	"context"
	"fmt"

	"github.com/oarkflow/squealx"
	_ "github.com/oarkflow/squealx/drivers/mssql"
	_ "github.com/oarkflow/squealx/drivers/mysql"
	_ "github.com/oarkflow/squealx/drivers/postgres"
	sqlitedriver "github.com/oarkflow/squealx/drivers/sqlite"
)

func FromConfig(cfg squealx.Config) (*squealx.DB, string, error) {
	return FromConfigContext(context.Background(), cfg)
}

// FromConfigContext opens the configured database with a cancellable startup
// ping. Configured pool limits are applied before the ping uses a connection.
func FromConfigContext(ctx context.Context, cfg squealx.Config) (*squealx.DB, string, error) {
	poolConfig, err := cfg.PoolConfig()
	if err != nil {
		return nil, "", err
	}
	dsn := cfg.ToString()
	driverName := ""
	switch cfg.Driver {
	case "postgresql", "postgres", "psql", "pgx":
		driverName = "pgx"
	case "mysql", "mariadb":
		driverName = "mysql"
	case "sqlite", "sqlite3":
		driverName = "sqlite"
	case "mssql", "sqlserver", "sql-server":
		driverName = "sqlserver"
	default:
		return nil, "", fmt.Errorf("driver not supported %s", cfg.Driver)
	}
	if cfg.MaxOpenCons != 0 || cfg.MaxIdleCons != 0 || cfg.MaxLifetime != 0 || cfg.MaxIdleTime != 0 {
		if driverName == "sqlite" {
			db, err := sqlitedriver.OpenWithPoolConfigContext(ctx, dsn, cfg.Key, poolConfig)
			return db, cfg.Driver, err
		}
		db, err := squealx.ConnectWithPoolConfig(ctx, driverName, dsn, cfg.Key, poolConfig)
		return db, cfg.Driver, err
	}
	if driverName == "sqlite" {
		db, err := sqlitedriver.OpenContext(ctx, dsn, cfg.Key)
		return db, cfg.Driver, err
	}
	db, err := squealx.ConnectContext(ctx, driverName, dsn, cfg.Key)
	if err != nil {
		return nil, "", err
	}
	return db, cfg.Driver, nil
}
