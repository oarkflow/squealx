package connection

import (
	"fmt"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/drivers/mssql"
	"github.com/oarkflow/squealx/drivers/mysql"
	"github.com/oarkflow/squealx/drivers/postgres"
	"github.com/oarkflow/squealx/drivers/sqlite"
)

func FromConfig(cfg squealx.Config) (*squealx.DB, string, error) {
	dsn := cfg.ToString()
	var db *squealx.DB
	var err error
	switch cfg.Driver {
	case "postgresql", "postgres", "psql", "pgx":
		db, err = postgres.Open(dsn, cfg.Key)
	case "mysql", "mariadb":
		db, err = mysql.Open(dsn, cfg.Key)
	case "sqlite", "sqlite3":
		db, err = sqlite.Open(dsn, cfg.Key)
	case "mssql", "sqlserver", "sql-server":
		db, err = mssql.Open(dsn, cfg.Key)
	}
	if err != nil {
		return nil, "", err
	}
	if db == nil {
		return nil, "", fmt.Errorf("driver not supported %s", cfg.Driver)
	}
	return db, cfg.Driver, nil
}
