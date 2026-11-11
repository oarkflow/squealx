package postgres

import (
	"context"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/oarkflow/squealx"
)

// Open - "host=localhost user=postgres password=postgres dbname=sujit sslmode=disable"
func Open(dsn string, id string) (*squealx.DB, error) {
	return squealx.Connect("pgx", dsn, id)
}

// OpenContext opens PostgreSQL and verifies connectivity with a cancellable ping.
func OpenContext(ctx context.Context, dsn string, id string) (*squealx.DB, error) {
	return squealx.ConnectContext(ctx, "pgx", dsn, id)
}

// OpenWithPoolConfig opens PostgreSQL with explicit connection-pool limits.
func OpenWithPoolConfig(dsn string, id string, pool squealx.PoolConfig) (*squealx.DB, error) {
	return OpenWithPoolConfigContext(context.Background(), dsn, id, pool)
}

// OpenWithPoolConfigContext applies pool limits before the cancellable startup ping.
func OpenWithPoolConfigContext(ctx context.Context, dsn string, id string, pool squealx.PoolConfig) (*squealx.DB, error) {
	if err := pool.Validate(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	db, err := squealx.Open("pgx", dsn, id)
	if err != nil {
		return nil, err
	}
	if err := db.ApplyPoolConfig(pool); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func MustOpen(dsn string, id string) *squealx.DB {
	db, err := Open(dsn, id)
	if err != nil {
		panic(err)
	}
	return db
}
