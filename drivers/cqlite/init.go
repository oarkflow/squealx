// Package cqlite registers the cqlite database/sql driver for Squealx.
package cqlite

import (
	"context"

	_ "github.com/oarkflow/cqlite/driver"
	"github.com/oarkflow/squealx"
)

func init() { squealx.BindDriver("cqlite", squealx.QUESTION) }

// Open opens a cqlite database and verifies connectivity.
func Open(dsn, id string) (*squealx.DB, error) { return squealx.Connect("cqlite", dsn, id) }

// OpenContext opens cqlite with a cancellable startup ping.
func OpenContext(ctx context.Context, dsn, id string) (*squealx.DB, error) {
	return squealx.ConnectContext(ctx, "cqlite", dsn, id)
}

// OpenWithPoolConfig applies pool limits before checking connectivity.
func OpenWithPoolConfig(dsn, id string, pool squealx.PoolConfig) (*squealx.DB, error) {
	return OpenWithPoolConfigContext(context.Background(), dsn, id, pool)
}

// OpenWithPoolConfigContext opens cqlite with explicit pool limits and a
// cancellable startup ping. cqlite shares a file-backed engine among driver
// connections, so database/sql pool limits control concurrency, not engine
// instance count.
func OpenWithPoolConfigContext(ctx context.Context, dsn, id string, pool squealx.PoolConfig) (*squealx.DB, error) {
	if err := pool.Validate(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	db, err := squealx.Open("cqlite", dsn, id)
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

// MustOpen opens cqlite and panics on error.
func MustOpen(dsn, id string) *squealx.DB {
	db, err := Open(dsn, id)
	if err != nil {
		panic(err)
	}
	return db
}
