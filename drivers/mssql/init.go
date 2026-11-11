package mssql

import (
	"context"

	_ "github.com/microsoft/go-mssqldb"

	"github.com/oarkflow/squealx"
)

// Open
/*
sqlserver://username:password@host/instance?param1=value&param2=value
sqlserver://username:password@host:port?param1=value&param2=value
sqlserver://sa@localhost/SQLExpress?database=master&connection+timeout=30 // `SQLExpress instance.
sqlserver://sa:mypass@localhost?database=master&connection+timeout=30 // username=sa, password=mypass.
sqlserver://sa:mypass@localhost:1234?database=master&connection+timeout=30 // port 1234 on localhost.
sqlserver://sa:my%7Bpass@somehost?connection+timeout=30 // password is "my{pass" A string of this format can be constructed using the URL type in the net/url package.
*/
func Open(dsn string, id string) (*squealx.DB, error) {
	return squealx.Connect("mssql", dsn, id)
}

// OpenContext opens SQL Server and verifies connectivity with a cancellable ping.
func OpenContext(ctx context.Context, dsn string, id string) (*squealx.DB, error) {
	return squealx.ConnectContext(ctx, "mssql", dsn, id)
}

// OpenWithPoolConfig opens SQL Server with explicit connection-pool limits.
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
	db, err := squealx.Open("mssql", dsn, id)
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
