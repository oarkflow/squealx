package connection

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/oarkflow/squealx"
	sqlitedriver "github.com/oarkflow/squealx/drivers/sqlite"
)

func TestFromConfigSQLitePool(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "pool.sqlite")
	db, driverName, err := FromConfig(squealx.Config{
		Driver:      "sqlite",
		Database:    dsn,
		Key:         "connection-config-test",
		MaxOpenCons: 3,
		MaxIdleCons: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if driverName != "sqlite" {
		t.Fatalf("driver = %q, want sqlite", driverName)
	}
	if got := db.Stats().MaxOpenConnections; got != 3 {
		t.Fatalf("max open connections = %d, want 3", got)
	}
	var result int
	if err := db.Get(&result, "SELECT 1"); err != nil {
		t.Fatalf("query configured SQLite database: %v", err)
	}
	if result != 1 {
		t.Fatalf("query result = %d, want 1", result)
	}
	if stats := db.Stats(); stats.Idle != 1 {
		t.Fatalf("released connection idle count = %d, want 1", stats.Idle)
	}
}

func TestFromConfigSQLiteDefaultsBoundPrivateMemoryPool(t *testing.T) {
	db, driverName, err := FromConfig(squealx.Config{
		Driver:   "sqlite",
		Database: ":memory:",
		Key:      "connection-memory-default-test",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if driverName != "sqlite" {
		t.Fatalf("driver = %q, want sqlite", driverName)
	}
	if got := db.Stats().MaxOpenConnections; got != 1 {
		t.Fatalf("private in-memory max open connections = %d, want 1", got)
	}
	if _, err := db.Exec("CREATE TABLE retained (value INTEGER)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO retained VALUES (42)"); err != nil {
		t.Fatal(err)
	}
	var got int
	if err := db.Get(&got, "SELECT value FROM retained"); err != nil || got != 42 {
		t.Fatalf("query after connection reuse = %d, err=%v", got, err)
	}
}

func TestFromConfigSQLiteDefaultsBoundFilePool(t *testing.T) {
	db, _, err := FromConfig(squealx.Config{
		Driver:   "sqlite",
		Database: filepath.Join(t.TempDir(), "default-pool.sqlite"),
		Key:      "connection-file-default-test",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if got, want := db.Stats().MaxOpenConnections, 4; got != want {
		t.Fatalf("file-backed max open connections = %d, want %d", got, want)
	}
}

func TestFromConfigRejectsUnsafePrivateMemoryPool(t *testing.T) {
	for _, config := range []squealx.Config{
		{Driver: "sqlite", Database: ":memory:", MaxOpenCons: 2, MaxIdleCons: 1},
		{Driver: "sqlite", Database: ":memory:", MaxLifetime: 30},
		{Driver: "sqlite", Database: ":memory:", MaxOpenCons: 1, MaxIdleCons: 0},
	} {
		_, _, err := FromConfig(config)
		if !errors.Is(err, sqlitedriver.ErrPrivateMemoryPool) {
			t.Errorf("config %+v error = %v, want ErrPrivateMemoryPool", config, err)
		}
	}
}

func TestFromConfigZeroIdleClosesReleasedConnection(t *testing.T) {
	db, _, err := FromConfig(squealx.Config{
		Driver:      "sqlite",
		Database:    filepath.Join(t.TempDir(), "zero-idle.sqlite"),
		Key:         "connection-zero-idle-test",
		MaxOpenCons: 2,
		MaxIdleCons: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var result int
	if err := db.Get(&result, "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	if stats := db.Stats(); stats.Idle != 0 || stats.OpenConnections != 0 {
		t.Fatalf("zero-idle pool retained connections: %+v", stats)
	}
}

func TestFromConfigContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := FromConfigContext(ctx, squealx.Config{
		Driver:   "sqlite",
		Database: filepath.Join(t.TempDir(), "cancelled.sqlite"),
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("startup error = %v, want context.Canceled", err)
	}
}
