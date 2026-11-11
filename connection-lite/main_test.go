package connectionlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/internal/sqliteutil"
)

type liteSQLiteTestDriver struct{}
type liteSQLiteTestConn struct{}

var liteSQLiteLastDSN atomic.Value

func (liteSQLiteTestDriver) Open(dsn string) (driver.Conn, error) {
	liteSQLiteLastDSN.Store(dsn)
	return liteSQLiteTestConn{}, nil
}
func (liteSQLiteTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("connection-lite test driver does not prepare statements")
}
func (liteSQLiteTestConn) Close() error { return nil }
func (liteSQLiteTestConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions are not used")
}
func (liteSQLiteTestConn) Ping(context.Context) error { return nil }

func init() {
	sql.Register("sqlite", liteSQLiteTestDriver{})
}

func TestRegisteredDriverName(t *testing.T) {
	cases := map[string]string{
		"postgresql": "pgx",
		"postgres":   "pgx",
		"psql":       "pgx",
		"pgx":        "pgx",
		"mysql":      "mysql",
		"mariadb":    "mysql",
		"sqlite":     "sqlite",
		"sqlite3":    "sqlite",
		"mssql":      "sqlserver",
		"sqlserver":  "sqlserver",
		"sql-server": "sqlserver",
	}
	for input, want := range cases {
		got, err := registeredDriverName(input)
		if err != nil || got != want {
			t.Errorf("registeredDriverName(%q) = %q, %v; want %q", input, got, err, want)
		}
	}
	if _, err := registeredDriverName("unknown"); err == nil {
		t.Fatal("expected unknown driver error")
	}
}

func TestFromConfigRejectsInvalidPoolBeforeDriverLookup(t *testing.T) {
	_, _, err := FromConfig(squealx.Config{Driver: "sqlite", MaxOpenCons: 1, MaxIdleCons: 2})
	if !errors.Is(err, squealx.ErrInvalidPool) {
		t.Fatalf("error = %v, want ErrInvalidPool", err)
	}
}

func TestFromConfigSQLiteAppliesDriverFreePoolDefaults(t *testing.T) {
	cases := []struct {
		name    string
		dsn     string
		want    int
		wantDSN string
	}{
		{name: "private memory", dsn: ":memory:", want: 1, wantDSN: ":memory:?_busy_timeout=5000"},
		{name: "file", dsn: filepath.Join(t.TempDir(), "lite.sqlite"), want: 4},
		{name: "custom timeout", dsn: filepath.Join(t.TempDir(), "custom.sqlite") + "?_timeout=250", want: 4},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			db, driverName, err := FromConfig(squealx.Config{Driver: "sqlite", Database: test.dsn})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if driverName != "sqlite" {
				t.Fatalf("driver = %q, want sqlite", driverName)
			}
			if got := db.Stats().MaxOpenConnections; got != test.want {
				t.Fatalf("max open connections = %d, want %d", got, test.want)
			}
			wantDSN := test.wantDSN
			if wantDSN == "" {
				wantDSN = sqliteutil.WithDefaultBusyTimeout(test.dsn, sqliteutil.DefaultBusyTimeoutMS)
			}
			if got := liteSQLiteLastDSN.Load(); got != wantDSN {
				t.Fatalf("driver DSN = %v, want %q", got, wantDSN)
			}
		})
	}
}

func TestFromConfigSQLiteRejectsConnectionLossForPrivateMemory(t *testing.T) {
	configs := []squealx.Config{
		{Driver: "sqlite", Database: ":memory:", MaxOpenCons: 2, MaxIdleCons: 1},
		{Driver: "sqlite", Database: ":memory:", MaxOpenCons: 1, MaxIdleCons: 0},
		{Driver: "sqlite", Database: ":memory:", MaxOpenCons: 1, MaxIdleCons: 1, MaxLifetime: 10},
	}
	for _, config := range configs {
		if _, _, err := FromConfig(config); !errors.Is(err, sqliteutil.ErrPrivateMemoryPool) {
			t.Errorf("config %+v error = %v, want ErrPrivateMemoryPool", config, err)
		}
	}
}
