//go:build integration
// +build integration

package hooks

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/drivers/mssql"
	"github.com/oarkflow/squealx/drivers/mysql"
	"github.com/oarkflow/squealx/drivers/postgres"
)

type integrationScopeKey string

const integrationUserKey integrationScopeKey = "user_id"

func TestResourceScope_Integration_MySQL_Postgres_SQLServer(t *testing.T) {
	if os.Getenv("RUN_DB_INTEGRATION") != "1" {
		t.Skip("set RUN_DB_INTEGRATION=1 to run container-backed DB integration tests")
	}

	backends := []struct {
		name         string
		dsnEnv       string
		defaultDSN   string
		open         func(string) (*squealx.DB, error)
		ensureReady  func(t *testing.T)
		resetAndSeed func(t *testing.T, db *squealx.DB)
	}{
		{
			name:       "mysql",
			dsnEnv:     "MYSQL_DSN",
			defaultDSN: "root:root@tcp(127.0.0.1:3306)/test?parseTime=true",
			open: func(dsn string) (*squealx.DB, error) {
				return mysql.Open(dsn, "integration-mysql")
			},
			ensureReady:  func(t *testing.T) {},
			resetAndSeed: resetAndSeedMySQL,
		},
		{
			name:       "postgres",
			dsnEnv:     "POSTGRES_DSN",
			defaultDSN: "host=localhost port=5432 user=postgres password=postgres dbname=tests sslmode=disable",
			open: func(dsn string) (*squealx.DB, error) {
				return postgres.Open(dsn, "integration-postgres")
			},
			ensureReady:  ensurePostgresDatabase,
			resetAndSeed: resetAndSeedPostgres,
		},
		{
			name:       "sqlserver",
			dsnEnv:     "SQLSERVER_DSN",
			defaultDSN: "sqlserver://sa:YourStrong!Passw0rd@127.0.0.1:1433?database=test&encrypt=disable",
			open: func(dsn string) (*squealx.DB, error) {
				return mssql.Open(dsn, "integration-sqlserver")
			},
			ensureReady:  ensureSQLServerDatabase,
			resetAndSeed: resetAndSeedSQLServer,
		},
	}

	for _, backend := range backends {
		backend := backend
		t.Run(backend.name, func(t *testing.T) {
			backend.ensureReady(t)
			dsn := envOrDefault(backend.dsnEnv, backend.defaultDSN)
			db := mustOpenWithRetry(t, backend.open, dsn)
			backend.resetAndSeed(t, db)
			runScopeScenarios(t, db, backend.name)
			_ = db.Close()

			compatDB := mustOpenWithRetry(t, backend.open, dsn)
			defer compatDB.Close()
			backend.resetAndSeed(t, compatDB)
			runCompatibilityBudgetScenario(t, compatDB, backend.name)
		})
	}
}

func ensureSQLServerDatabase(t *testing.T) {
	t.Helper()
	masterDSN := envOrDefault("SQLSERVER_MASTER_DSN", "sqlserver://sa:YourStrong!Passw0rd@127.0.0.1:1433?database=master&encrypt=disable")
	db := mustOpenWithRetry(t, func(dsn string) (*squealx.DB, error) {
		return mssql.Open(dsn, "integration-sqlserver-master")
	}, masterDSN)
	defer db.Close()
	if _, err := db.Exec("IF DB_ID('test') IS NULL CREATE DATABASE test;"); err != nil {
		t.Fatalf("failed to ensure sqlserver test database: %v", err)
	}
}

func ensurePostgresDatabase(t *testing.T) {
	t.Helper()
	adminDSN := envOrDefault("POSTGRES_ADMIN_DSN", "host=localhost port=5432 user=postgres password=postgres dbname=postgres sslmode=disable")
	db := mustOpenWithRetry(t, func(dsn string) (*squealx.DB, error) {
		return postgres.Open(dsn, "integration-postgres-admin")
	}, adminDSN)
	defer db.Close()

	var exists int
	err := db.Get(&exists, "SELECT 1 FROM pg_database WHERE datname = $1", "tests")
	if err == nil {
		return
	}
	if err != sql.ErrNoRows {
		t.Fatalf("failed checking postgres database existence: %v", err)
	}
	if _, err := db.Exec("CREATE DATABASE tests"); err != nil {
		t.Fatalf("failed creating postgres database tests: %v", err)
	}
}

func runScopeScenarios(t *testing.T, db *squealx.DB, backend string) {
	t.Helper()
	scope := NewResourceScopeHook(
		ArgsFromContextValue(integrationUserKey),
		ScopeRule{Table: "pipelines", Column: "user_id"},
	).
		SetStrictMode(true).
		SetRejectUnknownShapes(true).
		SetAllowTrustedBypass(true).
		SetRequireBypassToken(true)
	db.Use(scope)

	ctxUser1 := context.WithValue(context.Background(), integrationUserKey, 1)
	ctxUser2 := context.WithValue(context.Background(), integrationUserKey, 2)

	type row struct {
		PipelineID int    `db:"pipeline_id"`
		UserID     int    `db:"user_id"`
		Name       string `db:"name"`
	}

	var user1Rows []row
	if err := db.SelectContext(ctxUser1, &user1Rows, "SELECT * FROM pipelines ORDER BY pipeline_id"); err != nil {
		t.Fatalf("[%s] scoped select failed: %v", backend, err)
	}
	if len(user1Rows) != 1 || user1Rows[0].UserID != 1 {
		t.Fatalf("[%s] expected only user 1 rows, got: %#v", backend, user1Rows)
	}

	var user2Rows []row
	if err := db.SelectContext(ctxUser2, &user2Rows, "SELECT * FROM pipelines ORDER BY pipeline_id"); err != nil {
		t.Fatalf("[%s] scoped select user2 failed: %v", backend, err)
	}
	if len(user2Rows) != 1 || user2Rows[0].UserID != 2 {
		t.Fatalf("[%s] expected only user 2 rows, got: %#v", backend, user2Rows)
	}

	err := db.SelectContext(context.Background(), &[]row{}, "SELECT * FROM pipelines")
	if err == nil {
		t.Fatalf("[%s] expected missing context to fail", backend)
	}
	if code, ok := ScopeDenyCodeFromError(err); !ok || code != ScopeDenyMissingContext {
		t.Fatalf("[%s] expected missing_context deny code, got err=%v code=%q", backend, err, code)
	}

	err = db.SelectContext(context.Background(), &[]row{}, "/* scope:bypass */ SELECT * FROM pipelines")
	if err == nil {
		t.Fatalf("[%s] expected bypass token without trusted context to fail", backend)
	}
	if code, ok := ScopeDenyCodeFromError(err); !ok || code != ScopeDenyBypassNotAllowed {
		t.Fatalf("[%s] expected bypass_not_allowed deny code, got err=%v code=%q", backend, err, code)
	}

	if _, err := db.ExecContext(ctxUser1, "UPDATE pipelines SET name = 'blocked-change' WHERE pipeline_id = 2"); err != nil {
		t.Fatalf("[%s] scoped update execution failed: %v", backend, err)
	}

	var verifyUser2 []row
	if err := db.SelectContext(ctxUser2, &verifyUser2, "SELECT * FROM pipelines WHERE pipeline_id = 2"); err != nil {
		t.Fatalf("[%s] verify update failed: %v", backend, err)
	}
	if len(verifyUser2) != 1 || verifyUser2[0].Name != "u2-row" {
		t.Fatalf("[%s] cross-tenant update was not blocked, got: %#v", backend, verifyUser2)
	}

	bypassCtx := WithTrustedScopeBypass(context.Background(), "integration-check")
	var bypassRows []row
	if err := db.SelectContext(bypassCtx, &bypassRows, "/* scope:bypass */ SELECT * FROM pipelines ORDER BY pipeline_id"); err != nil {
		t.Fatalf("[%s] trusted bypass should succeed: %v", backend, err)
	}
	if len(bypassRows) != 2 {
		t.Fatalf("[%s] expected bypass to return all rows, got: %#v", backend, bypassRows)
	}

}

func runCompatibilityBudgetScenario(t *testing.T, db *squealx.DB, backend string) {
	t.Helper()
	ctxUser1 := context.WithValue(context.Background(), integrationUserKey, 1)
	compatHook := NewResourceScopeHook(
		ArgsFromContextValue(integrationUserKey),
		ScopeRule{Table: "pipelines", Column: "user_id"},
	).
		SetStrictMode(true).
		SetRejectUnknownShapes(true).
		SetCompatibilityMode(true).
		SetPassthroughBudget(0.40, 3)
	db.Use(compatHook)

	var compatibilityRows []struct {
		X int `db:"x"`
	}
	unknownShapeQuery := "SELECT * FROM (SELECT 1 AS x) v"
	if err := db.SelectContext(ctxUser1, &compatibilityRows, unknownShapeQuery); err != nil {
		t.Fatalf("[%s] first compatibility passthrough should succeed: %v", backend, err)
	}
	err := db.SelectContext(ctxUser1, &compatibilityRows, unknownShapeQuery)
	if err == nil {
		t.Fatalf("[%s] second compatibility passthrough should trip budget gate", backend)
	}
	if code, ok := ScopeDenyCodeFromError(err); !ok || code != ScopeDenyPassthroughBudget {
		t.Fatalf("[%s] expected passthrough_budget_exceeded, got err=%v code=%q", backend, err, code)
	}
	tax, ok := ScopeReasonTaxonomyForCode(ScopeDenyPassthroughBudget)
	if !ok || tax.Category != ScopeReasonCategoryBudget || tax.Severity != ScopeReasonSeverityCritical {
		t.Fatalf("[%s] taxonomy mismatch for passthrough budget: %#v", backend, tax)
	}
}

func mustOpenWithRetry(t *testing.T, open func(string) (*squealx.DB, error), dsn string) *squealx.DB {
	t.Helper()
	var lastErr error
	for i := 0; i < 45; i++ {
		db, err := open(dsn)
		if err == nil {
			pingErr := db.Ping()
			if pingErr == nil {
				return db
			}
			_ = db.Close()
			lastErr = pingErr
		} else {
			lastErr = err
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("database did not become ready for dsn %q: %v", sanitizeDSN(dsn), lastErr)
	return nil
}

func resetAndSeedMySQL(t *testing.T, db *squealx.DB) {
	t.Helper()
	mustExecMany(t, db, []string{
		"DROP TABLE IF EXISTS pipelines",
		"CREATE TABLE pipelines (pipeline_id BIGINT PRIMARY KEY AUTO_INCREMENT, user_id BIGINT NOT NULL, name VARCHAR(128) NOT NULL)",
		"INSERT INTO pipelines (user_id, name) VALUES (1, 'u1-row')",
		"INSERT INTO pipelines (user_id, name) VALUES (2, 'u2-row')",
	})
}

func resetAndSeedPostgres(t *testing.T, db *squealx.DB) {
	t.Helper()
	mustExecMany(t, db, []string{
		"DROP TABLE IF EXISTS pipelines",
		"CREATE TABLE pipelines (pipeline_id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL, name TEXT NOT NULL)",
		"INSERT INTO pipelines (user_id, name) VALUES (1, 'u1-row')",
		"INSERT INTO pipelines (user_id, name) VALUES (2, 'u2-row')",
	})
}

func resetAndSeedSQLServer(t *testing.T, db *squealx.DB) {
	t.Helper()
	mustExecMany(t, db, []string{
		"IF OBJECT_ID('dbo.pipelines','U') IS NOT NULL DROP TABLE dbo.pipelines",
		"CREATE TABLE pipelines (pipeline_id BIGINT IDENTITY(1,1) PRIMARY KEY, user_id BIGINT NOT NULL, name NVARCHAR(128) NOT NULL)",
		"INSERT INTO pipelines (user_id, name) VALUES (1, 'u1-row')",
		"INSERT INTO pipelines (user_id, name) VALUES (2, 'u2-row')",
	})
}

func mustExecMany(t *testing.T, db *squealx.DB, statements []string) {
	t.Helper()
	for _, stmt := range statements {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("statement failed: %s: %v", stmt, err)
		}
	}
}

func envOrDefault(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}

func sanitizeDSN(dsn string) string {
	parts := strings.SplitN(dsn, "@", 2)
	if len(parts) != 2 {
		return dsn
	}
	head := parts[0]
	if i := strings.Index(head, ":"); i >= 0 {
		return head[:i+1] + "***@" + parts[1]
	}
	return fmt.Sprintf("***@%s", parts[1])
}
