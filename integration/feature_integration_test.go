package integration

import (
	"context"
	"fmt"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/oarkflow/squealx"
)

// scratchFeatureRow is used for both the statement-cache and CreateMany
// subtests below; Age is deliberately exercised at 0 to prove CreateMany
// includes zero-valued columns (unlike the single-row Create/DirtyFields
// path, which would silently omit them).
type scratchFeatureRow struct {
	ID   int64  `db:"id"`
	Name string `db:"name"`
	Age  int    `db:"age"`
}

// TestNewSquealxFeatures_Integration_MySQL exercises, against a real MySQL
// server, the features added in this session that the existing fake-driver
// unit tests can only approximate: the opt-in prepared-statement cache
// (verified via MySQL's own Com_stmt_prepare counter, not just call counts
// on a fake driver), batch insert via CreateMany, and connection-pool
// warm-up. It reuses a dedicated scratch table, never the shared
// application tables in this database.
func TestNewSquealxFeatures_Integration_MySQL(t *testing.T) {
	if os.Getenv("RUN_DB_INTEGRATION") != "1" {
		t.Skip("set RUN_DB_INTEGRATION=1 to run container-backed DB integration tests")
	}

	dsn := envOrDefault("MYSQL_FEATURES_DSN", "service:service@tcp(127.0.0.1:3306)/cleardb?parseTime=true&loc=UTC")
	db := mustOpenWithRetry(t, func(dsn string) (*squealx.DB, error) {
		return squealx.Open("mysql", dsn, "integration-mysql-features")
	}, dsn)
	defer db.Close()

	mustExecMany(t, db, []string{
		"DROP TABLE IF EXISTS squealx_feature_scratch",
		"CREATE TABLE squealx_feature_scratch (id BIGINT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(64) NOT NULL, age INT NOT NULL)",
	})
	defer mustExecMany(t, db, []string{"DROP TABLE IF EXISTS squealx_feature_scratch"})

	t.Run("StatementCache", func(t *testing.T) { testStatementCacheMySQL(t, db) })
	t.Run("CreateMany", func(t *testing.T) { testCreateManyMySQL(t, db) })
	t.Run("WarmPool", func(t *testing.T) { testWarmPoolMySQL(t, dsn) })
}

// comStmtPrepare reads MySQL's own count of PREPARE calls on the current
// session. The test pins MaxOpenConns to 1 so every call in a phase runs on
// the same underlying connection, making this a real (not simulated)
// measurement of whether squealx actually re-prepared the query each time.
func comStmtPrepare(t *testing.T, db *squealx.DB) int64 {
	t.Helper()
	var row struct {
		VariableName string `db:"Variable_name"`
		Value        int64  `db:"Value"`
	}
	if err := db.Get(&row, "SHOW SESSION STATUS LIKE 'Com_stmt_prepare'"); err != nil {
		t.Fatalf("failed reading Com_stmt_prepare: %v", err)
	}
	return row.Value
}

func testStatementCacheMySQL(t *testing.T, db *squealx.DB) {
	if err := db.ApplyPoolConfig(squealx.PoolConfig{MaxOpenConns: 1, MaxIdleConns: 1}); err != nil {
		t.Fatalf("ApplyPoolConfig failed: %v", err)
	}
	if _, err := db.Exec("DELETE FROM squealx_feature_scratch"); err != nil {
		t.Fatalf("cleanup failed: %v", err)
	}

	const n = 50
	const query = "INSERT INTO squealx_feature_scratch (name, age) VALUES (?, ?)"

	before := comStmtPrepare(t, db)
	for i := range n {
		if _, err := db.Exec(query, "nocache", i); err != nil {
			t.Fatalf("exec without cache: %v", err)
		}
	}
	withoutCache := comStmtPrepare(t, db) - before

	db.EnableStatementCache(16)
	defer func() {
		if err := db.DisableStatementCache(); err != nil {
			t.Fatalf("DisableStatementCache failed: %v", err)
		}
	}()

	beforeCached := comStmtPrepare(t, db)
	for i := range n {
		if _, err := db.Exec(query, "cached", i); err != nil {
			t.Fatalf("exec with cache: %v", err)
		}
	}
	withCache := comStmtPrepare(t, db) - beforeCached

	t.Logf("Com_stmt_prepare delta over %d calls: without cache=%d, with cache=%d", n, withoutCache, withCache)
	if withCache >= withoutCache {
		t.Fatalf("expected statement cache to reduce real MySQL PREPARE calls: without=%d with=%d", withoutCache, withCache)
	}
	// With caching on, the query should be prepared once (plus a small,
	// constant number of prepares from the measurement query itself), not
	// once per Exec call.
	if withCache > 5 {
		t.Fatalf("statement cache still caused %d PREPARE calls for %d Execs", withCache, n)
	}

	var count int
	if err := db.Get(&count, "SELECT COUNT(*) FROM squealx_feature_scratch"); err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != n*2 {
		t.Fatalf("count = %d, want %d", count, n*2)
	}
}

func testCreateManyMySQL(t *testing.T, db *squealx.DB) {
	if _, err := db.Exec("DELETE FROM squealx_feature_scratch"); err != nil {
		t.Fatalf("cleanup failed: %v", err)
	}

	repo := squealx.New[scratchFeatureRow](db, "squealx_feature_scratch", "id")

	const total = 237 // deliberately not a multiple of the chunk size below
	rows := make([]any, 0, total)
	for i := range total {
		rows = append(rows, scratchFeatureRow{Name: fmt.Sprintf("bulk-%d", i), Age: i % 100})
	}
	if err := repo.CreateMany(context.Background(), rows, 50); err != nil {
		t.Fatalf("CreateMany failed: %v", err)
	}

	var count int
	if err := db.Get(&count, "SELECT COUNT(*) FROM squealx_feature_scratch"); err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != total {
		t.Fatalf("count = %d, want %d", count, total)
	}

	// i=100 -> Age 0: proves CreateMany persisted a zero-valued column
	// rather than silently dropping it the way single-row DirtyFields would.
	var sample scratchFeatureRow
	if err := db.Get(&sample, "SELECT id, name, age FROM squealx_feature_scratch WHERE name = ?", "bulk-100"); err != nil {
		t.Fatalf("spot check failed: %v", err)
	}
	if sample.Age != 0 {
		t.Fatalf("age = %d, want 0", sample.Age)
	}
}

func testWarmPoolMySQL(t *testing.T, dsn string) {
	db, err := squealx.Open("mysql", dsn, "integration-mysql-warmpool")
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(5)

	if err := db.WarmPool(context.Background(), 3); err != nil {
		t.Fatalf("WarmPool failed: %v", err)
	}
	stats := db.Stats()
	if stats.Idle < 3 {
		t.Fatalf("idle connections = %d, want >= 3 after warming", stats.Idle)
	}
}
