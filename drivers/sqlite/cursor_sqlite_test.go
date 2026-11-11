package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/internal/sqliteutil"
)

type cursorUser struct {
	ID   int    `db:"id"`
	Name string `db:"name"`
}

type cursorProfile struct {
	Name string `db:"name"`
}

type cursorUserWithProfile struct {
	ID      int            `db:"id"`
	Profile *cursorProfile `db:"profile"`
}

func setupCursorDB(t testing.TB) *squealx.DB {
	t.Helper()
	db, err := Open(":memory:", "cursor-test")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`
		CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
		INSERT INTO users(id,name) VALUES (1,'Alice'),(2,'Bob'),(3,'Carol');
	`); err != nil {
		_ = db.Close()
		t.Fatalf("setup: %v", err)
	}
	if t, ok := t.(*testing.T); ok {
		t.Cleanup(func() { _ = db.Close() })
	}
	return db
}

func TestTypedCursorStruct(t *testing.T) {
	db := setupCursorDB(t)
	cursor, err := squealx.QueryCursor[cursorUser](context.Background(), db, "SELECT id,name FROM users ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()

	var ids []int
	for cursor.Next() {
		ids = append(ids, cursor.Value().ID)
	}
	if err := cursor.Err(); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(ids) != "[1 2 3]" {
		t.Fatalf("ids=%v", ids)
	}
	if stats := cursor.Stats(); stats.Rows != 3 || !stats.Closed {
		t.Fatalf("stats=%+v", stats)
	}
}

func TestTypedCursorNestedPointerIsResetBetweenRows(t *testing.T) {
	db := setupCursorDB(t)
	cursor, err := squealx.QueryCursor[cursorUserWithProfile](context.Background(), db, `
		SELECT 1 AS id, 'Alice' AS "profile.name"
		UNION ALL
		SELECT 2 AS id, NULL AS "profile.name"`)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	if !cursor.Next() || cursor.Value().Profile == nil || cursor.Value().Profile.Name != "Alice" {
		t.Fatalf("first nested row = %+v, err=%v", cursor.Value(), cursor.Err())
	}
	if !cursor.Next() || cursor.Value().ID != 2 || cursor.Value().Profile != nil {
		t.Fatalf("second nested row retained stale pointer data: %+v, err=%v", cursor.Value(), cursor.Err())
	}
}

func TestEmptyCursorRepeatedEOFStaysClean(t *testing.T) {
	db := setupCursorDB(t)
	cursor, err := squealx.QueryCursor[cursorUser](context.Background(), db,
		"SELECT id,name FROM users WHERE id = -1")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if cursor.Next() {
			t.Fatal("unexpected row")
		}
		if err := cursor.Err(); err != nil {
			t.Fatalf("EOF attempt %d returned error: %v", i+1, err)
		}
	}
}

func TestCursorCloseAfterEOFKeepsEOFClean(t *testing.T) {
	db := setupCursorDB(t)
	cursor, err := squealx.QueryCursor[cursorUser](context.Background(), db,
		"SELECT id,name FROM users WHERE id=-1")
	if err != nil {
		t.Fatal(err)
	}
	if next := cursor.Next(); next || cursor.Err() != nil {
		t.Fatalf("initial EOF: next=%v err=%v", next, cursor.Err())
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	if next := cursor.Next(); next || cursor.Err() != nil {
		t.Fatalf("idempotent close changed EOF state: next=%v err=%v", next, cursor.Err())
	}
}

func TestExplicitlyClosedCursorReportsClosed(t *testing.T) {
	db := setupCursorDB(t)
	cursor, err := squealx.QueryCursor[cursorUser](context.Background(), db,
		"SELECT id,name FROM users ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	if cursor.Next() || !errors.Is(cursor.Err(), squealx.ErrCursorClosed) {
		t.Fatalf("Next after explicit Close: err=%v", cursor.Err())
	}
}

func TestCursorStatsElapsedStopsAfterClose(t *testing.T) {
	db := setupCursorDB(t)
	cursor, err := squealx.QueryCursor[cursorUser](context.Background(), db, "SELECT id,name FROM users")
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() {
		t.Fatal(cursor.Err())
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	closedStats := cursor.Stats()
	if !closedStats.Closed {
		t.Fatalf("cursor is not marked closed: %+v", closedStats)
	}
	time.Sleep(10 * time.Millisecond)
	if got := cursor.Stats().Elapsed; got != closedStats.Elapsed {
		t.Fatalf("elapsed advanced after close: before=%s after=%s", closedStats.Elapsed, got)
	}
}

func TestRepositoryFindCursorStreamsFilteredRows(t *testing.T) {
	db := setupCursorDB(t)
	repo := squealx.New[cursorUser](db, "users", "id")
	cursor, err := squealx.FindCursor(context.Background(), repo, map[string]any{"id": 2})
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	if !cursor.Next() || cursor.Value().ID != 2 || cursor.Value().Name != "Bob" {
		t.Fatalf("row=%+v err=%v", cursor.Value(), cursor.Err())
	}
	if cursor.Next() {
		t.Fatalf("unexpected extra row: %+v", cursor.Value())
	}
	if err := cursor.Err(); err != nil {
		t.Fatal(err)
	}

	preloaded := repo.Preload(squealx.Relation{With: "orders", LocalField: "id", RelatedField: "user_id"})
	if _, err := squealx.FindCursor(context.Background(), preloaded, nil); !errors.Is(err, squealx.ErrRepositoryCursorPreload) {
		t.Fatalf("preloaded FindCursor error = %v", err)
	}
}

func TestPreloadedCursorRejectsSingleConnectionPool(t *testing.T) {
	db := setupCursorDB(t)
	repo := squealx.New[cursorUser](db, "users", "id").Preload(squealx.Relation{
		With: "orders", LocalField: "id", RelatedField: "user_id",
	})
	if _, err := squealx.FindPreloadedCursor(context.Background(), repo, nil); !errors.Is(err, squealx.ErrPreloadedCursorPool) {
		t.Fatalf("single-connection pool error = %v", err)
	}
}

func TestRepositoryRawCursorUsesAllowlistAndLimit(t *testing.T) {
	db := setupCursorDB(t)
	repo := squealx.New[cursorUser](db, "users", "id")
	ctx := squealx.WithQueryParams(context.Background(), squealx.QueryParams{
		AllowedRaw: map[string]string{
			"all_users": "SELECT id,name FROM users ORDER BY id",
		},
	})
	cursor, err := squealx.RawCursorConfig(ctx, repo, "all_users", squealx.CursorConfig{MaxRows: 2})
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 2; i++ {
		if !cursor.Next() || cursor.Value().ID != i {
			t.Fatalf("row %d=%+v err=%v", i, cursor.Value(), cursor.Err())
		}
	}
	if cursor.Next() || !errors.Is(cursor.Err(), squealx.ErrCursorLimit) {
		t.Fatalf("raw cursor limit error = %v", cursor.Err())
	}
	if _, err := squealx.RawCursor[cursorUser](context.Background(), repo, "SELECT id,name FROM users"); err == nil {
		t.Fatal("unallowlisted raw query was accepted")
	}
}

func TestTypedCursorPointerReuseAndCopy(t *testing.T) {
	db := setupCursorDB(t)
	cursor, err := squealx.QueryCursor[*cursorUser](context.Background(), db, "SELECT id,name FROM users ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() {
		t.Fatal(cursor.Err())
	}
	firstReusable := cursor.Value()
	firstCopy := cursor.Copy()
	if !cursor.Next() {
		t.Fatal(cursor.Err())
	}
	if firstReusable != cursor.Value() || firstReusable.ID != 2 {
		t.Fatalf("pointer storage was not reused: first=%p current=%p value=%+v", firstReusable, cursor.Value(), firstReusable)
	}
	if firstCopy.ID != 1 || firstCopy == firstReusable {
		t.Fatalf("copy was not independent: copy=%+v", firstCopy)
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	if cursor.Value() != nil || cursor.Copy() != nil {
		t.Fatalf("closed pointer cursor retained row: value=%v copy=%v", cursor.Value(), cursor.Copy())
	}
}

func TestCursorValueRefReuse(t *testing.T) {
	db := setupCursorDB(t)
	cursor, err := squealx.QueryCursor[cursorUser](context.Background(), db, "SELECT id,name FROM users ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	first, ok := cursor.NextRef()
	if !ok || (*first).ID != 1 {
		t.Fatalf("first row=%v err=%v", first, cursor.Err())
	}
	copy := cursor.Copy()
	second, ok := cursor.NextRef()
	if !ok || first != second || (*first).ID != 2 {
		t.Fatalf("reference was not reused: first=%p second=%p value=%+v", first, second, *first)
	}
	if copy.ID != 1 {
		t.Fatalf("copied row changed: %+v", copy)
	}
}

func TestTypedCursorMapScalarAndNamed(t *testing.T) {
	db := setupCursorDB(t)
	maps, err := squealx.QueryCursor[map[string]any](context.Background(), db, "SELECT id,name FROM users WHERE id=1")
	if err != nil {
		t.Fatal(err)
	}
	if !maps.Next() || maps.Value()["name"] != "Alice" {
		t.Fatalf("map=%v err=%v", maps.Value(), maps.Err())
	}
	if err := maps.Close(); err != nil {
		t.Fatal(err)
	}

	scalar, err := squealx.QueryCursor[int](context.Background(), db, "SELECT COUNT(*) FROM users")
	if err != nil {
		t.Fatal(err)
	}
	if !scalar.Next() || scalar.Value() != 3 {
		t.Fatalf("count=%d err=%v", scalar.Value(), scalar.Err())
	}
	if err := scalar.Close(); err != nil {
		t.Fatal(err)
	}

	named, err := squealx.NamedQueryCursor[cursorUser](context.Background(), db,
		"SELECT id,name FROM users WHERE id IN :ids ORDER BY id", map[string]any{"ids": []int{1, 3}})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := named.Collect(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].ID != 1 || rows[1].ID != 3 {
		t.Fatalf("rows=%+v", rows)
	}
}

func TestConfiguredNamedAndInCursorRowLimit(t *testing.T) {
	db := setupCursorDB(t)
	cases := []struct {
		name string
		open func() (*squealx.Cursor[cursorUser], error)
	}{
		{
			name: "named",
			open: func() (*squealx.Cursor[cursorUser], error) {
				return squealx.NamedQueryCursorConfig[cursorUser](context.Background(), db,
					squealx.CursorConfig{MaxRows: 1},
					"SELECT id,name FROM users WHERE id IN (:ids) ORDER BY id",
					map[string]any{"ids": []int{1, 2, 3}})
			},
		},
		{
			name: "in",
			open: func() (*squealx.Cursor[cursorUser], error) {
				return squealx.InQueryCursorConfig[cursorUser](context.Background(), db,
					squealx.CursorConfig{MaxRows: 1},
					"SELECT id,name FROM users WHERE id IN (?) ORDER BY id", []int{1, 2, 3})
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cursor, err := tc.open()
			if err != nil {
				t.Fatal(err)
			}
			if !cursor.Next() || cursor.Value().ID != 1 {
				t.Fatalf("first row=%+v err=%v", cursor.Value(), cursor.Err())
			}
			if cursor.Next() || !errors.Is(cursor.Err(), squealx.ErrCursorLimit) {
				t.Fatalf("second Next err=%v", cursor.Err())
			}
		})
	}
}

func TestCursorConfigRejectsNegativeLimitBeforeQuery(t *testing.T) {
	db := setupCursorDB(t)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	config := squealx.CursorConfig{MaxRows: -1}
	checks := []struct {
		name string
		open func() error
	}{
		{
			name: "positional",
			open: func() error {
				_, err := squealx.QueryCursorConfig[cursorUser](context.Background(), db, config, "SELECT id,name FROM users")
				return err
			},
		},
		{
			name: "named",
			open: func() error {
				_, err := squealx.NamedQueryCursorConfig[cursorUser](context.Background(), db, config, "SELECT id,name FROM users", nil)
				return err
			},
		},
		{
			name: "in",
			open: func() error {
				_, err := squealx.InQueryCursorConfig[cursorUser](context.Background(), db, config, "SELECT id,name FROM users WHERE id IN (?)", []int{1})
				return err
			},
		},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			if err := check.open(); !errors.Is(err, squealx.ErrInvalidCursorConfig) {
				t.Fatalf("error = %v, want ErrInvalidCursorConfig", err)
			}
		})
	}
}

func TestTypedCursorPointerMapCopyIsIndependent(t *testing.T) {
	db := setupCursorDB(t)
	cursor, err := squealx.QueryCursor[*map[string]any](context.Background(), db,
		"SELECT id,name FROM users ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()

	if !cursor.Next() {
		t.Fatal(cursor.Err())
	}
	first := cursor.Copy()
	if !cursor.Next() {
		t.Fatal(cursor.Err())
	}
	if (*first)["name"] != "Alice" {
		t.Fatalf("copied first map changed after cursor advanced: %v", *first)
	}
	if first == cursor.Value() || (*first)["name"] == (*cursor.Value())["name"] {
		t.Fatalf("pointer-map copy aliases cursor storage: copy=%p current=%p", first, cursor.Value())
	}
}

func TestCursorCloseReleasesReusableMapRowButKeepsCopy(t *testing.T) {
	db := setupCursorDB(t)
	cursor, err := squealx.QueryCursor[map[string]any](context.Background(), db,
		"SELECT id,name FROM users WHERE id=1")
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() {
		t.Fatal(cursor.Err())
	}
	retained := cursor.Copy()
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	if cursor.Value() != nil {
		t.Fatalf("cursor retained reusable map row after close: %#v", cursor.Value())
	}
	if columns := cursor.Columns(); columns != nil {
		t.Fatalf("cursor retained column schema after close: %v", columns)
	}
	if retained["name"] != "Alice" {
		t.Fatalf("copied row changed after close: %#v", retained)
	}
}

func TestCursorConstructionFailureClosesRows(t *testing.T) {
	db := setupCursorDB(t)
	rows, err := db.Queryx("SELECT id,name FROM users")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := squealx.CursorFromRowsConfig[cursorUser](rows, squealx.CursorConfig{MaxRows: -1}); !errors.Is(err, squealx.ErrInvalidCursorConfig) {
		t.Fatalf("construction error = %v, want ErrInvalidCursorConfig", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var result int
	if err := db.GetContext(ctx, &result, "SELECT 1"); err != nil {
		t.Fatalf("connection unavailable after constructor failure: %v", err)
	}
}

func TestPoolHealthReportsSaturation(t *testing.T) {
	db := setupCursorDB(t)
	db.SetMaxOpenConns(1)
	conn, err := db.Connx(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	pressure := db.PoolSnapshot()
	if !pressure.Saturated || pressure.Utilization != 1 || pressure.Stats.InUse != 1 {
		t.Fatalf("pool snapshot did not report pressure without a connection: %+v", pressure)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	health := db.HealthContext(ctx)
	if !errors.Is(health.Error, context.DeadlineExceeded) {
		t.Fatalf("health error = %v, want deadline exceeded while pool is occupied", health.Error)
	}
	if !health.Saturated || health.Utilization != 1 || health.Stats.InUse != 1 {
		t.Fatalf("pool pressure not reported: %+v", health)
	}
}

func TestPoolRecoversAfterCanceledWaiter(t *testing.T) {
	db := setupCursorDB(t)
	db.SetMaxOpenConns(1)
	conn, err := db.Connx(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	var result int
	if err := db.GetContext(ctx, &result, "SELECT 1"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked query error = %v, want deadline exceeded", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := db.GetContext(ctx, &result, "SELECT 1"); err != nil || result != 1 {
		t.Fatalf("query after returning connection = %d, err=%v", result, err)
	}
}

func TestConnectWithPoolConfigSQLite(t *testing.T) {
	db, err := squealx.ConnectWithPoolConfig(context.Background(), "sqlite", ":memory:",
		"configured-connect", squealx.PoolConfig{MaxOpenConns: 4, MaxIdleConns: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if got := db.Stats().MaxOpenConnections; got != 4 {
		t.Fatalf("max open connections = %d, want 4", got)
	}
	var result int
	if err := db.Get(&result, "SELECT 1"); err != nil || result != 1 {
		t.Fatalf("SELECT 1 = %d, err=%v", result, err)
	}
}

func TestOpenPrivateMemorySQLiteBoundsPool(t *testing.T) {
	db, err := Open(":memory:", "private-memory-pool")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
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
		t.Fatalf("query after pooled connection reuse = %d, err=%v", got, err)
	}
}

func TestOpenFileSQLiteBoundsPool(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "bounded-pool.sqlite"), "file-pool-default")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if got := db.Stats().MaxOpenConnections; got != defaultMaxOpenConns {
		t.Fatalf("file-backed max open connections = %d, want %d", got, defaultMaxOpenConns)
	}
}

func TestOpenFileSQLiteBusyTimeoutDefaultsAndCanBeOverridden(t *testing.T) {
	for _, test := range []struct {
		name string
		dsn  string
		want int
	}{
		{name: "default", dsn: filepath.Join(t.TempDir(), "default-timeout.sqlite"), want: sqliteutil.DefaultBusyTimeoutMS},
		{name: "explicit", dsn: filepath.Join(t.TempDir(), "explicit-timeout.sqlite") + "?_busy_timeout=250", want: 250},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, err := Open(test.dsn, "busy-timeout-"+test.name)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var got int
			if err := db.Get(&got, "PRAGMA busy_timeout"); err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("busy_timeout = %d, want %d", got, test.want)
			}
		})
	}
}

func TestOpenFileSQLiteWaitsForConcurrentWriter(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "write-contention.sqlite"), "write-contention")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE writes (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Beginx()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO writes(id) VALUES (1)`); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		_, err := db.Exec(`INSERT INTO writes(id) VALUES (2)`)
		done <- err
	}()
	<-started
	time.Sleep(50 * time.Millisecond)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("concurrent insert after writer commit: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent insert did not resume after writer commit")
	}
}

func TestSQLiteBusyWaitHonorsContextCancellation(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "write-cancel.sqlite")+"?_busy_timeout=200", "write-cancel")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE writes (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Beginx()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO writes(id) VALUES (1)`); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	done := make(chan error, 1)
	go func() {
		_, err := db.ExecContext(ctx, `INSERT INTO writes(id) VALUES (2)`)
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			_ = tx.Rollback()
			t.Fatalf("canceled insert error = %v, want context deadline exceeded", err)
		}
	case <-time.After(time.Second):
		_ = tx.Rollback()
		<-done
		t.Fatal("canceled insert remained blocked past the busy timeout")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		_ = tx.Rollback()
		t.Fatalf("canceled insert took %s, want less than 1 second", elapsed)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenWithPoolConfigAppliesLimitsBeforeUse(t *testing.T) {
	db, err := OpenWithPoolConfig("file:explicit-pool?mode=memory&cache=shared", "explicit-pool",
		squealx.PoolConfig{MaxOpenConns: 3, MaxIdleConns: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if got := db.Stats().MaxOpenConnections; got != 3 {
		t.Fatalf("max open connections = %d, want 3", got)
	}
	if _, err := db.Exec("CREATE TABLE configured (value INTEGER)"); err != nil {
		t.Fatal(err)
	}
}

func TestOpenWithPoolConfigRejectsMultiConnectionPrivateMemory(t *testing.T) {
	for _, pool := range []squealx.PoolConfig{
		{MaxOpenConns: 0, MaxIdleConns: 1},
		{MaxOpenConns: 2, MaxIdleConns: 1},
		{MaxOpenConns: 1, MaxIdleConns: 0},
		{MaxOpenConns: 1, MaxIdleConns: 1, ConnMaxLifetime: time.Minute},
		{MaxOpenConns: 1, MaxIdleConns: 1, ConnMaxIdleTime: time.Minute},
	} {
		_, err := OpenWithPoolConfig(":memory:", "invalid-private-pool", pool)
		if !errors.Is(err, ErrPrivateMemoryPool) {
			t.Errorf("pool %+v error = %v, want ErrPrivateMemoryPool", pool, err)
		}
	}
}

func TestPrivateMemoryDSNDetectionPreservesSharedCache(t *testing.T) {
	for _, dsn := range []string{":memory:", ":memory:?_pragma=busy_timeout(5000)", "file::memory:", "file:session?mode=memory&cache=private"} {
		if !sqliteutil.IsPrivateMemoryDSN(dsn) {
			t.Errorf("IsPrivateMemoryDSN(%q) = false, want true", dsn)
		}
	}
	for _, dsn := range []string{"file::memory:?cache=shared", "file:session?mode=memory&cache=shared", "./app.db"} {
		if sqliteutil.IsPrivateMemoryDSN(dsn) {
			t.Errorf("IsPrivateMemoryDSN(%q) = true, want false", dsn)
		}
	}
}

func TestOpenSharedMemorySQLiteSupportsMultipleConnections(t *testing.T) {
	db, err := Open("file:shared-pool?mode=memory&cache=shared", "shared-memory-pool")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if got := db.Stats().MaxOpenConnections; got != defaultMaxOpenConns {
		t.Fatalf("shared-memory max open connections = %d, want default %d", got, defaultMaxOpenConns)
	}
	if _, err := db.Exec("CREATE TABLE shared (value INTEGER)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO shared VALUES (42)"); err != nil {
		t.Fatal(err)
	}
	first, err := db.Connx(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := db.Connx(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	for index, conn := range []*squealx.Conn{first, second} {
		var got int
		if err := conn.QueryRowContext(context.Background(), "SELECT value FROM shared").Scan(&got); err != nil || got != 42 {
			t.Fatalf("connection %d read %d, err=%v", index+1, got, err)
		}
	}
}

func TestConnectExistWithPoolConfigSQLite(t *testing.T) {
	raw, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db, err := squealx.ConnectExistWithPoolConfig(context.Background(), "sqlite", raw, "existing-configured",
		squealx.PoolConfig{MaxOpenConns: 2, MaxIdleConns: 1})
	if err != nil {
		_ = raw.Close()
		t.Fatal(err)
	}
	defer db.Close()
	if db.ID != "existing-configured" || db.Stats().MaxOpenConnections != 2 {
		t.Fatalf("ID=%q pool=%+v", db.ID, db.Stats())
	}

	untouched, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer untouched.Close()
	if _, err := squealx.ConnectExistWithPoolConfig(context.Background(), "sqlite", untouched, "invalid",
		squealx.PoolConfig{MaxOpenConns: 1, MaxIdleConns: 2}); !errors.Is(err, squealx.ErrInvalidPool) {
		t.Fatalf("invalid config error = %v", err)
	}
	if err := untouched.PingContext(context.Background()); err != nil {
		t.Fatalf("raw DB was closed after config validation failed: %v", err)
	}
}

func TestCursorLimitAndStrictColumns(t *testing.T) {
	db := setupCursorDB(t)
	exact, err := squealx.QueryCursorConfig[cursorUser](context.Background(), db,
		squealx.CursorConfig{MaxRows: 1}, "SELECT id,name FROM users WHERE id=1")
	if err != nil {
		t.Fatal(err)
	}
	if !exact.Next() || exact.Next() || exact.Err() != nil {
		t.Fatalf("exact-limit result err=%v", exact.Err())
	}
	if !exact.Stats().Closed {
		t.Fatalf("exact-limit cursor did not close at EOF: %+v", exact.Stats())
	}

	cursor, err := squealx.QueryCursorConfig[cursorUser](context.Background(), db,
		squealx.CursorConfig{MaxRows: 1}, "SELECT id,name FROM users ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() || cursor.Next() || !errors.Is(cursor.Err(), squealx.ErrCursorLimit) {
		t.Fatalf("limit err=%v", cursor.Err())
	}

	type onlyID struct {
		ID int `db:"id"`
	}
	if _, err := squealx.QueryCursor[onlyID](context.Background(), db, "SELECT id,name FROM users"); err == nil {
		t.Fatal("expected strict missing-column error")
	}
	unsafeCursor, err := squealx.QueryCursor[onlyID](context.Background(), db.Unsafe(), "SELECT id,name FROM users")
	if err != nil {
		t.Fatal(err)
	}
	_ = unsafeCursor.Close()
}

func TestRowErrDoesNotConsumeRowAndUnsafePreservesHooks(t *testing.T) {
	db := setupCursorDB(t)
	row := db.QueryRowx("SELECT id,name FROM users WHERE id=1")
	if err := row.Err(); err != nil {
		t.Fatal(err)
	}
	var user cursorUser
	if err := row.StructScan(&user); err != nil {
		t.Fatalf("row was consumed by Err: %v", err)
	}

	called := 0
	db.UseBefore(func(ctx context.Context, query string, args ...any) (context.Context, string, []any, error) {
		called++
		return ctx, query, args, nil
	})
	var count int
	if err := db.Unsafe().Get(&count, "SELECT COUNT(*) FROM users"); err != nil {
		t.Fatal(err)
	}
	if called == 0 {
		t.Fatal("Unsafe dropped hooks")
	}
	if name, err := db.GetDBName(); err != nil || name != "main" {
		t.Fatalf("db name=%q err=%v", name, err)
	}
}

func TestHookErrorAndPanicBecomeRowErrors(t *testing.T) {
	db := setupCursorDB(t)
	blocked := errors.New("blocked")
	db.UseBefore(func(ctx context.Context, query string, args ...any) (context.Context, string, []any, error) {
		return ctx, query, args, blocked
	})
	row := db.QueryRowx("SELECT 1")
	if !errors.Is(row.Err(), blocked) {
		t.Fatalf("err=%v", row.Err())
	}
	if err := row.Scan(new(int)); !errors.Is(err, blocked) {
		t.Fatalf("scan err=%v", err)
	}

	db2 := setupCursorDB(t)
	db2.UseBefore(func(context.Context, string, ...any) (context.Context, string, []any, error) {
		panic("hook boom")
	})
	if _, err := db2.Exec("SELECT 1"); err == nil {
		t.Fatal("expected hook panic error")
	} else {
		var panicErr *squealx.HookPanicError
		if !errors.As(err, &panicErr) {
			t.Fatalf("err=%T %v", err, err)
		}
	}
}

func TestSelectTypedPointer(t *testing.T) {
	db := setupCursorDB(t)
	user, err := squealx.SelectTyped[*cursorUser](db, "SELECT id,name FROM users WHERE id=1")
	if err != nil {
		t.Fatal(err)
	}
	if user == nil || user.ID != 1 {
		t.Fatalf("user=%+v", user)
	}
}

func BenchmarkCursorStruct(b *testing.B) {
	db := setupCursorDB(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cursor, err := squealx.QueryCursor[cursorUser](context.Background(), db, "SELECT id,name FROM users ORDER BY id")
		if err != nil {
			b.Fatal(err)
		}
		for cursor.Next() {
			_ = cursor.Value().ID
		}
		if err := cursor.Err(); err != nil {
			b.Fatal(err)
		}
	}
}

func TestExecWithReturnContextSQLite(t *testing.T) {
	db := setupCursorDB(t)
	user := &cursorUser{Name: "Dora"}
	if err := db.ExecWithReturnContext(context.Background(),
		"INSERT INTO users(name) VALUES (:name)", user); err != nil {
		t.Fatal(err)
	}
	if user.ID == 0 || user.Name != "Dora" {
		t.Fatalf("returned user=%+v", user)
	}

	user.Name = "Dora Updated"
	if err := db.ExecWithReturnContext(context.Background(),
		"UPDATE users SET name=:name WHERE id=:id", user); err != nil {
		t.Fatal(err)
	}
	if user.Name != "Dora Updated" {
		t.Fatalf("updated user=%+v", user)
	}
}

func TestSmartSelectContextNamedAndIN(t *testing.T) {
	db := setupCursorDB(t)
	var one cursorUser
	if err := db.SmartSelectContext(context.Background(), &one,
		"SELECT id,name FROM users WHERE id=:id", map[string]any{"id": 2}); err != nil {
		t.Fatal(err)
	}
	if one.ID != 2 {
		t.Fatalf("one=%+v", one)
	}
	var many []cursorUser
	if err := db.SmartSelectContext(context.Background(), &many,
		"SELECT id,name FROM users WHERE id IN (?) ORDER BY id", []int{1, 3}); err != nil {
		t.Fatal(err)
	}
	if len(many) != 2 || many[1].ID != 3 {
		t.Fatalf("many=%+v", many)
	}
}

const cursorManyRowsQuery = `WITH RECURSIVE seq(id) AS (
	SELECT 1 UNION ALL SELECT id + 1 FROM seq WHERE id < 1000
) SELECT id, 'user-' || id AS name FROM seq`

func BenchmarkCursorStruct1000(b *testing.B) {
	db := setupCursorDB(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cursor, err := squealx.QueryCursor[cursorUser](context.Background(), db, cursorManyRowsQuery)
		if err != nil {
			b.Fatal(err)
		}
		for cursor.Next() {
			_ = cursor.Value().ID
		}
		if err := cursor.Err(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCursorStruct1000ValueRef(b *testing.B) {
	db := setupCursorDB(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cursor, err := squealx.QueryCursor[cursorUser](context.Background(), db, cursorManyRowsQuery)
		if err != nil {
			b.Fatal(err)
		}
		for row, ok := cursor.NextRef(); ok; row, ok = cursor.NextRef() {
			_ = row.ID
		}
		if err := cursor.Err(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRawSQLRowsStruct1000(b *testing.B) {
	db := setupCursorDB(b)
	stdDB := db.DB()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := stdDB.Query(cursorManyRowsQuery)
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
			var user cursorUser
			if err := rows.Scan(&user.ID, &user.Name); err != nil {
				_ = rows.Close()
				b.Fatal(err)
			}
			_ = user.ID
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			b.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSelectEachStruct1000(b *testing.B) {
	db := setupCursorDB(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := squealx.SelectEach(db, func(user cursorUser) error {
			_ = user.ID
			return nil
		}, cursorManyRowsQuery)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestQueryIterStruct(t *testing.T) {
	db := setupCursorDB(t)
	var ids []int
	for u, err := range squealx.QueryIter[cursorUser](context.Background(), db, "SELECT id,name FROM users ORDER BY id") {
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, u.ID)
	}
	if fmt.Sprint(ids) != "[1 2 3]" {
		t.Fatalf("ids=%v", ids)
	}
	// early break closes the cursor; connection (MaxOpenConns=1) must be reusable
	for _, err := range squealx.QueryIter[cursorUser](context.Background(), db, "SELECT id,name FROM users") {
		if err != nil {
			t.Fatal(err)
		}
		break
	}
	var n int
	if err := db.Get(&n, "SELECT COUNT(*) FROM users"); err != nil || n != 3 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	for _, err := range squealx.QueryIter[cursorUser](context.Background(), db, "SELECT nope FROM missing") {
		if err == nil {
			t.Fatal("expected error")
		}
	}
}

func TestTxAndStmtCursor(t *testing.T) {
	db := setupCursorDB(t)
	tx, err := db.Beginx()
	if err != nil {
		t.Fatal(err)
	}
	cur, err := squealx.TxQueryCursor[cursorUser](context.Background(), tx, "SELECT id,name FROM users ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for u, err := range cur.All() {
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, u.Name)
	}
	if fmt.Sprint(names) != "[Alice Bob Carol]" {
		t.Fatalf("names=%v", names)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	stmt, err := db.Preparex("SELECT id,name FROM users WHERE id > ? ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	scur, err := squealx.StmtQueryCursor[cursorUser](context.Background(), stmt, 1)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := scur.Collect(0)
	if err != nil || len(rows) != 2 {
		t.Fatalf("rows=%v err=%v", rows, err)
	}
}

func TestConfiguredTxConnAndStmtCursors(t *testing.T) {
	db := setupCursorDB(t)
	verifyLimit := func(t *testing.T, cursor *squealx.Cursor[cursorUser]) {
		t.Helper()
		for i := 0; i < 2; i++ {
			if !cursor.Next() {
				t.Fatalf("row %d missing: %v", i, cursor.Err())
			}
		}
		if cursor.Next() || !errors.Is(cursor.Err(), squealx.ErrCursorLimit) {
			t.Fatalf("cursor limit error = %v", cursor.Err())
		}
	}
	config := squealx.CursorConfig{MaxRows: 2}
	query := "SELECT id,name FROM users ORDER BY id"

	tx, err := db.Beginx()
	if err != nil {
		t.Fatal(err)
	}
	txCursor, err := squealx.TxQueryCursorConfig[cursorUser](context.Background(), tx, config, query)
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	verifyLimit(t, txCursor)
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	conn, err := db.Connx(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	connCursor, err := squealx.ConnQueryCursorConfig[cursorUser](context.Background(), conn, config, query)
	if err != nil {
		_ = conn.Close()
		t.Fatal(err)
	}
	verifyLimit(t, connCursor)
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}

	stmt, err := db.Preparex(query)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	stmtCursor, err := squealx.StmtQueryCursorConfig[cursorUser](context.Background(), stmt, config)
	if err != nil {
		t.Fatal(err)
	}
	verifyLimit(t, stmtCursor)
}

func TestSelectEachVariants(t *testing.T) {
	db := setupCursorDB(t)
	q := "SELECT id,name FROM users ORDER BY id"
	var vals []cursorUser
	if err := squealx.SelectEach(db, func(u cursorUser) error { vals = append(vals, u); return nil }, q); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(vals) != "[{1 Alice} {2 Bob} {3 Carol}]" {
		t.Fatalf("vals=%v", vals)
	}
	var ptrs []*cursorUser
	if err := squealx.SelectEach(db, func(u *cursorUser) error { ptrs = append(ptrs, u); return nil }, q); err != nil {
		t.Fatal(err)
	}
	if ptrs[0] == ptrs[1] || ptrs[0].Name != "Alice" || ptrs[2].Name != "Carol" {
		t.Fatalf("ptrs not independent")
	}
	var ids []int
	if err := squealx.SelectEach(db, func(i int) error { ids = append(ids, i); return nil }, "SELECT id FROM users ORDER BY id"); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(ids) != "[1 2 3]" {
		t.Fatalf("ids=%v", ids)
	}
	var maps []map[string]any
	if err := squealx.SelectEach(db, func(m map[string]any) error { maps = append(maps, m); return nil }, q); err != nil {
		t.Fatal(err)
	}
	if len(maps) != 3 || maps[0]["name"] == maps[1]["name"] {
		t.Fatalf("maps=%v", maps)
	}
	type bad struct {
		Other int `db:"other"`
	}
	err := squealx.SelectEach(db, func(bad) error { return nil }, q)
	if err == nil {
		t.Fatal("expected missing destination error")
	}
	if err := squealx.SelectEach(db, func(any) error { return nil }, q); err == nil {
		t.Fatal("expected ambiguity error")
	}
	stop := errors.New("stop")
	if err := squealx.SelectEach(db, func(cursorUser) error { return stop }, q); !errors.Is(err, stop) {
		t.Fatalf("err=%v", err)
	}
}

func TestSelectEachContextCancellation(t *testing.T) {
	db := setupCursorDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := squealx.SelectEachContext(ctx, db, func(cursorUser) error { return nil },
		"SELECT id,name FROM users ORDER BY id")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("SelectEachContext error = %v, want context.Canceled", err)
	}
}

func TestSelectClearsStaleSliceTail(t *testing.T) {
	db := setupCursorDB(t)
	users := []*cursorUser{
		{ID: 90, Name: "stale-a"},
		{ID: 91, Name: "stale-b"},
		{ID: 92, Name: "stale-c"},
	}
	if err := db.Select(&users, "SELECT id,name FROM users WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	if len(users) != 1 || users[0].ID != 1 {
		t.Fatalf("users = %#v", users)
	}
	backing := users[:cap(users)]
	for i := 1; i < len(backing); i++ {
		if backing[i] != nil {
			t.Fatalf("stale pointer remains in backing slot %d: %#v", i, backing[i])
		}
	}
}
