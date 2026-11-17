package squealx

import (
	"context"
	"database/sql"
	"sync"
)

const (
	defaultStmtCacheMaxEntries = 256
	// stmtCacheMaxQueryLen mirrors namedCacheMaxQueryLen: dynamically rebound
	// queries (e.g. an IN (...) expansion) can produce near-unbounded query
	// text, and caching those would both bloat this cache and burn
	// server-side prepared-statement slots for a statement that will likely
	// never be reused verbatim.
	stmtCacheMaxQueryLen = 8 << 10
)

// stmtCache is a bounded, FIFO-evicted cache of prepared statements keyed by
// exact query text, used to avoid re-preparing the same query on every
// Exec/Query call (the default database/sql behavior for most drivers).
// Evicted and replaced statements are closed to release server-side
// resources. The zero value is not usable; use newStmtCache.
type stmtCache struct {
	mu    sync.RWMutex
	m     map[string]SQLStmt
	order []string
	next  int
	max   int
}

func newStmtCache(max int) *stmtCache {
	if max <= 0 {
		max = defaultStmtCacheMaxEntries
	}
	return &stmtCache{m: make(map[string]SQLStmt, max), order: make([]string, max), max: max}
}

// getOrPrepare returns a cached statement for query, preparing and storing
// one via db.PrepareContext if none exists yet. ok is false when query is too
// long to cache or preparation failed; callers should fall back to a direct,
// uncached Exec/Query in that case rather than failing outright.
func (c *stmtCache) getOrPrepare(ctx context.Context, db SQLDB, query string) (SQLStmt, bool) {
	if len(query) > stmtCacheMaxQueryLen {
		return nil, false
	}

	c.mu.RLock()
	if s, ok := c.m[query]; ok {
		c.mu.RUnlock()
		return s, true
	}
	c.mu.RUnlock()

	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		return nil, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	// Another goroutine may have populated this key while we were preparing.
	if existing, ok := c.m[query]; ok {
		_ = stmt.Close()
		return existing, true
	}
	if len(c.m) >= c.max {
		oldest := c.order[c.next]
		if old, ok := c.m[oldest]; ok {
			delete(c.m, oldest)
			_ = old.Close()
		}
	}
	c.m[query] = stmt
	c.order[c.next] = query
	c.next = (c.next + 1) % c.max
	return stmt, true
}

// closeAll closes every cached statement and empties the cache.
func (c *stmtCache) closeAll() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var firstErr error
	for k, s := range c.m {
		if err := s.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(c.m, k)
	}
	c.next = 0
	return firstErr
}

// EnableStatementCache turns on a bounded cache of prepared statements,
// transparently reused by Exec/Query/QueryRow/Select/Get/NamedExec (and their
// Context variants) whenever the exact query text repeats — as in point
// lookups or per-row loop writes — instead of re-preparing on every call.
// maxEntries <= 0 uses a sensible default (256). Disabled by default; call
// DisableStatementCache to turn it back off. Safe to call again to resize
// (the old cache's statements are left to be garbage collected by the
// driver/connection, not explicitly closed, since they may still be in use).
func (db *DB) EnableStatementCache(maxEntries int) {
	db.stmtCache.Store(newStmtCache(maxEntries))
}

// DisableStatementCache turns off statement caching and closes every
// currently cached prepared statement. A no-op if caching isn't enabled.
func (db *DB) DisableStatementCache() error {
	old := db.stmtCache.Swap(nil)
	if old == nil {
		return nil
	}
	return old.closeAll()
}

// cachedExec routes through the statement cache when enabled, falling back to
// a direct, uncached Exec otherwise (including when preparation fails, so a
// transient prepare error never fails the caller's query).
func (db *DB) cachedExec(ctx context.Context, query string, args []any) (sql.Result, error) {
	if cache := db.stmtCache.Load(); cache != nil {
		if stmt, ok := cache.getOrPrepare(ctx, db.SQLDB, query); ok {
			return stmt.ExecContext(ctx, args...)
		}
	}
	return db.SQLDB.ExecContext(ctx, query, args...)
}

// cachedQuery is cachedExec's counterpart for Query/Queryx-style calls.
func (db *DB) cachedQuery(ctx context.Context, query string, args []any) (SQLRows, error) {
	if cache := db.stmtCache.Load(); cache != nil {
		if stmt, ok := cache.getOrPrepare(ctx, db.SQLDB, query); ok {
			return stmt.QueryContext(ctx, args...)
		}
	}
	return db.SQLDB.QueryContext(ctx, query, args...)
}
