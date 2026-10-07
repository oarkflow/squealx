# Squealx Streaming Cursor

`Cursor[T]` is the low-memory query path for large result sets. It consumes `database/sql` rows directly, builds the reflection/scan plan once, and reuses one destination plus one scan-argument slice for the lifetime of the cursor.

## Properties

- O(1) row memory unless the caller collects rows.
- Pull-based; no goroutine, channel, queue, or hidden buffering.
- Context-aware query opening and cancellation.
- Struct, pointer-to-struct, scalar, pointer-scalar, and string-keyed map results.
- Strict column-to-field validation by default; `db.Unsafe()` opts into ignoring unknown columns.
- Named and positional `IN` query variants.
- Automatic close at EOF and on decode failure.
- Defensive `MaxRows` limit and progress statistics.
- Reusable pointer/map storage with an explicit `Copy` method for retained rows.

## Typed cursor

```go
cursor, err := squealx.QueryCursor[User](ctx, db,
    "SELECT id, name FROM users WHERE id > ? ORDER BY id", lastID)
if err != nil {
    return err
}
defer cursor.Close()

for cursor.Next() {
    user := cursor.Value()
    if err := consume(user); err != nil {
        return err
    }
}
return cursor.Err()
```

## Bounded query

```go
cursor, err := squealx.QueryCursorConfig[Event](ctx, db,
    squealx.CursorConfig{MaxRows: 100_000},
    "SELECT id, payload FROM events ORDER BY id")
```

Attempting to advance beyond `MaxRows` returns `ErrCursorLimit` and closes the rows.

## Pointer and map lifetimes

For `Cursor[*T]` and `Cursor[map[string]any]`, `Value` is reused on the next `Next` call. This eliminates a row allocation. Process the value immediately or retain an independent shallow copy:

```go
for cursor.Next() {
    saved = append(saved, cursor.Copy())
}
```

`Collect` automatically uses independent copies.

## Named and `IN` cursors

```go
cursor, err := squealx.NamedQueryCursor[User](ctx, db,
    "SELECT id,name FROM users WHERE tenant_id=:tenant AND id IN :ids",
    map[string]any{"tenant": tenantID, "ids": ids})

cursor, err = squealx.InQueryCursor[User](ctx, db,
    "SELECT id,name FROM users WHERE id IN (?)", ids)
```

## Performance validation

The included SQLite end-to-end benchmark decodes 1,000 rows. On the validation host, the cursor used approximately 39 KB and 3,768 allocations per query versus approximately 227 KB and 10,768 allocations for the prior callback scanner. Driver, CGO, database, schema, and hardware costs dominate absolute timings; run the benchmarks on the deployment platform:

```bash
cd drivers/sqlite && go test -run '^$' \
  -bench 'CursorStruct1000|SelectEachStruct1000' -benchmem -count=5
```

## Iterators, sinks and pagination

```go
// Range-over-func; the cursor closes when the loop ends or breaks.
for user, err := range squealx.QueryIter[User](ctx, db, "SELECT id,name FROM users") {
    if err != nil { return err }
    use(user)
}
```

- `Cursor.All()`, `TxQueryCursor`, `ConnQueryCursor`, `StmtQueryCursor` open streaming cursors from any handle.
- `SelectEach` / `ScanEach` build the scan plan once and reuse it per row.
- `WriteJSONLines`, `WriteCSV` and `WriteMapCursorJSONLines` stream rows to an `io.Writer` holding one row at a time.
- `LimitedBytes` caps a scanned column; `RawColumn` exposes one column as an `io.Reader`. `database/sql` cannot stream a single column from the wire; use driver features (e.g. pgx large objects) for values larger than memory.
- `datatypes.MaxGzipDecompressedSize` (default 256 MB, 0 = unlimited) bounds `GzippedText` decompression.
- `KeysetPaginate` gives O(1)-per-page seek pagination over a unique, non-NULL key and streams through the cursor. Prefer it to `Pages` (OFFSET) for deep pages.

## Row lifetime guarantees

Value rows (`Cursor[T]`, `SelectEach`) are safe to retain: the reused destination is cleared between rows, so pointer fields and embedded pointer structs are re-allocated per row and never alias later rows or carry stale values. `Cursor[*T]` and map rows reuse storage; call `Copy()` to retain them.

## Scanning semantics

- NULL into a field yields its zero value (`nil` for pointer fields). Unparseable values yield zero rather than an error, for struct scans.
- Fast paths avoid reflection for native `database/sql` types; anything else falls back to the lenient path.
- Map/slice scans: integers are `int64`, `DECIMAL` is an exact string (set `squealx.DecimalAsFloat = true` for `float64`), and values that fail conversion keep their original string instead of becoming zero.

## Performance notes

- `Select` into `[]T` scans rows in place into the slice's backing array; scan arguments are retargeted per row instead of rebuilt (no per-row `reflect.New`/`Append`, no per-row wrapper allocations).
- Compiled named queries (`:name` parsing) are cached (bounded, concurrency-safe); `IsNamedQuery` is allocation-free for queries without a `:name` candidate.
- With no hooks registered, `Query`/`Exec`/`Get` skip the hook pipeline and driver-name context entirely.
- `Get`/`Select` only fetch `ColumnTypes` for map destinations.
- `[]*scalar` destinations map NULL to a nil element.
- Benchmarks: `cd drivers/sqlite && go test -run '^$' -bench 'Op|Cursor|SelectEach' -benchmem` (`ops_bench_test.go` compares squealx with raw `database/sql`).
- One-shot struct scans (`Get`, `Select`) draw their scan arguments, object context and NULL-safe wrappers from a pooled scratch, so they allocate none of it; field traversals are cached per mapper (`Mapper.TraversalsByNameCached`) by type and column list.
- `SanitizeQuery`/`SafeQuery` no longer force the caller's variadic argument slice onto the heap when `EnableSafeQuery` is off.
- Remaining per-call allocations are the driver/`database/sql` ones plus the `Row`/`Rows` objects and the variadic slice passed through the `SQLDB` interface.
