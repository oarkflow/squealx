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
go test ./drivers/sqlite -run '^$' \
  -bench 'CursorStruct1000|SelectEachStruct1000' -benchmem -count=5
```
