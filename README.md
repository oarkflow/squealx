# Squealx

Squealx is a Go database toolkit built on top of `database/sql`. It keeps the familiar sqlx-style ergonomics for scanning, named parameters, bind rebinding, transactions, and prepared statements, then adds project-level features such as generic repositories, SQL file loading, query hooks, read/write database resolution, PostgreSQL JSONB query builders, encrypted JSONB shadow columns, and application-level resource scoping.

The module is intended for services that still want to write SQL directly, but need a stronger application layer around query execution, scanning, routing, and policy enforcement.

## Features

- sqlx-like `*DB`, `*Tx`, `*Stmt`, `*Rows`, and `*Row` wrappers over `database/sql`.
- Driver-aware bind placeholders for MySQL, PostgreSQL/pgx, SQLite, SQL Server, Oracle-style named binds, and dollar binds.
- Struct, map, scalar, slice, and typed generic scanning helpers.
- Named query support with `:field` placeholders from structs and maps.
- `IN` expansion for slice arguments.
- Context-aware query, exec, prepare, transaction, select, get, and named-statement methods.
- Generic repository API with CRUD, filters, sorting, field selection, joins, grouping, pagination, raw SQL, soft delete, lifecycle hooks, and relation preloading.
- Query hook pipeline for before, after, error, logging, slow-query, and notifier hooks.
- Resource scoping hook for application-level row access control.
- SQL file loader with named query blocks and templating.
- DB resolver for master/replica, read/write routing, prepared statement fan-out, default DB selection, and load balancing.
- PostgreSQL JSONB query builder with JSON path expressions, select/insert/update/remove/delete helpers, batch insert, pagination, indexes, SQL template parsing, JSON helper rewrites, decrypted reads, and encrypted shadow-column support.
- PostgreSQL monitoring query collection.
- Scanner/valuer datatypes for JSON, gzip text, binary payloads, arrays, maps, structs, `time.Time`, and nullable values.
- Configuration helpers, table-field introspection, SQL placeholder replacement, LIKE helpers, file execution, and direct scan utilities.

## Installation

```bash
go get github.com/oarkflow/squealx
```

The root module includes driver dependencies. Use one of the driver helper packages when you want Squealx to register and open a database for you:

```go
import (
    "github.com/oarkflow/squealx/drivers/postgres"
    "github.com/oarkflow/squealx/drivers/mysql"
    "github.com/oarkflow/squealx/drivers/sqlite"
    "github.com/oarkflow/squealx/drivers/mssql"
)
```

## Quick Start

```go
package main

import (
    "log"

    "github.com/oarkflow/squealx/drivers/postgres"
)

type User struct {
    UserID    int    `db:"user_id"`
    Username  string `db:"username"`
    FirstName string `db:"first_name"`
    Email     string `db:"email"`
}

func main() {
    db, err := postgres.Open(
        "host=localhost user=postgres password=postgres dbname=app sslmode=disable",
        "primary",
    )
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    var users []User
    if err := db.Select(&users, "SELECT * FROM users WHERE active = ?", true); err != nil {
        log.Fatal(err)
    }
}
```

Driver helpers call `squealx.Connect`, so they ping the database before returning. If you already have a `*sql.DB`, wrap it with `squealx.ConnectExist` or `squealx.OpenExist`.

## Supported Drivers

| Package | Driver name | Backing driver |
| --- | --- | --- |
| `drivers/postgres` | `pgx` | `github.com/jackc/pgx/v5/stdlib` |
| `drivers/mysql` | `mysql` | `github.com/go-sql-driver/mysql` |
| `drivers/sqlite` | `sqlite` | `modernc.org/sqlite` |
| `drivers/mssql` | `mssql` | `github.com/microsoft/go-mssqldb` |

The generic `squealx.Open(driverName, dsn, id)` and `squealx.Connect(driverName, dsn, id)` functions are also available.

## Configuration

`squealx.Config` can decode JSON-like configuration and build DSNs for MySQL, PostgreSQL, and SQL Server. Extra keys are moved into `Params`.

```go
cfg, err := squealx.DecodeConfig(data)
if err != nil {
    return err
}

db, driver, err := connection.FromConfig(cfg)
_ = driver
_ = db
```

## Core Query API

Squealx keeps the common sqlx-style methods on both `*squealx.DB` and `*squealx.Tx`:

```go
var user User
err := db.Get(&user, "SELECT * FROM users WHERE user_id = ?", 10)

var users []User
err = db.Select(&users, "SELECT * FROM users WHERE org_id = ?", 7)

rows, err := db.Queryx("SELECT * FROM users")
if err != nil {
    return err
}
defer rows.Close()

for rows.Next() {
    var u User
    if err := rows.StructScan(&u); err != nil {
        return err
    }
}
```

Useful helpers include:

- `Get`, `Select`, `Queryx`, `QueryRowx`, `Exec`, `MustExec`
- `GetContext`, `SelectContext`, `QueryxContext`, `QueryRowxContext`, `ExecContext`
- `Preparex`, `PreparexContext`, `PrepareNamed`, `PrepareNamedContext`
- `Beginx`, `BeginTxx`, `MustBegin`, `MustBeginTx`, `With`, `WithTx`, `Withx`, `WithTxx`
- `LazyExec`, `LazySelect`, `LazySelect[T]`, `SelectTyped[T]`, `SelectEach[T]`
- `ExecWithReturn` for insert/update/delete workflows that should populate the supplied struct or map with the returned row where supported

By default, field names map with `xstrings.ToSnakeCase` and `db` struct tags are honored. Override globally through `squealx.NameMapper` before first use, or per database with `db.MapperFunc`.

### Connection And Pool Controls

`*squealx.DB` keeps access to the underlying `database/sql` controls:

```go
db.SetMaxOpenConns(25)
db.SetMaxIdleConns(25)
db.SetConnMaxLifetime(time.Hour)
db.SetConnMaxIdleTime(10 * time.Minute)

stats := db.Stats()
driverName := db.DriverName()
raw := db.DB()
_ = stats
_ = driverName
_ = raw
```

Open and wrap helpers:

- `Open`, `MustOpen`: open without pinging.
- `Connect`, `MustConnect`, `ConnectContext`: open and ping.
- `OpenExist`, `ConnectExist`, `NewDb`: wrap an existing `*sql.DB`.
- `NewSQLDb`: wrap a custom implementation of Squealx's `SQLDB` interface.

### Binding Utilities

Squealx can be used as a standalone binder:

```go
query := squealx.Rebind(squealx.DOLLAR, "SELECT * FROM users WHERE id = ?")
// SELECT * FROM users WHERE id = $1

query, args, err := squealx.In(
    "SELECT * FROM users WHERE id IN (?) AND active = ?",
    []int{1, 2, 3},
    true,
)

query, args, err = squealx.Named(
    "SELECT * FROM users WHERE org_id = :org_id",
    map[string]any{"org_id": 7},
)
```

Bind constants are `UNKNOWN`, `QUESTION`, `DOLLAR`, `NAMED`, and `AT`. Use `BindDriver(driverName, bindType)` to register or override a driver's placeholder style, and `BindType(driverName)` to inspect it.

### Smart `db.Select`

`db.Select` is the main read convenience method. It inspects the destination and SQL, then dispatches to the right helper automatically:

- Slice destination + positional placeholders: regular multi-row `Select`.
- Single struct/map/scalar destination: `Get`-style single-row scan.
- Named placeholders such as `:org_id`: `NamedSelect` for slices or `NamedGet` for single values.
- `IN (?)` patterns with slice arguments: `InSelect` for slices or `InGet` for single values.
- Map destinations: scans rows into `map[string]any`, typed maps, slices of maps, and slices of pointer maps.

That means these all work through `db.Select`:

```go
// Plain positional query into a slice.
var users []User
err := db.Select(&users, "SELECT * FROM users WHERE active = ?", true)

// Single row into a struct. db.Select notices the destination is not a slice.
var user User
err = db.Select(&user, "SELECT * FROM users WHERE user_id = ?", 10)

// Named parameters from a map.
err = db.Select(&users, `
    SELECT * FROM users
    WHERE org_id = :org_id AND active = :active
`, map[string]any{
    "org_id": 7,
    "active": true,
})

// Named parameters into a single row.
err = db.Select(&user, `
    SELECT * FROM users
    WHERE org_id = :org_id AND user_id = :user_id
`, map[string]any{
    "org_id":  7,
    "user_id": 10,
})

// IN expansion from a slice.
err = db.Select(&users, "SELECT * FROM users WHERE user_id IN (?)", []int{1, 2, 3})

// Single row with IN expansion.
err = db.Select(&user, "SELECT * FROM users WHERE user_id IN (?) LIMIT 1", []int{10, 11})

// Dynamic result rows.
var rows []map[string]any
err = db.Select(&rows, "SELECT user_id, username FROM users WHERE active = ?", true)
```

Use the explicit methods when you want to be direct about intent:

- `db.NamedSelect`, `db.NamedGet`, `db.NamedQuery`, `db.NamedExec`
- `db.InSelect`, `db.InGet`, `db.InExec`
- `db.Get` for a positional single-row read

`db.SelectContext` is context-aware but lower-level: it calls `QueryxContext` and scans the result. For context-aware named queries, use `NamedQueryContext` or a prepared named statement; for context-aware `IN` queries, expand with `db.In(...)` and then call a context method.

### Scanning Utilities

Rows and row wrappers support several scan styles:

```go
rows, err := db.Queryx("SELECT user_id, username FROM users")
if err != nil {
    return err
}
defer rows.Close()

for rows.Next() {
    values, err := rows.SliceScan()
    _ = values
    _ = err
}

var row map[string]any
err = db.Select(&row, "SELECT user_id, username FROM users WHERE user_id = ?", 10)

var typed []map[string]string
err = db.Select(&typed, "SELECT username, email FROM users")
```

Public scan helpers include `StructScan`, `MapScan`, `SliceScan`, `ScannAll`, and generic `ScanEach[T]`. Map scanning supports `map[string]any`, typed string-key maps, `[]map[string]any`, `[]any` containing row maps, and slices of typed maps or pointer maps.

## Named Queries

Named parameters bind from maps or structs:

```go
params := map[string]any{
    "org_id": 7,
    "active": true,
}

var users []User
err := db.NamedSelect(&users, `
    SELECT * FROM users
    WHERE org_id = :org_id AND active = :active
`, params)
```

`NamedExec`, `NamedQuery`, `NamedGet`, `NamedSelect`, `PrepareNamed`, `BindNamed`, `squealx.Named`, and `squealx.NamedDollar` are available.

## `IN` Queries

Slice arguments are expanded with `In`, `InSelect`, `InGet`, and `InExec`.

```go
ids := []int{1, 2, 3}

var users []User
err := db.InSelect(&users, "SELECT * FROM users WHERE user_id IN (?)", ids)
```

The query is rebound to the active driver's placeholder style.

Named slice placeholders can be written with or without parentheses:

```go
params := map[string]any{"ids": []int{1, 2, 3}}

var users []User

err := db.NamedSelect(&users, `
    SELECT * FROM users
    WHERE user_id IN :ids
`, params)

err = db.NamedSelect(&users, `
    SELECT * FROM users
    WHERE user_id IN (:ids)
`, params)
```

## Returning Rows From Writes

`ExecWithReturn` runs an `INSERT`, `UPDATE`, or `DELETE` and writes the affected row back into the same struct or `map[string]any` used for named binding.

```go
type User struct {
    ID        int64     `db:"id"`
    Name      string    `db:"name"`
    Email     string    `db:"email"`
    CreatedAt time.Time `db:"created_at"`
}

user := &User{Name: "Alice", Email: "alice@example.com"}

err := db.ExecWithReturn(`
    INSERT INTO users (name, email)
    VALUES (:name, :email)
`, user)
if err != nil {
    return err
}

// user now contains returned columns such as ID and CreatedAt when available.
```

On PostgreSQL-compatible drivers such as `pgx`, Squealx executes the write as a single statement with `RETURNING *`. If the SQL already contains a `RETURNING` clause, `WithReturning` replaces it with `RETURNING *`; otherwise it appends `RETURNING *`.

```go
sql := squealx.WithReturning("UPDATE users SET email = :email WHERE id = :id")
// UPDATE users SET email = :email WHERE id = :id RETURNING *
```

For drivers without native `RETURNING *` support, Squealx falls back where possible:

- `INSERT`: executes the write, reads `LastInsertId`, then selects the row by primary key.
- `UPDATE`: executes the write, then selects the row by primary key from the provided arguments.
- `DELETE`: selects the row by primary key before deleting, so the deleted row can still be copied into the destination.

```go
user.Email = "alice+updated@example.com"

err = db.ExecWithReturn(`
    UPDATE users
    SET email = :email
    WHERE id = :id
`, user)
if err != nil {
    return err
}
```

`ExecWithReturn` requires a pointer to a struct or map because it both reads bind values and writes returned values. The fallback path depends on table metadata and a discoverable primary key, so native `RETURNING *` drivers are the most reliable option for complex write statements.

## Prepared Statements

Prepared statements keep the same scanning conveniences:

```go
stmt, err := db.Preparex("SELECT * FROM users WHERE org_id = ?")
if err != nil {
    return err
}
defer stmt.Close()

var users []User
err = stmt.Select(&users, 7)

named, err := db.PrepareNamed(`
    SELECT * FROM users
    WHERE org_id = :org_id AND active = :active
`)
if err != nil {
    return err
}
defer named.Close()

err = named.Select(&users, map[string]any{"org_id": 7, "active": true})
```

`NamedStmt` and `Stmt` also provide context methods such as `SelectContext`, `GetContext`, `QueryxContext`, `QueryRowxContext`, `ExecContext`, and `MustExecContext`.

## Transactions

Squealx exposes both manual transaction methods and callback helpers. The callback helpers commit when the function returns `nil` and roll back when the function returns an error.

```go
err := db.With(func(tx squealx.SQLTx) error {
    if _, err := tx.Exec("UPDATE accounts SET balance = balance - ? WHERE id = ?", 100, 1); err != nil {
        return err
    }
    if _, err := tx.Exec("UPDATE accounts SET balance = balance + ? WHERE id = ?", 100, 2); err != nil {
        return err
    }
    return nil
})
```

Use the helper that matches the transaction shape you need:

- `With(func(tx SQLTx) error)`: starts a transaction with `context.Background()` and passes the portable `SQLTx` interface. This is useful for code that should work with either standard or wrapped transaction implementations.
- `WithTx(ctx, opts, func(tx SQLTx) error)`: like `With`, but accepts a context and optional `*sql.TxOptions` for isolation level or read-only transactions.
- `Withx(func(tx *squealx.Tx) error)`: starts a transaction and passes the Squealx transaction wrapper, so code can use helpers such as `Get`, `Select`, `NamedExec`, `Queryx`, hooks, mapper settings, and driver-aware rebinding.
- `WithTxx(ctx, opts, func(tx *squealx.Tx) error)`: context-aware `Withx` with optional `*sql.TxOptions`.

```go
err := db.WithTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable}, func(tx *squealx.Tx) error {
    var order Order
    if err := tx.Get(&order, "SELECT * FROM orders WHERE order_id = ? FOR UPDATE", orderID); err != nil {
        return err
    }

    _, err := tx.NamedExec(`
        UPDATE orders
        SET status = :status
        WHERE order_id = :order_id
    `, map[string]any{
        "order_id": order.OrderID,
        "status":   "paid",
    })
    return err
})
```

Manual transaction control is also available when application flow needs explicit commit or rollback:

```go
tx, err := db.Beginx()
if err != nil {
    return err
}
defer tx.Rollback()

if _, err := tx.Exec("INSERT INTO audit_logs (message) VALUES (?)", "started"); err != nil {
    return err
}

return tx.Commit()
```

Manual helpers include `Begin`, `BeginTx`, `Beginx`, `BeginTxx`, `MustBegin`, and `MustBeginTx`. `Begin` and `BeginTx` return standard transaction interfaces, while `Beginx` and `BeginTxx` return `*squealx.Tx`.

## Typed Helpers

```go
user, err := squealx.SelectTyped[User](
    db,
    "SELECT * FROM users WHERE user_id = ?",
    10,
)

users, err := squealx.SelectTyped[[]User](
    db,
    "SELECT * FROM users WHERE active = ?",
    true,
)

err = squealx.SelectEach[User](db, func(row User) error {
    // stream row-by-row
    return nil
}, "SELECT * FROM users")
```

For non-slice `SelectTyped[T]`, Squealx appends or replaces `LIMIT 1`.

## Generic Repository

The repository layer provides CRUD and filtering on top of `*squealx.DB`.

```go
type Author struct {
    AID      int    `db:"a_id" json:"a_id"`
    AuthID   int    `db:"auth_id" json:"auth_id"`
    Name     string `db:"name" json:"name"`
    Lastname string `db:"lastname" json:"lastname"`
}

func (Author) TableName() string {
    return "authors"
}

repo := squealx.New[Author](db, "authors", "a_id")

authors, err := repo.Find(ctx, map[string]any{
    "auth_id": []int{1, 2, 3},
})

first, err := repo.First(ctx, map[string]any{"a_id": 1})
count, err := repo.Count(ctx, map[string]any{"lastname": "Doe"})
```

Repository methods include `Find`, `All`, `First`, `Count`, `Create`, `Update`, `Delete`, `SoftDelete`, `Raw`, `RawExec`, `Paginate`, and `PaginateRaw`.

### Query Parameters

Repository queries can read `squealx.QueryParams` from context. Prefer `squealx.WithQueryParams` over raw context keys:

```go
ctx = squealx.WithQueryParams(ctx, squealx.QueryParams{
    Fields: []string{"a_id", "name"},
    Sort:   squealx.Sort{Field: "name", Dir: "asc"},
    Limit:  20,
    Offset: 0,
    AllowedFields: map[string]string{
        "a_id": "a_id",
        "name": "name",
    },
})
```

`QueryParams` supports selected fields, excluded fields, joins, group by, having, sorting, limit, and offset. Identifier values are validated. `Join`, `Having`, and repository raw SQL are allowlist-key based by default:

```go
ctx = squealx.WithQueryParams(ctx, squealx.QueryParams{
    Fields: []string{"author", "book_count"},
    Join:   []string{"books"},
    Having: "has_books",
    Sort:   squealx.Sort{Field: "author", Dir: "asc"},
    AllowedFields: map[string]string{
        "author":     "authors.name AS author",
        "book_count": "COUNT(books.id) AS book_count",
    },
    AllowedJoins: map[string]string{
        "books": "LEFT JOIN books ON books.author_id = authors.a_id",
    },
    AllowedHaving: map[string]string{
        "has_books": "COUNT(books.id) > 0",
    },
    AllowedRaw: map[string]string{
        "active_authors": "SELECT * FROM authors WHERE active = :active",
    },
})

authors, err := repo.Raw(ctx, "active_authors", map[string]any{"active": true})
```

For trusted server-side constants only, `AllowUnsafeRawSQL: true` preserves the older raw `Join`, `Having`, and `Raw` behavior. Do not enable it for request-derived values.

### Lifecycle Hooks

Entities can implement lifecycle hooks:

```go
func (a *Author) BeforeCreate(db *squealx.DB) error { return nil }
func (a *Author) AfterCreate(db *squealx.DB) error  { return nil }
func (a *Author) BeforeUpdate(db *squealx.DB) error { return nil }
func (a *Author) AfterUpdate(db *squealx.DB) error  { return nil }
func (a *Author) BeforeDelete(db *squealx.DB) error { return nil }
func (a *Author) AfterDelete(db *squealx.DB) error  { return nil }
```

### Trusted Expressions

Raw condition/update expressions must be marked as trusted application SQL. The legacy `squealx.ExprPrefix` string escape hatch is rejected by repository builders.

```go
err := repo.Update(ctx, map[string]any{
    "updated_at": squealx.Expr("CURRENT_TIMESTAMP"),
}, map[string]any{"a_id": 1})

rows, err := repo.Find(ctx, map[string]any{
    "not_deleted": squealx.Expr("deleted_at IS NULL"),
})
```

### Relation Preloading

Relations can be preloaded directly, through join tables, nested with dot notation, and filtered.

`Relation.With` names the related table for a direct preload, or a nested path for a nested preload. For example, `With: "books"` attaches related rows under the `books` key, while `With: "books.comments"` loads comments into each preloaded book.

```go
repo := squealx.New[map[string]any](db, "authors", "a_id")

books := squealx.Relation{
    With:                 "books",
    LocalField:           "a_id",
    RelatedField:         "b_id",
    JoinTable:            "author_books",
    JoinWithLocalField:   "auth_id",
    JoinWithRelatedField: "book_id",
}

comments := squealx.Relation{
    With:         "books.comments",
    LocalField:   "b_id",
    RelatedField: "book_id",
}

authors, err := repo.
    Preload(books, map[string]any{"book_name": "Go Programming"}).
    Preload(comments, map[string]any{"comment_text": "Great book!"}).
    Find(ctx, map[string]any{"a_id": []int{1}})
```

Relation fields:

- `With`: related table name or nested preload path, such as `books` or `books.comments`.
- `LocalField`: field on the current record used to collect lookup keys. For nested preloads, this field belongs to the already-loaded parent relation.
- `RelatedField`: field on the related table that matches the collected keys.
- `JoinTable`: optional intermediate table for many-to-many relations.
- `JoinWithLocalField`: field on the join table that points back to the current table.
- `JoinWithRelatedField`: field on the join table that points to the related table.
- `Filters`: optional relation-specific conditions. Passing a `map[string]any` as the second `Preload` argument sets this field for that preload call.

Direct one-to-many preload:

```go
orders := squealx.Relation{
    With:         "orders",
    LocalField:   "user_id",
    RelatedField: "user_id",
}

users, err := userRepo.Preload(orders).Find(ctx, map[string]any{"active": true})
```

Many-to-many preload through a join table:

```go
roles := squealx.Relation{
    With:                 "roles",
    LocalField:           "user_id",
    RelatedField:         "role_id",
    JoinTable:            "user_roles",
    JoinWithLocalField:   "user_id",
    JoinWithRelatedField: "role_id",
}

users, err := userRepo.Preload(roles).Find(ctx, map[string]any{"user_id": []int{1, 2}})
```

Nested preloads should be chained after the parent relation they depend on:

```go
lineItems := squealx.Relation{
    With:         "orders.line_items",
    LocalField:   "order_id",
    RelatedField: "order_id",
}

users, err := userRepo.
    Preload(orders).
    Preload(lineItems, map[string]any{"status": "ready"}).
    Find(ctx, map[string]any{"user_id": []int{1, 2}})
```

Preloaded rows are attached to map results using lower-case relation keys, such as `books` or `orders`. For struct repositories, Squealx converts the enriched row back into `T`, so define matching relation fields with compatible `db` or `json` tags when you want preloaded data on the struct.

## Pagination

```go
response := squealx.PaginateTyped[map[string]any](
    db,
    "SELECT * FROM charge_master",
    squealx.Paging{Page: 1, Limit: 20, Sort: "charge_master_id", Dir: "desc"},
)

items := response.Items
page := response.Pagination
_ = items
_ = page
```

Pagination returns item data plus metadata such as total count, page, limit, and page count.

Pagination helpers include:

- `Pages` for computing pagination metadata from a total and a result set.
- `Paginate` for scanning into a provided destination.
- `PaginateTyped[T]` for typed item responses.
- Repository `Paginate` and `PaginateRaw`.
- JSONB query `Paginate`, `PaginateResponse`, and typed JSONB pagination helpers.

`squealx.Paging` supports `Page`, `Limit`, `Sort`, and `Dir`.

## SQL File Loader

`LoadFromFile` and `LoadFromDir` read query blocks from SQL files.

```sql
-- sql-name: list-cpt
-- doc: Get CPT codes
-- connection: primary

SELECT charge_master_id, work_item_id, cpt_hcpcs_code
FROM charge_master
WHERE work_item_id = @work_item_id
LIMIT {{if work_item_id == 33}} 1 {{else}} 10 {{end}};

-- sql-end
```

Use the query by name:

```go
loader, err := squealx.LoadFromFile("queries.sql")
if err != nil {
    return err
}

var rows []map[string]any
err = loader.Select(db, &rows, "list-cpt", map[string]any{
    "work_item_id": 33,
})
```

The loader exposes the same style of query, exec, named, `IN`, prepare, and connection methods, resolving the SQL by query name before execution.

Loader query blocks store metadata:

- `-- sql-name:` unique query name.
- `-- doc:` query documentation text.
- `-- connection:` optional connection key used by resolver workflows.
- SQL body between `-- sql-name:` and `-- sql-end`.

The loader also exposes `Queries()` and `GetQuery(name)` so applications can introspect loaded SQL definitions.

## Query Hooks

Hooks can rewrite queries, append arguments, audit execution, or transform errors. `DB` and `Tx` execution paths propagate hooks.

```go
db.UseBefore(func(ctx context.Context, query string, args ...any) (context.Context, string, []any, error) {
    // inspect or rewrite query before execution
    return ctx, query, args, nil
})

db.UseAfter(func(ctx context.Context, query string, args ...any) (context.Context, string, []any, error) {
    // audit successful execution
    return ctx, query, args, nil
})

db.UseOnError(func(ctx context.Context, err error, query string, args ...any) error {
    // replace or enrich the error
    return err
})
```

Objects implementing `BeforeHook`, `AfterHook`, or `ErrorerHook` can be installed with `db.Use(...)`.

### Logger Hook

The `hooks` package includes a ready-made logger hook:

```go
loggerHook := hooks.NewLogger(logger, true, 250*time.Millisecond, func(query string, args []any, latency string) {
    // optional notification sink for query logs, slow queries, or errors
})

db.Use(loggerHook)
```

When `logSlowQuery` is true, only queries slower than the configured duration are logged as slow queries. When false, successful queries are logged normally. Errors are logged through `OnError`.

## Resource Scoping

The `hooks` package includes an application-level row access control hook. It rewrites supported SQL statements by injecting predicates for configured tables.

```go
type scopeKey string

const userIDKey scopeKey = "user_id"

scope := hooks.NewResourceScopeHook(
    hooks.ArgsFromContextValue(userIDKey),
    hooks.ScopeRule{Table: "pipelines", Column: "user_id"},
    hooks.ScopeRule{
        Table:     "entries",
        Predicate: "{{alias}}.pipeline_id IN (SELECT p.pipeline_id FROM pipelines p WHERE p.user_id = {{param}})",
    },
).
    SetStrictMode(true).
    SetRejectUnknownShapes(true).
    SetCompatibilityMode(false).
    SetAllowTrustedBypass(true).
    SetRequireBypassToken(true).
    SetAuditSink(func(ctx context.Context, d hooks.ScopeDecision) {
        // send d.Action, d.ReasonCode, d.MatchedTables, d.AppliedRules, etc. to telemetry
    })

db.Use(scope)

ctx := context.WithValue(context.Background(), userIDKey, 123)

var rows []Pipeline
err := db.SelectContext(ctx, &rows, "SELECT * FROM pipelines ORDER BY pipeline_id")
```

Supported statement families include `SELECT`, `UPDATE`, `DELETE`, `INSERT ... SELECT`, `MERGE`, `WITH` main statements, multi-statement SQL strings, joins, set operators, and nested subqueries. `INSERT ... VALUES` is not rewritten and can be rejected when `SetRejectUnknownShapes(true)` is enabled.

Important resource-scoping knobs:

- `SetStrictMode(true)` fails closed on missing context and invalid rule resolution.
- `SetRejectUnknownShapes(true)` rejects unrecognized query shapes.
- `SetStrictAllTables(true)` requires every discovered top-level table to have a rule.
- `SetCompatibilityMode(true)` allows low-confidence passthroughs during rollout.
- `SetPassthroughBudget(threshold, minSamples)` gates compatibility-mode passthroughs.
- `SetAuditSink` and `SetBudgetSink` expose policy decisions.
- `WithTrustedScopeBypass(ctx, reason)` allows explicit bypass when enabled and paired with the configured bypass token.

See [RESOURCE_SCOPING.md](RESOURCE_SCOPING.md) and [RESOURCE_SCOPING_TEST_CASES.md](RESOURCE_SCOPING_TEST_CASES.md) for the full security model, predicate template rules, rollout guidance, and coverage cases.

## DB Resolver

The `dbresolver` package routes reads and writes across registered databases.

```go
resolver := dbresolver.MustNew(
    dbresolver.WithMasterDBs(primary),
    dbresolver.WithReplicaDBs(replica1, replica2),
    dbresolver.WithDefaultDB(primary),
    dbresolver.WithReadWritePolicy(dbresolver.ReadWrite),
)

var users []User
err := resolver.Select(&users, "SELECT * FROM users")

_, err = resolver.Exec("UPDATE users SET active = ? WHERE user_id = ?", false, 10)
```

Resolvers support master, replica, read-only, write-only, default DB selection, load balancing, query file loading, hooks, prepared statements, transactions, and most `*squealx.DB` query helpers.

Resolver features include:

- `WithMasterDBs`, `WithReplicaDBs`, `WithDefaultDB`, `WithReadWritePolicy`, `WithLoadBalancer`, and `WithFileLoader`.
- Read/write policies: `ReadWrite`, `ReadOnly`, and `WriteOnly`.
- Load balancers: `NewRandomLoadBalancer` and `NewRoundRobinLoadBalancer`, plus custom `LoadBalancer` implementations.
- Runtime registration with `Register`, `RegisterMaster`, `RegisterReplica`, and `RegisterRead`.
- Direct DB selection with `Use(id)`, `UseDefault()`, `SetDefaultDB(id)`, and `GetDB(ctx, ids)`.
- Fan-out prepared statements across registered DBs through resolver `Prepare`, `Preparex`, and `PrepareNamed`.
- Pool controls, stats, ping, close, connection, transaction, lazy query, and hook methods.

## PostgreSQL JSONB Query Builder

The `jsonbq` package wraps `*squealx.DB` for tables that store documents in a JSONB column.

```go
db := jsonbq.MustOpen(
    "postgres://postgres:postgres@localhost/app?sslmode=disable",
    "data",
    "jsonb",
)
defer db.Close()

type AthleteData struct {
    Name   string         `json:"name"`
    Sport  string         `json:"sport"`
    Age    int            `json:"age"`
    Stats  map[string]any `json:"stats"`
    Active bool           `json:"active"`
}

var id int
err := db.Insert("athletes").
    Data(AthleteData{Name: "LeBron James", Sport: "Basketball", Age: 39, Active: true}).
    Returning("id").
    Get(&id)

var athletes []struct {
    ID   int    `db:"id"`
    Data string `db:"data"`
}

err = db.Query().
    Select("id", "data").
    From("athletes").
    Where(
        jsonbq.At("sport").Eq("Basketball"),
        jsonbq.At("age").Int().Gt(30),
        jsonbq.At("active").Bool().Eq(true),
    ).
    OrderByDesc("data->>'name'").
    Limit(10).
    Exec(&athletes)
```

JSONB features include:

- `At("path", "to", "field")` selectors with text, JSON, int, numeric, and boolean casts.
- `Field`, `FieldJSON`, `Path`, `PathJSON`, `Raw`, `Col`, `RawColumn`, `Value`, `ValueAt`, `Bool`, `BoolAt`, `Int`, `IntAt`, `Numeric`, and `NumericAt` expression helpers.
- Comparisons: `Eq`, `NotEq`, `Gt`, `Gte`, `Lt`, `Lte`, expression-to-expression comparisons, `Like`, `ILike`, `Regex`, `In`, `NotIn`, `IsNull`, and `IsNotNull`.
- JSONB operators: `Contains`, `ContainedBy`, `HasKey`, `HasAnyKey`, `HasAllKeys`, `JSONPath`, `Exists`, and `NotExists`.
- Expression helpers: `As`, `Cast`, `Asc`, `Desc`, `TextAt`, `JSONAt`, `PathText`, `PathJSON`, `Val`, `Coalesce`, `Count`, `CountAll`, `Sum`, and `CaseWhen`.
- Logical `And`, `Or`, and `Not` conditions.
- Structured joins, custom joins, distinct, grouping, having, order, limit, offset, and page helpers.
- Select execution helpers: `Build`, `Exec`, `Get`, `Query`, `QueryRow`, `Count`, `Exists`, `Paginate`, `PaginateResponse`, and typed paginated responses.
- Insert, batch insert, update, remove, and delete builders.
- Raw SQL template parsing with `:name` and `{{kind:name}}` placeholders.
- Rewrites for JSON helper functions, JSON dot notation, implicit numeric casts, and encrypted search predicates.
- Transaction-aware builders from `jsonbq.Tx`.

### JSONB Updates, Removes, Deletes

```go
_, err := db.Update("athletes").
    Set(jsonbq.Set("stats", "ppg").To(28.4)).
    Where(jsonbq.At("name").Eq("LeBron James")).
    Exec()

_, err = db.Remove("athletes").
    Fields(jsonbq.RemoveKey("temporary"), jsonbq.RemovePath("stats", "old_rank")).
    Where(jsonbq.At("active").Eq(true)).
    Exec()

_, err = db.Delete("athletes").
    Where(jsonbq.At("active").Eq(false)).
    Exec()
```

### JSONB Indexes

```go
err := db.DefaultJSONBIndexesFor("athletes",
    "sport",
    "active:bool",
    "age:int",
    "stats.ppg:numeric",
)

err = db.AddIndex("athletes",
    jsonbq.Index("idx_athletes_lower_name", jsonbq.Raw("lower(data->>'name')")).
        Using("btree").
        Partial("data ? 'name'"),
)
```

Index helpers include `Index`, `ExprIndex`, `Using`, `UniqueIndex`, `Partial`, `WithoutIfNotExists`, `AddIndex`, `AddIndexes`, `DefaultJSONBIndexes`, and `DefaultJSONBIndexesFor`.

### SQL Template Parsing

`jsonbq.ParseNormalSQL` accepts normal SQL with named placeholders and JSON conveniences, then returns PostgreSQL-ready SQL and args:

```go
sqlText, args, err := db.ParseNormalSQL(`
    SELECT *
    FROM athletes
    WHERE data.sport = :sport
      AND data.stats.ppg > :min_ppg
      AND {{ident:sort_col}} IS NOT NULL
`, map[string]any{
    "sport":    "Basketball",
    "min_ppg":  20,
    "sort_col": "created_at",
})
```

Template placeholder kinds:

- `:name`, `{{param:name}}`, and `{{arg:name}}` become bind parameters.
- `{{ident:name}}` quotes a SQL identifier.
- `{{literal:name}}`, `{{string:name}}`, and `{{key:name}}` quote a SQL string literal.
- `{{raw:name}}` injects raw SQL and should only be used with trusted input.

`RawExecTemplate`, `RawGetTemplate`, and `RawSelectTemplate` parse and execute templates in one call.

## Encrypted JSONB Mode

`jsonbq` can dual-write JSONB data into encrypted shadow columns and an HMAC column. It can also encrypt selected fields inside the JSON payload and create searchable blind indexes for selected paths.

```go
db.EnableEncryptedMode("encryption-key", "hmac-key").
    EnableEncryptedIntegrityAutoRepair().
    EncryptFields("email:search", "profile.ssn")

if err := db.MigrateEncryptedMode("athletes"); err != nil {
    return err
}

health, err := db.CheckEncryptedHealth("athletes")
_ = health
```

Encrypted mode can:

- Add and backfill encrypted shadow columns through `MigrateEncryptedMode`.
- Check and repair HMAC integrity with `CheckEncryptedHealth` and `RepairEncryptedData`.
- Use strict integrity mode to block writes on mismatch.
- Use auto-repair mode to repair before writes.
- Create encrypted field indexes for searchable fields.
- Decrypt selected paths during reads with `DecryptSelect` and `DecryptSelectTyped`.

This feature is PostgreSQL-oriented and uses `pgcrypto`.

## PostgreSQL Monitoring

```go
stats, err := monitor.GetPostgresStats(db)
if err != nil {
    return err
}

longRunning := stats["long_running_queries"]
_ = longRunning
```

Collected groups include long-running queries, lock status, tuple information, index usage, cache ratios, disk usage, relation sizes, database size, and table/index bloat.

## Datatypes

The `datatypes` package provides `sql.Scanner` and `driver.Valuer` helpers:

- `Any`
- `Array[T]`
- `JSONText`
- `NullJSONText`
- `GzippedText`
- `Binary[T]` and `NullBinary[T]`
- `Time` and `NullTime`
- `Map[K,V]`
- `Struct[T]` and `NullStruct[T]`
- `BitBool`
- `DetectType` for simple string type inference

These are useful for fields that need transparent serialization or custom scan behavior.

## Utility Helpers

Root-level helpers include:

- `Contains`, `StartsWith`, and `EndsWith` for LIKE patterns.
- `Sum` for simple aggregate expression generation.
- `IsNamedQuery` to detect named placeholders while ignoring casts, comments, and literals.
- `LimitQuery` and `WithReturning` for appending/replacing `LIMIT 1` and `RETURNING *`.
- `ReplacePlaceholders` to convert safe `@name` placeholders to `:name` while skipping strings and comments.
- `SafeQuery`, `SanitizeQuery`, `RemoveSQLComments`, and `CanError`.
- `ParseDBName` and `db.GetDBName`.
- `db.GetTableFields(table, dbName)` for PostgreSQL, MySQL, and SQLite column metadata.
- `LoadFile` and `LoadFileContext` for executing SQL files.

## SQL Safety Notes

Squealx is designed around bound parameters, named parameters, and driver-aware rebinding. Keep user values in arguments rather than interpolating strings into SQL.

`SanitizeQuery` is called internally by Squealx query helpers. It converts safe `@name` placeholders to `:name`, renders `{{ ... }}` templates when template data is supplied, and then calls `SafeQuery`.

`SafeQuery` is an optional regex-based tripwire. It is disabled by default because it can reject valid administrative SQL such as DDL. Enable it only when you want this extra heuristic check on top of bound parameters:

```go
squealx.EnableSafeQuery = true

if err := squealx.SafeQuery(
    "SELECT * FROM users WHERE email = :email",
    map[string]any{"email": "ada@example.test"},
); err != nil {
    return err
}

query, err := squealx.SanitizeQuery(
    "SELECT * FROM users WHERE email = @email AND active = :active",
    map[string]any{
        "email":  "ada@example.test",
        "active": true,
    },
)
if err != nil {
    return err
}
// query == "SELECT * FROM users WHERE email = :email AND active = :active"
```

Template rendering is supported for trusted application templates. Keep user values in bind args, not in the template text:

```go
query, err := squealx.SanitizeQuery(
    "SELECT * FROM {{ .Table }} WHERE id = :id",
    map[string]any{"Table": "authors", "id": 1},
)
```

Treat `SafeQuery` as a helper, not a replacement for parameterized queries, repository allowlists, resource scoping, database permissions, and PostgreSQL RLS where appropriate.

Resource scoping is application-level access control. For high-security workloads, combine it with database-native controls such as PostgreSQL RLS, restricted service credentials, views, and stored procedures.

## Examples

The `examples/` directory includes small programs for:

- A SQLite README tour covering the common examples in this document: `go run examples/readme_tour/main.go`.
- A complete SQLite repository example with CRUD, hooks, allowlisted query parameters, preloads, raw SQL, pagination, soft delete, and trusted expressions: `go run examples/repository_complete/main.go`.
- Basic PostgreSQL querying.
- SQLite with resource scoping.
- Generic repositories and relation preloading.
- SQL file loading.
- Pagination.
- Named `IN :ids` and `IN (:ids)` placeholders.
- `ExecWithReturn` write-and-return workflows.
- PostgreSQL JSONB querying, indexing, and encrypted mode.
- Query hooks.
- Database resolver workflows.
- Monitoring queries.

The README tour and named `IN` examples use in-memory SQLite and run without a local database server. Most PostgreSQL, MySQL, and SQL Server examples expect local databases and DSNs to be adjusted before running.

## Testing

Run the test suite with:

```bash
go test ./...
```

Some examples are standalone programs and may require PostgreSQL, MySQL, SQLite, or SQL Server instances depending on the file.

## Project Layout

```text
.
├── sqlx.go, sqlx_context.go     # core DB/Tx/Stmt/Rows wrappers and context APIs
├── named.go, bind.go            # named binding, bind rebinding, IN expansion
├── repository.go                # generic repository implementation
├── paging.go                    # pagination helpers
├── file_loader.go               # SQL file loader
├── hook.go                      # query hook interfaces
├── hooks/                       # resource scoping hook
├── dbresolver/                  # read/write and master/replica routing
├── jsonbq/                      # PostgreSQL JSONB builders, parser, encryption
├── datatypes/                   # scanner/valuer datatypes
├── drivers/                     # driver-specific open helpers
├── monitor/                     # PostgreSQL monitoring queries
├── connection/                  # Config-to-driver connection helper
└── examples/                    # usage examples
```

## License

No license file is currently present in this repository. Add one before publishing or distributing the module.
