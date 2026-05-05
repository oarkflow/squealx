package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/datatypes"
	"github.com/oarkflow/squealx/dbresolver"
	"github.com/oarkflow/squealx/drivers/sqlite"
	"github.com/oarkflow/squealx/hooks"
)

type User struct {
	UserID    int            `db:"user_id" json:"user_id"`
	OrgID     int            `db:"org_id" json:"org_id"`
	Username  string         `db:"username" json:"username"`
	FirstName string         `db:"first_name" json:"first_name"`
	Email     string         `db:"email" json:"email"`
	Active    bool           `db:"active" json:"active"`
	CreatedAt datatypes.Time `db:"created_at" json:"created_at"`
}

func (User) TableName() string {
	return "users"
}

type AuditLog struct {
	LogID   int    `db:"log_id" json:"log_id"`
	Message string `db:"message" json:"message"`
}

type Setting struct {
	ID      int                `db:"id"`
	Payload datatypes.JSONText `db:"payload"`
}

func main() {
	ctx := context.Background()

	db := openDB()
	defer db.Close()

	seed(db)

	coreQueryAPI(db)
	connectionAndBinding(db)
	smartSelect(db)
	scanningUtilities(db)
	namedQueries(db)
	inQueries(db)
	returningRowsFromWrites(db)
	preparedStatements(db)
	transactions(ctx, db)
	typedHelpers(db)
	genericRepository(ctx, db)
	pagination(db)
	sqlFileLoader(db)
	queryHooks()
	resourceScoping(ctx)
	resolverExample(db)
	datatypesExample(db)
	utilityHelpers(db)

	fmt.Println("README tour complete")
}

func openDB() *squealx.DB {
	db, err := sqlite.Open(":memory:", "readme-tour")
	if err != nil {
		log.Fatalln(err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db
}

func seed(db *squealx.DB) {
	db.MustExec(`
		CREATE TABLE users (
			user_id INTEGER PRIMARY KEY AUTOINCREMENT,
			org_id INTEGER NOT NULL,
			username TEXT NOT NULL,
			first_name TEXT NOT NULL,
			email TEXT NOT NULL,
			active BOOLEAN NOT NULL,
			created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
			deleted_at TEXT
		);

		CREATE TABLE audit_logs (
			log_id INTEGER PRIMARY KEY AUTOINCREMENT,
			message TEXT NOT NULL
		);

		CREATE TABLE settings (
			id INTEGER PRIMARY KEY,
			payload TEXT NOT NULL
		);
	`)

	for _, u := range []User{
		{OrgID: 7, Username: "alice", FirstName: "Alice", Email: "alice@example.com", Active: true},
		{OrgID: 7, Username: "bob", FirstName: "Bob", Email: "bob@example.com", Active: true},
		{OrgID: 8, Username: "carol", FirstName: "Carol", Email: "carol@example.com", Active: false},
		{OrgID: 8, Username: "dave", FirstName: "Dave", Email: "dave@example.com", Active: true},
	} {
		if _, err := db.NamedExec(`
			INSERT INTO users (org_id, username, first_name, email, active)
			VALUES (:org_id, :username, :first_name, :email, :active)
		`, u); err != nil {
			log.Fatalln(err)
		}
	}
}

func coreQueryAPI(db *squealx.DB) {
	var user User
	must(db.Get(&user, "SELECT * FROM users WHERE user_id = ?", 1))

	var users []User
	must(db.Select(&users, "SELECT * FROM users WHERE org_id = ?", 7))

	rows, err := db.Queryx("SELECT * FROM users ORDER BY user_id")
	must(err)
	defer rows.Close()

	var scanned int
	for rows.Next() {
		var u User
		must(rows.StructScan(&u))
		scanned++
	}
	must(rows.Err())

	result := db.MustExec("INSERT INTO audit_logs (message) VALUES (?)", "core query api")
	affected, _ := result.RowsAffected()
	fmt.Println("core", user.Username, len(users), scanned, affected)
}

func connectionAndBinding(db *squealx.DB) {
	db.SetConnMaxLifetime(time.Hour)
	db.SetConnMaxIdleTime(10 * time.Minute)

	stats := db.Stats()
	raw := db.DB()
	wrapped := squealx.OpenExist("sqlite", raw)

	dollar := squealx.Rebind(squealx.DOLLAR, "SELECT * FROM users WHERE user_id = ?")
	inSQL, inArgs, err := squealx.In("SELECT * FROM users WHERE user_id IN (?)", []int{1, 2})
	must(err)
	namedSQL, namedArgs, err := squealx.Named(
		"SELECT * FROM users WHERE org_id = :org_id",
		map[string]any{"org_id": 7},
	)
	must(err)

	fmt.Println("binding", db.DriverName(), wrapped.DriverName(), stats.OpenConnections, dollar, inSQL, len(inArgs), namedSQL, len(namedArgs))
}

func smartSelect(db *squealx.DB) {
	var activeUsers []User
	must(db.Select(&activeUsers, "SELECT * FROM users WHERE active = ?", true))

	var user User
	must(db.Select(&user, "SELECT * FROM users WHERE user_id = ?", 1))

	var orgUsers []User
	must(db.Select(&orgUsers, `
		SELECT * FROM users
		WHERE org_id = :org_id AND active = :active
	`, map[string]any{"org_id": 7, "active": true}))

	var inUser User
	must(db.Select(&inUser, "SELECT * FROM users WHERE user_id IN (?) LIMIT 1", []int{1, 2}))

	var maps []map[string]any
	must(db.Select(&maps, "SELECT user_id, username FROM users WHERE active = ?", true))

	fmt.Println("smart select", len(activeUsers), user.Username, len(orgUsers), inUser.Username, len(maps))
}

func scanningUtilities(db *squealx.DB) {
	rows, err := db.Queryx("SELECT user_id, username FROM users ORDER BY user_id LIMIT 1")
	must(err)

	if rows.Next() {
		values, err := rows.SliceScan()
		must(err)
		fmt.Println("slice scan", values)
	}
	must(rows.Close())

	var row map[string]any
	must(db.Select(&row, "SELECT user_id, username FROM users WHERE user_id = ?", 1))

	var typed []map[string]string
	must(db.Select(&typed, "SELECT username, email FROM users ORDER BY user_id LIMIT 2"))

	fmt.Println("map scan", row["username"], len(typed))
}

func namedQueries(db *squealx.DB) {
	params := map[string]any{"org_id": 7, "active": true}

	var users []User
	must(db.NamedSelect(&users, `
		SELECT * FROM users
		WHERE org_id = :org_id AND active = :active
	`, params))

	var user User
	must(db.NamedGet(&user, "SELECT * FROM users WHERE username = :username", map[string]any{"username": "alice"}))

	if _, err := db.NamedExec(
		"INSERT INTO audit_logs (message) VALUES (:message)",
		map[string]any{"message": "named exec"},
	); err != nil {
		log.Fatalln(err)
	}

	fmt.Println("named", len(users), user.Email)
}

func inQueries(db *squealx.DB) {
	ids := []int{1, 2, 4}

	var positional []User
	must(db.InSelect(&positional, "SELECT * FROM users WHERE user_id IN (?) ORDER BY user_id", ids))

	var namedNoParens []User
	must(db.NamedSelect(&namedNoParens, `
		SELECT * FROM users
		WHERE user_id IN :ids
		ORDER BY user_id
	`, map[string]any{"ids": ids}))

	var namedParens []User
	must(db.NamedSelect(&namedParens, `
		SELECT * FROM users
		WHERE user_id IN (:ids)
		ORDER BY user_id
	`, map[string]any{"ids": ids}))

	fmt.Println("in", len(positional), len(namedNoParens), len(namedParens))
}

func returningRowsFromWrites(db *squealx.DB) {
	user := &User{OrgID: 9, Username: "eve", FirstName: "Eve", Email: "eve@example.com", Active: true}
	must(db.ExecWithReturn(`
		INSERT INTO users (org_id, username, first_name, email, active)
		VALUES (:org_id, :username, :first_name, :email, :active)
	`, user))

	user.Email = "eve+updated@example.com"
	must(db.ExecWithReturn(`
		UPDATE users
		SET email = :email
		WHERE user_id = :user_id
	`, user))

	fmt.Println("returning", user.UserID, user.Email, squealx.WithReturning("UPDATE users SET email = :email WHERE user_id = :user_id"))
}

func preparedStatements(db *squealx.DB) {
	stmt, err := db.Preparex("SELECT * FROM users WHERE org_id = ? ORDER BY user_id")
	must(err)
	defer stmt.Close()

	var users []User
	must(stmt.Select(&users, 7))

	named, err := db.PrepareNamed(`
		SELECT * FROM users
		WHERE org_id = :org_id AND active = :active
		ORDER BY user_id
	`)
	must(err)
	defer named.Close()

	var namedUsers []User
	must(named.Select(&namedUsers, map[string]any{"org_id": 7, "active": true}))

	fmt.Println("prepared", len(users), len(namedUsers))
}

func transactions(ctx context.Context, db *squealx.DB) {
	must(db.With(func(tx squealx.SQLTx) error {
		_, err := tx.Exec("INSERT INTO audit_logs (message) VALUES (?)", "with transaction")
		return err
	}))

	must(db.WithTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable}, func(tx *squealx.Tx) error {
		var user User
		if err := tx.Get(&user, "SELECT * FROM users WHERE username = ?", "alice"); err != nil {
			return err
		}
		_, err := tx.NamedExec(`
			UPDATE users
			SET first_name = :first_name
			WHERE user_id = :user_id
		`, map[string]any{"user_id": user.UserID, "first_name": "Alicia"})
		return err
	}))

	tx, err := db.Beginx()
	must(err)
	defer tx.Rollback()
	_, err = tx.Exec("INSERT INTO audit_logs (message) VALUES (?)", "manual transaction")
	must(err)
	must(tx.Commit())

	fmt.Println("transactions ok")
}

func typedHelpers(db *squealx.DB) {
	user, err := squealx.SelectTyped[User](db, "SELECT * FROM users WHERE user_id = ?", 1)
	must(err)

	users, err := squealx.SelectTyped[[]User](db, "SELECT * FROM users WHERE active = ?", true)
	must(err)

	var streamed int
	must(squealx.SelectEach[User](db, func(row User) error {
		streamed++
		return nil
	}, "SELECT * FROM users ORDER BY user_id"))

	lazy, err := squealx.LazySelect[[]User](db, "SELECT * FROM users WHERE org_id = ?")(7)
	must(err)

	fmt.Println("typed", user.Username, len(users), streamed, len(lazy))
}

func genericRepository(ctx context.Context, db *squealx.DB) {
	repo := squealx.New[User](db, "users", "user_id")

	users, err := repo.Find(ctx, map[string]any{"org_id": []int{7, 8}})
	must(err)

	first, err := repo.First(ctx, map[string]any{"username": "alice"})
	must(err)

	count, err := repo.Count(ctx, map[string]any{"active": true})
	must(err)

	queryCtx := context.WithValue(ctx, "query_params", squealx.QueryParams{
		Fields: []string{"user_id", "username", "email"},
		Sort:   squealx.Sort{Field: "username", Dir: "asc"},
		Limit:  2,
		Offset: 0,
	})
	selected, err := repo.Find(queryCtx, map[string]any{"active": true})
	must(err)

	fmt.Println("repository", len(users), first.Username, count, len(selected))
}

func pagination(db *squealx.DB) {
	response := squealx.PaginateTyped[User](
		db,
		"SELECT * FROM users",
		squealx.Paging{Page: 1, Limit: 2, OrderBy: []string{"user_id asc"}},
	)
	if response.Error != nil {
		log.Fatalln(response.Error)
	}

	fmt.Println("pagination", len(response.Items), response.Pagination.TotalRecords, response.Pagination.TotalPage)
}

func sqlFileLoader(db *squealx.DB) {
	dir, err := os.MkdirTemp("", "squealx-readme-tour-*")
	must(err)
	defer os.RemoveAll(dir)

	queryFile := filepath.Join(dir, "queries.sql")
	must(os.WriteFile(queryFile, []byte(`
-- sql-name: list-users
-- doc: List users in an organization

SELECT user_id, username, email
FROM users
WHERE org_id = :org_id
ORDER BY user_id;

-- sql-end
`), 0o600))

	loader, err := squealx.LoadFromFile(queryFile)
	must(err)

	var rows []map[string]any
	must(loader.Select(db, &rows, "list-users", map[string]any{"org_id": 7}))

	fmt.Println("loader", len(loader.Queries()), loader.GetQuery("list-users").Doc, len(rows))
}

func queryHooks() {
	db := openDB()
	defer db.Close()
	seed(db)

	var before, after int
	db.UseBefore(func(ctx context.Context, query string, args ...any) (context.Context, string, []any, error) {
		before++
		return ctx, query, args, nil
	})
	db.UseAfter(func(ctx context.Context, query string, args ...any) (context.Context, string, []any, error) {
		after++
		return ctx, query, args, nil
	})

	db.Use(hooks.NewLogger(nil, true, time.Hour, func(query string, args []any, latency string) {
		fmt.Println("logger hook", latency, len(args), query != "")
	}))

	var users []User
	must(db.Select(&users, "SELECT * FROM users WHERE active = ?", true))

	fmt.Println("hooks", before, after, len(users))
}

type scopeKey string

const userIDKey scopeKey = "user_id"

func resourceScoping(ctx context.Context) {
	db := openDB()
	defer db.Close()

	db.MustExec(`
		CREATE TABLE pipelines (
			pipeline_id INTEGER PRIMARY KEY AUTOINCREMENT,
			user_id INTEGER NOT NULL,
			name TEXT NOT NULL
		);

		INSERT INTO pipelines (user_id, name)
		VALUES (1, 'Alice pipeline'), (2, 'Bob pipeline');
	`)

	scope := hooks.NewResourceScopeHook(
		hooks.ArgsFromContextValue(userIDKey),
		hooks.ScopeRule{Table: "pipelines", Column: "user_id"},
	).
		SetStrictMode(true).
		SetRejectUnknownShapes(true)
	db.Use(scope)

	scopedCtx := context.WithValue(ctx, userIDKey, 1)

	var rows []map[string]any
	must(db.SelectContext(scopedCtx, &rows, "SELECT * FROM pipelines ORDER BY pipeline_id"))

	fmt.Println("resource scope", len(rows))
}

func resolverExample(db *squealx.DB) {
	resolver := dbresolver.MustNew(
		dbresolver.WithMasterDBs(db),
		dbresolver.WithReplicaDBs(db),
		dbresolver.WithDefaultDB(db),
		dbresolver.WithReadWritePolicy(dbresolver.ReadWrite),
	)

	var users []User
	must(resolver.Select(&users, "SELECT * FROM users WHERE active = ?", true))

	_, err := resolver.Exec("UPDATE users SET active = ? WHERE username = ?", true, "carol")
	must(err)

	fmt.Println("resolver", len(users))
}

func datatypesExample(db *squealx.DB) {
	must(db.ExecWithReturn(
		"INSERT INTO settings (id, payload) VALUES (:id, :payload)",
		&Setting{ID: 1, Payload: datatypes.JSONText(`{"theme":"dark","retries":3}`)},
	))

	var setting Setting
	must(db.Get(&setting, "SELECT * FROM settings WHERE id = ?", 1))

	var payload map[string]any
	must(setting.Payload.Unmarshal(&payload))

	fmt.Println("datatypes", setting.Payload.String(), datatypes.DetectType("123"), payload["theme"])
}

func utilityHelpers(db *squealx.DB) {
	var users []User
	must(db.Select(&users, "SELECT * FROM users WHERE username LIKE ?", squealx.Contains("ali")))

	limited := squealx.LimitQuery("SELECT * FROM users")
	returning := squealx.WithReturning("UPDATE users SET active = :active WHERE user_id = :user_id")
	replaced := squealx.ReplacePlaceholders("SELECT * FROM users WHERE user_id = @user_id")
	isNamed := squealx.IsNamedQuery(replaced)
	dbName, _ := db.GetDBName()
	fields, err := db.GetTableFields("users", dbName)
	must(err)

	fmt.Println("utilities", len(users), limited, returning, replaced, isNamed, len(fields))
}

func must(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}
