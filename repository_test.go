package squealx

import (
	"context"
	"math"
	"strings"
	"testing"
)

type repositoryIgnoredFields struct {
	ID     int    `db:"id"`
	Name   string `db:"name"`
	Secret string `db:"-"`
	hidden string
}

type repositoryEntity struct {
	IDValue int `db:"id"`
	Name    string
}

func (repositoryEntity) TableName() string  { return "repository_entities" }
func (repositoryEntity) PrimaryKey() string { return "id" }
func (e repositoryEntity) ID() string       { return e.Name }

func TestRelationKeyNormalizesComparableValues(t *testing.T) {
	if relationKey(1) != relationKey(int64(1)) || relationKey(float64(1)) != relationKey(uint(1)) {
		t.Fatal("equivalent numeric keys did not normalize to the same value")
	}
	if relationKey([]byte("key")) != relationKey("key") {
		t.Fatal("binary and string keys did not normalize to the same value")
	}
	if key := relationKey([]int{1, 2}); key != "[]int:[1 2]" {
		t.Fatalf("non-comparable fallback key = %#v", key)
	}
	if relationKey(math.NaN()) != relationKey(math.NaN()) {
		t.Fatal("NaN relation keys no longer match the formatted-key behavior")
	}
}

func TestBuildWhereClauseDoesNotMutateCondition(t *testing.T) {
	cond := map[string]any{
		"id":         10,
		"deleted_at": nil,
		"email":      NotNull,
	}

	where, params, err := buildWhereClause(cond)
	if err != nil {
		t.Fatalf("build where clause: %v", err)
	}

	if !strings.Contains(where, "id = :id") {
		t.Fatalf("expected id predicate in %q", where)
	}
	if !strings.Contains(where, "deleted_at IS NULL") {
		t.Fatalf("expected null predicate in %q", where)
	}
	if !strings.Contains(where, "email IS NOT NULL") {
		t.Fatalf("expected not-null predicate in %q", where)
	}
	if _, ok := cond["deleted_at"]; !ok {
		t.Fatalf("condition map was mutated: %#v", cond)
	}
	if _, ok := cond["email"]; !ok {
		t.Fatalf("condition map was mutated: %#v", cond)
	}
	if len(params) != 1 || params["id"] != 10 {
		t.Fatalf("unexpected params: %#v", params)
	}
}

func TestRepositoryBuildersGenerateStableSQL(t *testing.T) {
	repo := &repository[map[string]any]{table: "users", primaryKey: "id"}
	insertData := map[string]any{"zeta": 2, "alpha": 1}
	updateData := map[string]any{"id": 7, "zeta": 2, "alpha": 1}
	condition := map[string]any{"zeta": 3, "alpha": 4}
	var insertSQL, updateSQL, whereSQL string
	for i := 0; i < 100; i++ {
		query, _, err := repo.buildInsertQuery(insertData, QueryParams{})
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			insertSQL = query
		} else if query != insertSQL {
			t.Fatalf("insert SQL changed: %q != %q", query, insertSQL)
		}

		query, _, err = repo.buildUpdateQuery(updateData, condition, QueryParams{})
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			updateSQL = query
		} else if query != updateSQL {
			t.Fatalf("update SQL changed: %q != %q", query, updateSQL)
		}

		where, _, err := buildWhereClause(condition)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			whereSQL = where
		} else if where != whereSQL {
			t.Fatalf("where SQL changed: %q != %q", where, whereSQL)
		}
	}
	if insertSQL != "INSERT INTO users (alpha, zeta) VALUES (:alpha, :zeta)" {
		t.Fatalf("insert SQL = %q", insertSQL)
	}
	if whereSQL != "alpha = :alpha AND zeta = :zeta" {
		t.Fatalf("where SQL = %q", whereSQL)
	}
}

type bulkInsertRow struct {
	ID   int    `db:"id"`
	Name string `db:"name"`
	Age  int    `db:"age"`
}

func TestBuildBulkInsertQueryGeneratesMultiRowValues(t *testing.T) {
	repo := &repository[bulkInsertRow]{table: "users", primaryKey: "id"}
	rows := []any{
		bulkInsertRow{ID: 1, Name: "Alice", Age: 30},
		bulkInsertRow{ID: 2, Name: "Bob", Age: 0}, // zero-valued Age must still be included
	}

	query, args, err := repo.buildBulkInsertQuery(rows, QueryParams{})
	if err != nil {
		t.Fatal(err)
	}
	const want = "INSERT INTO users (age, id, name) VALUES (:age_0, :id_0, :name_0), (:age_1, :id_1, :name_1)"
	if query != want {
		t.Fatalf("query = %q, want %q", query, want)
	}
	if args["age_0"] != 30 || args["id_0"] != 1 || args["name_0"] != "Alice" {
		t.Fatalf("row 0 args = %#v", args)
	}
	if args["age_1"] != 0 || args["id_1"] != 2 || args["name_1"] != "Bob" {
		t.Fatalf("row 1 args (including zero-valued Age) = %#v", args)
	}
}

func TestBuildBulkInsertQueryRejectsMismatchedColumns(t *testing.T) {
	repo := &repository[map[string]any]{table: "users", primaryKey: "id"}
	rows := []any{
		map[string]any{"id": 1, "name": "Alice"},
		map[string]any{"id": 2}, // missing "name"
	}
	_, _, err := repo.buildBulkInsertQuery(rows, QueryParams{})
	if err == nil {
		t.Fatal("expected mismatched-columns error")
	}
}

func TestBuildBulkInsertQueryRejectsEmptyRows(t *testing.T) {
	repo := &repository[map[string]any]{table: "users", primaryKey: "id"}
	_, _, err := repo.buildBulkInsertQuery(nil, QueryParams{})
	if err == nil {
		t.Fatal("expected empty-rows error")
	}
}

func TestCreateManyChunksAndExecutesOnePerBatch(t *testing.T) {
	drv, name := registerStmtCacheTestDriver(t)
	db, err := Open(name, "", "create-many")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	repo := New[bulkInsertRow](db, "users", "id")
	rows := make([]any, 5)
	for i := range rows {
		rows[i] = bulkInsertRow{ID: i, Name: "n", Age: i}
	}

	if err := repo.CreateMany(context.Background(), rows, 2); err != nil {
		t.Fatal(err)
	}
	// 5 rows in chunks of 2 -> 3 INSERT statements (2, 2, 1), each prepared once.
	if got := drv.prepares.Load(); got != 3 {
		t.Fatalf("prepares = %d, want 3", got)
	}
}

func TestCreateManyEmptyIsNoop(t *testing.T) {
	repo := New[bulkInsertRow](nil, "users", "id")
	if err := repo.CreateMany(context.Background(), nil, 0); err != nil {
		t.Fatalf("expected no-op, got %v", err)
	}
}

func TestBuildWhereClauseRejectsEmptyInSlice(t *testing.T) {
	_, _, err := buildWhereClause(map[string]any{"id": []int{}})
	if err == nil {
		t.Fatal("expected empty IN slice to fail")
	}
}

func TestDirtyFieldsSkipsIgnoredAndUnexportedFields(t *testing.T) {
	fields, err := DirtyFields(repositoryIgnoredFields{
		ID:     1,
		Name:   "Alice",
		Secret: "token",
		hidden: "private",
	})
	if err != nil {
		t.Fatalf("dirty fields: %v", err)
	}
	if _, ok := fields["-"]; ok {
		t.Fatalf("db:\"-\" field should be skipped: %#v", fields)
	}
	if _, ok := fields["secret"]; ok {
		t.Fatalf("ignored field should be skipped: %#v", fields)
	}
	if _, ok := fields["hidden"]; ok {
		t.Fatalf("unexported field should be skipped: %#v", fields)
	}
}

func TestDirtyFieldsUsesGoZeroValueSemantics(t *testing.T) {
	type nested struct{ Count int }
	type input struct {
		Zero    nested
		Empty   []int
		Pointer *int
		NonNil  []string
		NonZero nested
	}
	zero := 0
	fields, err := DirtyFields(input{
		Pointer: &zero,
		NonNil:  []string{},
		NonZero: nested{Count: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := fields["zero"]; ok {
		t.Fatalf("zero nested field included: %#v", fields)
	}
	if _, ok := fields["empty"]; ok {
		t.Fatalf("nil slice included: %#v", fields)
	}
	if fields["pointer"] != &zero {
		t.Fatalf("pointer to zero was omitted: %#v", fields)
	}
	if value, ok := fields["non_nil"]; !ok || value == nil {
		t.Fatalf("non-nil empty slice was omitted: %#v", fields)
	}
	if fields["non_zero"] != (nested{Count: 1}) {
		t.Fatalf("non-zero nested field missing: %#v", fields)
	}
}

func TestRepositoryPreloadClonesState(t *testing.T) {
	base := New[map[string]any](nil, "users", "id")
	withOrders := base.Preload(Relation{With: "orders", LocalField: "id", RelatedField: "user_id"})
	withRoles := base.Preload(Relation{With: "roles", LocalField: "id", RelatedField: "role_id"})

	if got := len(base.(*repository[map[string]any]).preloadRelations); got != 0 {
		t.Fatalf("base repository should not be mutated, got %d preloads", got)
	}
	if got := len(withOrders.(*repository[map[string]any]).preloadRelations); got != 1 {
		t.Fatalf("orders repository got %d preloads", got)
	}
	if got := withRoles.(*repository[map[string]any]).preloadRelations[0].With; got != "roles" {
		t.Fatalf("preload state leaked between clones: %q", got)
	}
}

func TestBuildUpdateQueryRequiresConditionAndAvoidsParamCollisions(t *testing.T) {
	repo := &repository[map[string]any]{table: "users", primaryKey: "id"}
	_, _, err := repo.buildUpdateQuery(map[string]any{"name": "New"}, nil, QueryParams{})
	if err == nil {
		t.Fatal("expected update without condition to fail")
	}

	query, params, err := repo.buildUpdateQuery(
		map[string]any{"id": 7, "name": "New"},
		map[string]any{"name": "Old"},
		QueryParams{},
	)
	if err != nil {
		t.Fatalf("build update query: %v", err)
	}
	if !strings.Contains(query, "name = :name") || !strings.Contains(query, "WHERE name = :where_name") {
		t.Fatalf("unexpected update query: %s", query)
	}
	if params["name"] != "New" || params["where_name"] != "Old" {
		t.Fatalf("unexpected params: %#v", params)
	}
}

func TestBuildUpdateQueryRequiresTrustedExpression(t *testing.T) {
	repo := &repository[map[string]any]{table: "users", primaryKey: "id"}
	_, _, err := repo.buildUpdateQuery(
		map[string]any{"updated_at": ExprPrefix + "CURRENT_TIMESTAMP"},
		map[string]any{"id": 1},
		QueryParams{},
	)
	if err == nil {
		t.Fatal("expected ExprPrefix update expression to fail")
	}

	query, _, err := repo.buildUpdateQuery(
		map[string]any{"updated_at": Expr("CURRENT_TIMESTAMP")},
		map[string]any{"id": 1},
		QueryParams{},
	)
	if err != nil {
		t.Fatalf("trusted expression update failed: %v", err)
	}
	if !strings.Contains(query, "updated_at = CURRENT_TIMESTAMP") {
		t.Fatalf("expected trusted expression in query: %s", query)
	}
}

func TestBuildWhereClauseRequiresTrustedExpression(t *testing.T) {
	_, _, err := buildWhereClause(map[string]any{ExprPrefix + "deleted_at IS NULL": nil})
	if err == nil {
		t.Fatal("expected ExprPrefix condition to fail")
	}

	where, params, err := buildWhereClause(map[string]any{"active_only": Expr("deleted_at IS NULL")})
	if err != nil {
		t.Fatalf("trusted condition expression failed: %v", err)
	}
	if where != "deleted_at IS NULL" {
		t.Fatalf("unexpected where expression: %q", where)
	}
	if len(params) != 0 {
		t.Fatalf("trusted expression should not bind params: %#v", params)
	}
}

func TestBuildQueryRejectsUnallowlistedJoinHavingAndUnsafeIdentifiers(t *testing.T) {
	repo := &repository[map[string]any]{table: "users", primaryKey: "id"}

	_, _, err := repo.buildQuery(nil, QueryParams{Join: []string{"JOIN roles ON roles.id = users.role_id"}})
	if err == nil {
		t.Fatal("expected raw join to fail without allowlist")
	}

	_, _, err = repo.buildQuery(nil, QueryParams{Having: "COUNT(*) > 1"})
	if err == nil {
		t.Fatal("expected raw having to fail without allowlist")
	}

	_, _, err = repo.buildQuery(nil, QueryParams{Fields: []string{"name; DROP TABLE users"}})
	if err == nil {
		t.Fatal("expected unsafe field to fail")
	}

	query, _, err := repo.buildQuery(nil, QueryParams{
		Fields: []string{"id", "name", "role_count"},
		Join:   []string{"roles"},
		GroupBy: []string{
			"id",
			"name",
		},
		Having: "has_roles",
		Sort:   Sort{Field: "name", Dir: "DESC"},
		AllowedFields: map[string]string{
			"id":         "users.id",
			"name":       "users.name",
			"role_count": "COUNT(roles.id) AS role_count",
		},
		AllowedJoins: map[string]string{
			"roles": "LEFT JOIN roles ON roles.id = users.role_id",
		},
		AllowedHaving: map[string]string{
			"has_roles": "COUNT(roles.id) > 0",
		},
	})
	if err != nil {
		t.Fatalf("allowlisted query failed: %v", err)
	}
	if !strings.Contains(query, "LEFT JOIN roles") || !strings.Contains(query, "HAVING COUNT(roles.id) > 0") {
		t.Fatalf("allowlisted fragments missing from query: %s", query)
	}
}

func TestRepositoryRawRequiresAllowlist(t *testing.T) {
	repo := &repository[map[string]any]{table: "users", primaryKey: "id"}
	_, err := repo.resolveRawQuery(context.Background(), "SELECT * FROM users")
	if err == nil {
		t.Fatal("expected raw query without allowlist to fail")
	}

	ctx := WithQueryParams(context.Background(), QueryParams{
		AllowedRaw: map[string]string{
			"active_users": "SELECT * FROM users WHERE active = :active",
		},
	})
	query, err := repo.resolveRawQuery(ctx, "active_users")
	if err != nil {
		t.Fatalf("allowlisted raw query failed: %v", err)
	}
	if query != "SELECT * FROM users WHERE active = :active" {
		t.Fatalf("unexpected raw query: %s", query)
	}
}

func TestBuildDeleteQueryRequiresCondition(t *testing.T) {
	repo := &repository[map[string]any]{table: "users", primaryKey: "id"}
	_, _, err := repo.buildDeleteQuery(nil)
	if err == nil {
		t.Fatal("expected delete without condition to fail")
	}
}

func TestRepositoryEntityPrimaryKeyUsesPrimaryKeyMethod(t *testing.T) {
	repo := &repository[repositoryEntity]{table: "fallback", primaryKey: "fallback_id"}
	if got := repo.getPrimaryKey(); got != "id" {
		t.Fatalf("expected primary key column, got %q", got)
	}
}

func TestWithQueryParamsUsesTypedContextKey(t *testing.T) {
	repo := &repository[map[string]any]{table: "users", primaryKey: "id"}
	ctx := WithQueryParams(context.Background(), QueryParams{Limit: 3})
	if got := repo.getQueryParams(ctx).Limit; got != 3 {
		t.Fatalf("expected typed context query params, got %d", got)
	}
}
