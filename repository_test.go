package squealx

import (
	"context"
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
