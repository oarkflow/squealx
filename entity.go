package squealx

import "context"

type queryParamsContextKey struct{}

// WithQueryParams returns a child context carrying repository query options.
func WithQueryParams(ctx context.Context, params QueryParams) context.Context {
	return context.WithValue(ctx, queryParamsContextKey{}, params)
}

type Entity interface {
	TableName() string
	PrimaryKey() string
	ID() string
}

type BeforeCreateHook interface {
	BeforeCreate(rx *DB) error
}

type AfterCreateHook interface {
	AfterCreate(rx *DB) error
}

type BeforeUpdateHook interface {
	BeforeUpdate(rx *DB) error
}

type AfterUpdateHook interface {
	AfterUpdate(rx *DB) error
}

type BeforeDeleteHook interface {
	BeforeDelete(rx *DB) error
}

type AfterDeleteHook interface {
	AfterDelete(rx *DB) error
}

type Sort struct {
	Field string `json:"field"`
	Dir   string `json:"dir"`
}

// SQLExpression marks a SQL fragment as trusted application code.
// Do not construct SQLExpression from request input.
type SQLExpression string

// Expr returns a trusted SQL expression for repository conditions/updates.
func Expr(sql string) SQLExpression {
	return SQLExpression(sql)
}

type QueryParams struct {
	Fields []string `json:"fields"`
	Except []string `json:"except"`

	// Join and Having are allowlist keys by default. Put trusted SQL fragments
	// in AllowedJoins/AllowedHaving and pass only their keys in request-facing code.
	Join   []string `json:"join"`
	Having string   `json:"having"`

	GroupBy []string `json:"group_by"`
	Sort    Sort     `json:"sort"`
	Limit   int      `json:"limit"`
	Offset  int      `json:"offset"`

	AllowedFields map[string]string `json:"-"`
	AllowedJoins  map[string]string `json:"-"`
	AllowedHaving map[string]string `json:"-"`
	AllowedRaw    map[string]string `json:"-"`

	// AllowUnsafeRawSQL keeps backward compatibility for applications that
	// build QueryParams from trusted server-side constants.
	AllowUnsafeRawSQL bool `json:"-"`
}

type Repository[T any] interface {
	Find(context.Context, map[string]any) ([]T, error)
	All(context.Context) ([]T, error)
	Create(context.Context, any) error
	Preload(relation Relation, args ...any) Repository[T]
	Update(context.Context, any, map[string]any) error
	Delete(context.Context, any) error
	Count(ctx context.Context, cond map[string]any) (int64, error)
	SoftDelete(context.Context, map[string]any) error
	First(context.Context, map[string]any) (T, error)
	Raw(ctx context.Context, query string, args ...any) ([]T, error)
	RawExec(ctx context.Context, query string, args any) error
	Paginate(context.Context, Paging, ...map[string]any) PaginatedResponse
	PaginateRaw(ctx context.Context, paging Paging, query string, condition ...map[string]any) PaginatedResponse
	GetDB() *DB
}
