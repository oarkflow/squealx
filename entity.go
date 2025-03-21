package squealx

import "context"

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

type QueryParams struct {
	Fields  []string `json:"fields"`
	Except  []string `json:"except"`
	Join    []string `json:"join"`
	GroupBy []string `json:"group_by"`
	Having  string   `json:"having"`
	Sort    Sort     `json:"sort"`
	Limit   int      `json:"limit"`
	Offset  int      `json:"offset"`
}

type Repository[T any] interface {
	Find(context.Context, map[string]any) ([]T, error)
	All(context.Context) ([]T, error)
	Create(context.Context, any) error
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
