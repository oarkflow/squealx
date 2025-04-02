package squealx

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/oarkflow/json"
)

type repository[T any] struct {
	db         *DB
	table      string
	primaryKey string
}

func New[T any](db *DB, table, primaryKey string) Repository[T] {
	return &repository[T]{db: db, table: table, primaryKey: primaryKey}
}

func (r *repository[T]) getQueryParams(ctx context.Context) QueryParams {
	queryParams, ok := ctx.Value("query_params").(QueryParams)
	if !ok {
		return QueryParams{}
	}
	return queryParams
}

func (r *repository[T]) GetDB() *DB {
	return r.db
}

func (r *repository[T]) First(ctx context.Context, cond map[string]any) (T, error) {
	var rt T
	queryParams := r.getQueryParams(ctx)
	query, _, err := r.buildQuery(cond, queryParams)
	if err != nil {
		return rt, err
	}
	return SelectTyped[T](r.db, fmt.Sprintf(`%s LIMIT 1`, query), cond)
}

func (r *repository[T]) Find(ctx context.Context, cond map[string]any) ([]T, error) {
	var rt []T
	queryParams := r.getQueryParams(ctx)
	query, _, err := r.buildQuery(cond, queryParams)
	if err != nil {
		return rt, err
	}
	return SelectTyped[[]T](r.db, query, cond)
}

func (r *repository[T]) Count(ctx context.Context, cond map[string]any) (int64, error) {
	queryParams := r.getQueryParams(ctx)
	query, _, err := r.buildQuery(cond, queryParams, true)
	if err != nil {
		return 0, err
	}
	data, err := SelectTyped[map[string]any](r.db, query, cond)
	if err != nil || data == nil {
		return 0, err
	}
	switch count := data["total_rows"].(type) {
	case int:
		return int64(count), nil
	case int64:
		return count, nil
	case float32:
		return int64(count), nil
	case float64:
		return int64(count), nil
	}
	return 0, fmt.Errorf("Cannot query count")
}

func (r *repository[T]) All(ctx context.Context) ([]T, error) {
	var rt []T
	queryParams := r.getQueryParams(ctx)
	query, _, err := r.buildQuery(nil, queryParams)
	if err != nil {
		return rt, err
	}
	return SelectTyped[[]T](r.db, query)
}

func (r *repository[T]) Paginate(ctx context.Context, paging Paging, condition ...map[string]any) PaginatedResponse {
	var rt []T
	queryParams := r.getQueryParams(ctx)
	var cond map[string]any
	if len(condition) > 0 {
		cond = condition[0]
	}
	query, _, err := r.buildQuery(cond, queryParams)
	if err != nil {
		return PaginatedResponse{Error: err}
	}
	return Paginate(r.db, query, &rt, paging, condition...)
}

func (r *repository[T]) PaginateRaw(ctx context.Context, paging Paging, query string, condition ...map[string]any) PaginatedResponse {
	var rt []T
	return Paginate(r.db, query, &rt, paging, condition...)
}

func (r *repository[T]) Create(ctx context.Context, data any) error {
	queryParams := r.getQueryParams(ctx)
	switch data := data.(type) {
	case BeforeCreateHook:
		err := data.BeforeCreate(r.db)
		if err != nil {
			return err
		}
	}
	query, _, err := r.buildInsertQuery(data, queryParams)
	if err != nil {
		return err
	}
	err = r.db.ExecWithReturn(query, data)
	if err != nil {
		return err
	}
	switch data := data.(type) {
	case AfterCreateHook:
		err := data.AfterCreate(r.db)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *repository[T]) Update(ctx context.Context, data any, condition map[string]any) error {
	queryParams := r.getQueryParams(ctx)
	switch data := data.(type) {
	case BeforeUpdateHook:
		err := data.BeforeUpdate(r.db)
		if err != nil {
			return err
		}
	}
	query, args, err := r.buildUpdateQuery(data, condition, queryParams)
	if err != nil {
		return err
	}
	err = r.db.ExecWithReturn(query, &args)
	if err != nil {
		return err
	}
	switch data := data.(type) {
	case Entity:
		bt, err := json.Marshal(args)
		if err != nil {
			return err
		}
		err = json.Unmarshal(bt, data)
		if err != nil {
			return err
		}
	case *Entity:
		bt, err := json.Marshal(args)
		if err != nil {
			return err
		}
		err = json.Unmarshal(bt, data)
		if err != nil {
			return err
		}
	case *map[string]any:
		*data = args
	case map[string]any:
		for k, v := range args {
			data[k] = v
		}
	}
	switch data := data.(type) {
	case AfterUpdateHook:
		err := data.AfterUpdate(r.db)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *repository[T]) Delete(ctx context.Context, data any) error {
	query, _, err := r.buildDeleteQuery(data)
	if err != nil {
		return err
	}
	switch data := data.(type) {
	case BeforeDeleteHook:
		err := data.BeforeDelete(r.db)
		if err != nil {
			return err
		}
	}
	err = r.db.ExecWithReturn(query, data)
	if err != nil {
		return err
	}
	switch data := data.(type) {
	case AfterDeleteHook:
		err := data.AfterDelete(r.db)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *repository[T]) SoftDelete(ctx context.Context, condition map[string]any) error {
	data := map[string]any{"deleted_at": time.Now()}
	return r.Update(ctx, data, condition)
}

func (r *repository[T]) Raw(ctx context.Context, query string, args ...any) ([]T, error) {
	return SelectTyped[[]T](r.db, query, args...)
}

func (r *repository[T]) RawExec(ctx context.Context, query string, args any) error {
	return r.db.ExecWithReturn(query, args)
}

func (r *repository[T]) getTableName() string {
	var t T
	switch t := any(t).(type) {
	case Entity:
		return t.TableName()
	}
	return r.table
}

func (r *repository[T]) buildQuery(condition map[string]any, queryParams QueryParams, isCount ...bool) (string, map[string]any, error) {
	tableName := r.getTableName()
	fields := "*"
	if len(queryParams.Fields) > 0 {
		fields = strings.Join(queryParams.Fields, ", ")
	} else if len(queryParams.Except) > 0 {
		allFields := getAllColumns[T]()
		fields = strings.Join(excludeFieldsSlice(allFields, queryParams.Except), ", ")
	}
	var query string
	if len(isCount) > 0 && isCount[0] {
		query = fmt.Sprintf("SELECT COUNT(*) as total_rows FROM %s", tableName)
	} else {
		query = fmt.Sprintf("SELECT %s FROM %s", fields, tableName)
	}
	if len(queryParams.Join) > 0 {
		query += " " + strings.Join(queryParams.Join, " ")
	}
	whereClause := ""
	params := map[string]any{}
	if condition != nil {
		var err error
		whereClause, params, err = buildWhereClause(condition)
		if err != nil {
			return "", nil, err
		}
	}
	if whereClause != "" {
		query += " WHERE " + whereClause
	}
	if len(queryParams.GroupBy) > 0 {
		query += " GROUP BY " + strings.Join(queryParams.GroupBy, ", ")
	}
	if queryParams.Having != "" {
		query += " HAVING " + queryParams.Having
	}
	if queryParams.Sort.Field != "" {
		sortDir := strings.ToUpper(queryParams.Sort.Dir)
		if sortDir != "ASC" && sortDir != "DESC" {
			sortDir = "ASC"
		}
		query += fmt.Sprintf(" ORDER BY %s %s", queryParams.Sort.Field, sortDir)
	}
	if queryParams.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", queryParams.Limit)
	}
	if queryParams.Offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", queryParams.Offset)
	}
	return query, params, nil
}

func (r *repository[T]) buildInsertQuery(data any, queryParams QueryParams) (string, map[string]any, error) {
	tableName := r.getTableName()
	fields, err := DirtyFields(data)
	if err != nil {
		return "", nil, err
	}
	if len(queryParams.Fields) > 0 {
		fields = filterFields(fields, queryParams.Fields)
	} else if len(queryParams.Except) > 0 {
		fields = excludeFields(fields, queryParams.Except)
	}
	columns := make([]string, 0, len(fields))
	placeholders := make([]string, 0, len(fields))
	values := make(map[string]any, len(fields))
	for col, val := range fields {
		columns = append(columns, col)
		placeholders = append(placeholders, ":"+col)
		values[col] = val
	}
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, strings.Join(columns, ", "), strings.Join(placeholders, ", "))
	return query, values, nil
}

func (r *repository[T]) buildDeleteQuery(condition any) (string, map[string]any, error) {
	tableName := r.getTableName()
	var whereClause string
	params := make(map[string]any)
	if condition != nil {
		condClause, condParams, err := buildWhereClause(condition)
		if err != nil {
			return "", nil, err
		}
		whereClause += condClause
		for k, v := range condParams {
			params[k] = v
		}
	}
	query := fmt.Sprintf("DELETE FROM %s WHERE %s", tableName, whereClause)
	return query, params, nil
}

func (r *repository[T]) buildUpdateQuery(data any, condition map[string]any, queryParams QueryParams) (string, map[string]any, error) {
	var err error
	tableName := r.getTableName()
	var fields map[string]any
	pkColumn := r.getPrimaryKey()
	switch t := data.(type) {
	case Entity:
		fields, err = DirtyFields(t)
		if err != nil {
			return "", nil, err
		}
	case map[string]any:
		fields = t
	case *map[string]any:
		fields = *t
	default:
		return "", nil, fmt.Errorf("invalid data type for update query: %T", t)
	}
	delete(fields, pkColumn)
	if len(queryParams.Fields) > 0 {
		fields = filterFields(fields, queryParams.Fields)
	} else if len(queryParams.Except) > 0 {
		fields = excludeFields(fields, queryParams.Except)
	}
	setClauses := make([]string, 0, len(fields))
	values := make(map[string]any, len(fields)+1)
	for col, val := range fields {
		setClauses = append(setClauses, fmt.Sprintf("%s = :%s", col, col))
		values[col] = val
	}
	whereClause := ""
	if condition != nil {
		condClause, condParams, err := buildWhereClause(condition)
		if err != nil {
			return "", nil, err
		}
		whereClause = condClause
		for k, v := range condParams {
			values[k] = v
		}
	}
	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s", tableName, strings.Join(setClauses, ", "), whereClause)
	return query, values, nil
}

func (r *repository[T]) getPrimaryKey() string {
	var t T
	switch t := any(t).(type) {
	case Entity:
		return t.ID()
	default:
		return r.primaryKey
	}
}
