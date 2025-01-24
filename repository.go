package squealx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/oarkflow/squealx/utils/xstrings"
)

type Entity interface {
	TableName() string
	PrimaryKey() string
	ID() string
}

type Sort struct {
	Field string `json:"field"`
	Dir   string `json:"dir"`
}

type QueryParams struct {
	Sort   Sort     `json:"sort"`
	Fields []string `json:"fields"`
	Except []string `json:"except"`
}

type Repository[T any] interface {
	Find(context.Context, map[string]any) (T, error)
	All(context.Context) ([]T, error)
	Create(context.Context, any) error
	Update(context.Context, any, map[string]any) error
	Delete(context.Context, any) error
	First(context.Context, map[string]any) (T, error)
	Raw(ctx context.Context, query string, args ...any) ([]T, error)
	RawExec(ctx context.Context, query string, args any) error
	Paginate(context.Context, Paging, ...map[string]any) PaginatedResponse
	PaginateRaw(ctx context.Context, paging Paging, query string, condition ...map[string]any) PaginatedResponse
	GetDB() *DB
}

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

func (r *repository[T]) Find(ctx context.Context, cond map[string]any) (T, error) {
	var rt T
	queryParams := r.getQueryParams(ctx)
	query, _, err := r.buildQuery(cond, queryParams)
	if err != nil {
		return rt, err
	}
	return SelectTyped[T](r.db, query, cond)
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
	query, _, err := r.buildInsertQuery(data, queryParams)
	if err != nil {
		return err
	}
	return r.db.ExecWithReturn(query, data)
}

func (r *repository[T]) Update(ctx context.Context, data any, condition map[string]any) error {
	queryParams := r.getQueryParams(ctx)
	query, args, err := r.buildUpdateQuery(data, condition, queryParams)
	if err != nil {
		return err
	}
	err = r.db.ExecWithReturn(query, &args)
	if err != nil {
		return err
	}
	switch data := data.(type) {
	case *Entity:
		var t Entity
		bt, err := json.Marshal(args)
		if err != nil {
			return err
		}
		err = json.Unmarshal(bt, &t)
		if err != nil {
			return err
		}
		*data = t
	case *map[string]any:
		*data = args
	case map[string]any:
		for k, v := range args {
			data[k] = v
		}
	}
	return nil
}

func (r *repository[T]) Delete(ctx context.Context, data any) error {
	query, _, err := r.buildDeleteQuery(data)
	if err != nil {
		return err
	}
	return r.db.ExecWithReturn(query, data)
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

func (r *repository[T]) buildQuery(condition map[string]any, queryParams QueryParams) (string, map[string]any, error) {
	tableName := r.getTableName()
	fields := "*"
	if len(queryParams.Fields) > 0 {
		fields = strings.Join(queryParams.Fields, ", ")
	} else if len(queryParams.Except) > 0 {
		allFields := getAllColumns[T]()
		fields = strings.Join(excludeFieldsSlice(allFields, queryParams.Except), ", ")
	}
	query := fmt.Sprintf("SELECT %s FROM %s", fields, tableName)
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
	if queryParams.Sort.Field != "" {
		sortDir := strings.ToUpper(queryParams.Sort.Dir)
		if sortDir != "ASC" && sortDir != "DESC" {
			sortDir = "ASC"
		}
		query += fmt.Sprintf(" ORDER BY %s %s", queryParams.Sort.Field, sortDir)
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

func DirtyFields(u any) (map[string]interface{}, error) {
	switch u := u.(type) {
	case map[string]any:
		return u, nil
	case *map[string]any:
		return *u, nil
	}
	v := reflect.ValueOf(u)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected a struct or struct pointer, got %s", v.Kind())
	}
	setFields := make(map[string]interface{})
	t := v.Type()
	zero := reflect.Zero(v.Type()).Interface()
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldType := t.Field(i)
		fieldName := fieldType.Tag.Get("db")
		if fieldName == "" {
			fieldName = xstrings.ToSnakeCase(fieldType.Name)
		}
		zeroField := reflect.ValueOf(zero).Field(i)
		if !reflect.DeepEqual(field.Interface(), zeroField.Interface()) {
			setFields[fieldName] = field.Interface()
		}
	}
	return setFields, nil
}

func getAllColumns[T any]() []string {
	var t T
	var columns []string
	tValue := reflect.TypeOf(t)
	if tValue.Kind() == reflect.Ptr {
		tValue = tValue.Elem()
	}
	if tValue.Kind() == reflect.Struct {
		for i := 0; i < tValue.NumField(); i++ {
			field := tValue.Field(i)
			columnName := field.Tag.Get("db")
			if columnName == "" {
				columnName = xstrings.ToSnakeCase(field.Name)
			}
			columns = append(columns, columnName)
		}
	}
	return columns
}

func filterFields(fields map[string]any, allowed []string) map[string]any {
	filtered := make(map[string]any)
	for _, key := range allowed {
		if value, exists := fields[key]; exists {
			filtered[key] = value
		}
	}
	return filtered
}

func excludeFieldsSlice(fields []string, excluded []string) (f []string) {
	excludedSet := make(map[string]bool)
	for _, ex := range excluded {
		excludedSet[ex] = true
	}
	for _, key := range fields {
		if !excludedSet[key] {
			f = append(f, key)
		}
	}
	return
}

func excludeFields(fields map[string]any, excluded []string) map[string]any {
	filtered := make(map[string]any)
	for key, value := range fields {
		excludedSet := make(map[string]bool)
		for _, ex := range excluded {
			excludedSet[ex] = true
		}
		if !excludedSet[key] {
			filtered[key] = value
		}
	}
	return filtered
}

func GetFields(entity any) (map[string]any, error) {
	switch entity := entity.(type) {
	case map[string]any:
		return entity, nil
	case *map[string]any:
		return *entity, nil
	}
	fields := make(map[string]any)
	v := reflect.ValueOf(entity)
	t := v.Type()

	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}

	if t.Kind() != reflect.Struct {
		return nil, errors.New("entity must be a struct")
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		value := v.Field(i).Interface()

		columnName := field.Tag.Get("db")
		if columnName == "" {
			columnName = xstrings.ToSnakeCase(field.Name)
		}

		fields[columnName] = value
	}
	return fields, nil
}

// buildWhereClause generates a WHERE clause from a condition struct or map, using DirtyFields for structs
func buildWhereClause(condition any) (string, map[string]any, error) {
	var whereClauses []string
	params := map[string]any{}

	switch c := condition.(type) {
	case map[string]any:
		for key, value := range c {
			paramName := ":" + key
			whereClauses = append(whereClauses, fmt.Sprintf("%s = %s", key, paramName))
			params[key] = value
		}
	case *map[string]any:
		for key, value := range *c {
			paramName := ":" + key
			whereClauses = append(whereClauses, fmt.Sprintf("%s = %s", key, paramName))
			params[key] = value
		}
	default:
		// Handle struct or struct pointer
		fields, err := DirtyFields(condition)
		if err != nil {
			return "", nil, fmt.Errorf("expected map or struct for condition, got %T", condition)
		}
		for key, value := range fields {
			paramName := ":" + key
			whereClauses = append(whereClauses, fmt.Sprintf("%s = %s", key, paramName))
			params[key] = value
		}
	}
	return strings.Join(whereClauses, " AND "), params, nil
}
