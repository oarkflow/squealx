package squealx

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/oarkflow/json"
)

// Added Filters allows passing extra conditions to filter related data.
type Relation struct {
	With                 string         // related table name or nested path, e.g. "books.comments"
	LocalField           string         // current table field used in join condition
	RelatedField         string         // related table field used in join condition
	JoinTable            string         // optional join (intermediate) table
	JoinWithLocalField   string         // field in join table that relates to current table
	JoinWithRelatedField string         // field in join table that relates to related table
	Filters              map[string]any // optional filter conditions for the relation
}

type repository[T any] struct {
	db               *DB
	table            string
	primaryKey       string
	preloadRelations []Relation // added preload field
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

// Refactored Preload accepts a relation and optional filter args.
// If a filter map is provided as the first arg, set it into relation.Filters.
func (r *repository[T]) Preload(relation Relation, args ...any) Repository[T] {
	if len(args) > 0 {
		if cond, ok := args[0].(map[string]any); ok {
			relation.Filters = cond
		}
	}
	r.preloadRelations = append(r.preloadRelations, relation)
	return r
}

func (r *repository[T]) preloadData(data []map[string]any) ([]map[string]any, error) {
	for _, rel := range r.preloadRelations {
		// If the relation's With string contains a dot, handle deep nesting.
		if strings.Contains(rel.With, ".") {
			parts := strings.Split(rel.With, ".")
			if err := r.preloadDeep(data, parts, rel); err != nil {
				return nil, err
			}
			continue
		}
		// ...existing base preload code...
		keySet := make(map[string]struct{})
		for _, rec := range data {
			if val, ok := rec[rel.LocalField]; ok {
				keySet[fmt.Sprintf("%v", val)] = struct{}{}
			} else if val, ok := rec[strings.ToLower(rel.LocalField)]; ok {
				keySet[fmt.Sprintf("%v", val)] = struct{}{}
			}
		}
		if len(keySet) == 0 {
			continue
		}
		var keys []any
		for k := range keySet {
			keys = append(keys, k)
		}

		params := map[string]any{"keys": keys}
		var query string
		if rel.JoinTable != "" {
			query = fmt.Sprintf(
				"SELECT jt.%s AS local_key, r.* FROM %s r "+
					"JOIN %s jt ON r.%s = jt.%s "+
					"WHERE jt.%s IN (:keys)",
				strings.ToLower(rel.JoinWithLocalField),
				strings.ToLower(rel.With),
				rel.JoinTable,
				strings.ToLower(rel.RelatedField),
				strings.ToLower(rel.JoinWithRelatedField),
				strings.ToLower(rel.JoinWithLocalField),
			)
		} else {
			query = fmt.Sprintf(
				"SELECT * FROM %s WHERE %s IN (:keys)",
				strings.ToLower(rel.With),
				strings.ToLower(rel.RelatedField),
			)
		}
		// Append additional filtering if defined.
		if rel.Filters != nil {
			whereClause, filterParams, err := buildWhereClause(rel.Filters)
			if err != nil {
				return nil, err
			}
			if whereClause != "" {
				query += " AND " + whereClause
			}
			for k, v := range filterParams {
				params[k] = v
			}
		}
		relatedRows, err := SelectTyped[[]map[string]any](r.db, query, params)
		if err != nil {
			return nil, err
		}
		mapping := make(map[string][]map[string]any)
		for _, rrec := range relatedRows {
			var keyVal any
			if rel.JoinTable != "" {
				keyVal = rrec["local_key"]
			} else {
				keyVal = rrec[strings.ToLower(rel.RelatedField)]
			}
			mapping[fmt.Sprintf("%v", keyVal)] = append(mapping[fmt.Sprintf("%v", keyVal)], rrec)
		}
		for i, rec := range data {
			var lookupKey any
			if rel.JoinTable != "" {
				lookupKey = rec[strings.ToLower(rel.JoinWithLocalField)]
			} else {
				lookupKey = rec[strings.ToLower(rel.LocalField)]
			}
			data[i][strings.ToLower(rel.With)] = mapping[fmt.Sprintf("%v", lookupKey)]
		}
	}
	return data, nil
}

// preloadDeep recursively preloads nested relations.
// path is the slice of relation names e.g. ["books","comments","..."]
func (r *repository[T]) preloadDeep(data []map[string]any, path []string, rel Relation) error {
	// path[0] is the parent field that already exists in data.
	currentKey := strings.ToLower(path[0])
	if len(path) < 2 {
		return nil
	}
	nextTable := strings.ToLower(path[1])

	// Gather keys from the already loaded parent relation.
	keysSet := make(map[string]struct{})
	for _, rec := range data {
		val, ok := rec[currentKey]
		if !ok {
			continue
		}
		// Expect a slice of map[string]any.
		children, ok := val.([]map[string]any)
		if !ok {
			continue
		}
		for _, child := range children {
			key := fmt.Sprintf("%v", child[strings.ToLower(rel.LocalField)])
			keysSet[key] = struct{}{}
		}
	}
	if len(keysSet) == 0 {
		return nil
	}
	var keys []any
	for k := range keysSet {
		keys = append(keys, k)
	}
	params := map[string]any{"keys": keys}
	// Build query for the next level.
	baseQuery := fmt.Sprintf("SELECT * FROM %s WHERE %s IN (:keys)", nextTable, strings.ToLower(rel.RelatedField))
	if rel.Filters != nil {
		whereClause, filterParams, err := buildWhereClause(rel.Filters)
		if err != nil {
			return err
		}
		if whereClause != "" {
			baseQuery += " AND " + whereClause
		}
		for k, v := range filterParams {
			params[k] = v
		}
	}
	relatedRows, err := SelectTyped[[]map[string]any](r.db, baseQuery, params)
	if err != nil {
		return err
	}
	// Group fetched rows by the relatedField.
	mapping := make(map[string][]map[string]any)
	for _, row := range relatedRows {
		key := fmt.Sprintf("%v", row[strings.ToLower(rel.RelatedField)])
		mapping[key] = append(mapping[key], row)
	}
	// Attach the fetched rows to each child record.
	for _, rec := range data {
		val, ok := rec[currentKey]
		if !ok {
			continue
		}
		children, ok := val.([]map[string]any)
		if !ok {
			continue
		}
		for _, child := range children {
			key := fmt.Sprintf("%v", child[strings.ToLower(rel.LocalField)])
			child[nextTable] = mapping[key]
		}
	}
	// If there are more nested levels, recurse.
	if len(path) > 2 {
		for _, rec := range data {
			val, ok := rec[currentKey]
			if !ok {
				continue
			}
			children, ok := val.([]map[string]any)
			if !ok {
				continue
			}
			for _, child := range children {
				if nextData, ok := child[nextTable].([]map[string]any); ok {
					if err := r.preloadDeep(nextData, path[1:], rel); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func (r *repository[T]) First(ctx context.Context, cond map[string]any) (T, error) {
	var rt T
	queryParams := r.getQueryParams(ctx)
	query, _, err := r.buildQuery(cond, queryParams)
	if err != nil {
		return rt, err
	}

	// fetch single record
	rt, err = SelectTyped[T](r.db, fmt.Sprintf(`%s LIMIT 1`, query), cond)
	if err != nil {
		return rt, err
	}

	if len(r.preloadRelations) > 0 {
		// convert result to map[string]any regardless of type
		recMap, err := toMap(rt)
		if err != nil {
			return rt, fmt.Errorf("preload: conversion to map failed: %w", err)
		}
		loaded, err := r.preloadData([]map[string]any{recMap})
		if err != nil {
			return rt, err
		}
		var newVal T
		if err := fromMap(loaded[0], &newVal); err != nil {
			return rt, fmt.Errorf("preload: conversion from map failed: %w", err)
		}
		return newVal, nil
	}
	return rt, nil
}

func (r *repository[T]) Find(ctx context.Context, cond map[string]any) ([]T, error) {
	var rt []T
	queryParams := r.getQueryParams(ctx)
	query, _, err := r.buildQuery(cond, queryParams)
	if err != nil {
		return rt, err
	}

	rt, err = SelectTyped[[]T](r.db, query, cond)
	if err != nil {
		return rt, err
	}
	if len(r.preloadRelations) > 0 {
		var records []map[string]any
		for _, item := range rt {
			rec, err := toMap(item)
			if err != nil {
				return nil, fmt.Errorf("preload: conversion to map failed: %w", err)
			}
			records = append(records, rec)
		}
		loaded, err := r.preloadData(records)
		if err != nil {
			return nil, err
		}
		var out []T
		for _, m := range loaded {
			var converted T
			if err := fromMap(m, &converted); err != nil {
				return nil, fmt.Errorf("preload: conversion from map failed: %w", err)
			}
			out = append(out, converted)
		}
		return out, nil
	}
	return rt, nil
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

	rt, err = SelectTyped[[]T](r.db, query)
	if err != nil {
		return rt, err
	}

	if len(r.preloadRelations) > 0 {
		var records []map[string]any
		for _, item := range rt {
			rec, err := toMap(item)
			if err != nil {
				return nil, fmt.Errorf("preload: conversion to map failed: %w", err)
			}
			records = append(records, rec)
		}
		loaded, err := r.preloadData(records)
		if err != nil {
			return nil, err
		}
		var out []T
		for _, m := range loaded {
			var converted T
			if err := fromMap(m, &converted); err != nil {
				return nil, fmt.Errorf("preload: conversion from map failed: %w", err)
			}
			out = append(out, converted)
		}
		return out, nil
	}
	return rt, nil
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
		if str, ok := val.(string); ok && strings.HasPrefix(str, ExprPrefix) {
			rawSQL := strings.TrimPrefix(str, ExprPrefix)
			setClauses = append(setClauses, fmt.Sprintf("%s = %s", col, rawSQL))
		} else {
			setClauses = append(setClauses, fmt.Sprintf("%s = :%s", col, col))
			values[col] = val
		}
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

// Helper functions to convert between any type and map[string]any
func toMap(v any) (map[string]any, error) {
	// marshal the value then unmarshal into a map
	bt, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	err = json.Unmarshal(bt, &m)
	return m, err
}

func fromMap(m map[string]any, dest any) error {
	// marshal the map then unmarshal into the destination type
	bt, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return json.Unmarshal(bt, dest)
}
