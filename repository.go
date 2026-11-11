package squealx

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	"encoding/json"
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
	preloadPaths     [][]string
}

const preloadKeyBatchSize = 500

var ErrRepositoryCursorPreload = errors.New("squealx: repository cursor does not support relation preloads")
var ErrRepositoryCursorUnsupported = errors.New("squealx: repository does not support streaming cursors")
var ErrPreloadedCursorPool = errors.New("squealx: streaming relation preloads require at least two open connections")

// PreloadedCursorConfig controls the row limit and parent batch size used by
// streaming repository preloads. BatchSize zero selects the default of 500.
type PreloadedCursorConfig struct {
	Cursor    CursorConfig
	BatchSize int
}

func validatePreloadedCursorPool(db *DB, relations []Relation) error {
	if db != nil && len(relations) > 0 && db.Stats().MaxOpenConnections == 1 {
		return ErrPreloadedCursorPool
	}
	return nil
}

type configuredRepositoryCursor[T any] interface {
	FindCursorConfig(context.Context, map[string]any, CursorConfig) (*Cursor[T], error)
}

type rawRepositoryCursor[T any] interface {
	RawCursorConfig(context.Context, string, CursorConfig, ...any) (*Cursor[T], error)
}

type preloadedRepositoryCursor[T any] interface {
	FindPreloadedCursorWithConfig(context.Context, map[string]any, PreloadedCursorConfig) (*PreloadedCursor[T], error)
}

// FindCursor streams rows from a repository that supports cursor reads. The
// optional capability keeps existing custom Repository implementations source
// compatible while the built-in repository provides bounded-memory reads.
func FindCursor[T any](ctx context.Context, repo Repository[T], cond map[string]any) (*Cursor[T], error) {
	return FindCursorConfig(ctx, repo, cond, CursorConfig{})
}

// FindCursorConfig streams a repository query with cursor limits such as
// MaxRows. Cursor support remains optional for custom Repository
// implementations.
func FindCursorConfig[T any](ctx context.Context, repo Repository[T], cond map[string]any, config CursorConfig) (*Cursor[T], error) {
	cursorRepo, ok := repo.(configuredRepositoryCursor[T])
	if !ok {
		return nil, ErrRepositoryCursorUnsupported
	}
	return cursorRepo.FindCursorConfig(ctx, cond, config)
}

// RawCursor streams rows from an allowlisted repository raw query.
func RawCursor[T any](ctx context.Context, repo Repository[T], query string, args ...any) (*Cursor[T], error) {
	return RawCursorConfig(ctx, repo, query, CursorConfig{}, args...)
}

// RawCursorConfig streams an allowlisted repository raw query with cursor
// limits such as MaxRows. Custom Repository implementations may opt in by
// providing RawCursorConfig.
func RawCursorConfig[T any](ctx context.Context, repo Repository[T], query string, config CursorConfig, args ...any) (*Cursor[T], error) {
	cursorRepo, ok := repo.(rawRepositoryCursor[T])
	if !ok {
		return nil, ErrRepositoryCursorUnsupported
	}
	return cursorRepo.RawCursorConfig(ctx, query, config, args...)
}

// FindPreloadedCursor streams repository rows with relation preloads assembled
// in bounded parent batches.
func FindPreloadedCursor[T any](ctx context.Context, repo Repository[T], cond map[string]any) (*PreloadedCursor[T], error) {
	return FindPreloadedCursorConfig(ctx, repo, cond, CursorConfig{})
}

// FindPreloadedCursorConfig streams preloaded repository rows with cursor
// limits such as MaxRows. Parent rows are buffered in batches to keep memory
// bounded while avoiding one related query per parent row.
func FindPreloadedCursorConfig[T any](ctx context.Context, repo Repository[T], cond map[string]any, config CursorConfig) (*PreloadedCursor[T], error) {
	return FindPreloadedCursorWithConfig(ctx, repo, cond, PreloadedCursorConfig{Cursor: config})
}

// FindPreloadedCursorWithConfig streams preloaded repository rows with a
// configurable parent batch size and cursor row limit.
func FindPreloadedCursorWithConfig[T any](ctx context.Context, repo Repository[T], cond map[string]any, config PreloadedCursorConfig) (*PreloadedCursor[T], error) {
	cursorRepo, ok := repo.(preloadedRepositoryCursor[T])
	if !ok {
		return nil, ErrRepositoryCursorUnsupported
	}
	return cursorRepo.FindPreloadedCursorWithConfig(ctx, cond, config)
}

func New[T any](db *DB, table, primaryKey string) Repository[T] {
	return &repository[T]{db: db, table: table, primaryKey: primaryKey}
}

func (r *repository[T]) getQueryParams(ctx context.Context) QueryParams {
	if ctx == nil {
		return QueryParams{}
	}
	if queryParams, ok := ctx.Value(queryParamsContextKey{}).(QueryParams); ok {
		return queryParams
	}
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
			relation.Filters = cloneMap(cond)
		}
	} else if relation.Filters != nil {
		relation.Filters = cloneMap(relation.Filters)
	}
	next := *r
	next.preloadRelations = append(append([]Relation(nil), r.preloadRelations...), relation)
	next.preloadPaths = append(append([][]string(nil), r.preloadPaths...), strings.Split(relation.With, "."))
	return &next
}

func relationKey(value any) any {
	value = normalizeKeyValue(value)
	if number, ok := value.(float64); ok {
		if math.IsNaN(number) {
			return fmt.Sprintf("%T:%v", value, value)
		}
		if !math.IsInf(number, 0) && math.Trunc(number) == number && number >= math.MinInt64 && number < math.MaxInt64 {
			value = int64(number)
		}
	}
	if value == nil || reflect.TypeOf(value).Comparable() {
		return value
	}
	return fmt.Sprintf("%T:%v", value, value)
}

func namedArgs(params map[string]any) []any {
	if len(params) == 0 {
		return nil
	}
	return []any{params}
}

func forEachRelationBatch(ctx context.Context, db *DB, query string, params map[string]any, keys []any, visit func(map[string]any) error) error {
	for start := 0; start < len(keys); start += preloadKeyBatchSize {
		end := min(start+preloadKeyBatchSize, len(keys))
		batchParams := make(map[string]any, len(params))
		for key, value := range params {
			batchParams[key] = value
		}
		batchParams["keys"] = keys[start:end]

		cursor, err := NamedQueryCursor[map[string]any](ctx, db, query, batchParams)
		if err != nil {
			return err
		}
		if err := consumeCursor(cursor, visit); err != nil {
			return err
		}
	}
	return nil
}

func consumeCursor[T any](cursor *Cursor[T], visit func(T) error) (returnErr error) {
	defer func() {
		returnErr = joinCloseError(returnErr, cursor.Close())
	}()
	for cursor.Next() {
		if err := visit(cursor.Copy()); err != nil {
			return err
		}
	}
	return cursor.Err()
}

func (r *repository[T]) preloadData(ctx context.Context, data []map[string]any) ([]map[string]any, error) {
	for i, rel := range r.preloadRelations {
		var path []string
		if i < len(r.preloadPaths) {
			path = r.preloadPaths[i]
		}
		if path == nil {
			path = strings.Split(rel.With, ".")
		}
		if err := validateRelationWithPath(rel, path); err != nil {
			return nil, err
		}
		// If the relation's With string contains a dot, handle deep nesting.
		if len(path) > 1 {
			if err := r.preloadDeep(ctx, data, path, rel); err != nil {
				return nil, err
			}
			continue
		}
		// ...existing base preload code...
		keySet := make(map[any]any, len(data))
		for _, rec := range data {
			if val, ok := mapValue(rec, rel.LocalField); ok {
				keySet[relationKey(val)] = val
			}
		}
		if len(keySet) == 0 {
			continue
		}
		keys := make([]any, 0, len(keySet))
		for _, value := range keySet {
			keys = append(keys, value)
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
		mapping := make(map[any][]map[string]any, len(keySet))
		if err := forEachRelationBatch(ctx, r.db, query, params, keys, func(rrec map[string]any) error {
			var keyVal any
			if rel.JoinTable != "" {
				keyVal, _ = mapValue(rrec, "local_key")
			} else {
				keyVal, _ = mapValue(rrec, rel.RelatedField)
			}
			key := relationKey(keyVal)
			mapping[key] = append(mapping[key], rrec)
			return nil
		}); err != nil {
			return nil, err
		}
		for i, rec := range data {
			var lookupKey any
			var ok bool
			lookupKey, ok = mapValue(rec, rel.LocalField)
			if !ok {
				data[i][strings.ToLower(rel.With)] = []map[string]any{}
				continue
			}
			data[i][strings.ToLower(rel.With)] = mapping[relationKey(lookupKey)]
		}
	}
	return data, nil
}

// preloadDeep recursively preloads nested relations.
// path is the slice of relation names e.g. ["books","comments","..."]
func (r *repository[T]) preloadDeep(ctx context.Context, data []map[string]any, path []string, rel Relation) error {
	if err := validateRelationPath(path); err != nil {
		return err
	}
	// path[0] is the parent field that already exists in data.
	currentKey := strings.ToLower(path[0])
	if len(path) < 2 {
		return nil
	}
	nextTable := strings.ToLower(path[1])

	// Gather keys from the already loaded parent relation.
	keysSet := make(map[any]any, len(data))
	for _, rec := range data {
		val, ok := mapValue(rec, currentKey)
		if !ok {
			continue
		}
		// Expect a slice of map[string]any.
		children, ok := val.([]map[string]any)
		if !ok {
			continue
		}
		for _, child := range children {
			if val, ok := mapValue(child, rel.LocalField); ok {
				keysSet[relationKey(val)] = val
			}
		}
	}
	if len(keysSet) == 0 {
		return nil
	}
	keys := make([]any, 0, len(keysSet))
	for _, value := range keysSet {
		keys = append(keys, value)
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
	// Group fetched rows by the relatedField.
	mapping := make(map[any][]map[string]any, len(keysSet))
	if err := forEachRelationBatch(ctx, r.db, baseQuery, params, keys, func(row map[string]any) error {
		val, _ := mapValue(row, rel.RelatedField)
		key := relationKey(val)
		mapping[key] = append(mapping[key], row)
		return nil
	}); err != nil {
		return err
	}
	// Attach the fetched rows to each child record.
	for _, rec := range data {
		val, ok := mapValue(rec, currentKey)
		if !ok {
			continue
		}
		children, ok := val.([]map[string]any)
		if !ok {
			continue
		}
		for _, child := range children {
			val, ok := mapValue(child, rel.LocalField)
			if !ok {
				child[nextTable] = []map[string]any{}
				continue
			}
			key := relationKey(val)
			child[nextTable] = mapping[key]
		}
	}
	// If there are more nested levels, recurse.
	if len(path) > 2 {
		for _, rec := range data {
			val, ok := mapValue(rec, currentKey)
			if !ok {
				continue
			}
			children, ok := val.([]map[string]any)
			if !ok {
				continue
			}
			for _, child := range children {
				if nextData, ok := child[nextTable].([]map[string]any); ok {
					if err := r.preloadDeep(ctx, nextData, path[1:], rel); err != nil {
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
	query, params, err := r.buildQuery(cond, queryParams)
	if err != nil {
		return rt, err
	}

	// fetch single record
	rt, err = SelectTypedContext[T](ctx, r.db, query, namedArgs(params)...)
	if err != nil {
		return rt, err
	}

	if len(r.preloadRelations) > 0 {
		// convert result to map[string]any regardless of type
		recMap, err := toMap(rt)
		if err != nil {
			return rt, fmt.Errorf("preload: conversion to map failed: %w", err)
		}
		loaded, err := r.preloadData(ctx, []map[string]any{recMap})
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
	query, params, err := r.buildQuery(cond, queryParams)
	if err != nil {
		return rt, err
	}

	rt, err = SelectTypedContext[[]T](ctx, r.db, query, namedArgs(params)...)
	if err != nil {
		return rt, err
	}
	if len(r.preloadRelations) > 0 {
		var records []map[string]any
		if len(rt) > 0 {
			records = make([]map[string]any, 0, len(rt))
		}
		for _, item := range rt {
			rec, err := toMap(item)
			if err != nil {
				return nil, fmt.Errorf("preload: conversion to map failed: %w", err)
			}
			records = append(records, rec)
		}
		loaded, err := r.preloadData(ctx, records)
		if err != nil {
			return nil, err
		}
		var out []T
		if len(loaded) > 0 {
			out = make([]T, 0, len(loaded))
		}
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

// FindCursor returns a pull-based cursor for the repository query. It applies
// the same context query parameters and conditions as Find while keeping only
// the current row in memory. Relation preloads are rejected because they
// require buffering or would otherwise introduce an N+1 query pattern.
func (r *repository[T]) FindCursor(ctx context.Context, cond map[string]any) (*Cursor[T], error) {
	return r.FindCursorConfig(ctx, cond, CursorConfig{})
}

// FindCursorConfig returns a bounded, pull-based cursor for the repository
// query. Relation preloads are rejected because they require buffering or
// would otherwise introduce an N+1 query pattern.
func (r *repository[T]) FindCursorConfig(ctx context.Context, cond map[string]any, config CursorConfig) (*Cursor[T], error) {
	if r == nil || r.db == nil {
		return nil, errors.New("squealx: nil repository database")
	}
	if len(r.preloadRelations) > 0 {
		return nil, ErrRepositoryCursorPreload
	}
	query, params, err := r.buildQuery(cond, r.getQueryParams(ctx))
	if err != nil {
		return nil, err
	}
	if len(params) == 0 {
		return QueryCursorConfig[T](ctx, r.db, config, query)
	}
	return NamedQueryCursorConfig[T](ctx, r.db, config, query, params)
}

func (r *repository[T]) FindPreloadedCursorConfig(ctx context.Context, cond map[string]any, config CursorConfig) (*PreloadedCursor[T], error) {
	return r.FindPreloadedCursorWithConfig(ctx, cond, PreloadedCursorConfig{Cursor: config})
}

func (r *repository[T]) FindPreloadedCursorWithConfig(ctx context.Context, cond map[string]any, options PreloadedCursorConfig) (*PreloadedCursor[T], error) {
	if r == nil || r.db == nil {
		return nil, errors.New("squealx: nil repository database")
	}
	if err := options.Cursor.validate(); err != nil {
		return nil, err
	}
	if options.BatchSize < 0 {
		return nil, fmt.Errorf("%w: preload batch size cannot be negative", ErrInvalidCursorConfig)
	}
	batchSize := options.BatchSize
	if batchSize == 0 {
		batchSize = preloadKeyBatchSize
	}
	if err := validatePreloadedCursorPool(r.db, r.preloadRelations); err != nil {
		return nil, err
	}
	query, params, err := r.buildQuery(cond, r.getQueryParams(ctx))
	if err != nil {
		return nil, err
	}
	var source *Cursor[T]
	if len(params) == 0 {
		source, err = QueryCursorConfig[T](ctx, r.db, options.Cursor, query)
	} else {
		source, err = NamedQueryCursorConfig[T](ctx, r.db, options.Cursor, query, params)
	}
	if err != nil {
		return nil, err
	}
	return &PreloadedCursor[T]{ctx: ctx, repo: r, source: source, batchSize: batchSize}, nil
}

func (r *repository[T]) Count(ctx context.Context, cond map[string]any) (int64, error) {
	queryParams := r.getQueryParams(ctx)
	query, params, err := r.buildQuery(cond, queryParams, true)
	if err != nil {
		return 0, err
	}
	data, err := SelectTypedContext[map[string]any](ctx, r.db, query, namedArgs(params)...)
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
	query, params, err := r.buildQuery(nil, queryParams)
	if err != nil {
		return rt, err
	}

	rt, err = SelectTypedContext[[]T](ctx, r.db, query, namedArgs(params)...)
	if err != nil {
		return rt, err
	}

	if len(r.preloadRelations) > 0 {
		var records []map[string]any
		if len(rt) > 0 {
			records = make([]map[string]any, 0, len(rt))
		}
		for _, item := range rt {
			rec, err := toMap(item)
			if err != nil {
				return nil, fmt.Errorf("preload: conversion to map failed: %w", err)
			}
			records = append(records, rec)
		}
		loaded, err := r.preloadData(ctx, records)
		if err != nil {
			return nil, err
		}
		var out []T
		if len(loaded) > 0 {
			out = make([]T, 0, len(loaded))
		}
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
	query, params, err := r.buildQuery(cond, queryParams)
	if err != nil {
		return PaginatedResponse{Error: err}
	}
	return Paginate(r.db, query, &rt, paging, params)
}

func (r *repository[T]) PaginateRaw(ctx context.Context, paging Paging, query string, condition ...map[string]any) PaginatedResponse {
	var rt []T
	query, err := r.resolveRawQuery(ctx, query)
	if err != nil {
		return PaginatedResponse{Error: err}
	}
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
	execArgs := execReturnArgs(data, nil)
	err = r.db.ExecWithReturnContext(ctx, query, execArgs)
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
	err = r.db.ExecWithReturnContext(ctx, query, &args)
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
	err = r.db.ExecWithReturnContext(ctx, query, data)
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
	resolved, err := r.resolveRawQuery(ctx, query)
	if err != nil {
		return nil, err
	}
	query = resolved
	return SelectTypedContext[[]T](ctx, r.db, query, args...)
}

// RawCursorConfig streams an allowlisted raw repository query without
// collecting the full result slice.
func (r *repository[T]) RawCursorConfig(ctx context.Context, query string, config CursorConfig, args ...any) (*Cursor[T], error) {
	if r == nil || r.db == nil {
		return nil, errors.New("squealx: nil repository database")
	}
	if err := config.validate(); err != nil {
		return nil, err
	}
	resolved, err := r.resolveRawQuery(ctx, query)
	if err != nil {
		return nil, err
	}
	return QueryCursorConfig[T](ctx, r.db, config, resolved, args...)
}

func (r *repository[T]) RawExec(ctx context.Context, query string, args any) error {
	resolved, err := r.resolveRawQuery(ctx, query)
	if err != nil {
		return err
	}
	query = resolved
	return r.db.ExecWithReturnContext(ctx, query, args)
}

func (r *repository[T]) resolveRawQuery(ctx context.Context, query string) (string, error) {
	queryParams := r.getQueryParams(ctx)
	if resolved, ok := queryParams.AllowedRaw[query]; ok {
		return resolved, nil
	}
	if queryParams.AllowUnsafeRawSQL {
		return query, nil
	}
	return "", fmt.Errorf("raw query %q is not allowlisted", query)
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
	if err := validateIdentifier(tableName); err != nil {
		return "", nil, err
	}
	fields := "*"
	if len(queryParams.Fields) > 0 {
		selectedFields, err := resolveColumns(queryParams.Fields, queryParams)
		if err != nil {
			return "", nil, err
		}
		fields = strings.Join(selectedFields, ", ")
	} else if len(queryParams.Except) > 0 {
		if _, err := resolveColumns(queryParams.Except, queryParams); err != nil {
			return "", nil, err
		}
		allFields := getAllColumns[T]()
		fields = strings.Join(excludeFieldsSlice(allFields, queryParams.Except), ", ")
	}
	var query string
	if len(isCount) > 0 && isCount[0] {
		query = fmt.Sprintf("SELECT COUNT(*) as total_rows FROM %s", tableName)
	} else {
		query = fmt.Sprintf("SELECT %s FROM %s", fields, tableName)
	}
	joins, err := resolveSQLFragments("join", queryParams.Join, queryParams.AllowedJoins, queryParams.AllowUnsafeRawSQL)
	if err != nil {
		return "", nil, err
	}
	if len(joins) > 0 {
		query += " " + strings.Join(joins, " ")
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
		groupBy, err := resolveColumns(queryParams.GroupBy, queryParams)
		if err != nil {
			return "", nil, err
		}
		query += " GROUP BY " + strings.Join(groupBy, ", ")
	}
	if queryParams.Having != "" {
		having, err := resolveSQLFragment("having", queryParams.Having, queryParams.AllowedHaving, queryParams.AllowUnsafeRawSQL)
		if err != nil {
			return "", nil, err
		}
		query += " HAVING " + having
	}
	if queryParams.Sort.Field != "" {
		sortField, err := resolveColumn(queryParams.Sort.Field, queryParams)
		if err != nil {
			return "", nil, err
		}
		sortDir := strings.ToUpper(queryParams.Sort.Dir)
		if sortDir != "ASC" && sortDir != "DESC" {
			sortDir = "ASC"
		}
		query += fmt.Sprintf(" ORDER BY %s %s", sortField, sortDir)
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
	if err := validateIdentifier(tableName); err != nil {
		return "", nil, err
	}
	fields, err := DirtyFields(data)
	if err != nil {
		return "", nil, err
	}
	if len(queryParams.Fields) > 0 {
		fields = filterFields(fields, queryParams.Fields)
	} else if len(queryParams.Except) > 0 {
		fields = excludeFields(fields, queryParams.Except)
	}
	if len(fields) == 0 {
		return "", nil, errors.New("insert requires at least one field")
	}
	columns := make([]string, 0, len(fields))
	placeholders := make([]string, 0, len(fields))
	values := make(map[string]any, len(fields))
	for _, col := range sortedStringKeys(fields) {
		val := fields[col]
		if err := validateIdentifier(col); err != nil {
			return "", nil, err
		}
		columns = append(columns, col)
		placeholders = append(placeholders, ":"+col)
		values[col] = val
	}
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, strings.Join(columns, ", "), strings.Join(placeholders, ", "))
	return query, values, nil
}

func (r *repository[T]) buildDeleteQuery(condition any) (string, map[string]any, error) {
	tableName := r.getTableName()
	if err := validateIdentifier(tableName); err != nil {
		return "", nil, err
	}
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
	if strings.TrimSpace(whereClause) == "" {
		return "", nil, errors.New("delete requires at least one condition")
	}
	query := fmt.Sprintf("DELETE FROM %s WHERE %s", tableName, whereClause)
	return query, params, nil
}

func (r *repository[T]) buildUpdateQuery(data any, condition map[string]any, queryParams QueryParams) (string, map[string]any, error) {
	var err error
	tableName := r.getTableName()
	if err := validateIdentifier(tableName); err != nil {
		return "", nil, err
	}
	var fields map[string]any
	pkColumn := r.getPrimaryKey()
	switch t := data.(type) {
	case Entity:
		fields, err = DirtyFields(t)
		if err != nil {
			return "", nil, err
		}
	case map[string]any:
		fields = cloneMap(t)
	case *map[string]any:
		if t == nil {
			return "", nil, errors.New("invalid nil map pointer for update query")
		}
		fields = cloneMap(*t)
	default:
		return "", nil, fmt.Errorf("invalid data type for update query: %T", t)
	}
	delete(fields, pkColumn)
	if len(queryParams.Fields) > 0 {
		fields = filterFields(fields, queryParams.Fields)
	} else if len(queryParams.Except) > 0 {
		fields = excludeFields(fields, queryParams.Except)
	}
	if len(fields) == 0 {
		return "", nil, errors.New("update requires at least one field")
	}
	setClauses := make([]string, 0, len(fields))
	values := make(map[string]any, len(fields)+1)
	for _, col := range sortedStringKeys(fields) {
		val := fields[col]
		if err := validateIdentifier(col); err != nil {
			return "", nil, err
		}
		switch val := val.(type) {
		case SQLExpression:
			if strings.TrimSpace(string(val)) == "" {
				return "", nil, errors.New("empty SQL expression")
			}
			setClauses = append(setClauses, fmt.Sprintf("%s = %s", col, string(val)))
		case string:
			if strings.HasPrefix(val, ExprPrefix) {
				return "", nil, errors.New("ExprPrefix is disabled for raw updates; use squealx.Expr with trusted application SQL")
			}
			setClauses = append(setClauses, fmt.Sprintf("%s = :%s", col, col))
			values[col] = val
		default:
			setClauses = append(setClauses, fmt.Sprintf("%s = :%s", col, col))
			values[col] = val
		}
	}
	whereClause := ""
	if condition != nil {
		condClause, condParams, err := buildWhereClauseWithPrefix(condition, "where_")
		if err != nil {
			return "", nil, err
		}
		whereClause = condClause
		for k, v := range condParams {
			values[k] = v
		}
	}
	if strings.TrimSpace(whereClause) == "" {
		return "", nil, errors.New("update requires at least one condition")
	}
	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s", tableName, strings.Join(setClauses, ", "), whereClause)
	return query, values, nil
}

func (r *repository[T]) getPrimaryKey() string {
	var t T
	switch t := any(t).(type) {
	case Entity:
		return t.PrimaryKey()
	default:
		return r.primaryKey
	}
}

// Helper functions to convert between any type and map[string]any
func toMap(v any) (map[string]any, error) {
	switch value := v.(type) {
	case map[string]any:
		return value, nil
	case *map[string]any:
		if value == nil {
			return nil, nil
		}
		return *value, nil
	}
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
	switch target := dest.(type) {
	case *map[string]any:
		if target == nil {
			return errors.New("preload: nil map destination")
		}
		normalizePreloadedMap(m)
		*target = m
		return nil
	case **map[string]any:
		if target == nil {
			return errors.New("preload: nil map destination")
		}
		normalizePreloadedMap(m)
		copyOfMap := m
		*target = &copyOfMap
		return nil
	}
	// marshal the map then unmarshal into the destination type
	bt, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return json.Unmarshal(bt, dest)
}

func execReturnArgs(data any, fallback map[string]any) any {
	switch data := data.(type) {
	case map[string]any:
		return &data
	case *map[string]any:
		return data
	}
	v := reflectValueOf(data)
	if v.IsValid() && v.Kind() == reflect.Ptr {
		return data
	}
	if fallback != nil {
		return &fallback
	}
	return &data
}

func reflectValueOf(v any) reflect.Value {
	return reflect.ValueOf(v)
}

func mapValue(m map[string]any, key string) (any, bool) {
	if val, ok := m[key]; ok {
		return val, true
	}
	lower := strings.ToLower(key)
	if val, ok := m[lower]; ok {
		return val, true
	}
	for k, val := range m {
		if strings.EqualFold(k, key) {
			return val, true
		}
	}
	return nil, false
}

func resolveColumns(columns []string, queryParams QueryParams) ([]string, error) {
	resolved := make([]string, 0, len(columns))
	for _, column := range columns {
		value, err := resolveColumn(column, queryParams)
		if err != nil {
			return nil, err
		}
		resolved = append(resolved, value)
	}
	return resolved, nil
}

func resolveColumn(column string, queryParams QueryParams) (string, error) {
	if resolved, ok := queryParams.AllowedFields[column]; ok {
		return resolved, nil
	}
	if len(queryParams.AllowedFields) > 0 {
		return "", fmt.Errorf("field %q is not allowlisted", column)
	}
	if err := validateIdentifier(column); err != nil {
		return "", err
	}
	return column, nil
}

func resolveSQLFragments(kind string, keys []string, allowed map[string]string, allowUnsafe bool) ([]string, error) {
	fragments := make([]string, 0, len(keys))
	for _, key := range keys {
		fragment, err := resolveSQLFragment(kind, key, allowed, allowUnsafe)
		if err != nil {
			return nil, err
		}
		fragments = append(fragments, fragment)
	}
	return fragments, nil
}

func resolveSQLFragment(kind, key string, allowed map[string]string, allowUnsafe bool) (string, error) {
	if fragment, ok := allowed[key]; ok {
		return fragment, nil
	}
	if allowUnsafe {
		return key, nil
	}
	return "", fmt.Errorf("%s %q is not allowlisted", kind, key)
}

func validateRelation(rel Relation) error {
	return validateRelationWithPath(rel, strings.Split(rel.With, "."))
}

func validateRelationWithPath(rel Relation, path []string) error {
	if err := validateRelationPath(path); err != nil {
		return err
	}
	for _, identifier := range []string{rel.LocalField, rel.RelatedField} {
		if err := validateIdentifier(identifier); err != nil {
			return err
		}
	}
	if rel.JoinTable != "" {
		for _, identifier := range []string{rel.JoinTable, rel.JoinWithLocalField, rel.JoinWithRelatedField} {
			if err := validateIdentifier(identifier); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateRelationPath(path []string) error {
	for _, part := range path {
		if err := validateIdentifier(part); err != nil {
			return err
		}
	}
	return nil
}
