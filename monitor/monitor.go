package monitor

import (
	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/monitor/queries"
)

var statQueries = map[string]func() string{
	"long_running_queries":     queries.GetLongRunningQueries,
	"queries_with_lock_status": queries.GetQueriesWithLockStatus,
	"tuple_info":               queries.GetTupleInfo,
	"index_usage":              queries.GetIndexUsage,
	"cached_tables":            queries.GetCachedTables,
	"cached_total":             queries.GetCachedTotal,
	"disk_usage":               queries.GetDiskUsages,
	"relation_sizes":           queries.GetRelationSizes,
	"db_size":                  queries.GetDBSize,
	"table_and_index_bloat":    queries.GetTableAndIndexBloat,
}

func GetPostgresStats(db *squealx.DB) (map[string][]map[string]any, error) {
	collection := make(map[string][]map[string]any)
	for key, query := range statQueries {
		var data []map[string]any
		err := db.Select(&data, query())
		if err != nil {
			return nil, err
		}
		collection[key] = data
	}
	return collection, nil
}
