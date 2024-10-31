package squealx

import (
	"fmt"
	"math"
	"strings"
)

type Pagination struct {
	TotalRecords int64 `json:"total_records" query:"total_records" form:"total_records"`
	TotalPage    int   `json:"total_page" query:"total_page" form:"total_page"`
	Offset       int   `json:"offset" query:"offset" form:"offset"`
	Limit        int   `json:"limit" query:"limit" form:"limit"`
	Page         int   `json:"page" query:"page" form:"page"`
	PrevPage     int   `json:"prev_page" query:"prev_page" form:"prev_page"`
	NextPage     int   `json:"next_page" query:"" form:""`
}

type Paging struct {
	Limit  int `json:"limit" query:"limit" form:"limit"`
	Page   int `json:"page" query:"page" form:"page"`
	offset int
}

type PaginatedResponse struct {
	Items      any         `json:"data"`
	Pagination *Pagination `json:"pagination"`
	Error      error       `json:"error,omitempty"`
}

type Param struct {
	DB     *DB
	Query  string
	Param  map[string]any
	Paging *Paging
}

func prepareRawQuery(db *DB, query string, paging *Paging) string {
	var (
		defPage  = 1
		defLimit = 20
	)

	// if not defined
	if paging == nil {
		paging = &Paging{}
	}

	// limit
	if paging.Limit == 0 {
		paging.Limit = defLimit
	}
	// page
	if paging.Page < 1 {
		paging.Page = defPage
	} else if paging.Page > 1 {
		paging.offset = (paging.Page - 1) * paging.Limit
	}
	queryWithoutLimit := strings.Split(query, "LIMIT")[0]
	switch db.driverName {
	case "mysql", "sqlite3", "nrmysql", "nrsqlite3", "mariadb":
		queryWithoutLimit += " LIMIT :limit, :offset"
	case "postgres", "pgx", "pgx/v4", "pgx/v5", "pq-timeouts", "cloudsqlpostgres", "ql", "nrpostgres", "cockroach":
		queryWithoutLimit += " LIMIT :limit OFFSET :offset"
	case "sql-server", "sqlserver", "mssql", "ms-sql":
		queryWithoutLimit += " LIMIT :limit, :offset"
	}
	return queryWithoutLimit
}

// Pages Endpoint for pagination
func Pages(p *Param, result any) (paginator *Pagination, err error) {
	var (
		done  = make(chan bool, 1)
		db    = p.DB
		count int64
	)

	// get all counts
	go getRawCounts(db, p.Query, done, &count, p.Param)
	sql := prepareRawQuery(db, p.Query, p.Paging)
	// get
	if p.Param == nil {
		p.Param = make(map[string]any)
	}
	p.Param["limit"] = p.Paging.Limit
	p.Param["offset"] = p.Paging.offset
	err = db.NamedSelect(result, sql, p.Param)
	if err != nil {
		return nil, err
	}
	<-done
	// total pages
	total := int(math.Ceil(float64(count) / float64(p.Paging.Limit)))

	// construct pagination
	paginator = &Pagination{
		TotalRecords: count,
		Page:         p.Paging.Page,
		Offset:       p.Paging.offset,
		Limit:        p.Paging.Limit,
		TotalPage:    total,
		PrevPage:     p.Paging.Page,
		NextPage:     p.Paging.Page,
	}

	// prev page
	if p.Paging.Page > 1 {
		paginator.PrevPage = p.Paging.Page - 1
	}
	// next page
	if p.Paging.Page != paginator.TotalPage {
		paginator.NextPage = p.Paging.Page + 1
	}

	return paginator, nil
}

func getRawCounts(db *DB, query string, done chan bool, count *int64, params map[string]any) error {
	err := db.NamedSelect(count, fmt.Sprintf("SELECT count(*) FROM (%s) AS count_query", query), params)
	done <- true
	return err
}

func (p Pagination) IsEmpty() bool {
	return p.TotalRecords <= 0
}

func Paginate(db *DB, query string, result any, paging Paging, params ...map[string]any) PaginatedResponse {
	p := &Param{
		DB:     db,
		Query:  query,
		Paging: &paging,
	}
	if len(params) > 0 {
		p.Param = params[0]
	}
	pages, err := Pages(p, result)
	if err != nil {
		return PaginatedResponse{
			Error: err,
		}
	}
	return PaginatedResponse{
		Items:      result,
		Pagination: pages,
	}
}

type PaginatedTypedResponse[T any] struct {
	Items      []T         `json:"data"`
	Pagination *Pagination `json:"pagination"`
	Error      error       `json:"error,omitempty"`
}

func PaginateTyped[T any](db *DB, query string, paging Paging, params ...map[string]any) PaginatedTypedResponse[T] {
	p := &Param{
		DB:     db,
		Query:  query,
		Paging: &paging,
	}
	if len(params) > 0 {
		p.Param = params[0]
	}
	var result []T
	pages, err := Pages(p, &result)
	if err != nil {
		return PaginatedTypedResponse[T]{
			Items: result,
			Error: err,
		}
	}
	return PaginatedTypedResponse[T]{
		Items:      result,
		Pagination: pages,
	}
}
