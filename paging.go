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
	NextPage     int   `json:"next_page" query:"next_page" form:"next_page"`
}

type Paging struct {
	OrderBy []string `json:"order_by" query:"order_by" form:"order_by"`
	Limit   int      `json:"limit" query:"limit" form:"limit"`
	Page    int      `json:"page" query:"page" form:"page"`
	// MaxPageLimit, when > 0, caps Limit. It is never read from user input.
	MaxPageLimit int `json:"-" query:"-" form:"-"`
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

const defaultPageLimit = 20

// normalize returns the effective page, limit and offset without mutating p.
func (p *Paging) normalize() (page, limit, offset int, err error) {
	var in Paging
	if p != nil {
		in = *p
	}
	if in.Limit < 0 {
		return 0, 0, 0, fmt.Errorf("invalid page limit %d", in.Limit)
	}
	if in.Page < 0 {
		return 0, 0, 0, fmt.Errorf("invalid page number %d", in.Page)
	}
	limit = in.Limit
	if limit == 0 {
		limit = defaultPageLimit
	}
	if in.MaxPageLimit > 0 && limit > in.MaxPageLimit {
		limit = in.MaxPageLimit
	}
	page = in.Page
	if page < 1 {
		page = 1
	}
	if page > 1 && (page-1) > math.MaxInt/limit {
		return 0, 0, 0, fmt.Errorf("page %d out of range", page)
	}
	offset = (page - 1) * limit
	return page, limit, offset, nil
}

// scanTopLevel walks the query and calls fn for every word token found outside
// quotes, comments and parentheses. fn receives the token start/end offsets;
// returning true stops the scan.
func scanTopLevel(query string, fn func(word string, start, end int) bool) {
	depth := 0
	n := len(query)
	for i := 0; i < n; {
		c := query[i]
		switch {
		case c == '\'' || c == '"' || c == '`':
			j := i + 1
			for j < n {
				if query[j] == c {
					if j+1 < n && query[j+1] == c {
						j += 2
						continue
					}
					break
				}
				j++
			}
			i = j + 1
		case c == '[':
			j := strings.IndexByte(query[i:], ']')
			if j < 0 {
				i = n
			} else {
				i += j + 1
			}
		case c == '-' && i+1 < n && query[i+1] == '-':
			j := strings.IndexByte(query[i:], '\n')
			if j < 0 {
				i = n
			} else {
				i += j + 1
			}
		case c == '/' && i+1 < n && query[i+1] == '*':
			j := strings.Index(query[i+2:], "*/")
			if j < 0 {
				i = n
			} else {
				i += j + 4
			}
		case c == '(':
			depth++
			i++
		case c == ')':
			if depth > 0 {
				depth--
			}
			i++
		case c == '_' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= 0x80 || c >= '0' && c <= '9':
			j := i
			for j < n {
				d := query[j]
				if d == '_' || d == '$' || d >= 'A' && d <= 'Z' || d >= 'a' && d <= 'z' || d >= '0' && d <= '9' || d >= 0x80 {
					j++
					continue
				}
				break
			}
			// a word preceded by ':' is a named parameter, preceded by '.' a qualified name.
			prev := byte(0)
			if i > 0 {
				prev = query[i-1]
			}
			if depth == 0 && prev != ':' && prev != '.' && (j >= n || query[j] != '.') {
				if fn(query[i:j], i, j) {
					return
				}
			}
			i = j
		default:
			i++
		}
	}
}

// topLevelOrderByIndex returns the index of the top-level ORDER BY, or -1.
func topLevelOrderByIndex(query string) int {
	idx, last := -1, -1
	scanTopLevel(query, func(w string, s, _ int) bool {
		if strings.EqualFold(w, "ORDER") {
			last = s
		} else if strings.EqualFold(w, "BY") && last >= 0 {
			idx = last
			return true
		} else {
			last = -1
		}
		return false
	})
	return idx
}

// stripPaging removes a trailing semicolon and any top-level LIMIT/OFFSET/FETCH clause.
func stripPaging(query string) string {
	query = strings.TrimSpace(query)
	query = strings.TrimSpace(strings.TrimRight(query, ";"))
	cut := -1
	scanTopLevel(query, func(w string, s, _ int) bool {
		if strings.EqualFold(w, "LIMIT") || strings.EqualFold(w, "OFFSET") || strings.EqualFold(w, "FETCH") {
			cut = s
			return true
		}
		return false
	})
	if cut >= 0 {
		query = strings.TrimSpace(query[:cut])
	}
	return query
}

func prepareRawQuery(db *DB, query string, paging *Paging) (string, error) {
	if paging != nil && len(paging.OrderBy) > 0 {
		if err := validateOrderBy(paging.OrderBy); err != nil {
			return "", err
		}
	}
	if _, _, _, err := paging.normalize(); err != nil {
		return "", err
	}
	q := stripPaging(query)
	hasOrder := topLevelOrderByIndex(q) >= 0
	if paging != nil && len(paging.OrderBy) > 0 {
		if hasOrder {
			q += ", " + strings.Join(paging.OrderBy, ", ")
		} else {
			q += " ORDER BY " + strings.Join(paging.OrderBy, ", ")
			hasOrder = true
		}
	}
	driver := ""
	if db != nil {
		driver = db.driverName
	}
	switch driver {
	case "mysql", "nrmysql", "mariadb":
		q += " LIMIT :offset, :limit"
	case "sql-server", "sqlserver", "mssql", "ms-sql":
		if !hasOrder {
			q += " ORDER BY (SELECT NULL)"
		}
		q += " OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY"
	default:
		q += " LIMIT :limit OFFSET :offset"
	}
	return q, nil
}

// countQuery builds the count SQL; ORDER BY is dropped because some engines
// reject it inside a derived table.
func countQuery(query string) string {
	q := stripPaging(query)
	if i := topLevelOrderByIndex(q); i >= 0 {
		q = strings.TrimSpace(q[:i])
	}
	return "SELECT count(*) FROM (" + q + ") AS count_query"
}

// Pages Endpoint for pagination
func Pages(p *Param, result any) (paginator *Pagination, err error) {
	db := p.DB
	page, limit, offset, err := p.Paging.normalize()
	if err != nil {
		return nil, err
	}
	sql, err := prepareRawQuery(db, p.Query, p.Paging)
	if err != nil {
		return nil, err
	}
	countSQL := countQuery(p.Query)

	// Run the count concurrently; the buffered channel guarantees no leak.
	type countOutcome struct {
		n   int64
		err error
	}
	countResult := make(chan countOutcome, 1)
	countParams := p.Param
	go func() {
		var n int64
		err := db.NamedGet(&n, countSQL, countParams)
		countResult <- countOutcome{n, err}
	}()

	params := cloneMap(p.Param)
	if params == nil {
		params = make(map[string]any)
	}
	params["limit"] = limit
	params["offset"] = offset
	err = db.NamedSelect(result, sql, params)
	oc := <-countResult
	if err != nil {
		return nil, err
	}
	if oc.err != nil {
		return nil, oc.err
	}
	count := oc.n
	total := int(math.Ceil(float64(count) / float64(limit)))

	paginator = &Pagination{
		TotalRecords: count,
		Page:         page,
		Offset:       offset,
		Limit:        limit,
		TotalPage:    total,
		PrevPage:     page,
		NextPage:     page,
	}
	if page > 1 {
		paginator.PrevPage = page - 1
	}
	if page < paginator.TotalPage {
		paginator.NextPage = page + 1
	}
	return paginator, nil
}

func validateOrderBy(orderBy []string) error {
	for _, order := range orderBy {
		parts := strings.Fields(order)
		if len(parts) == 0 || len(parts) > 2 {
			return fmt.Errorf("unsafe order by expression %q", order)
		}
		if err := validateIdentifier(parts[0]); err != nil {
			return err
		}
		if len(parts) == 2 {
			dir := strings.ToUpper(parts[1])
			if dir != "ASC" && dir != "DESC" {
				return fmt.Errorf("unsafe order direction %q", parts[1])
			}
		}
	}
	return nil
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
