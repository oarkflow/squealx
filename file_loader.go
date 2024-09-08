package squealx

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/squealx/utils/xstrings"
)

type Query struct {
	Doc        string `json:"doc"`
	Name       string `json:"name"`
	Query      string `json:"query"`
	Connection string `json:"connection"`
}

type FileLoader struct {
	dir     string
	file    string
	queries map[string]*Query
	mu      *sync.RWMutex
	db      *DB
}

func (f *FileLoader) DriverName(db *DB) string {
	return db.DriverName()
}

func (f *FileLoader) GetQuery(query string) *Query {
	if sqlQuery, exists := f.queries[query]; exists {
		return sqlQuery
	}
	return nil
}

func (f *FileLoader) Rebind(db *DB, sql string) string {
	st := f.GetQuery(sql)
	if st == nil {
		return db.Rebind(sql)
	}
	return db.Rebind(st.Query)
}

func (f *FileLoader) BindNamed(db *DB, sql string, args any) (string, []any, error) {
	st := f.GetQuery(sql)
	if st == nil {
		return db.BindNamed(sql, args)
	}
	return db.BindNamed(st.Query, args)
}

func (f *FileLoader) In(db *DB, query string, args ...any) (string, []any, error) {
	st := f.GetQuery(query)
	if st == nil {
		return db.In(query, args...)
	}
	return db.In(st.Query, args...)
}

func (f *FileLoader) QueryxContext(db *DB, ctx context.Context, query string, args ...any) (*Rows, error) {
	st := f.GetQuery(query)
	if st == nil {
		return db.QueryxContext(ctx, query, args...)
	}
	return db.QueryxContext(ctx, st.Query, args...)
}

func (f *FileLoader) QueryRowxContext(db *DB, ctx context.Context, query string, args ...any) *Row {
	st := f.GetQuery(query)
	if st != nil {
		return db.QueryRowxContext(ctx, st.Query, args...)
	}
	return db.QueryRowxContext(ctx, query, args...)
}

func (f *FileLoader) GetContext(db *DB, ctx context.Context, dest any, sql string, args ...any) error {
	st := f.GetQuery(sql)
	if st != nil {
		return db.GetContext(ctx, dest, st.Query, args...)
	}
	return db.GetContext(ctx, dest, sql, args...)
}

func (f *FileLoader) SelectContext(db *DB, ctx context.Context, dest any, sql string, args ...any) error {
	st := f.GetQuery(sql)
	if st != nil {
		return db.SelectContext(ctx, dest, st.Query, args...)
	}
	return db.SelectContext(ctx, dest, sql, args...)
}

func (f *FileLoader) Get(db *DB, dest any, sql string, args ...any) error {
	st := f.GetQuery(sql)
	if st != nil {
		return db.Get(dest, st.Query, args...)
	}
	return db.Get(dest, sql, args...)
}

func (f *FileLoader) MustExecContext(db *DB, ctx context.Context, sql string, args ...any) sql.Result {
	st := f.GetQuery(sql)
	if st != nil {
		return db.MustExecContext(ctx, st.Query, args...)
	}
	return db.MustExecContext(ctx, sql, args...)
}

func (f *FileLoader) PreparexContext(db *DB, ctx context.Context, sql string) (*Stmt, error) {
	st := f.GetQuery(sql)
	if st != nil {
		return db.PreparexContext(ctx, st.Query)
	}
	return db.PreparexContext(ctx, sql)
}

func (f *FileLoader) Select(db *DB, dest any, sql string, args ...any) error {
	st := f.GetQuery(sql)
	if st != nil {
		return db.Select(dest, st.Query, args...)
	}
	return db.Select(dest, sql, args...)
}

func (f *FileLoader) PrepareNamedContext(db *DB, ctx context.Context, sql string) (*NamedStmt, error) {
	st := f.GetQuery(sql)
	if st != nil {
		return db.PrepareNamedContext(ctx, st.Query)
	}
	return db.PrepareNamedContext(ctx, sql)
}

func (f *FileLoader) PrepareNamed(db *DB, sql string) (*NamedStmt, error) {
	st := f.GetQuery(sql)
	if st != nil {
		return db.PrepareNamed(st.Query)
	}
	return db.PrepareNamed(sql)
}

func (f *FileLoader) Preparex(db *DB, sql string) (*Stmt, error) {
	st := f.GetQuery(sql)
	if st != nil {
		return db.Preparex(st.Query)
	}
	return db.Preparex(sql)
}

func (f *FileLoader) NamedExec(db *DB, sql string, args any) (sql.Result, error) {
	st := f.GetQuery(sql)
	if st != nil {
		return db.NamedExec(st.Query, args)
	}
	return db.NamedExec(sql, args)
}

func (f *FileLoader) NamedExecContext(db *DB, ctx context.Context, sql string, args any) (sql.Result, error) {
	st := f.GetQuery(sql)
	if st != nil {
		return db.NamedExecContext(ctx, st.Query, args)
	}
	return db.NamedExecContext(ctx, sql, args)
}

func (f *FileLoader) MustExec(db *DB, sql string, args ...any) sql.Result {
	st := f.GetQuery(sql)
	if st != nil {
		return db.MustExec(st.Query, args...)
	}
	return db.MustExec(sql, args...)
}

func (f *FileLoader) NamedQuery(db *DB, sql string, args any) (*Rows, error) {
	st := f.GetQuery(sql)
	if st != nil {
		return db.NamedQuery(st.Query, args)
	}
	return db.NamedQuery(sql, args)
}

func (f *FileLoader) InGet(db *DB, dest any, sql string, args ...any) error {
	st := f.GetQuery(sql)
	if st != nil {
		return db.InGet(dest, st.Query, args...)
	}
	return db.InGet(dest, sql, args...)
}

func (f *FileLoader) InSelect(db *DB, dest any, sql string, args ...any) error {
	st := f.GetQuery(sql)
	if st != nil {
		return db.InSelect(dest, st.Query, args...)
	}
	return db.InSelect(dest, sql, args...)
}

func (f *FileLoader) InExec(db *DB, sql string, args ...any) (sql.Result, error) {
	st := f.GetQuery(sql)
	if st != nil {
		return db.InExec(st.Query, args...)
	}
	return db.InExec(sql, args...)
}

func (f *FileLoader) MustInExec(db *DB, sql string, args ...any) sql.Result {
	st := f.GetQuery(sql)
	if st != nil {
		return db.MustExec(st.Query, args...)
	}
	return db.MustExec(sql, args...)
}

func (f *FileLoader) Queryx(db *DB, query string, args ...any) (*Rows, error) {
	st := f.GetQuery(query)
	if st != nil {
		return db.Queryx(st.Query, args...)
	}
	return db.Queryx(query, args...)
}

func (f *FileLoader) QueryRowx(db *DB, query string, args ...any) *Row {
	st := f.GetQuery(query)
	if st != nil {
		return db.QueryRowx(st.Query, args...)
	}
	return db.QueryRowx(query, args...)
}

func (f *FileLoader) Query(db *DB, query string, args ...any) (SQLRows, error) {
	st := f.GetQuery(query)
	if st != nil {
		return db.Query(st.Query, args...)
	}
	return db.Query(query, args...)
}

func (f *FileLoader) QueryContext(db *DB, ctx context.Context, query string, args ...any) (SQLRows, error) {
	st := f.GetQuery(query)
	if st != nil {
		return db.QueryContext(ctx, st.Query, args...)
	}
	return db.QueryContext(ctx, query, args...)
}

func (f *FileLoader) QueryRow(db *DB, query string, args ...any) SQLRow {
	st := f.GetQuery(query)
	if st != nil {
		return db.QueryRow(st.Query, args...)
	}
	return db.QueryRow(query, args...)
}

func (f *FileLoader) Driver(db *DB) driver.Driver {
	return db.Driver()
}

func (f *FileLoader) SetConnMaxLifetime(db *DB, duration time.Duration) {
	db.SetConnMaxLifetime(duration)
}

func (f *FileLoader) SetConnMaxIdleTime(db *DB, duration time.Duration) {
	db.SetConnMaxIdleTime(duration)
}

func (f *FileLoader) SetMaxIdleConns(db *DB, maxIdleConns int) {
	db.SetMaxIdleConns(maxIdleConns)
}

func (f *FileLoader) SetMaxOpenConns(db *DB, maxOpenConns int) {
	db.SetMaxOpenConns(maxOpenConns)
}

func (f *FileLoader) Stats(db *DB) sql.DBStats {
	return db.Stats()
}

func (f *FileLoader) QueryRowContext(db *DB, ctx context.Context, query string, args ...any) SQLRow {
	st := f.GetQuery(query)
	if st != nil {
		return db.QueryRowContext(ctx, st.Query, args...)
	}
	return db.QueryRowContext(ctx, query, args...)
}

func (f *FileLoader) Exec(db *DB, query string, args ...any) (sql.Result, error) {
	st := f.GetQuery(query)
	if st != nil {
		return db.Exec(st.Query, args...)
	}
	return db.Exec(query, args...)
}

func (f *FileLoader) ExecContext(db *DB, ctx context.Context, query string, args ...any) (sql.Result, error) {
	st := f.GetQuery(query)
	if st != nil {
		return db.ExecContext(ctx, st.Query, args...)
	}
	return db.ExecContext(ctx, query, args...)
}

func (f *FileLoader) Prepare(db *DB, query string) (SQLStmt, error) {
	st := f.GetQuery(query)
	if st != nil {
		return db.Prepare(st.Query)
	}
	return db.Prepare(query)
}

func (f *FileLoader) PrepareContext(db *DB, ctx context.Context, query string) (SQLStmt, error) {
	st := f.GetQuery(query)
	if st != nil {
		return db.PrepareContext(ctx, st.Query)
	}
	return db.PrepareContext(ctx, query)
}

func (f *FileLoader) Ping(db *DB) error {
	return db.Ping()
}

func (f *FileLoader) PingContext(db *DB, ctx context.Context) error {
	return db.PingContext(ctx)
}

func (f *FileLoader) Begin(db *DB) (SQLTx, error) {
	return db.Begin()
}

func (f *FileLoader) BeginTx(db *DB, ctx context.Context, opts *sql.TxOptions) (SQLTx, error) {
	return db.BeginTx(ctx, opts)
}

func (f *FileLoader) Conn(db *DB, ctx context.Context) (SQLConn, error) {
	return db.Conn(ctx)
}

func (f *FileLoader) Close(db *DB) error {
	return db.Close()
}

func (f *FileLoader) Queries() map[string]*Query {
	return f.queries
}

func LoadFromFile(file string) (*FileLoader, error) {
	fileLoader := &FileLoader{
		file:    file,
		mu:      &sync.RWMutex{},
		queries: make(map[string]*Query),
	}
	content, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	fileLoader.queries = scanContent(xstrings.FromByte(content))
	return fileLoader, nil
}

func LoadFromDir(dir string) (*FileLoader, error) {
	fileLoader := &FileLoader{
		dir:     dir,
		mu:      &sync.RWMutex{},
		queries: make(map[string]*Query),
	}
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".sql") {
			sqlFile := filepath.Join(dir, file.Name())
			content, err := os.ReadFile(sqlFile)
			if err != nil {
				return nil, err
			}
			queries := scanContent(xstrings.FromByte(content))
			for name, query := range queries {
				fileLoader.queries[name] = query
			}
		}
	}
	return fileLoader, nil
}

var (
	sqlTemplateRE        = regexp.MustCompile(`(?s)--\s*sql-name:\s*(.+?)\s*\n(.*?)\s*--\s*sql-end`)
	docTemplateRE        = regexp.MustCompile(`(?s)--\s*doc:\s*(.+?)\s*\n`)
	connectionTemplateRE = regexp.MustCompile(`(?s)--\s*connection:\s*(.+?)\s*\n`)
)

func scanContent(content string) map[string]*Query {
	queries := make(map[string]*Query)
	matches := sqlTemplateRE.FindAllStringSubmatch(content, -1)
	for _, match := range matches {
		name := strings.TrimSpace(match[1])
		query := strings.TrimSpace(match[2])
		q := &Query{Name: name}
		docMatches := docTemplateRE.FindStringSubmatch(query)
		connectionMatches := connectionTemplateRE.FindStringSubmatch(query)
		if len(docMatches) == 2 {
			query = docTemplateRE.ReplaceAllString(query, "")
			q.Doc = docMatches[1]
		}
		if len(connectionMatches) == 2 {
			query = connectionTemplateRE.ReplaceAllString(query, "")
			q.Connection = connectionMatches[1]
		}
		query = strings.TrimSpace(query)
		q.Query = query
		if name != "" && query != "" {
			queries[name] = q
		}
	}
	return queries
}
