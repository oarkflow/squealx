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

type FileLoader struct {
	dir     string
	file    string
	queries map[string]string
	mu      *sync.RWMutex
	db      *DB
}

func (f *FileLoader) DriverName(db *DB) string {
	return db.DriverName()
}

func (f *FileLoader) GetQuery(query string) string {
	if sqlQuery, exists := f.queries[query]; exists {
		return sqlQuery
	}
	return query
}

func (f *FileLoader) Rebind(db *DB, s string) string {
	s = f.GetQuery(s)
	return db.Rebind(s)
}

func (f *FileLoader) BindNamed(db *DB, s string, a any) (string, []any, error) {
	s = f.GetQuery(s)
	return db.BindNamed(s, a)
}

func (f *FileLoader) In(db *DB, query string, args ...any) (string, []any, error) {
	query = f.GetQuery(query)
	return db.In(query, args...)
}

func (f *FileLoader) QueryxContext(db *DB, ctx context.Context, query string, args ...any) (*Rows, error) {
	query = f.GetQuery(query)
	return db.QueryxContext(ctx, query, args...)
}

func (f *FileLoader) QueryRowxContext(db *DB, ctx context.Context, query string, args ...any) *Row {
	query = f.GetQuery(query)
	return db.QueryRowxContext(ctx, query, args...)
}

func (f *FileLoader) GetContext(db *DB, ctx context.Context, a any, s string, a2 ...any) error {
	s = f.GetQuery(s)
	return db.GetContext(ctx, a, s, a2...)
}

func (f *FileLoader) SelectContext(db *DB, ctx context.Context, a any, s string, a2 ...any) error {
	s = f.GetQuery(s)
	return db.SelectContext(ctx, a, s, a2...)
}

func (f *FileLoader) Get(db *DB, a any, s string, a2 ...any) error {
	s = f.GetQuery(s)
	return db.Get(a, s, a2...)
}

func (f *FileLoader) MustExecContext(db *DB, ctx context.Context, s string, a ...any) sql.Result {
	s = f.GetQuery(s)
	return db.MustExecContext(ctx, s, a...)
}

func (f *FileLoader) PreparexContext(db *DB, ctx context.Context, s string) (*Stmt, error) {
	s = f.GetQuery(s)
	return db.PreparexContext(ctx, s)
}

func (f *FileLoader) Select(db *DB, a any, s string, a2 ...any) error {
	s = f.GetQuery(s)
	return db.Select(a, s, a2...)
}

func (f *FileLoader) PrepareNamedContext(db *DB, ctx context.Context, s string) (*NamedStmt, error) {
	s = f.GetQuery(s)
	return db.PrepareNamedContext(ctx, s)
}

func (f *FileLoader) PrepareNamed(db *DB, s string) (*NamedStmt, error) {
	s = f.GetQuery(s)
	return db.PrepareNamed(s)
}

func (f *FileLoader) Preparex(db *DB, s string) (*Stmt, error) {
	s = f.GetQuery(s)
	return db.Preparex(s)
}

func (f *FileLoader) NamedExec(db *DB, s string, a any) (sql.Result, error) {
	s = f.GetQuery(s)
	return db.NamedExec(s, a)
}

func (f *FileLoader) NamedExecContext(db *DB, ctx context.Context, s string, a any) (sql.Result, error) {
	s = f.GetQuery(s)
	return db.NamedExecContext(ctx, s, a)
}

func (f *FileLoader) MustExec(db *DB, s string, a ...any) sql.Result {
	s = f.GetQuery(s)
	return db.MustExec(s, a...)
}

func (f *FileLoader) NamedQuery(db *DB, s string, a any) (*Rows, error) {
	s = f.GetQuery(s)
	return db.NamedQuery(s, a)
}

func (f *FileLoader) InGet(db *DB, a any, s string, a2 ...any) error {
	s = f.GetQuery(s)
	return db.InGet(a, s, a2...)
}

func (f *FileLoader) InSelect(db *DB, a any, s string, a2 ...any) error {
	s = f.GetQuery(s)
	return db.InSelect(a, s, a2...)
}

func (f *FileLoader) InExec(db *DB, s string, a ...any) (sql.Result, error) {
	s = f.GetQuery(s)
	return db.InExec(s, a...)
}

func (f *FileLoader) MustInExec(db *DB, s string, a ...any) sql.Result {
	s = f.GetQuery(s)
	return db.MustExec(s, a...)
}

func (f *FileLoader) Queryx(db *DB, query string, args ...any) (*Rows, error) {
	query = f.GetQuery(query)
	return db.Queryx(query, args...)
}

func (f *FileLoader) QueryRowx(db *DB, query string, args ...any) *Row {
	query = f.GetQuery(query)
	return db.QueryRowx(query, args...)
}

func (f *FileLoader) Query(db *DB, query string, args ...any) (SQLRows, error) {
	query = f.GetQuery(query)
	return db.Query(query, args...)
}

func (f *FileLoader) QueryContext(db *DB, ctx context.Context, query string, args ...any) (SQLRows, error) {
	query = f.GetQuery(query)
	return db.QueryContext(ctx, query, args...)
}

func (f *FileLoader) QueryRow(db *DB, query string, args ...any) SQLRow {
	query = f.GetQuery(query)
	return db.QueryRow(query, args...)
}

func (f *FileLoader) Driver(db *DB) driver.Driver {
	return db.Driver()
}

func (f *FileLoader) SetConnMaxLifetime(db *DB, d time.Duration) {
	db.SetConnMaxLifetime(d)
}

func (f *FileLoader) SetConnMaxIdleTime(db *DB, d time.Duration) {
	db.SetConnMaxIdleTime(d)
}

func (f *FileLoader) SetMaxIdleConns(db *DB, n int) {
	db.SetMaxIdleConns(n)
}

func (f *FileLoader) SetMaxOpenConns(db *DB, n int) {
	db.SetMaxOpenConns(n)
}

func (f *FileLoader) Stats(db *DB) sql.DBStats {
	return db.Stats()
}

func (f *FileLoader) QueryRowContext(db *DB, ctx context.Context, query string, args ...any) SQLRow {
	query = f.GetQuery(query)
	return db.QueryRowContext(ctx, query, args...)
}

func (f *FileLoader) Exec(db *DB, query string, args ...any) (sql.Result, error) {
	query = f.GetQuery(query)
	return db.Exec(query, args...)
}

func (f *FileLoader) ExecContext(db *DB, ctx context.Context, query string, args ...any) (sql.Result, error) {
	query = f.GetQuery(query)
	return db.ExecContext(ctx, query, args...)
}

func (f *FileLoader) Prepare(db *DB, query string) (SQLStmt, error) {
	query = f.GetQuery(query)
	return db.Prepare(query)
}

func (f *FileLoader) PrepareContext(db *DB, ctx context.Context, query string) (SQLStmt, error) {
	query = f.GetQuery(query)
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

func (f *FileLoader) Queries() map[string]string {
	return f.queries
}

func LoadFromFile(file string) (*FileLoader, error) {
	fileLoader := &FileLoader{
		file:    file,
		mu:      &sync.RWMutex{},
		queries: make(map[string]string),
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
		queries: make(map[string]string),
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
	sqlTemplateRE = regexp.MustCompile(`(?s)--\s*sql-name:\s*(.+?)\s*\n(.*?)\s*--\s*sql-end`)
)

func scanContent(content string) map[string]string {
	queries := make(map[string]string)
	matches := sqlTemplateRE.FindAllStringSubmatch(content, -1)
	for _, match := range matches {
		name := strings.TrimSpace(match[1])
		query := strings.TrimSpace(match[2])
		if name != "" && query != "" {
			queries[name] = query
		}
	}
	return queries
}
