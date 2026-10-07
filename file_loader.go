package squealx

import (
	"context"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// Query is a named SQL statement loaded from a SQL file.
type Query struct {
	Doc        string `json:"doc"`
	Name       string `json:"name"`
	Query      string `json:"query"`
	Connection string `json:"connection"`
	Source     string `json:"source"`
	Line       int    `json:"line"`
	Hash       string `json:"hash"`
}

// ChangeSet reports how a reload changed the loaded query registry.
type ChangeSet struct {
	Added   []string `json:"added"`
	Updated []string `json:"updated"`
	Removed []string `json:"removed"`
}

// Empty reports whether the reload left the registry untouched.
func (c *ChangeSet) Empty() bool {
	return c == nil || (len(c.Added) == 0 && len(c.Updated) == 0 && len(c.Removed) == 0)
}

func (c *ChangeSet) String() string {
	if c.Empty() {
		return "no changes"
	}
	var parts []string
	if len(c.Added) > 0 {
		parts = append(parts, "added: "+strings.Join(c.Added, ", "))
	}
	if len(c.Updated) > 0 {
		parts = append(parts, "updated: "+strings.Join(c.Updated, ", "))
	}
	if len(c.Removed) > 0 {
		parts = append(parts, "removed: "+strings.Join(c.Removed, ", "))
	}
	return strings.Join(parts, "; ")
}

// ParseError locates a malformed query block inside its source file.
type ParseError struct {
	Source  string
	Line    int
	Name    string
	Problem string
}

func (e *ParseError) Error() string {
	loc := e.Source
	if e.Line > 0 {
		loc = fmt.Sprintf("%s:%d", e.Source, e.Line)
	}
	if e.Name != "" {
		return fmt.Sprintf("squealx: %s: query %q: %s", loc, e.Name, e.Problem)
	}
	return fmt.Sprintf("squealx: %s: %s", loc, e.Problem)
}

var (
	// ErrQueryNotFound is returned in strict mode when a query name has no
	// loaded definition and the caller input does not look like raw SQL.
	ErrQueryNotFound = errors.New("squealx: query name not found in file loader")
	// ErrDuplicateQuery is returned when two query blocks share one name.
	ErrDuplicateQuery = errors.New("squealx: duplicate query name")
	// ErrConnectionMismatch is returned when a query declares a connection
	// key that differs from the target database ID.
	ErrConnectionMismatch = errors.New("squealx: query is bound to a different connection")
	// ErrNoQueries is returned when a source yields no named queries.
	ErrNoQueries = errors.New("squealx: no named queries found")
)

const defaultWatchInterval = 2 * time.Second

type loaderOptions struct {
	strict        bool
	recursive     bool
	extensions    []string
	watchInterval time.Duration
	onReload      []func(*ChangeSet)
	stmtCache     bool
}

// LoaderOption configures FileLoader construction.
type LoaderOption func(*loaderOptions)

// WithStrict makes the loader fail on unknown query names instead of passing
// the caller string through as raw SQL. It also enforces connection keys.
func WithStrict() LoaderOption {
	return func(o *loaderOptions) { o.strict = true }
}

// WithRecursive makes LoadFromDir descend into sub-directories.
func WithRecursive() LoaderOption {
	return func(o *loaderOptions) { o.recursive = true }
}

// WithExtensions overrides the file extensions LoadFromDir picks up
// (default ".sql"). Extensions include the leading dot and are matched
// case-insensitively.
func WithExtensions(exts ...string) LoaderOption {
	return func(o *loaderOptions) {
		o.extensions = o.extensions[:0]
		for _, ext := range exts {
			ext = strings.ToLower(strings.TrimSpace(ext))
			if ext == "" {
				continue
			}
			if !strings.HasPrefix(ext, ".") {
				ext = "." + ext
			}
			o.extensions = append(o.extensions, ext)
		}
	}
}

// WithWatchInterval sets the polling interval used by Watch (default 2s).
func WithWatchInterval(d time.Duration) LoaderOption {
	return func(o *loaderOptions) { o.watchInterval = d }
}

// WithOnReload registers a callback invoked after every successful reload
// that changed the query registry.
func WithOnReload(fn func(*ChangeSet)) LoaderOption {
	return func(o *loaderOptions) {
		if fn != nil {
			o.onReload = append(o.onReload, fn)
		}
	}
}

// WithStmtCache enables caching of prepared statement handles for named
// queries. Cached handles are invalidated automatically when a reload
// changes or removes the underlying SQL and are closed by CloseStmts.
func WithStmtCache() LoaderOption {
	return func(o *loaderOptions) { o.stmtCache = true }
}

type sourceFile struct {
	path        string
	fingerprint string
	queries     map[string]*Query
}

type stmtKey struct {
	db   *DB
	name string
}

type cachedStmt struct {
	hash  string
	stmt  *Stmt
	named *NamedStmt
}

// FileLoader resolves named SQL statements loaded from one or more SQL files.
// Query blocks look like:
//
//	-- sql-name: list-users
//	-- doc: List users in an organization
//	-- connection: primary
//	SELECT id, name FROM users WHERE org_id = :org_id;
//	-- sql-end
//
// The loader is safe for concurrent use. Reload swaps the registry atomically,
// and Watch reloads automatically when source files change on disk.
type FileLoader struct {
	file    string
	dir     string
	opts    loaderOptions
	mu      sync.RWMutex
	queries map[string]*Query
	sources map[string]*sourceFile

	cbMu      sync.Mutex
	onReload  []func(*ChangeSet)
	stmtMu    sync.Mutex
	stmtCache map[stmtKey]*cachedStmt
}

// LoadFromFile loads named query blocks from a single SQL file.
func LoadFromFile(file string, opts ...LoaderOption) (*FileLoader, error) {
	f := newFileLoader("", file, opts)
	if _, err := f.reload(true); err != nil {
		return nil, err
	}
	return f, nil
}

// LoadFromDir loads named query blocks from every matching SQL file in dir.
// Files are merged in sorted path order, so loads are deterministic.
func LoadFromDir(dir string, opts ...LoaderOption) (*FileLoader, error) {
	f := newFileLoader(dir, "", opts)
	if _, err := f.reload(true); err != nil {
		return nil, err
	}
	return f, nil
}

func newFileLoader(dir, file string, opts []LoaderOption) *FileLoader {
	cfg := loaderOptions{extensions: []string{".sql"}, watchInterval: defaultWatchInterval}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if len(cfg.extensions) == 0 {
		cfg.extensions = []string{".sql"}
	}
	f := &FileLoader{
		file:      file,
		dir:       dir,
		opts:      cfg,
		queries:   make(map[string]*Query),
		sources:   make(map[string]*sourceFile),
		onReload:  cfg.onReload,
		stmtCache: make(map[stmtKey]*cachedStmt),
	}
	return f
}

// GetQuery returns the named query or nil when the name is unknown. A nil
// result is not an error: callers may be passing raw SQL through.
func (f *FileLoader) GetQuery(query string) *Query {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.queries[query]
}

// Lookup returns the named query and whether it exists.
func (f *FileLoader) Lookup(name string) (*Query, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	q, ok := f.queries[name]
	return q, ok
}

// MustGetQuery returns the named query and panics when the name is unknown.
// Use it at wiring time to fail fast on typos.
func (f *FileLoader) MustGetQuery(name string) *Query {
	q, ok := f.Lookup(name)
	if !ok {
		panic(fmt.Sprintf("%v: %q", ErrQueryNotFound, name))
	}
	return q
}

// Has reports whether a query name is loaded.
func (f *FileLoader) Has(name string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	_, ok := f.queries[name]
	return ok
}

// Names returns every loaded query name in sorted order.
func (f *FileLoader) Names() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	names := make([]string, 0, len(f.queries))
	for name := range f.queries {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Len returns the number of loaded queries.
func (f *FileLoader) Len() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.queries)
}

// Queries returns a snapshot copy of the loaded registry.
func (f *FileLoader) Queries() map[string]*Query {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make(map[string]*Query, len(f.queries))
	for name, q := range f.queries {
		out[name] = q
	}
	return out
}

// Sources returns the source files backing the registry in merge order.
func (f *FileLoader) Sources() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	paths := make([]string, 0, len(f.sources))
	for path := range f.sources {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths
}

// Resolve returns the SQL text for a query name, or the input unchanged when
// it is not a loaded name (raw SQL passthrough).
func (f *FileLoader) Resolve(sqlOrName string) string {
	if q := f.GetQuery(sqlOrName); q != nil {
		return q.Query
	}
	return sqlOrName
}

// OnReload registers a callback invoked after every successful reload that
// changed the query registry.
func (f *FileLoader) OnReload(fn func(*ChangeSet)) {
	if fn == nil {
		return
	}
	f.cbMu.Lock()
	defer f.cbMu.Unlock()
	f.onReload = append(f.onReload, fn)
}

// Reload re-reads the source files and swaps the registry atomically. File
// contents are fingerprinted, so unchanged files keep their previously parsed
// statements and only changed files are re-parsed. When any file fails to
// parse, the current registry is left untouched and the error is returned.
func (f *FileLoader) Reload() (*ChangeSet, error) {
	return f.reload(false)
}

// Watch polls the loader sources and reloads queries when files change on
// disk, notifying OnReload callbacks for every non-empty change set. Reload
// failures leave the current registry in place and are retried on the next
// poll. Watch returns nil when ctx is cancelled.
func (f *FileLoader) Watch(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	interval := f.opts.watchInterval
	if interval <= 0 {
		interval = defaultWatchInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			cs, err := f.reload(false)
			if err != nil || cs.Empty() {
				continue
			}
			f.notifyReload(cs)
		}
	}
}

// CloseStmts closes every cached prepared statement handle. Cached handles
// are only created when WithStmtCache is enabled.
func (f *FileLoader) CloseStmts() error {
	f.stmtMu.Lock()
	defer f.stmtMu.Unlock()
	var errs []error
	for key, cached := range f.stmtCache {
		if cached.stmt != nil {
			errs = append(errs, cached.stmt.Close())
		}
		if cached.named != nil {
			errs = append(errs, cached.named.Close())
		}
		delete(f.stmtCache, key)
	}
	return errors.Join(errs...)
}

// DriverName reports the driver name of the given database.
func (f *FileLoader) DriverName(db *DB) string {
	return db.DriverName()
}

// Rebind rebinds the loaded query (or raw SQL) to the database bind type.
func (f *FileLoader) Rebind(db *DB, sql string) string {
	return db.Rebind(f.Resolve(sql))
}

// BindNamed binds named parameters of the loaded query (or raw SQL).
func (f *FileLoader) BindNamed(db *DB, sql string, args any) (string, []any, error) {
	return db.BindNamed(f.Resolve(sql), args)
}

// In expands slice arguments for the loaded query (or raw SQL).
func (f *FileLoader) In(db *DB, query string, args ...any) (string, []any, error) {
	return db.In(f.Resolve(query), args...)
}

// Driver returns the driver of the given database.
func (f *FileLoader) Driver(db *DB) driver.Driver {
	return db.Driver()
}

func (f *FileLoader) notifyReload(cs *ChangeSet) {
	f.cbMu.Lock()
	handlers := append([]func(*ChangeSet){}, f.onReload...)
	f.cbMu.Unlock()
	for _, fn := range handlers {
		fn(cs)
	}
}

// resolveQuery maps caller input onto a loaded query. It returns (nil, nil)
// when the input should be treated as raw SQL.
func (f *FileLoader) resolveQuery(sqlOrName string) (*Query, error) {
	if q := f.GetQuery(sqlOrName); q != nil {
		return q, nil
	}
	if f.opts.strict && !looksLikeSQL(sqlOrName) {
		return nil, fmt.Errorf("%w: %q", ErrQueryNotFound, sqlOrName)
	}
	return nil, nil
}

// sqlFor resolves caller input to SQL text for the given database, applying
// strict-mode checks for unknown names and connection keys.
func (f *FileLoader) sqlFor(db *DB, sqlOrName string) (string, error) {
	q, err := f.resolveQuery(sqlOrName)
	if err != nil {
		return "", err
	}
	if q == nil {
		return sqlOrName, nil
	}
	if f.opts.strict {
		if err := connectionError(db, q); err != nil {
			return "", err
		}
	}
	return q.Query, nil
}

func connectionError(db *DB, q *Query) error {
	if db == nil || q == nil || q.Connection == "" || q.Connection == db.ID {
		return nil
	}
	return fmt.Errorf("%w: query %q targets connection %q, got %q", ErrConnectionMismatch, q.Name, q.Connection, db.ID)
}

// looksLikeSQL distinguishes raw SQL passthrough from a bare query name.
func looksLikeSQL(s string) bool {
	if strings.ContainsAny(s, " \t\r\n(;") {
		return true
	}
	return strings.HasPrefix(s, "--") || strings.HasPrefix(s, "/*")
}

// isNamedArg reports whether arg carries named parameter values (a map or a
// struct, optionally behind a pointer).
func isNamedArg(arg any) bool {
	switch arg.(type) {
	case map[string]any, map[string]string:
		return true
	}
	v := reflect.ValueOf(arg)
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return false
		}
		v = v.Elem()
	}
	return v.Kind() == reflect.Struct
}

// compileArgs renders SQL templates and binds named parameters from a single
// map or struct argument, so every query verb accepts the same argument
// shapes. Positional calls pass through unchanged. Slice arguments for IN
// expansion are left untouched.
func (f *FileLoader) compileArgs(db *DB, query string, args []any) (string, []any, error) {
	rendered, err := SanitizeQuery(query, args...)
	if err != nil {
		return "", nil, err
	}
	if len(args) != 1 || args[0] == nil || !IsNamedQuery(rendered) || !isNamedArg(args[0]) {
		return rendered, args, nil
	}
	bound, params, err := bindNamedMapper(BindType(db.DriverName()), rendered, args[0], mapperFor(db))
	if err != nil {
		return "", nil, err
	}
	return bound, params, nil
}

func (f *FileLoader) reload(force bool) (*ChangeSet, error) {
	paths, err := f.listSources()
	if err != nil {
		return nil, err
	}
	previous := f.snapshotRegistry()

	nextSources := make(map[string]*sourceFile, len(paths))
	merged := make(map[string]*Query)
	owners := make(map[string]string, len(merged))
	var errs []error

	for _, path := range paths {
		content, err := os.ReadFile(path)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		fingerprint := hashSQL(string(content))
		if cached, ok := f.sources[path]; ok && !force && cached.fingerprint == fingerprint {
			nextSources[path] = cached
			errs = append(errs, mergeQueries(merged, owners, path, cached.queries)...)
			continue
		}
		queries, err := parseQueries(path, string(content))
		if err != nil {
			errs = append(errs, err)
			continue
		}
		src := &sourceFile{path: path, fingerprint: fingerprint, queries: queries}
		nextSources[path] = src
		errs = append(errs, mergeQueries(merged, owners, path, src.queries)...)
	}

	if len(errs) > 0 {
		// Fail safe: a broken edit never replaces a working registry.
		return nil, errors.Join(errs...)
	}
	if len(merged) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrNoQueries, f.describeSources())
	}

	cs := diffQueries(previous, merged)
	f.mu.Lock()
	f.queries = merged
	f.sources = nextSources
	f.mu.Unlock()

	if !cs.Empty() {
		f.invalidateStmts(cs)
	}
	return cs, nil
}

func (f *FileLoader) snapshotRegistry() map[string]string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make(map[string]string, len(f.queries))
	for name, q := range f.queries {
		out[name] = q.Hash
	}
	return out
}

func (f *FileLoader) describeSources() string {
	if f.file != "" {
		return f.file
	}
	return f.dir
}

// listSources returns the sorted set of source files backing the loader.
func (f *FileLoader) listSources() ([]string, error) {
	if f.file != "" {
		return []string{f.file}, nil
	}
	var paths []string
	walk := func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if path == f.dir || f.opts.recursive {
				return nil
			}
			return fs.SkipDir
		}
		if f.matchesExt(entry.Name()) {
			paths = append(paths, path)
		}
		return nil
	}
	if f.opts.recursive {
		if err := filepath.WalkDir(f.dir, walk); err != nil {
			return nil, err
		}
	} else {
		entries, err := os.ReadDir(f.dir)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if entry.IsDir() || !f.matchesExt(entry.Name()) {
				continue
			}
			paths = append(paths, filepath.Join(f.dir, entry.Name()))
		}
	}
	sort.Strings(paths)
	return paths, nil
}

func (f *FileLoader) matchesExt(name string) bool {
	ext := strings.ToLower(filepath.Ext(name))
	for _, want := range f.opts.extensions {
		if ext == want {
			return true
		}
	}
	return false
}

func mergeQueries(into map[string]*Query, owners map[string]string, path string, queries map[string]*Query) []error {
	var errs []error
	names := make([]string, 0, len(queries))
	for name := range queries {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		q := queries[name]
		if owner, ok := owners[name]; ok {
			errs = append(errs, fmt.Errorf("%w: %q defined in %s and %s", ErrDuplicateQuery, name, owner, q.location()))
			continue
		}
		owners[name] = q.location()
		into[name] = q
	}
	return errs
}

func diffQueries(previous map[string]string, next map[string]*Query) *ChangeSet {
	cs := &ChangeSet{}
	for name, q := range next {
		old, ok := previous[name]
		switch {
		case !ok:
			cs.Added = append(cs.Added, name)
		case old != q.Hash:
			cs.Updated = append(cs.Updated, name)
		}
	}
	for name := range previous {
		if _, ok := next[name]; !ok {
			cs.Removed = append(cs.Removed, name)
		}
	}
	sort.Strings(cs.Added)
	sort.Strings(cs.Updated)
	sort.Strings(cs.Removed)
	return cs
}

// invalidateStmts closes cached prepared statement handles whose query text
// changed or disappeared, so stale SQL is never re-executed.
func (f *FileLoader) invalidateStmts(cs *ChangeSet) {
	if cs.Empty() {
		return
	}
	stale := make(map[string]struct{}, len(cs.Updated)+len(cs.Removed))
	for _, name := range cs.Updated {
		stale[name] = struct{}{}
	}
	for _, name := range cs.Removed {
		stale[name] = struct{}{}
	}
	f.stmtMu.Lock()
	defer f.stmtMu.Unlock()
	for key, cached := range f.stmtCache {
		if _, ok := stale[key.name]; !ok {
			continue
		}
		if cached.stmt != nil {
			_ = cached.stmt.Close()
		}
		if cached.named != nil {
			_ = cached.named.Close()
		}
		delete(f.stmtCache, key)
	}
}

// cachedPrepare returns a cached prepared statement for a named query,
// preparing and storing it on miss. Callers must hold no locks.
func (f *FileLoader) cachedPrepare(db *DB, q *Query, named bool) (*Stmt, *NamedStmt, error) {
	key := stmtKey{db: db, name: q.Name}
	f.stmtMu.Lock()
	defer f.stmtMu.Unlock()
	if cached, ok := f.stmtCache[key]; ok && cached.hash == q.Hash {
		return cached.stmt, cached.named, nil
	}
	if cached, ok := f.stmtCache[key]; ok {
		if cached.stmt != nil {
			_ = cached.stmt.Close()
		}
		if cached.named != nil {
			_ = cached.named.Close()
		}
		delete(f.stmtCache, key)
	}
	entry := &cachedStmt{hash: q.Hash}
	var err error
	if named {
		entry.named, err = db.PrepareNamed(q.Query)
	} else {
		entry.stmt, err = db.Preparex(q.Query)
	}
	if err != nil {
		return nil, nil, err
	}
	f.stmtCache[key] = entry
	return entry.stmt, entry.named, nil
}

var (
	sqlNameRE = regexp.MustCompile(`^\s*--\s*sql-name\s*:\s*(.*?)\s*$`)
	docRE     = regexp.MustCompile(`^\s*--\s*doc\s*:\s*(.*?)\s*$`)
	connRE    = regexp.MustCompile(`^\s*--\s*connection\s*:\s*(.*?)\s*$`)
	endRE     = regexp.MustCompile(`^\s*--\s*sql-end\b`)
)

// parseQueries extracts named query blocks from one SQL file. Blocks start at
// `-- sql-name:`, may carry `-- doc:` and `-- connection:` metadata lines at
// the top of the block, and end at `-- sql-end`. Malformed files produce
// *ParseError values with file and line information.
func parseQueries(source, content string) (map[string]*Query, error) {
	content = strings.TrimPrefix(content, "\ufeff")
	lines := strings.Split(content, "\n")

	queries := make(map[string]*Query)
	var errs []error

	var (
		name     string
		start    int
		docParts []string
		conn     string
		body     []string
		inBlock  bool
		inHeader bool
	)

	flush := func() {
		if !inBlock {
			return
		}
		defer func() {
			inBlock = false
			inHeader = false
			name, conn = "", ""
			docParts, body = nil, nil
		}()
		if name == "" {
			errs = append(errs, &ParseError{Source: source, Line: start, Problem: "missing query name after -- sql-name:"})
			return
		}
		sqlText := strings.TrimSpace(strings.Join(body, "\n"))
		if sqlText == "" {
			errs = append(errs, &ParseError{Source: source, Line: start, Name: name, Problem: "empty query body"})
			return
		}
		if prev, ok := queries[name]; ok {
			errs = append(errs, &ParseError{
				Source: source, Line: start, Name: name,
				Problem: fmt.Sprintf("%v: first defined at %s", ErrDuplicateQuery, prev.location()),
			})
			return
		}
		queries[name] = &Query{
			Doc:        strings.Join(docParts, " "),
			Name:       name,
			Query:      sqlText,
			Connection: conn,
			Source:     source,
			Line:       start,
			Hash:       hashSQL(sqlText),
		}
	}

	for i, raw := range lines {
		lineNo := i + 1
		line := strings.TrimSuffix(raw, "\r")

		switch {
		case sqlNameRE.MatchString(line):
			if inBlock {
				errs = append(errs, &ParseError{Source: source, Line: lineNo, Name: name, Problem: "missing -- sql-end before next -- sql-name:"})
				flush()
			}
			inBlock = true
			inHeader = true
			start = lineNo
			name = strings.TrimSpace(sqlNameRE.FindStringSubmatch(line)[1])
			continue
		case endRE.MatchString(line):
			if !inBlock {
				errs = append(errs, &ParseError{Source: source, Line: lineNo, Problem: "-- sql-end without a matching -- sql-name:"})
				continue
			}
			flush()
			continue
		}

		if !inBlock {
			continue
		}

		if inHeader {
			if m := docRE.FindStringSubmatch(line); m != nil {
				docParts = append(docParts, m[1])
				continue
			}
			if m := connRE.FindStringSubmatch(line); m != nil {
				if conn != "" {
					errs = append(errs, &ParseError{Source: source, Line: lineNo, Name: name, Problem: "multiple -- connection: lines"})
				}
				conn = m[1]
				continue
			}
			if strings.TrimSpace(line) == "" {
				continue
			}
			inHeader = false
		}
		body = append(body, line)
	}
	if inBlock {
		errs = append(errs, &ParseError{Source: source, Line: start, Name: name, Problem: "missing -- sql-end"})
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return queries, nil
}

func (q *Query) location() string {
	if q == nil {
		return ""
	}
	if q.Line > 0 {
		return fmt.Sprintf("%s:%d", q.Source, q.Line)
	}
	return q.Source
}

func hashSQL(sqlText string) string {
	sum := sha256.Sum256([]byte(sqlText))
	return hex.EncodeToString(sum[:])
}
