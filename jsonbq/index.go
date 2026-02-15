package jsonbq

import (
	"context"
	"fmt"
	"strings"
	"unicode"
)

// IndexSpec describes one index definition.
type IndexSpec struct {
	Name        string
	Expr        Expr
	Method      string
	Unique      bool
	IfNotExists bool
	Where       string
}

// DefaultIndexSpec is a compact index definition used with DefaultJSONBIndexes.
type DefaultIndexSpec struct {
	Suffix string
	Expr   Expr
	Method string
	Unique bool
	Where  string
}

// ExprIndex creates a compact index definition for DefaultJSONBIndexes.
func ExprIndex(suffix string, expr Expr) DefaultIndexSpec {
	return DefaultIndexSpec{Suffix: suffix, Expr: expr}
}

// Using sets index method (e.g. BTREE, GIN, GIST).
func (i DefaultIndexSpec) Using(method string) DefaultIndexSpec {
	i.Method = strings.TrimSpace(method)
	return i
}

// UniqueIndex marks this index as UNIQUE.
func (i DefaultIndexSpec) UniqueIndex() DefaultIndexSpec {
	i.Unique = true
	return i
}

// Partial adds a WHERE clause for partial index.
func (i DefaultIndexSpec) Partial(where string) DefaultIndexSpec {
	i.Where = strings.TrimSpace(where)
	return i
}

// Index creates a new index specification.
// It defaults to IF NOT EXISTS for idempotent setup.
func Index(name string, expr Expr) IndexSpec {
	return IndexSpec{
		Name:        name,
		Expr:        expr,
		IfNotExists: true,
	}
}

// Using sets index method (e.g. BTREE, GIN, GIST).
func (i IndexSpec) Using(method string) IndexSpec {
	i.Method = strings.TrimSpace(method)
	return i
}

// UniqueIndex marks this index as UNIQUE.
func (i IndexSpec) UniqueIndex() IndexSpec {
	i.Unique = true
	return i
}

// Partial adds a WHERE clause for partial index.
func (i IndexSpec) Partial(where string) IndexSpec {
	i.Where = strings.TrimSpace(where)
	return i
}

// WithoutIfNotExists disables IF NOT EXISTS.
func (i IndexSpec) WithoutIfNotExists() IndexSpec {
	i.IfNotExists = false
	return i
}

// AddIndex creates one index on the table.
func (db *DB) AddIndex(table string, index IndexSpec) error {
	return db.AddIndexContext(context.Background(), table, index)
}

// AddIndexContext creates one index on the table with context.
func (db *DB) AddIndexContext(ctx context.Context, table string, index IndexSpec) error {
	query, err := buildCreateIndexSQL(db.columnName, table, index)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, query)
	return err
}

// AddIndexes creates multiple indexes on the table.
func (db *DB) AddIndexes(table string, indexes ...IndexSpec) error {
	return db.AddIndexesContext(context.Background(), table, indexes...)
}

// AddIndexesContext creates multiple indexes on the table with context.
func (db *DB) AddIndexesContext(ctx context.Context, table string, indexes ...IndexSpec) error {
	for _, index := range indexes {
		if err := db.AddIndexContext(ctx, table, index); err != nil {
			return err
		}
	}
	return nil
}

// DefaultJSONBIndexes creates a baseline GIN index plus custom expression indexes.
// Names are generated as idx_<table>_<suffix> for simplicity and consistency.
func (db *DB) DefaultJSONBIndexes(table string, indexes ...DefaultIndexSpec) error {
	return db.DefaultJSONBIndexesContext(context.Background(), table, indexes...)
}

// DefaultJSONBIndexesContext creates default JSONB indexes with context.
func (db *DB) DefaultJSONBIndexesContext(ctx context.Context, table string, indexes ...DefaultIndexSpec) error {
	tablePart := sanitizeIndexPart(table)
	columnPart := sanitizeIndexPart(db.columnName)

	all := make([]IndexSpec, 0, len(indexes)+1)
	all = append(all, Index("idx_"+tablePart+"_"+columnPart+"_gin", RawColumn()).Using("GIN"))

	for _, idx := range indexes {
		suffix := sanitizeIndexPart(idx.Suffix)
		if suffix == "" {
			return fmt.Errorf("index suffix is required")
		}
		spec := Index("idx_"+tablePart+"_"+suffix, idx.Expr)
		if idx.Method != "" {
			spec = spec.Using(idx.Method)
		}
		if idx.Unique {
			spec = spec.UniqueIndex()
		}
		if idx.Where != "" {
			spec = spec.Partial(idx.Where)
		}
		all = append(all, spec)
	}

	return db.AddIndexesContext(ctx, table, all...)
}

// DefaultJSONBIndexesFor is an ultra-simple helper that accepts string specs.
// Spec format:
//   "field"                -> text index on At("field").Text()
//   "field:type"           -> typed index (type: text|json|bool|int|numeric)
//   "nested.path:type"     -> nested path with dot notation
//
// Example:
//   db.DefaultJSONBIndexesFor(table,
//     "sport", "name", "active:bool", "age:int", "stats.height:int", "stats.ppg:numeric")
func (db *DB) DefaultJSONBIndexesFor(table string, specs ...string) error {
	return db.DefaultJSONBIndexesForContext(context.Background(), table, specs...)
}

// DefaultJSONBIndexesForContext creates default JSONB indexes from string specs with context.
func (db *DB) DefaultJSONBIndexesForContext(ctx context.Context, table string, specs ...string) error {
	parsed := make([]DefaultIndexSpec, 0, len(specs))
	for _, spec := range specs {
		idx, err := parseDefaultIndexSpec(spec)
		if err != nil {
			return err
		}
		parsed = append(parsed, idx)
	}
	return db.DefaultJSONBIndexesContext(ctx, table, parsed...)
}

func buildCreateIndexSQL(columnName, table string, index IndexSpec) (string, error) {
	table = strings.TrimSpace(table)
	if table == "" {
		return "", fmt.Errorf("table name is required")
	}
	if strings.TrimSpace(index.Name) == "" {
		return "", fmt.Errorf("index name is required")
	}

	q := &Query{}
	index.Expr.Build(q, columnName)
	exprSQL := strings.TrimSpace(q.String())
	if exprSQL == "" {
		return "", fmt.Errorf("index expression is required")
	}
	if len(q.Args()) > 0 {
		return "", fmt.Errorf("index expression must be static (no parameters)")
	}

	var b strings.Builder
	b.WriteString("CREATE ")
	if index.Unique {
		b.WriteString("UNIQUE ")
	}
	b.WriteString("INDEX ")
	if index.IfNotExists {
		b.WriteString("IF NOT EXISTS ")
	}
	b.WriteString(quoteIdentifier(index.Name))
	b.WriteString(" ON ")
	b.WriteString(quoteIdentifier(table))
	if method := strings.TrimSpace(index.Method); method != "" {
		b.WriteString(" USING ")
		b.WriteString(method)
	}
	b.WriteString(" (")
	b.WriteString("(")
	b.WriteString(exprSQL)
	b.WriteString(")")
	b.WriteString(")")
	if where := strings.TrimSpace(index.Where); where != "" {
		b.WriteString(" WHERE ")
		b.WriteString(where)
	}
	return b.String(), nil
}

func quoteIdentifier(ident string) string {
	ident = strings.TrimSpace(ident)
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

func sanitizeIndexPart(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return ""
	}

	var b strings.Builder
	prevUnderscore := false
	for _, r := range value {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
			prevUnderscore = false
			continue
		}
		if !prevUnderscore {
			b.WriteByte('_')
			prevUnderscore = true
		}
	}

	clean := strings.Trim(b.String(), "_")
	return clean
}

func parseDefaultIndexSpec(spec string) (DefaultIndexSpec, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return DefaultIndexSpec{}, fmt.Errorf("index spec cannot be empty")
	}

	parts := strings.SplitN(spec, ":", 2)
	pathPart := strings.TrimSpace(parts[0])
	if pathPart == "" {
		return DefaultIndexSpec{}, fmt.Errorf("index path cannot be empty in spec %q", spec)
	}

	path := strings.Split(pathPart, ".")
	cleanPath := make([]string, 0, len(path))
	for _, p := range path {
		p = strings.TrimSpace(p)
		if p == "" {
			return DefaultIndexSpec{}, fmt.Errorf("invalid index path in spec %q", spec)
		}
		cleanPath = append(cleanPath, p)
	}

	kind := "text"
	if len(parts) == 2 {
		kind = strings.ToLower(strings.TrimSpace(parts[1]))
		if kind == "" {
			kind = "text"
		}
	}

	sel := At(cleanPath...)
	var expr Expr
	switch kind {
	case "text", "string":
		expr = sel.Text()
		kind = "text"
	case "json", "jsonb":
		expr = sel.JSON()
		kind = "json"
	case "bool", "boolean":
		expr = sel.Bool()
		kind = "bool"
	case "int", "integer":
		expr = sel.Int()
		kind = "int"
	case "numeric", "number", "decimal", "float":
		expr = sel.Numeric()
		kind = "numeric"
	default:
		return DefaultIndexSpec{}, fmt.Errorf("unsupported index type %q in spec %q", kind, spec)
	}

	suffix := strings.ReplaceAll(strings.Join(cleanPath, "_"), "-", "_")
	if kind != "text" {
		suffix += "_" + kind
	}

	return ExprIndex(suffix, expr), nil
}
