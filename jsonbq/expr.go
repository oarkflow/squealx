package jsonbq

import (
	"encoding/json"
)

// Selector provides a unified way to access JSON values without exposing
// PostgreSQL operator details in user code.
type Selector struct {
	parts []string
}

// At returns a selector for a top-level key or nested path.
// Examples:
//   At("sport")
//   At("stats", "height")
func At(parts ...string) Selector {
	return Selector{parts: parts}
}

// Text returns the selected JSON value as text.
func (s Selector) Text() Expr {
	if len(s.parts) == 0 {
		return castExpr(RawColumn(), "text")
	}
	if len(s.parts) == 1 {
		return Field(s.parts[0])
	}
	return Path(s.parts...)
}

// JSON returns the selected JSON value as JSONB.
func (s Selector) JSON() Expr {
	if len(s.parts) == 0 {
		return RawColumn()
	}
	if len(s.parts) == 1 {
		return FieldJSON(s.parts[0])
	}
	return PathJSON(s.parts...)
}

// Bool returns the selected JSON value cast to boolean.
func (s Selector) Bool() Expr {
	return castExpr(s.Text(), "boolean")
}

// Int returns the selected JSON value cast to int.
func (s Selector) Int() Expr {
	return castExpr(s.Text(), "int")
}

// Numeric returns the selected JSON value cast to numeric.
func (s Selector) Numeric() Expr {
	return castExpr(s.Text(), "numeric")
}

// Common comparisons default to text semantics for convenience.
func (s Selector) Eq(val any) Condition {
	return s.Text().Eq(val)
}

func (s Selector) NotEq(val any) Condition {
	return s.Text().NotEq(val)
}

func (s Selector) In(vals ...any) Condition {
	return s.Text().In(vals...)
}

func (s Selector) Like(pattern string) Condition {
	return s.Text().Like(pattern)
}

func (s Selector) ILike(pattern string) Condition {
	return s.Text().ILike(pattern)
}

// Field accesses a top-level JSONB field as text (->>'field').
// Deprecated: use At(name).Text().
func Field(name string) Expr {
	return Expr{
		build: func(q *Query, columnName string) {
			q.writeColumn(columnName)
			q.sql.WriteString("->>")
			q.writeStringLiteral(name)
		},
	}
}

// FieldJSON accesses a top-level JSONB field as JSONB ('->'field').
// Deprecated: use At(name).JSON().
func FieldJSON(name string) Expr {
	return Expr{
		build: func(q *Query, columnName string) {
			q.writeColumn(columnName)
			q.sql.WriteString("->")
			q.writeStringLiteral(name)
		},
	}
}

// Path accesses a nested JSONB path as text (#>>'{a,b,c}').
// Deprecated: use At(parts...).Text().
func Path(parts ...string) Expr {
	return Expr{
		build: func(q *Query, columnName string) {
			q.writeColumn(columnName)
			q.sql.WriteString("#>>")
			q.writeTextArrayLiteral(parts)
		},
	}
}

// PathJSON accesses a nested JSONB path as JSONB (#>'{a,b,c}').
// Deprecated: use At(parts...).JSON().
func PathJSON(parts ...string) Expr {
	return Expr{
		build: func(q *Query, columnName string) {
			q.writeColumn(columnName)
			q.sql.WriteString("#>")
			q.writeTextArrayLiteral(parts)
		},
	}
}

// Raw allows raw SQL expressions
func Raw(sql string) Expr {
	return Expr{
		build: func(q *Query, columnName string) {
			q.sql.WriteString(sql)
		},
	}
}

// RawColumn references the JSONB column directly
func RawColumn() Expr {
	return Expr{
		build: func(q *Query, columnName string) {
			q.writeColumn(columnName)
		},
	}
}

// Value returns a top-level JSON value as text.
// Deprecated: use At(name).Text().
func Value(name string) Expr {
	return Field(name)
}

// ValueAt returns a nested JSON value as text.
// Deprecated: use At(parts...).Text().
func ValueAt(parts ...string) Expr {
	return Path(parts...)
}

// Bool returns a top-level JSON value cast to boolean.
// Deprecated: use At(name).Bool().
func Bool(name string) Expr {
	return castExpr(Field(name), "boolean")
}

// BoolAt returns a nested JSON value cast to boolean.
// Deprecated: use At(parts...).Bool().
func BoolAt(parts ...string) Expr {
	return castExpr(Path(parts...), "boolean")
}

// Int returns a top-level JSON value cast to int.
// Deprecated: use At(name).Int().
func Int(name string) Expr {
	return castExpr(Field(name), "int")
}

// IntAt returns a nested JSON value cast to int.
// Deprecated: use At(parts...).Int().
func IntAt(parts ...string) Expr {
	return castExpr(Path(parts...), "int")
}

// Numeric returns a top-level JSON value cast to numeric.
// Deprecated: use At(name).Numeric().
func Numeric(name string) Expr {
	return castExpr(Field(name), "numeric")
}

// NumericAt returns a nested JSON value cast to numeric.
// Deprecated: use At(parts...).Numeric().
func NumericAt(parts ...string) Expr {
	return castExpr(Path(parts...), "numeric")
}

// Comparison operators

func (e Expr) Eq(val any) Condition {
	return comparison(e, "=", val)
}

func (e Expr) NotEq(val any) Condition {
	return comparison(e, "!=", val)
}

func (e Expr) Gt(val any) Condition {
	return comparison(e, ">", val)
}

func (e Expr) Gte(val any) Condition {
	return comparison(e, ">=", val)
}

func (e Expr) Lt(val any) Condition {
	return comparison(e, "<", val)
}

func (e Expr) Lte(val any) Condition {
	return comparison(e, "<=", val)
}

func (e Expr) Like(pattern string) Condition {
	return comparison(e, "LIKE", pattern)
}

func (e Expr) ILike(pattern string) Condition {
	return comparison(e, "ILIKE", pattern)
}

func (e Expr) In(vals ...any) Condition {
	return Expr{
		build: func(q *Query, columnName string) {
			e.build(q, columnName)
			q.sql.WriteString(" IN (")
			for i, val := range vals {
				if i > 0 {
					q.sql.WriteString(", ")
				}
				q.sql.WriteString(q.addArg(val))
			}
			q.sql.WriteString(")")
		},
	}
}

func (e Expr) NotIn(vals ...any) Condition {
	return Expr{
		build: func(q *Query, columnName string) {
			e.build(q, columnName)
			q.sql.WriteString(" NOT IN (")
			for i, val := range vals {
				if i > 0 {
					q.sql.WriteString(", ")
				}
				q.sql.WriteString(q.addArg(val))
			}
			q.sql.WriteString(")")
		},
	}
}

func (e Expr) IsNull() Condition {
	return Expr{
		build: func(q *Query, columnName string) {
			e.build(q, columnName)
			q.sql.WriteString(" IS NULL")
		},
	}
}

func (e Expr) IsNotNull() Condition {
	return Expr{
		build: func(q *Query, columnName string) {
			e.build(q, columnName)
			q.sql.WriteString(" IS NOT NULL")
		},
	}
}

// Helper for comparisons
func comparison(e Expr, op string, val any) Condition {
	return Expr{
		build: func(q *Query, columnName string) {
			e.build(q, columnName)
			q.sql.WriteString(" ")
			q.sql.WriteString(op)
			q.sql.WriteString(" ")
			q.sql.WriteString(q.addArg(val))
		},
	}
}

func castExpr(e Expr, castType string) Expr {
	return Expr{
		build: func(q *Query, columnName string) {
			q.sql.WriteString("(")
			e.build(q, columnName)
			q.sql.WriteString(")")
			q.sql.WriteString("::")
			q.sql.WriteString(castType)
		},
	}
}

// JSONB-specific operators

// Contains checks if JSONB contains the given object (@>)
func Contains(obj any) Condition {
	return Expr{
		build: func(q *Query, columnName string) {
			q.writeColumn(columnName)
			q.sql.WriteString(" @> ")
			jsonBytes, err := json.Marshal(obj)
			if err != nil {
				jsonBytes = []byte("{}")
			}
			q.sql.WriteString(q.addArg(string(jsonBytes)))
			q.sql.WriteString("::jsonb")
		},
	}
}

// ContainedBy checks if JSONB is contained by the given object (<@)
func ContainedBy(obj any) Condition {
	return Expr{
		build: func(q *Query, columnName string) {
			q.writeColumn(columnName)
			q.sql.WriteString(" <@ ")
			jsonBytes, err := json.Marshal(obj)
			if err != nil {
				jsonBytes = []byte("{}")
			}
			q.sql.WriteString(q.addArg(string(jsonBytes)))
			q.sql.WriteString("::jsonb")
		},
	}
}

// HasKey checks if JSONB has the given key (?)
func HasKey(key string) Condition {
	return Expr{
		build: func(q *Query, columnName string) {
			q.writeColumn(columnName)
			q.sql.WriteString(" ? ")
			q.writeStringLiteral(key)
		},
	}
}

// HasAnyKey checks if JSONB has any of the given keys (?|)
func HasAnyKey(keys ...string) Condition {
	return Expr{
		build: func(q *Query, columnName string) {
			q.writeColumn(columnName)
			q.sql.WriteString(" ?| ")
			q.writeTextArrayLiteral(keys)
		},
	}
}

// HasAllKeys checks if JSONB has all of the given keys (?&)
func HasAllKeys(keys ...string) Condition {
	return Expr{
		build: func(q *Query, columnName string) {
			q.writeColumn(columnName)
			q.sql.WriteString(" ?& ")
			q.writeTextArrayLiteral(keys)
		},
	}
}

// JSONPath queries using JSONPath expression (@@)
func JSONPath(path string) Condition {
	return Expr{
		build: func(q *Query, columnName string) {
			q.writeColumn(columnName)
			q.sql.WriteString(" @@ ")
			q.sql.WriteString(q.addArg(path))
		},
	}
}

// Exists checks if path exists
func Exists(parts ...string) Condition {
	return PathJSON(parts...).IsNotNull()
}

// NotExists checks if path does not exist
func NotExists(parts ...string) Condition {
	return PathJSON(parts...).IsNull()
}
