package jsonbq

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

var templateIdentPartPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]*$`)

type SQLParseOptions struct {
	JSONColumns          []string
	EncryptedSearchPaths map[string]bool
	EncryptedHMACKey     string
}

func ParseSQLTemplate(query string, vars map[string]any) (string, []any, error) {
	return ParseNormalSQL(query, vars)
}

// ParseNormalSQL parses human-friendly SQL (including JSON helper functions and :named params)
// into PostgreSQL-ready SQL + bind args.
func ParseNormalSQL(query string, vars map[string]any) (string, []any, error) {
	return ParseNormalSQLWithOptions(query, vars, SQLParseOptions{JSONColumns: []string{"data"}})
}

// ParseNormalSQLWithOptions parses normal SQL with configurable JSON columns for dot-notation rewrites.
func ParseNormalSQLWithOptions(query string, vars map[string]any, opts SQLParseOptions) (string, []any, error) {
	jsonCols := opts.JSONColumns
	if len(jsonCols) == 0 {
		jsonCols = []string{"data"}
	}
	rewritten, err := RewriteJSONFunctions(query)
	if err != nil {
		return "", nil, err
	}
	rewritten, err = RewriteJSONDotNotation(rewritten, jsonCols)
	if err != nil {
		return "", nil, err
	}
	rewritten, err = RewriteEncryptedSearchPredicates(rewritten, vars, opts.EncryptedSearchPaths, opts.EncryptedHMACKey)
	if err != nil {
		return "", nil, err
	}
	rewritten, err = RewriteImplicitNumericJSONCasts(rewritten)
	if err != nil {
		return "", nil, err
	}
	tpl, err := NewSQLTemplate(rewritten)
	if err != nil {
		return "", nil, err
	}
	return tpl.Compile(vars)
}

func compileSQLTemplateTokens(tokens []SQLToken, vars map[string]any) (string, []any, error) {
	var b strings.Builder
	args := make([]any, 0, 8)

	for _, tok := range tokens {
		if tok.Type != SQLTokenPlaceholder {
			b.WriteString(tok.Literal)
			continue
		}
		content := extractPlaceholderContent(tok.Literal)
		repl, arg, hasArg, err := resolveTemplatePlaceholder(content, vars)
		if err != nil {
			return "", nil, err
		}
		if hasArg {
			args = append(args, arg)
			repl = fmt.Sprintf("$%d", len(args))
		}
		b.WriteString(repl)
	}

	return b.String(), args, nil
}

func extractPlaceholderContent(lit string) string {
	if strings.HasPrefix(lit, ":") && len(lit) > 1 {
		return strings.TrimSpace(lit[1:])
	}
	if strings.HasPrefix(lit, "{{") && strings.HasSuffix(lit, "}}") && len(lit) >= 4 {
		return strings.TrimSpace(lit[2 : len(lit)-2])
	}
	return strings.TrimSpace(lit)
}

func resolveTemplatePlaceholder(content string, vars map[string]any) (replacement string, arg any, hasArg bool, err error) {
	if content == "" {
		return "", nil, false, fmt.Errorf("empty placeholder {{}} is not allowed")
	}
	kind := "param"
	key := content
	if idx := strings.Index(content, ":"); idx >= 0 {
		kind = strings.TrimSpace(content[:idx])
		key = strings.TrimSpace(content[idx+1:])
	}
	if key == "" {
		return "", nil, false, fmt.Errorf("invalid placeholder %q: missing variable key", content)
	}
	val, ok := vars[key]
	if !ok {
		return "", nil, false, fmt.Errorf("missing template variable %q", key)
	}

	switch strings.ToLower(kind) {
	case "param", "arg":
		return "", val, true, nil
	case "literal", "string", "key":
		return quoteSQLString(fmt.Sprint(val)), nil, false, nil
	case "ident", "identifier":
		s, ok := val.(string)
		if !ok {
			return "", nil, false, fmt.Errorf("identifier %q must be string, got %T", key, val)
		}
		ident, err := quoteTemplateIdentifier(s)
		if err != nil {
			return "", nil, false, err
		}
		return ident, nil, false, nil
	case "raw":
		s, ok := val.(string)
		if !ok {
			return "", nil, false, fmt.Errorf("raw placeholder %q must be string, got %T", key, val)
		}
		return s, nil, false, nil
	default:
		return "", nil, false, fmt.Errorf("unsupported placeholder kind %q", kind)
	}
}

func quoteSQLString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func quoteTemplateIdentifier(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", fmt.Errorf("empty identifier")
	}
	parts := strings.Split(s, ".")
	quoted := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if !templateIdentPartPattern.MatchString(p) {
			return "", fmt.Errorf("invalid identifier part %q", p)
		}
		quoted = append(quoted, `"`+p+`"`)
	}
	return strings.Join(quoted, "."), nil
}

func isJSONColumn(name string, cols []string) bool {
	name = strings.TrimSpace(strings.Trim(name, `"`))
	return slices.ContainsFunc(cols, func(c string) bool {
		return strings.EqualFold(strings.TrimSpace(strings.Trim(c, `"`)), name)
	})
}

func parseDollarTag(query string, start int) (string, bool) {
	if start >= len(query) || query[start] != '$' {
		return "", false
	}
	i := start + 1
	for i < len(query) {
		c := query[i]
		if c == '$' {
			return query[start : i+1], true
		}
		if !(c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) {
			return "", false
		}
		i++
	}
	return "", false
}
