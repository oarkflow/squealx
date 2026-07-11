package squealx

import "strings"

// topLevelKeyword returns the byte offset of the last top-level occurrence of
// keyword. It ignores quoted strings, quoted identifiers, comments,
// PostgreSQL dollar-quoted bodies, and nested parenthesized expressions.
func topLevelKeyword(query, keyword string) int {
	keyword = strings.ToUpper(keyword)
	last := -1
	depth := 0
	for i := 0; i < len(query); {
		switch query[i] {
		case '\'':
			i = skipQuoted(query, i, '\'')
			continue
		case '"':
			i = skipQuoted(query, i, '"')
			continue
		case '`':
			i = skipQuoted(query, i, '`')
			continue
		case '[':
			i = skipBracketIdentifier(query, i)
			continue
		case '-':
			if i+1 < len(query) && query[i+1] == '-' {
				i = skipLineComment(query, i+2)
				continue
			}
		case '#':
			i = skipLineComment(query, i+1)
			continue
		case '/':
			if i+1 < len(query) && query[i+1] == '*' {
				i = skipBlockComment(query, i+2)
				continue
			}
		case '$':
			if end, ok := skipDollarQuote(query, i); ok {
				i = end
				continue
			}
		case '(':
			depth++
			i++
			continue
		case ')':
			if depth > 0 {
				depth--
			}
			i++
			continue
		}

		if depth == 0 && hasKeywordAt(query, i, keyword) {
			last = i
			i += len(keyword)
			continue
		}
		i++
	}
	return last
}

func hasKeywordAt(query string, i int, upperKeyword string) bool {
	if i+len(upperKeyword) > len(query) || !strings.EqualFold(query[i:i+len(upperKeyword)], upperKeyword) {
		return false
	}
	if i > 0 && isSQLWordByte(query[i-1]) {
		return false
	}
	end := i + len(upperKeyword)
	return end == len(query) || !isSQLWordByte(query[end])
}

func isSQLWordByte(b byte) bool {
	return b == '_' || b == '$' || b >= '0' && b <= '9' || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z'
}

func skipQuoted(s string, start int, quote byte) int {
	for i := start + 1; i < len(s); i++ {
		if s[i] != quote {
			if s[i] == '\\' && quote == '\'' && i+1 < len(s) {
				i++
			}
			continue
		}
		if i+1 < len(s) && s[i+1] == quote {
			i++
			continue
		}
		return i + 1
	}
	return len(s)
}

func skipBracketIdentifier(s string, start int) int {
	for i := start + 1; i < len(s); i++ {
		if s[i] == ']' {
			if i+1 < len(s) && s[i+1] == ']' {
				i++
				continue
			}
			return i + 1
		}
	}
	return len(s)
}

func skipLineComment(s string, start int) int {
	if j := strings.IndexByte(s[start:], '\n'); j >= 0 {
		return start + j + 1
	}
	return len(s)
}

func skipBlockComment(s string, start int) int {
	if j := strings.Index(s[start:], "*/"); j >= 0 {
		return start + j + 2
	}
	return len(s)
}

func skipDollarQuote(s string, start int) (int, bool) {
	endTag := start + 1
	for endTag < len(s) && (s[endTag] == '_' || s[endTag] >= 'a' && s[endTag] <= 'z' || s[endTag] >= 'A' && s[endTag] <= 'Z' || s[endTag] >= '0' && s[endTag] <= '9') {
		endTag++
	}
	if endTag >= len(s) || s[endTag] != '$' {
		return start, false
	}
	tag := s[start : endTag+1]
	if j := strings.Index(s[endTag+1:], tag); j >= 0 {
		return endTag + 1 + j + len(tag), true
	}
	return len(s), true
}

func trimSQLTerminator(query string) string {
	query = strings.TrimSpace(query)
	for strings.HasSuffix(query, ";") {
		query = strings.TrimSpace(strings.TrimSuffix(query, ";"))
	}
	return query
}

// LimitQueryForDriver constrains a query to one row using the target driver's
// syntax. It is intentionally lexical rather than regex-based, so LIMIT text in
// strings, comments, CTEs, and subqueries is not mistaken for the outer clause.
func LimitQueryForDriver(driverName, query string) string {
	query = trimSQLTerminator(query)
	if query == "" {
		return query
	}

	switch strings.ToLower(driverName) {
	case "mssql", "sqlserver":
		return limitSQLServer(query)
	case "oracle", "godror", "oci8":
		if topLevelKeyword(query, "FETCH") >= 0 {
			return query
		}
		return query + " FETCH FIRST 1 ROWS ONLY"
	default:
		if idx := topLevelKeyword(query, "LIMIT"); idx >= 0 {
			return strings.TrimSpace(query[:idx]) + " LIMIT 1"
		}
		// OFFSET without LIMIT is legal in some dialects but cannot precede a
		// newly appended LIMIT. Preserve its semantics by wrapping the query.
		if topLevelKeyword(query, "OFFSET") >= 0 || topLevelKeyword(query, "FETCH") >= 0 {
			return "SELECT * FROM (" + query + ") AS squealx_limit_query LIMIT 1"
		}
		return query + " LIMIT 1"
	}
}

func limitSQLServer(query string) string {
	if topLevelKeyword(query, "TOP") >= 0 || topLevelKeyword(query, "FETCH") >= 0 {
		return query
	}
	selectAt := topLevelKeyword(query, "SELECT")
	if selectAt < 0 {
		return query
	}
	insertAt := selectAt + len("SELECT")
	rest := strings.TrimLeft(query[insertAt:], " \t\r\n")
	if len(rest) >= len("DISTINCT") && strings.EqualFold(rest[:len("DISTINCT")], "DISTINCT") &&
		(len(rest) == len("DISTINCT") || !isSQLWordByte(rest[len("DISTINCT")])) {
		distinctAt := strings.Index(query[insertAt:], rest) + insertAt
		insertAt = distinctAt + len("DISTINCT")
	}
	return query[:insertAt] + " TOP (1)" + query[insertAt:]
}

// WithReturning normalizes an INSERT/UPDATE/DELETE statement to RETURNING *.
// Existing outer RETURNING clauses are replaced while occurrences in nested
// expressions, comments, and string literals are preserved.
func WithReturning(query string) string {
	query = trimSQLTerminator(query)
	if idx := topLevelKeyword(query, "RETURNING"); idx >= 0 {
		return strings.TrimSpace(query[:idx]) + " RETURNING *"
	}
	return query + " RETURNING *"
}

// questionMarkOffsets returns bind-marker offsets while ignoring question marks
// inside SQL literals, quoted identifiers, comments, and dollar-quoted bodies.
func questionMarkOffsets(query string) []int {
	offsets := make([]int, 0, 8)
	for i := 0; i < len(query); {
		switch query[i] {
		case '\'':
			i = skipQuoted(query, i, '\'')
			continue
		case '"':
			i = skipQuoted(query, i, '"')
			continue
		case '`':
			i = skipQuoted(query, i, '`')
			continue
		case '[':
			i = skipBracketIdentifier(query, i)
			continue
		case '-':
			if i+1 < len(query) && query[i+1] == '-' {
				i = skipLineComment(query, i+2)
				continue
			}
		case '#':
			i = skipLineComment(query, i+1)
			continue
		case '/':
			if i+1 < len(query) && query[i+1] == '*' {
				i = skipBlockComment(query, i+2)
				continue
			}
		case '$':
			if end, ok := skipDollarQuote(query, i); ok {
				i = end
				continue
			}
		case '?':
			offsets = append(offsets, i)
		}
		i++
	}
	return offsets
}
