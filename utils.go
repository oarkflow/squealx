package squealx

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"
)

// Contains appends '%' on both sides of the input string
func Contains(val string) string {
	return "%" + val + "%"
}

// StartsWith appends '%' on the right side of the input string
func StartsWith(val string) string {
	return val + "%"
}

// EndsWith appends '%' on the left side of the input string
func EndsWith(val string) string {
	return "%" + val
}

// Sum appends '%' on the left side of the input string
func Sum(val string) string {
	return fmt.Sprintf("SUM(%s)", val)
}

var (
	namedRE = regexp.MustCompile(`\b[^:]+:[^:]+\b`)
)

func IsNamedQuery(query string) bool {
	return namedRE.MatchString(query)
}

// LimitQuery appends or replaces "LIMIT 1" in the SQL query.
func LimitQuery(query string) string {
	lowerQuery := strings.ToLower(query)
	limitIndex := strings.LastIndex(lowerQuery, " limit ")
	if limitIndex != -1 {
		return query[:limitIndex] + " LIMIT 1"
	}
	return strings.TrimSpace(query) + " LIMIT 1"
}

// WithReturning appends or replaces "LIMIT 1" in the SQL query.
func WithReturning(query string) string {
	lowerQuery := strings.ToLower(query)
	limitIndex := strings.LastIndex(lowerQuery, " returning ")
	if limitIndex != -1 {
		return query[:limitIndex] + " RETURNING *"
	}
	return strings.TrimSpace(query) + " RETURNING *"
}

// ReplacePlaceholders safely replaces placeholders (e.g., @work_item_id) with :work_item_id in an SQL query.
// It skips replacements inside strings and comments.
func ReplacePlaceholders(query string) string {
	var result strings.Builder
	result.Grow(len(query)) // Pre-allocate for efficiency.

	inSingleQuote := false
	inDoubleQuote := false
	inBracket := false
	inLineComment := false
	inBlockComment := false

	i := 0
	for i < len(query) {
		// Handle string literals and comments
		if inLineComment {
			if query[i] == '\n' {
				inLineComment = false
			}
			result.WriteByte(query[i])
		} else if inBlockComment {
			if i+1 < len(query) && query[i] == '*' && query[i+1] == '/' {
				inBlockComment = false
				result.WriteString("*/")
				i++
			} else {
				result.WriteByte(query[i])
			}
		} else if inSingleQuote {
			// Handle escaped single quote within single-quoted strings.
			if query[i] == '\'' {
				if i+1 < len(query) && query[i+1] == '\'' {
					result.WriteString("''") // Append escaped quote.
					i++                      // Skip both quotes.
				} else {
					inSingleQuote = false
					result.WriteByte(query[i])
				}
			} else {
				result.WriteByte(query[i])
			}
		} else if inDoubleQuote {
			// Handle escaped double quote within double-quoted strings.
			if query[i] == '"' {
				if i+1 < len(query) && query[i+1] == '"' {
					result.WriteString(`""`) // Append escaped quote.
					i++                      // Skip both quotes.
				} else {
					inDoubleQuote = false
					result.WriteByte(query[i])
				}
			} else {
				result.WriteByte(query[i])
			}
		} else if inBracket {
			if query[i] == ']' {
				inBracket = false
			}
			result.WriteByte(query[i])
		} else {
			// Detect start of line or block comments, single or double quotes, and brackets
			if i+1 < len(query) && query[i] == '-' && query[i+1] == '-' && (i == 0 || unicode.IsSpace(rune(query[i-1]))) {
				inLineComment = true
				result.WriteString("--")
				i++
			} else if i+1 < len(query) && query[i] == '/' && query[i+1] == '*' {
				inBlockComment = true
				result.WriteString("/*")
				i++
			} else if query[i] == '\'' {
				inSingleQuote = true
				result.WriteByte(query[i])
			} else if query[i] == '"' {
				inDoubleQuote = true
				result.WriteByte(query[i])
			} else if query[i] == '[' {
				inBracket = true
				result.WriteByte(query[i])
			} else if query[i] == '@' {
				// Replace `@` with `:` and retain the placeholder name.
				result.WriteByte(':')
				i++
				// Copy the alphanumeric placeholder name following `@`.
				for i < len(query) && (unicode.IsLetter(rune(query[i])) || unicode.IsDigit(rune(query[i]))) {
					result.WriteByte(query[i])
					i++
				}
				continue
			} else {
				// Append non-placeholder characters as they are.
				result.WriteByte(query[i])
			}
		}
		i++
	}

	return result.String()
}
