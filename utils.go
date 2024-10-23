package squealx

import (
	"fmt"
	"regexp"
	"strings"
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
