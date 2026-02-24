package jsonbq

import "strings"

type SQLTokenType int

const (
	SQLTokenUnknown SQLTokenType = iota
	SQLTokenWhitespace
	SQLTokenComment
	SQLTokenIdentifier
	SQLTokenKeyword
	SQLTokenNumber
	SQLTokenString
	SQLTokenOperator
	SQLTokenPunctuation
	SQLTokenPlaceholder
)

type SQLToken struct {
	Type    SQLTokenType
	Literal string
	Start   int
	End     int
}

type StatementKind string

const (
	StatementUnknown StatementKind = "unknown"
	StatementSelect  StatementKind = "select"
	StatementInsert  StatementKind = "insert"
	StatementUpdate  StatementKind = "update"
	StatementDelete  StatementKind = "delete"
)

var sqlKeywords = map[string]struct{}{
	"SELECT": {}, "FROM": {}, "WHERE": {}, "GROUP": {}, "BY": {}, "HAVING": {}, "ORDER": {}, "LIMIT": {}, "OFFSET": {},
	"INSERT": {}, "INTO": {}, "VALUES": {}, "UPDATE": {}, "SET": {}, "DELETE": {}, "USING": {}, "JOIN": {}, "LEFT": {},
	"RIGHT": {}, "FULL": {}, "INNER": {}, "OUTER": {}, "ON": {}, "RETURNING": {}, "DISTINCT": {}, "AS": {}, "CASE": {},
	"WHEN": {}, "THEN": {}, "ELSE": {}, "END": {}, "AND": {}, "OR": {}, "NOT": {}, "NULL": {}, "IS": {}, "IN": {},
	"UNION": {}, "ALL": {}, "WITH": {}, "CONFLICT": {},
}

func normalizeWord(word string) string {
	return strings.ToUpper(strings.TrimSpace(word))
}

func isKeyword(word string) bool {
	_, ok := sqlKeywords[normalizeWord(word)]
	return ok
}
