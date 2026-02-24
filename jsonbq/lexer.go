package jsonbq

import (
	"fmt"
	"unicode"
)

func LexSQL(query string) ([]SQLToken, error) {
	tokens := make([]SQLToken, 0, len(query)/4)
	n := len(query)

	for i := 0; i < n; {
		ch := query[i]
		start := i

		if isWhitespace(ch) {
			for i < n && isWhitespace(query[i]) {
				i++
			}
			tokens = append(tokens, SQLToken{Type: SQLTokenWhitespace, Literal: query[start:i], Start: start, End: i})
			continue
		}

		// line comment
		if ch == '-' && i+1 < n && query[i+1] == '-' {
			i += 2
			for i < n && query[i] != '\n' {
				i++
			}
			tokens = append(tokens, SQLToken{Type: SQLTokenComment, Literal: query[start:i], Start: start, End: i})
			continue
		}

		// block comment
		if ch == '/' && i+1 < n && query[i+1] == '*' {
			i += 2
			for i+1 < n {
				if query[i] == '*' && query[i+1] == '/' {
					i += 2
					break
				}
				i++
			}
			if i > n {
				i = n
			}
			tokens = append(tokens, SQLToken{Type: SQLTokenComment, Literal: query[start:i], Start: start, End: i})
			continue
		}

		// placeholder
		if ch == '{' && i+1 < n && query[i+1] == '{' {
			i += 2
			for i+1 < n {
				if query[i] == '}' && query[i+1] == '}' {
					i += 2
					break
				}
				i++
			}
			if i > n || (i == n && !(n >= 2 && query[n-2] == '}' && query[n-1] == '}')) {
				return nil, fmt.Errorf("unterminated placeholder at offset %d", start)
			}
			tokens = append(tokens, SQLToken{Type: SQLTokenPlaceholder, Literal: query[start:i], Start: start, End: i})
			continue
		}

		// named parameter placeholder :name (but not ::cast)
		if ch == ':' && i+1 < n && query[i+1] != ':' && isWordStart(query[i+1]) {
			i += 2
			for i < n && isWordPart(query[i]) {
				i++
			}
			tokens = append(tokens, SQLToken{Type: SQLTokenPlaceholder, Literal: query[start:i], Start: start, End: i})
			continue
		}

		// cast/type operator ::
		if ch == ':' && i+1 < n && query[i+1] == ':' {
			i += 2
			tokens = append(tokens, SQLToken{Type: SQLTokenOperator, Literal: query[start:i], Start: start, End: i})
			continue
		}

		// single quoted string
		if ch == '\'' {
			i++
			for i < n {
				if query[i] == '\'' {
					if i+1 < n && query[i+1] == '\'' {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			if i > n {
				i = n
			}
			tokens = append(tokens, SQLToken{Type: SQLTokenString, Literal: query[start:i], Start: start, End: i})
			continue
		}

		// quoted identifier
		if ch == '"' {
			i++
			for i < n {
				if query[i] == '"' {
					if i+1 < n && query[i+1] == '"' {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			if i > n {
				i = n
			}
			tokens = append(tokens, SQLToken{Type: SQLTokenIdentifier, Literal: query[start:i], Start: start, End: i})
			continue
		}

		// dollar-quoted string
		if ch == '$' {
			if tag, ok := parseDollarTag(query, i); ok {
				i += len(tag)
				found := false
				for i+len(tag) <= n {
					if query[i:i+len(tag)] == tag {
						i += len(tag)
						found = true
						break
					}
					i++
				}
				if !found {
					i = n
				}
				tokens = append(tokens, SQLToken{Type: SQLTokenString, Literal: query[start:i], Start: start, End: i})
				continue
			}
		}

		if isWordStart(ch) {
			i++
			for i < n && isWordPart(query[i]) {
				i++
			}
			word := query[start:i]
			tt := SQLTokenIdentifier
			if isKeyword(word) {
				tt = SQLTokenKeyword
			}
			tokens = append(tokens, SQLToken{Type: tt, Literal: word, Start: start, End: i})
			continue
		}

		if isDigit(ch) {
			i++
			for i < n && (isDigit(query[i]) || query[i] == '.') {
				i++
			}
			tokens = append(tokens, SQLToken{Type: SQLTokenNumber, Literal: query[start:i], Start: start, End: i})
			continue
		}

		if isPunctuation(ch) {
			i++
			tokens = append(tokens, SQLToken{Type: SQLTokenPunctuation, Literal: query[start:i], Start: start, End: i})
			continue
		}

		if isOperatorChar(ch) {
			i++
			for i < n && isOperatorChar(query[i]) {
				i++
			}
			tokens = append(tokens, SQLToken{Type: SQLTokenOperator, Literal: query[start:i], Start: start, End: i})
			continue
		}

		i++
		tokens = append(tokens, SQLToken{Type: SQLTokenUnknown, Literal: query[start:i], Start: start, End: i})
	}

	return tokens, nil
}

func isWhitespace(b byte) bool {
	return b == ' ' || b == '\n' || b == '\r' || b == '\t' || b == '\f' || b == '\v'
}

func isWordStart(b byte) bool {
	r := rune(b)
	return r == '_' || unicode.IsLetter(r)
}

func isWordPart(b byte) bool {
	r := rune(b)
	return r == '_' || r == '$' || unicode.IsLetter(r) || unicode.IsDigit(r)
}

func isDigit(b byte) bool { return b >= '0' && b <= '9' }

func isPunctuation(b byte) bool {
	switch b {
	case '(', ')', ',', ';', '.':
		return true
	default:
		return false
	}
}

func isOperatorChar(b byte) bool {
	switch b {
	case '+', '-', '*', '/', '=', '<', '>', '!', '~', '|', '&', '%', '^', '?':
		return true
	default:
		return false
	}
}
