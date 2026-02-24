package jsonbq

import "strings"

type ClauseRange struct {
	Name       string
	StartToken int
	EndToken   int
}

type SQLStatement struct {
	Kind    StatementKind
	Tokens  []SQLToken
	Clauses []ClauseRange
	AST     *StatementAST
}

func ParseSQL(query string) (*SQLStatement, error) {
	tokens, err := LexSQL(query)
	if err != nil {
		return nil, err
	}
	return ParseTokens(tokens), nil
}

func ParseTokens(tokens []SQLToken) *SQLStatement {
	stmt := &SQLStatement{
		Kind:   detectStatementKind(tokens),
		Tokens: tokens,
	}
	stmt.Clauses = parseClauses(stmt.Kind, tokens)
	stmt.AST = parseStatementAST(stmt.Kind, tokens, stmt.Clauses)
	return stmt
}

func detectStatementKind(tokens []SQLToken) StatementKind {
	for _, tok := range tokens {
		if tok.Type != SQLTokenKeyword && tok.Type != SQLTokenIdentifier {
			continue
		}
		switch normalizeWord(tok.Literal) {
		case "SELECT", "WITH":
			return StatementSelect
		case "INSERT":
			return StatementInsert
		case "UPDATE":
			return StatementUpdate
		case "DELETE":
			return StatementDelete
		default:
			return StatementUnknown
		}
	}
	return StatementUnknown
}

func parseClauses(kind StatementKind, tokens []SQLToken) []ClauseRange {
	switch kind {
	case StatementSelect:
		return parseSelectClauses(tokens)
	case StatementInsert:
		return parseInsertClauses(tokens)
	case StatementUpdate:
		return parseUpdateClauses(tokens)
	case StatementDelete:
		return parseDeleteClauses(tokens)
	default:
		return nil
	}
}

func parseSelectClauses(tokens []SQLToken) []ClauseRange {
	clauseStarts := map[string]int{}
	depth := 0

	for i := 0; i < len(tokens); i++ {
		tok := tokens[i]
		if tok.Type == SQLTokenPunctuation {
			if tok.Literal == "(" {
				depth++
			} else if tok.Literal == ")" && depth > 0 {
				depth--
			}
			continue
		}
		if depth != 0 || tok.Type != SQLTokenKeyword {
			continue
		}
		word := normalizeWord(tok.Literal)
		switch word {
		case "SELECT", "FROM", "WHERE", "HAVING", "LIMIT", "OFFSET":
			if _, exists := clauseStarts[word]; !exists {
				clauseStarts[word] = i
			}
		case "GROUP":
			if hasTopLevelKeyword(tokens, i+1, "BY", depth) {
				if _, exists := clauseStarts["GROUP BY"]; !exists {
					clauseStarts["GROUP BY"] = i
				}
			}
		case "ORDER":
			if hasTopLevelKeyword(tokens, i+1, "BY", depth) {
				if _, exists := clauseStarts["ORDER BY"]; !exists {
					clauseStarts["ORDER BY"] = i
				}
			}
		}
	}
	return buildClauseRanges(tokens, clauseStarts)
}

func parseInsertClauses(tokens []SQLToken) []ClauseRange {
	clauseStarts := map[string]int{}
	depth := 0
	for i := 0; i < len(tokens); i++ {
		tok := tokens[i]
		if tok.Type == SQLTokenPunctuation {
			if tok.Literal == "(" {
				depth++
			} else if tok.Literal == ")" && depth > 0 {
				depth--
			}
			continue
		}
		if depth != 0 || tok.Type != SQLTokenKeyword {
			continue
		}
		word := normalizeWord(tok.Literal)
		switch word {
		case "INSERT", "INTO", "VALUES", "RETURNING":
			if _, exists := clauseStarts[word]; !exists {
				clauseStarts[word] = i
			}
		case "ON":
			if hasTopLevelKeyword(tokens, i+1, "CONFLICT", depth) {
				if _, exists := clauseStarts["ON CONFLICT"]; !exists {
					clauseStarts["ON CONFLICT"] = i
				}
			}
		case "SELECT":
			if _, exists := clauseStarts["SELECT"]; !exists {
				clauseStarts["SELECT"] = i
			}
		}
	}
	return buildClauseRanges(tokens, clauseStarts)
}

func parseUpdateClauses(tokens []SQLToken) []ClauseRange {
	clauseStarts := map[string]int{}
	depth := 0
	for i := 0; i < len(tokens); i++ {
		tok := tokens[i]
		if tok.Type == SQLTokenPunctuation {
			if tok.Literal == "(" {
				depth++
			} else if tok.Literal == ")" && depth > 0 {
				depth--
			}
			continue
		}
		if depth != 0 || tok.Type != SQLTokenKeyword {
			continue
		}
		word := normalizeWord(tok.Literal)
		switch word {
		case "UPDATE", "SET", "WHERE", "RETURNING":
			if _, exists := clauseStarts[word]; !exists {
				clauseStarts[word] = i
			}
		}
	}
	return buildClauseRanges(tokens, clauseStarts)
}

func parseDeleteClauses(tokens []SQLToken) []ClauseRange {
	clauseStarts := map[string]int{}
	depth := 0
	for i := 0; i < len(tokens); i++ {
		tok := tokens[i]
		if tok.Type == SQLTokenPunctuation {
			if tok.Literal == "(" {
				depth++
			} else if tok.Literal == ")" && depth > 0 {
				depth--
			}
			continue
		}
		if depth != 0 || tok.Type != SQLTokenKeyword {
			continue
		}
		word := normalizeWord(tok.Literal)
		switch word {
		case "DELETE", "FROM", "USING", "WHERE", "RETURNING":
			if _, exists := clauseStarts[word]; !exists {
				clauseStarts[word] = i
			}
		}
	}
	return buildClauseRanges(tokens, clauseStarts)
}

func buildClauseRanges(tokens []SQLToken, starts map[string]int) []ClauseRange {
	if len(starts) == 0 {
		return nil
	}
	type kv struct {
		name string
		pos  int
	}
	all := make([]kv, 0, len(starts))
	for name, pos := range starts {
		all = append(all, kv{name: name, pos: pos})
	}
	// stable-ish small sort
	for i := 0; i < len(all); i++ {
		for j := i + 1; j < len(all); j++ {
			if all[j].pos < all[i].pos {
				all[i], all[j] = all[j], all[i]
			}
		}
	}
	clauses := make([]ClauseRange, 0, len(all))
	for i := 0; i < len(all); i++ {
		end := len(tokens)
		if i+1 < len(all) {
			end = all[i+1].pos
		}
		clauses = append(clauses, ClauseRange{
			Name:       all[i].name,
			StartToken: all[i].pos,
			EndToken:   end,
		})
	}
	return clauses
}

func hasTopLevelKeyword(tokens []SQLToken, idx int, word string, depthAtStart int) bool {
	depth := depthAtStart
	for ; idx < len(tokens); idx++ {
		tok := tokens[idx]
		switch tok.Type {
		case SQLTokenWhitespace, SQLTokenComment:
			continue
		case SQLTokenPunctuation:
			if tok.Literal == "(" {
				depth++
			} else if tok.Literal == ")" {
				if depth > 0 {
					depth--
				}
			}
			if depth == depthAtStart {
				return false
			}
		case SQLTokenKeyword, SQLTokenIdentifier:
			return depth == depthAtStart && strings.EqualFold(strings.TrimSpace(tok.Literal), word)
		default:
			return false
		}
	}
	return false
}

func parseStatementAST(kind StatementKind, tokens []SQLToken, clauses []ClauseRange) *StatementAST {
	ast := &StatementAST{}
	switch kind {
	case StatementSelect:
		parseSelectAST(ast, tokens, clauses)
	case StatementInsert:
		parseInsertAST(ast, tokens, clauses)
	case StatementUpdate:
		parseUpdateAST(ast, tokens, clauses)
	case StatementDelete:
		parseDeleteAST(ast, tokens, clauses)
	}
	return ast
}

func parseSelectAST(ast *StatementAST, tokens []SQLToken, clauses []ClauseRange) {
	if r, ok := findClause(clauses, "SELECT"); ok {
		ast.SelectItems = parseExprListForClause(tokens, r, 1)
	}
	if r, ok := findClause(clauses, "WHERE"); ok {
		ast.Where = parseClauseSingleExpr(tokens, r, 1)
	}
	if r, ok := findClause(clauses, "HAVING"); ok {
		ast.Having = parseClauseSingleExpr(tokens, r, 1)
	}
	if r, ok := findClause(clauses, "GROUP BY"); ok {
		ast.GroupBy = parseExprListForClause(tokens, r, 2)
	}
	if r, ok := findClause(clauses, "ORDER BY"); ok {
		ast.OrderBy = parseExprListForClause(tokens, r, 2)
	}
	if r, ok := findClause(clauses, "LIMIT"); ok {
		ast.Limit = parseClauseSingleExpr(tokens, r, 1)
	}
	if r, ok := findClause(clauses, "OFFSET"); ok {
		ast.Offset = parseClauseSingleExpr(tokens, r, 1)
	}
}

func parseInsertAST(ast *StatementAST, tokens []SQLToken, clauses []ClauseRange) {
	if r, ok := findClause(clauses, "VALUES"); ok {
		ast.Values = parseInsertValues(tokens, r)
	}
	if r, ok := findClause(clauses, "RETURNING"); ok {
		ast.Returning = parseExprListForClause(tokens, r, 1)
	}
}

func parseUpdateAST(ast *StatementAST, tokens []SQLToken, clauses []ClauseRange) {
	if r, ok := findClause(clauses, "SET"); ok {
		ast.Assignments = parseUpdateAssignments(tokens, r)
	}
	if r, ok := findClause(clauses, "WHERE"); ok {
		ast.Where = parseClauseSingleExpr(tokens, r, 1)
	}
	if r, ok := findClause(clauses, "RETURNING"); ok {
		ast.Returning = parseExprListForClause(tokens, r, 1)
	}
}

func parseDeleteAST(ast *StatementAST, tokens []SQLToken, clauses []ClauseRange) {
	if r, ok := findClause(clauses, "WHERE"); ok {
		ast.Where = parseClauseSingleExpr(tokens, r, 1)
	}
	if r, ok := findClause(clauses, "RETURNING"); ok {
		ast.Returning = parseExprListForClause(tokens, r, 1)
	}
}

func findClause(clauses []ClauseRange, name string) (ClauseRange, bool) {
	for _, c := range clauses {
		if strings.EqualFold(c.Name, name) {
			return c, true
		}
	}
	return ClauseRange{}, false
}

func parseClauseSingleExpr(tokens []SQLToken, r ClauseRange, skip int) SQLExpr {
	clauseTokens := sliceClauseTokens(tokens, r, skip)
	return parseSQLExpression(clauseTokens)
}

func parseExprListForClause(tokens []SQLToken, r ClauseRange, skip int) []SQLExpr {
	clauseTokens := compactTokens(sliceClauseTokens(tokens, r, skip))
	parts := splitTopLevelByComma(clauseTokens)
	exprs := make([]SQLExpr, 0, len(parts))
	for _, part := range parts {
		if len(part) == 0 {
			continue
		}
		exprs = append(exprs, parseAliasedExpr(part))
	}
	return exprs
}

func parseAliasedExpr(tokens []SQLToken) SQLExpr {
	if len(tokens) >= 3 {
		last := tokens[len(tokens)-1]
		prev := tokens[len(tokens)-2]
		if (last.Type == SQLTokenIdentifier || last.Type == SQLTokenKeyword) &&
			prev.Type == SQLTokenKeyword && strings.EqualFold(prev.Literal, "AS") {
			base := parseSQLExpression(tokens[:len(tokens)-2])
			return AliasExpr{Expr: base, Alias: last.Literal}
		}
	}
	if len(tokens) >= 2 {
		last := tokens[len(tokens)-1]
		prev := tokens[len(tokens)-2]
		if (last.Type == SQLTokenIdentifier || last.Type == SQLTokenKeyword) &&
			prev.Type != SQLTokenOperator && prev.Type != SQLTokenPunctuation {
			base := parseSQLExpression(tokens[:len(tokens)-1])
			// avoid treating DESC/ASC as alias in ORDER BY
			up := normalizeWord(last.Literal)
			if up != "ASC" && up != "DESC" {
				return AliasExpr{Expr: base, Alias: last.Literal}
			}
		}
	}
	return parseSQLExpression(tokens)
}

func parseUpdateAssignments(tokens []SQLToken, r ClauseRange) []SetAssignment {
	clauseTokens := compactTokens(sliceClauseTokens(tokens, r, 1))
	parts := splitTopLevelByComma(clauseTokens)
	assignments := make([]SetAssignment, 0, len(parts))
	for _, part := range parts {
		eqIdx := indexTopLevelOperator(part, "=")
		if eqIdx <= 0 || eqIdx >= len(part)-1 {
			continue
		}
		left := parseSQLExpression(part[:eqIdx])
		right := parseSQLExpression(part[eqIdx+1:])
		assignments = append(assignments, SetAssignment{Column: left, Value: right})
	}
	return assignments
}

func parseInsertValues(tokens []SQLToken, r ClauseRange) [][]SQLExpr {
	clauseTokens := compactTokens(sliceClauseTokens(tokens, r, 1))
	rows := make([][]SQLExpr, 0)
	for i := 0; i < len(clauseTokens); i++ {
		if clauseTokens[i].Type == SQLTokenPunctuation && clauseTokens[i].Literal == "(" {
			end := findMatchingParen(clauseTokens, i)
			if end <= i {
				break
			}
			inside := clauseTokens[i+1 : end]
			parts := splitTopLevelByComma(inside)
			row := make([]SQLExpr, 0, len(parts))
			for _, p := range parts {
				row = append(row, parseSQLExpression(p))
			}
			rows = append(rows, row)
			i = end
		}
	}
	return rows
}

func sliceClauseTokens(tokens []SQLToken, r ClauseRange, skip int) []SQLToken {
	start := r.StartToken + skip
	if start < 0 {
		start = 0
	}
	if start > len(tokens) {
		start = len(tokens)
	}
	end := r.EndToken
	if end < start {
		end = start
	}
	if end > len(tokens) {
		end = len(tokens)
	}
	return tokens[start:end]
}

func splitTopLevelByComma(tokens []SQLToken) [][]SQLToken {
	if len(tokens) == 0 {
		return nil
	}
	out := make([][]SQLToken, 0, 2)
	start := 0
	depth := 0
	for i, t := range tokens {
		if t.Type == SQLTokenPunctuation {
			if t.Literal == "(" {
				depth++
			} else if t.Literal == ")" && depth > 0 {
				depth--
			} else if t.Literal == "," && depth == 0 {
				out = append(out, tokens[start:i])
				start = i + 1
			}
		}
	}
	out = append(out, tokens[start:])
	return out
}

func indexTopLevelOperator(tokens []SQLToken, op string) int {
	depth := 0
	for i, t := range tokens {
		if t.Type == SQLTokenPunctuation {
			if t.Literal == "(" {
				depth++
			} else if t.Literal == ")" && depth > 0 {
				depth--
			}
			continue
		}
		if depth == 0 && t.Type == SQLTokenOperator && t.Literal == op {
			return i
		}
	}
	return -1
}

func findMatchingParen(tokens []SQLToken, openIdx int) int {
	depth := 0
	for i := openIdx; i < len(tokens); i++ {
		t := tokens[i]
		if t.Type != SQLTokenPunctuation {
			continue
		}
		if t.Literal == "(" {
			depth++
		} else if t.Literal == ")" {
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}
