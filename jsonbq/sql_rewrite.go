package jsonbq

import (
	"fmt"
	"sort"
	"strings"
)

// RewriteJSONFunctions rewrites human-friendly helpers into PostgreSQL syntax.
//
// Supported helpers:
//   - JSON_TEXT(col, key)
//   - JSON_JSON(col, key)
//   - JSON_NUM(col, key)
//   - JSON_INT(col, key)
//   - JSON_BOOL(col, key)
//   - JSON_DATE(col, key)
//   - JSON_TS(col, key)
//   - JSON_TSTZ(col, key)
//   - JSON_TEXT_PATH(col, key1, key2, ...)
//   - JSON_PATH(col, key1, key2, ...)
//   - IF(cond, then_expr, else_expr)
//   - IFNULL(a, b)
//   - NVL(a, b)
func RewriteJSONFunctions(query string) (string, error) {
	tokens, err := LexSQL(query)
	if err != nil {
		return "", err
	}
	rewritten, err := rewriteJSONFunctionTokens(tokens)
	if err != nil {
		return "", err
	}
	return rewritten, nil
}

// RewriteJSONDotNotation rewrites `<obj>.<jsonColumn>.<path...>` into jsonb_extract_path_text calls.
// Example:
//
//	pt.data.name -> jsonb_extract_path_text(pt.data, 'name')
//	c.data.procedure.code -> jsonb_extract_path_text(c.data, 'procedure', 'code')
func RewriteJSONDotNotation(query string, jsonColumns []string) (string, error) {
	tokens, err := LexSQL(query)
	if err != nil {
		return "", err
	}
	if len(jsonColumns) == 0 {
		jsonColumns = []string{"data"}
	}
	var b strings.Builder
	for i := 0; i < len(tokens); i++ {
		tok := tokens[i]
		if tok.Type == SQLTokenWhitespace || tok.Type == SQLTokenComment {
			b.WriteString(tok.Literal)
			continue
		}
		base, paths, endIdx, ok := parseJSONDotPath(tokens, i, jsonColumns)
		if ok {
			b.WriteString("jsonb_extract_path_text(")
			b.WriteString(base)
			for _, p := range paths {
				b.WriteString(", ")
				b.WriteString(quoteSQLString(p))
			}
			b.WriteString(")")
			i = endIdx
			continue
		}
		b.WriteString(tok.Literal)
	}
	return strings.TrimSpace(b.String()), nil
}

// RewriteEncryptedSearchPredicates rewrites searchable encrypted field equality predicates
// from plain JSON-path style into blind-index comparisons.
//
// Example:
//
//	jsonb_extract_path_text(p.data, 'email') = :email
//
// becomes:
//
//	jsonb_extract_path_text(p.data, '_secure_idx', 'email') = 'blind-index'
func RewriteEncryptedSearchPredicates(query string, vars map[string]any, searchablePaths map[string]bool, hmacKey string) (string, error) {
	if len(searchablePaths) == 0 || strings.TrimSpace(hmacKey) == "" {
		return query, nil
	}
	tokens, err := LexSQL(query)
	if err != nil {
		return "", err
	}
	type patch struct {
		start int
		end   int
		text  string
	}
	var patches []patch
	seen := make(map[string]struct{})

	for i := 0; i < len(tokens); i++ {
		fn := tokens[i]
		if (fn.Type != SQLTokenIdentifier && fn.Type != SQLTokenKeyword) || !strings.EqualFold(strings.TrimSpace(fn.Literal), "jsonb_extract_path_text") {
			continue
		}
		openIdx := nextSignificantTokenIndex(tokens, i+1)
		if openIdx < 0 || tokens[openIdx].Type != SQLTokenPunctuation || tokens[openIdx].Literal != "(" {
			continue
		}
		closeIdx := findMatchingParen(tokens, openIdx)
		if closeIdx < 0 {
			continue
		}
		baseExpr, dotPath, ok := parseJSONExtractPathTextCall(query, tokens, openIdx, closeIdx)
		if !ok || dotPath == "" || strings.HasPrefix(dotPath, "_secure_idx.") || !searchablePaths[dotPath] {
			i = closeIdx
			continue
		}

		opRight := nextSignificantTokenIndex(tokens, closeIdx+1)
		if opRight >= 0 && tokens[opRight].Type == SQLTokenOperator && tokens[opRight].Literal == "=" {
			rhs := nextSignificantTokenIndex(tokens, opRight+1)
			if rhs >= 0 {
				idx, ok, err := blindIndexFromToken(tokens[rhs], vars, hmacKey)
				if err != nil {
					return "", err
				}
				if ok {
					lhsSQL := blindIndexExpr(baseExpr, dotPath)
					key := fmt.Sprintf("%d:%d:%d:R", fn.Start, tokens[closeIdx].End, tokens[rhs].Start)
					if _, exists := seen[key]; !exists {
						patches = append(patches, patch{start: fn.Start, end: tokens[closeIdx].End, text: lhsSQL})
						patches = append(patches, patch{start: tokens[rhs].Start, end: tokens[rhs].End, text: quoteSQLString(idx)})
						seen[key] = struct{}{}
					}
				}
			}
		}

		opLeft := previousSignificantTokenIndex(tokens, i-1)
		if opLeft >= 0 && tokens[opLeft].Type == SQLTokenOperator && tokens[opLeft].Literal == "=" {
			lhs := previousSignificantTokenIndex(tokens, opLeft-1)
			if lhs >= 0 {
				idx, ok, err := blindIndexFromToken(tokens[lhs], vars, hmacKey)
				if err != nil {
					return "", err
				}
				if ok {
					rhsSQL := blindIndexExpr(baseExpr, dotPath)
					key := fmt.Sprintf("%d:%d:%d:L", tokens[lhs].Start, tokens[lhs].End, fn.Start)
					if _, exists := seen[key]; !exists {
						patches = append(patches, patch{start: fn.Start, end: tokens[closeIdx].End, text: rhsSQL})
						patches = append(patches, patch{start: tokens[lhs].Start, end: tokens[lhs].End, text: quoteSQLString(idx)})
						seen[key] = struct{}{}
					}
				}
			}
		}

		i = closeIdx
	}

	if len(patches) == 0 {
		return query, nil
	}
	// apply from left to right
	sort.SliceStable(patches, func(i, j int) bool {
		if patches[i].start == patches[j].start {
			return patches[i].end < patches[j].end
		}
		return patches[i].start < patches[j].start
	})
	var b strings.Builder
	cursor := 0
	for _, p := range patches {
		if p.start < cursor {
			continue
		}
		b.WriteString(query[cursor:p.start])
		b.WriteString(p.text)
		cursor = p.end
	}
	b.WriteString(query[cursor:])
	return b.String(), nil
}

// RewriteImplicitNumericJSONCasts auto-casts jsonb_extract_path_text(...) to numeric
// when used in clearly numeric contexts (SUM/AVG, arithmetic, > < >= <= comparisons).
func RewriteImplicitNumericJSONCasts(query string) (string, error) {
	tokens, err := LexSQL(query)
	if err != nil {
		return "", err
	}
	type patch struct {
		start int
		end   int
		text  string
	}
	var patches []patch
	for i := 0; i < len(tokens); i++ {
		tok := tokens[i]
		if (tok.Type != SQLTokenIdentifier && tok.Type != SQLTokenKeyword) || !strings.EqualFold(strings.TrimSpace(tok.Literal), "jsonb_extract_path_text") {
			continue
		}
		openIdx := nextSignificantTokenIndex(tokens, i+1)
		if openIdx < 0 || tokens[openIdx].Type != SQLTokenPunctuation || tokens[openIdx].Literal != "(" {
			continue
		}
		closeIdx := findMatchingParen(tokens, openIdx)
		if closeIdx < 0 {
			continue
		}
		if !shouldImplicitlyCastJSONTextNumeric(tokens, i, closeIdx) {
			i = closeIdx
			continue
		}
		start := tok.Start
		end := tokens[closeIdx].End
		if start < 0 || end > len(query) || start >= end {
			i = closeIdx
			continue
		}
		callSQL := query[start:end]
		patches = append(patches, patch{
			start: start,
			end:   end,
			text:  "(" + callSQL + ")::numeric",
		})
		i = closeIdx
	}

	rewritten := query
	if len(patches) > 0 {
		var b strings.Builder
		cursor := 0
		for _, p := range patches {
			if p.start < cursor {
				continue
			}
			b.WriteString(query[cursor:p.start])
			b.WriteString(p.text)
			cursor = p.end
		}
		b.WriteString(query[cursor:])
		rewritten = b.String()
	}
	rewritten, err = RewriteInferredNumericAliasCasts(rewritten)
	if err != nil {
		return "", err
	}
	return rewritten, nil
}

func shouldImplicitlyCastJSONTextNumeric(tokens []SQLToken, fnIdx, closeIdx int) bool {
	// already explicitly cast
	next := nextSignificantTokenIndex(tokens, closeIdx+1)
	if next >= 0 && tokens[next].Type == SQLTokenOperator && tokens[next].Literal == "::" {
		return false
	}

	// aggregate numeric context: SUM(...) / AVG(...)
	openIdx := previousSignificantTokenIndex(tokens, fnIdx-1)
	if openIdx >= 0 && tokens[openIdx].Type == SQLTokenPunctuation && tokens[openIdx].Literal == "(" {
		fn := previousSignificantTokenIndex(tokens, openIdx-1)
		if fn >= 0 && (tokens[fn].Type == SQLTokenIdentifier || tokens[fn].Type == SQLTokenKeyword) {
			name := normalizeWord(tokens[fn].Literal)
			if name == "SUM" || name == "AVG" {
				return true
			}
		}
	}

	// operator context around this expression
	prev := previousSignificantTokenIndex(tokens, fnIdx-1)
	if prev >= 0 && tokens[prev].Type == SQLTokenOperator && isNumericContextOperator(tokens[prev].Literal) {
		return true
	}
	if next >= 0 && tokens[next].Type == SQLTokenOperator && isNumericContextOperator(tokens[next].Literal) {
		return true
	}
	return false
}

func isNumericContextOperator(op string) bool {
	switch strings.TrimSpace(op) {
	case "+", "-", "*", "/", "%", ">", "<", ">=", "<=":
		return true
	default:
		return false
	}
}

// RewriteInferredNumericAliasCasts auto-casts identifiers like claim_amount/paid_total
// when used in numeric contexts (SUM/AVG and numeric operators).
func RewriteInferredNumericAliasCasts(query string) (string, error) {
	tokens, err := LexSQL(query)
	if err != nil {
		return "", err
	}
	numericAliases := collectNumericAliases(tokens)
	if len(numericAliases) == 0 {
		return query, nil
	}
	type patch struct {
		start int
		end   int
		text  string
	}
	var patches []patch

	for i := 0; i < len(tokens); i++ {
		tok := tokens[i]
		if !(tok.Type == SQLTokenIdentifier || tok.Type == SQLTokenKeyword) {
			continue
		}
		startIdx, endIdx, ident, ok := parseIdentifierChain(tokens, i)
		if !ok {
			continue
		}
		if startIdx != i {
			continue
		}
		leaf := identifierLeaf(ident)
		if !numericAliases[strings.ToLower(strings.Trim(strings.TrimSpace(leaf), `"`))] {
			i = endIdx
			continue
		}
		if !isIdentifierNumericContext(tokens, startIdx, endIdx) {
			i = endIdx
			continue
		}
		// skip already casted identifiers
		next := nextSignificantTokenIndex(tokens, endIdx+1)
		if next >= 0 && tokens[next].Type == SQLTokenOperator && tokens[next].Literal == "::" {
			i = endIdx
			continue
		}
		startPos := tokens[startIdx].Start
		endPos := tokens[endIdx].End
		patches = append(patches, patch{
			start: startPos,
			end:   endPos,
			text:  "(" + query[startPos:endPos] + ")::numeric",
		})
		i = endIdx
	}
	if len(patches) == 0 {
		return query, nil
	}
	var b strings.Builder
	cursor := 0
	for _, p := range patches {
		if p.start < cursor {
			continue
		}
		b.WriteString(query[cursor:p.start])
		b.WriteString(p.text)
		cursor = p.end
	}
	b.WriteString(query[cursor:])
	return b.String(), nil
}

func collectNumericAliases(tokens []SQLToken) map[string]bool {
	out := make(map[string]bool)
	for i := 0; i < len(tokens); i++ {
		tok := tokens[i]
		if tok.Type != SQLTokenKeyword || !strings.EqualFold(strings.TrimSpace(tok.Literal), "AS") {
			continue
		}
		aliasIdx := nextSignificantTokenIndex(tokens, i+1)
		if aliasIdx < 0 {
			continue
		}
		aliasTok := tokens[aliasIdx]
		if aliasTok.Type != SQLTokenIdentifier && aliasTok.Type != SQLTokenKeyword {
			continue
		}
		alias := strings.TrimSpace(strings.Trim(aliasTok.Literal, `"`))
		if alias == "" || !isLikelyNumericAlias(alias) {
			continue
		}
		out[strings.ToLower(alias)] = true
	}
	return out
}

func isIdentifierNumericContext(tokens []SQLToken, startIdx, endIdx int) bool {
	prev := previousSignificantTokenIndex(tokens, startIdx-1)
	next := nextSignificantTokenIndex(tokens, endIdx+1)

	// SUM(alias) / AVG(alias)
	if prev >= 0 && tokens[prev].Type == SQLTokenPunctuation && tokens[prev].Literal == "(" {
		fn := previousSignificantTokenIndex(tokens, prev-1)
		if fn >= 0 && (tokens[fn].Type == SQLTokenIdentifier || tokens[fn].Type == SQLTokenKeyword) {
			name := normalizeWord(tokens[fn].Literal)
			if name == "SUM" || name == "AVG" || name == "MAX" || name == "MIN" {
				return true
			}
		}
	}
	if prev >= 0 && tokens[prev].Type == SQLTokenOperator && isNumericContextOperator(tokens[prev].Literal) {
		return true
	}
	if next >= 0 && tokens[next].Type == SQLTokenOperator && isNumericContextOperator(tokens[next].Literal) {
		return true
	}
	return false
}

func isLikelyNumericAlias(name string) bool {
	name = strings.ToLower(strings.TrimSpace(strings.Trim(name, `"`)))
	if name == "" {
		return false
	}
	return strings.Contains(name, "amount") ||
		strings.Contains(name, "total") ||
		strings.Contains(name, "count") ||
		strings.Contains(name, "score") ||
		strings.Contains(name, "rank") ||
		strings.HasPrefix(name, "num_") ||
		strings.HasSuffix(name, "_num")
}

func identifierLeaf(name string) string {
	name = strings.TrimSpace(name)
	if idx := strings.LastIndex(name, "."); idx >= 0 {
		name = name[idx+1:]
	}
	return strings.TrimSpace(name)
}

func parseIdentifierChain(tokens []SQLToken, idx int) (start int, end int, ident string, ok bool) {
	if idx < 0 || idx >= len(tokens) {
		return 0, 0, "", false
	}
	if tokens[idx].Type != SQLTokenIdentifier && tokens[idx].Type != SQLTokenKeyword {
		return 0, 0, "", false
	}
	start = idx
	// if we're on the second/third part, backtrack to the start
	for {
		prevDot := previousSignificantTokenIndex(tokens, start-1)
		if prevDot < 0 || tokens[prevDot].Type != SQLTokenPunctuation || tokens[prevDot].Literal != "." {
			break
		}
		prevIdent := previousSignificantTokenIndex(tokens, prevDot-1)
		if prevIdent < 0 || (tokens[prevIdent].Type != SQLTokenIdentifier && tokens[prevIdent].Type != SQLTokenKeyword) {
			break
		}
		start = prevIdent
	}

	var b strings.Builder
	end = start
	cur := start
	for {
		if tokens[cur].Type != SQLTokenIdentifier && tokens[cur].Type != SQLTokenKeyword {
			break
		}
		if b.Len() > 0 {
			b.WriteByte('.')
		}
		b.WriteString(tokens[cur].Literal)
		end = cur
		dot := nextSignificantTokenIndex(tokens, cur+1)
		if dot < 0 || tokens[dot].Type != SQLTokenPunctuation || tokens[dot].Literal != "." {
			break
		}
		nextIdent := nextSignificantTokenIndex(tokens, dot+1)
		if nextIdent < 0 || (tokens[nextIdent].Type != SQLTokenIdentifier && tokens[nextIdent].Type != SQLTokenKeyword) {
			break
		}
		end = nextIdent
		cur = nextIdent
	}
	return start, end, b.String(), true
}

func parseJSONDotPath(tokens []SQLToken, start int, jsonColumns []string) (base string, paths []string, end int, ok bool) {
	// pattern: ident . ident(json_col) . ident [ . ident ... ]
	if start >= len(tokens) {
		return "", nil, start, false
	}
	first := tokens[start]
	if first.Type != SQLTokenIdentifier && first.Type != SQLTokenKeyword {
		return "", nil, start, false
	}
	dot1 := nextSignificantTokenIndex(tokens, start+1)
	if dot1 < 0 || tokens[dot1].Type != SQLTokenPunctuation || tokens[dot1].Literal != "." {
		return "", nil, start, false
	}
	colIdx := nextSignificantTokenIndex(tokens, dot1+1)
	if colIdx < 0 {
		return "", nil, start, false
	}
	colTok := tokens[colIdx]
	if colTok.Type != SQLTokenIdentifier && colTok.Type != SQLTokenKeyword {
		return "", nil, start, false
	}
	if !isJSONColumn(colTok.Literal, jsonColumns) {
		return "", nil, start, false
	}
	dot2 := nextSignificantTokenIndex(tokens, colIdx+1)
	if dot2 < 0 || tokens[dot2].Type != SQLTokenPunctuation || tokens[dot2].Literal != "." {
		return "", nil, start, false
	}

	pathIdx := nextSignificantTokenIndex(tokens, dot2+1)
	if pathIdx < 0 {
		return "", nil, start, false
	}
	pathTok := tokens[pathIdx]
	if pathTok.Type != SQLTokenIdentifier && pathTok.Type != SQLTokenKeyword {
		return "", nil, start, false
	}

	base = strings.TrimSpace(first.Literal) + "." + strings.TrimSpace(colTok.Literal)
	paths = []string{normalizePathPart(pathTok.Literal)}
	end = pathIdx

	for {
		nextDot := nextSignificantTokenIndex(tokens, end+1)
		if nextDot < 0 || tokens[nextDot].Type != SQLTokenPunctuation || tokens[nextDot].Literal != "." {
			break
		}
		nextPart := nextSignificantTokenIndex(tokens, nextDot+1)
		if nextPart < 0 {
			break
		}
		partTok := tokens[nextPart]
		if partTok.Type != SQLTokenIdentifier && partTok.Type != SQLTokenKeyword {
			break
		}
		paths = append(paths, normalizePathPart(partTok.Literal))
		end = nextPart
	}
	return base, paths, end, true
}

func normalizePathPart(part string) string {
	part = strings.TrimSpace(part)
	part = strings.Trim(part, `"`)
	part = strings.ReplaceAll(part, `""`, `"`)
	return part
}

func rewriteJSONFunctionTokens(tokens []SQLToken) (string, error) {
	var b strings.Builder
	for i := 0; i < len(tokens); i++ {
		tok := tokens[i]
		if tok.Type == SQLTokenWhitespace || tok.Type == SQLTokenComment {
			b.WriteString(tok.Literal)
			continue
		}
		nextIdx := nextSignificantTokenIndex(tokens, i+1)
		if (tok.Type == SQLTokenIdentifier || tok.Type == SQLTokenKeyword) && nextIdx >= 0 &&
			tokens[nextIdx].Type == SQLTokenPunctuation && tokens[nextIdx].Literal == "(" {
			name := normalizeWord(tok.Literal)
			if isJSONHelper(name) {
				end := findMatchingParen(tokens, nextIdx)
				if end < 0 {
					return "", fmt.Errorf("unclosed function call for %s", name)
				}
				args := splitTopLevelByComma(tokens[nextIdx+1 : end])
				fnSQL, err := buildJSONHelperSQL(name, args)
				if err != nil {
					return "", err
				}
				b.WriteString(fnSQL)
				i = end
				continue
			}
		}
		b.WriteString(tok.Literal)
	}
	return strings.TrimSpace(b.String()), nil
}

func isJSONHelper(name string) bool {
	switch name {
	case "JSON_TEXT", "JSON_JSON", "JSON_NUM", "JSON_INT", "JSON_BOOL", "JSON_DATE", "JSON_TS", "JSON_TSTZ",
		"JSON_TEXT_PATH", "JSON_PATH",
		"IF", "IFNULL", "NVL":
		return true
	default:
		return false
	}
}

func buildJSONHelperSQL(name string, rawArgs [][]SQLToken) (string, error) {
	args := make([]string, 0, len(rawArgs))
	for _, a := range rawArgs {
		part, err := rewriteJSONFunctionTokens(a)
		if err != nil {
			return "", err
		}
		args = append(args, strings.TrimSpace(part))
	}
	if len(args) < 2 {
		return "", fmt.Errorf("%s requires at least 2 arguments", name)
	}
	base := args[0]

	switch name {
	case "JSON_TEXT":
		return fmt.Sprintf("jsonb_extract_path_text(%s, %s)", base, strings.Join(args[1:], ", ")), nil
	case "JSON_JSON":
		return fmt.Sprintf("jsonb_extract_path(%s, %s)", base, strings.Join(args[1:], ", ")), nil
	case "JSON_NUM":
		return fmt.Sprintf("(jsonb_extract_path_text(%s, %s))::numeric", base, strings.Join(args[1:], ", ")), nil
	case "JSON_INT":
		return fmt.Sprintf("(jsonb_extract_path_text(%s, %s))::int", base, strings.Join(args[1:], ", ")), nil
	case "JSON_BOOL":
		return fmt.Sprintf("(jsonb_extract_path_text(%s, %s))::boolean", base, strings.Join(args[1:], ", ")), nil
	case "JSON_DATE":
		return fmt.Sprintf("(jsonb_extract_path_text(%s, %s))::date", base, strings.Join(args[1:], ", ")), nil
	case "JSON_TS":
		return fmt.Sprintf("(jsonb_extract_path_text(%s, %s))::timestamp", base, strings.Join(args[1:], ", ")), nil
	case "JSON_TSTZ":
		return fmt.Sprintf("(jsonb_extract_path_text(%s, %s))::timestamptz", base, strings.Join(args[1:], ", ")), nil
	case "JSON_TEXT_PATH":
		return fmt.Sprintf("jsonb_extract_path_text(%s, %s)", base, strings.Join(args[1:], ", ")), nil
	case "JSON_PATH":
		return fmt.Sprintf("jsonb_extract_path(%s, %s)", base, strings.Join(args[1:], ", ")), nil
	case "IF":
		if len(args) != 3 {
			return "", fmt.Errorf("IF requires exactly 3 arguments")
		}
		return fmt.Sprintf("(CASE WHEN %s THEN %s ELSE %s END)", args[0], args[1], args[2]), nil
	case "IFNULL", "NVL":
		if len(args) != 2 {
			return "", fmt.Errorf("%s requires exactly 2 arguments", name)
		}
		return fmt.Sprintf("COALESCE(%s, %s)", args[0], args[1]), nil
	default:
		return "", fmt.Errorf("unsupported helper %s", name)
	}
}

func nextSignificantTokenIndex(tokens []SQLToken, start int) int {
	for i := start; i < len(tokens); i++ {
		if tokens[i].Type == SQLTokenWhitespace || tokens[i].Type == SQLTokenComment {
			continue
		}
		return i
	}
	return -1
}

func parseJSONExtractPathTextCall(query string, tokens []SQLToken, openIdx, closeIdx int) (baseExpr string, dotPath string, ok bool) {
	args := splitTopLevelByComma(tokens[openIdx+1 : closeIdx])
	if len(args) < 2 {
		return "", "", false
	}
	baseExpr, ok = tokenSliceSQL(query, args[0])
	if !ok {
		return "", "", false
	}
	path := make([]string, 0, len(args)-1)
	for _, a := range args[1:] {
		lit, ok := tokenSliceAsSingleStringLiteral(a)
		if !ok {
			return "", "", false
		}
		path = append(path, lit)
	}
	return strings.TrimSpace(baseExpr), strings.Join(path, "."), true
}

func tokenSliceSQL(query string, tokens []SQLToken) (string, bool) {
	if len(tokens) == 0 {
		return "", false
	}
	start := -1
	end := -1
	for i := 0; i < len(tokens); i++ {
		if tokens[i].Type == SQLTokenWhitespace || tokens[i].Type == SQLTokenComment {
			continue
		}
		start = i
		break
	}
	if start < 0 {
		return "", false
	}
	for i := len(tokens) - 1; i >= 0; i-- {
		if tokens[i].Type == SQLTokenWhitespace || tokens[i].Type == SQLTokenComment {
			continue
		}
		end = i
		break
	}
	if end < start || tokens[start].Start < 0 || tokens[end].End > len(query) {
		return "", false
	}
	return query[tokens[start].Start:tokens[end].End], true
}

func tokenSliceAsSingleStringLiteral(tokens []SQLToken) (string, bool) {
	sig := make([]SQLToken, 0, 1)
	for _, tok := range tokens {
		if tok.Type == SQLTokenWhitespace || tok.Type == SQLTokenComment {
			continue
		}
		sig = append(sig, tok)
	}
	if len(sig) != 1 || sig[0].Type != SQLTokenString {
		return "", false
	}
	return decodeSQLStringLiteral(sig[0].Literal)
}

func decodeSQLStringLiteral(lit string) (string, bool) {
	if len(lit) < 2 || lit[0] != '\'' || lit[len(lit)-1] != '\'' {
		return "", false
	}
	body := lit[1 : len(lit)-1]
	body = strings.ReplaceAll(body, "''", "'")
	return body, true
}

func blindIndexFromToken(tok SQLToken, vars map[string]any, hmacKey string) (string, bool, error) {
	switch tok.Type {
	case SQLTokenPlaceholder:
		content := extractPlaceholderContent(tok.Literal)
		kind := "param"
		key := content
		if idx := strings.Index(content, ":"); idx >= 0 {
			kind = strings.ToLower(strings.TrimSpace(content[:idx]))
			key = strings.TrimSpace(content[idx+1:])
		}
		if kind != "param" && kind != "arg" {
			return "", false, nil
		}
		val, ok := vars[key]
		if !ok {
			return "", false, fmt.Errorf("missing template variable %q", key)
		}
		idx, err := blindIndexWithKey(val, hmacKey)
		if err != nil {
			return "", false, err
		}
		return idx, true, nil
	case SQLTokenString:
		val, ok := decodeSQLStringLiteral(tok.Literal)
		if !ok {
			return "", false, nil
		}
		idx, err := blindIndexWithKey(val, hmacKey)
		if err != nil {
			return "", false, err
		}
		return idx, true, nil
	default:
		return "", false, nil
	}
}

func blindIndexExpr(baseExpr, dotPath string) string {
	key := strings.ReplaceAll(dotPath, ".", "_")
	return "jsonb_extract_path_text(" + baseExpr + ", '_secure_idx', " + quoteSQLString(key) + ")"
}

func previousSignificantTokenIndex(tokens []SQLToken, start int) int {
	for i := start; i >= 0; i-- {
		if tokens[i].Type == SQLTokenWhitespace || tokens[i].Type == SQLTokenComment {
			continue
		}
		return i
	}
	return -1
}
