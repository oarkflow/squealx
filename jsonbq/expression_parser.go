package jsonbq

import "strings"

type exprParser struct {
	tokens []SQLToken
	pos    int
}

func parseSQLExpression(tokens []SQLToken) SQLExpr {
	p := &exprParser{tokens: compactTokens(tokens)}
	if len(p.tokens) == 0 {
		return nil
	}
	return p.parseExpr(1)
}

func (p *exprParser) parseExpr(minPrec int) SQLExpr {
	left := p.parsePrefix()
	for {
		op, prec, ok := p.currentOperator()
		if !ok || prec < minPrec {
			break
		}
		p.next() // operator head
		if op == "IS" && p.matchWord("NOT") {
			op = "IS NOT"
			p.next()
		}
		if op == "NOT" && p.matchWord("IN") {
			op = "NOT IN"
			p.next()
		}
		right := p.parseExpr(prec + 1)
		left = BinaryExpr{Left: left, Op: op, Right: right}
	}
	return left
}

func (p *exprParser) parsePrefix() SQLExpr {
	if p.eof() {
		return nil
	}
	tok := p.peek()

	// unary NOT / + / -
	if (tok.Type == SQLTokenKeyword && normalizeWord(tok.Literal) == "NOT") ||
		(tok.Type == SQLTokenOperator && (tok.Literal == "+" || tok.Literal == "-")) {
		op := normalizeWord(tok.Literal)
		p.next()
		return UnaryExpr{Op: op, Expr: p.parseExpr(7)}
	}

	// CASE ... END
	if tok.Type == SQLTokenKeyword && normalizeWord(tok.Literal) == "CASE" {
		return p.parseCase()
	}

	// parenthesized or tuple
	if tok.Type == SQLTokenPunctuation && tok.Literal == "(" {
		p.next()
		items := p.parseExprList(")")
		p.consumePunct(")")
		if len(items) == 1 {
			return items[0]
		}
		return TupleExpr{Items: items}
	}

	p.next()
	var expr SQLExpr
	switch tok.Type {
	case SQLTokenIdentifier, SQLTokenKeyword:
		name := tok.Literal
		// schema.table chain
		for p.matchPunct(".") {
			p.next()
			if p.eof() {
				break
			}
			n := p.peek()
			if n.Type != SQLTokenIdentifier && n.Type != SQLTokenKeyword {
				break
			}
			p.next()
			name += "." + n.Literal
		}
		// function call
		if p.matchPunct("(") {
			p.next()
			args := p.parseExprList(")")
			p.consumePunct(")")
			expr = FunctionCallExpr{Name: name, Args: args}
		} else {
			expr = IdentifierExpr{Name: name}
		}
	case SQLTokenString:
		expr = LiteralExpr{Value: tok.Literal, Kind: "string"}
	case SQLTokenNumber:
		expr = LiteralExpr{Value: tok.Literal, Kind: "number"}
	case SQLTokenPlaceholder:
		kind, key := parsePlaceholderMeta(tok.Literal)
		expr = PlaceholderExpr{Raw: tok.Literal, Kind: kind, Key: key}
	default:
		expr = LiteralExpr{Value: tok.Literal, Kind: "unknown"}
	}

	// postfix cast ::type
	for p.matchOperator("::") {
		p.next()
		typ := p.parseTypeName()
		expr = CastExpr{Expr: expr, Type: typ}
	}

	return expr
}

func (p *exprParser) parseCase() SQLExpr {
	// consume CASE
	p.next()
	var operand SQLExpr
	if !p.matchWord("WHEN") {
		operand = p.parseExpr(1)
	}
	whens := make([]CaseWhenBranch, 0, 2)
	for p.matchWord("WHEN") {
		p.next()
		cond := p.parseExpr(1)
		if p.matchWord("THEN") {
			p.next()
		}
		thenExpr := p.parseExpr(1)
		whens = append(whens, CaseWhenBranch{Cond: cond, Then: thenExpr})
	}
	var elseExpr SQLExpr
	if p.matchWord("ELSE") {
		p.next()
		elseExpr = p.parseExpr(1)
	}
	if p.matchWord("END") {
		p.next()
	}
	return CaseExpr{Operand: operand, When: whens, Else: elseExpr}
}

func (p *exprParser) parseExprList(closePunct string) []SQLExpr {
	if p.matchPunct(closePunct) {
		return nil
	}
	items := make([]SQLExpr, 0, 2)
	for !p.eof() && !p.matchPunct(closePunct) {
		items = append(items, p.parseExpr(1))
		if p.matchPunct(",") {
			p.next()
			continue
		}
		break
	}
	return items
}

func (p *exprParser) parseTypeName() string {
	start := p.pos
	depth := 0
	for !p.eof() {
		t := p.peek()
		if t.Type == SQLTokenPunctuation {
			if t.Literal == "(" {
				depth++
				p.next()
				continue
			}
			if t.Literal == ")" {
				if depth == 0 {
					break
				}
				depth--
				p.next()
				continue
			}
			if depth == 0 && t.Literal == "," {
				break
			}
		}
		if depth == 0 && (t.Type == SQLTokenOperator || (t.Type == SQLTokenKeyword && isOperatorKeyword(normalizeWord(t.Literal)))) {
			break
		}
		p.next()
	}
	return joinTokenLiterals(p.tokens[start:p.pos])
}

func (p *exprParser) currentOperator() (string, int, bool) {
	if p.eof() {
		return "", 0, false
	}
	t := p.peek()
	if t.Type == SQLTokenOperator {
		op := strings.ToUpper(strings.TrimSpace(t.Literal))
		return op, operatorPrecedence(op), operatorPrecedence(op) > 0
	}
	if t.Type == SQLTokenKeyword {
		op := normalizeWord(t.Literal)
		if !isOperatorKeyword(op) {
			return "", 0, false
		}
		return op, operatorPrecedence(op), operatorPrecedence(op) > 0
	}
	return "", 0, false
}

func operatorPrecedence(op string) int {
	switch op {
	case "OR":
		return 1
	case "AND":
		return 2
	case "=", "!=", "<>", "<", "<=", ">", ">=", "LIKE", "ILIKE", "~", "~*", "!~", "!~*", "IN", "IS", "IS NOT", "NOT IN":
		return 3
	case "||":
		return 4
	case "+", "-":
		return 5
	case "*", "/", "%":
		return 6
	default:
		return 0
	}
}

func isOperatorKeyword(word string) bool {
	switch word {
	case "OR", "AND", "LIKE", "ILIKE", "IN", "IS", "NOT":
		return true
	default:
		return false
	}
}

func parsePlaceholderMeta(raw string) (string, string) {
	content := extractPlaceholderContent(raw)
	kind := "param"
	key := content
	if idx := strings.Index(content, ":"); idx >= 0 {
		kind = strings.TrimSpace(content[:idx])
		key = strings.TrimSpace(content[idx+1:])
	}
	return kind, key
}

func compactTokens(tokens []SQLToken) []SQLToken {
	out := make([]SQLToken, 0, len(tokens))
	for _, t := range tokens {
		if t.Type == SQLTokenWhitespace || t.Type == SQLTokenComment {
			continue
		}
		out = append(out, t)
	}
	return out
}

func joinTokenLiterals(tokens []SQLToken) string {
	var b strings.Builder
	for _, t := range tokens {
		if b.Len() > 0 {
			last := b.String()[b.Len()-1]
			if needsSpace(last, t.Literal) {
				b.WriteByte(' ')
			}
		}
		b.WriteString(t.Literal)
	}
	return strings.TrimSpace(b.String())
}

func needsSpace(last byte, next string) bool {
	if next == "" {
		return false
	}
	first := next[0]
	isWordLike := func(c byte) bool {
		return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '$'
	}
	return isWordLike(last) && isWordLike(first)
}

func (p *exprParser) eof() bool { return p.pos >= len(p.tokens) }
func (p *exprParser) peek() SQLToken {
	if p.eof() {
		return SQLToken{}
	}
	return p.tokens[p.pos]
}
func (p *exprParser) next() SQLToken {
	t := p.peek()
	if !p.eof() {
		p.pos++
	}
	return t
}
func (p *exprParser) matchWord(word string) bool {
	if p.eof() {
		return false
	}
	t := p.peek()
	return (t.Type == SQLTokenKeyword || t.Type == SQLTokenIdentifier) && strings.EqualFold(strings.TrimSpace(t.Literal), word)
}
func (p *exprParser) matchPunct(punct string) bool {
	return !p.eof() && p.peek().Type == SQLTokenPunctuation && p.peek().Literal == punct
}
func (p *exprParser) consumePunct(punct string) bool {
	if p.matchPunct(punct) {
		p.next()
		return true
	}
	return false
}
func (p *exprParser) matchOperator(op string) bool {
	return !p.eof() && p.peek().Type == SQLTokenOperator && p.peek().Literal == op
}
