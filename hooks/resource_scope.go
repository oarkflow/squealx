package hooks

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/sqltoken"
)

type ScopeArgsResolver func(ctx context.Context) ([]any, error)

type ScopeDenyCode string

const (
	ScopeDenyMissingContext      ScopeDenyCode = "missing_context"
	ScopeDenyUnknownShape        ScopeDenyCode = "unknown_shape"
	ScopeDenyMissingRule         ScopeDenyCode = "missing_rule"
	ScopeDenyResolverRequired    ScopeDenyCode = "resolver_required"
	ScopeDenyResolverFailed      ScopeDenyCode = "resolver_failed"
	ScopeDenyParamMismatch       ScopeDenyCode = "param_mismatch"
	ScopeDenyUnscopedStatement   ScopeDenyCode = "unscoped_statement"
	ScopeDenyUnsupportedStmtType ScopeDenyCode = "unsupported_statement"
	ScopeDenyBypassNotAllowed    ScopeDenyCode = "bypass_not_allowed"
	ScopeDenyBypassMissingReason ScopeDenyCode = "bypass_missing_reason"
	ScopeDenyBypassTokenRequired ScopeDenyCode = "bypass_token_required"
)

type ScopeError struct {
	Code    ScopeDenyCode
	Message string
}

func (e *ScopeError) Error() string {
	if e == nil {
		return ""
	}
	if e.Code == "" {
		return e.Message
	}
	if e.Message == "" {
		return string(e.Code)
	}
	return string(e.Code) + ": " + e.Message
}

func ScopeDenyCodeFromError(err error) (ScopeDenyCode, bool) {
	if err == nil {
		return "", false
	}
	se, ok := err.(*ScopeError)
	if !ok {
		return "", false
	}
	return se.Code, true
}

func scopeErr(code ScopeDenyCode, message string) error {
	return &ScopeError{Code: code, Message: message}
}

type ScopeDecisionAction string

const (
	ScopeDecisionScoped      ScopeDecisionAction = "scoped"
	ScopeDecisionRejected    ScopeDecisionAction = "rejected"
	ScopeDecisionBypassed    ScopeDecisionAction = "bypassed"
	ScopeDecisionPassthrough ScopeDecisionAction = "passthrough"
)

type ScopeDecision struct {
	Action         ScopeDecisionAction
	ReasonCode     ScopeDenyCode
	Reason         string
	StatementType  string
	MatchedTables  []string
	AppliedRules   []string
	AddedPredicate int
	Query          string
}

type ScopeAuditSink func(ctx context.Context, decision ScopeDecision)

type scopeBypassCtxKey struct{}

type ScopeBypassRequest struct {
	Trusted bool
	Reason  string
}

func WithTrustedScopeBypass(ctx context.Context, reason string) context.Context {
	return context.WithValue(ctx, scopeBypassCtxKey{}, ScopeBypassRequest{Trusted: true, Reason: reason})
}

func ScopeBypassFromContext(ctx context.Context) (ScopeBypassRequest, bool) {
	req, ok := ctx.Value(scopeBypassCtxKey{}).(ScopeBypassRequest)
	if !ok {
		return ScopeBypassRequest{}, false
	}
	return req, true
}

type ScopeRule struct {
	Table      string
	Column     string
	Predicate  string
	ResolveArgs ScopeArgsResolver
}

type ResourceScopeHook struct {
	defaultResolver ScopeArgsResolver
	rules           map[string]compiledScopeRule
	strictMode      bool
	strictAllTables bool
	rejectUnknownShapes bool
	auditSink         ScopeAuditSink
	allowTrustedBypass bool
	requireBypassToken bool
	bypassToken        string
}

type compiledScopeRule struct {
	TableKey       string
	TableRaw       string
	Column         string
	Predicate      string
	ResolveArgs    ScopeArgsResolver
	ParamCount     int
	HasAliasToken  bool
	HasParamTokens bool
}

func NewResourceScopeHook(defaultResolver ScopeArgsResolver, rules ...ScopeRule) *ResourceScopeHook {
	index := make(map[string]compiledScopeRule, len(rules))
	for _, rule := range rules {
		compiled, ok := compileScopeRule(rule)
		if !ok {
			continue
		}
		table := compiled.TableKey
		if table == "" {
			continue
		}
		index[table] = compiled
	}
	return &ResourceScopeHook{
		defaultResolver:   defaultResolver,
		rules:             index,
		requireBypassToken: true,
		bypassToken:       "/* scope:bypass */",
	}
}

func compileScopeRule(rule ScopeRule) (compiledScopeRule, bool) {
	table := canonicalTableName(rule.Table)
	if table == "" {
		return compiledScopeRule{}, false
	}
	predicate := strings.TrimSpace(rule.Predicate)
	column := strings.TrimSpace(rule.Column)
	compiled := compiledScopeRule{
		TableKey:    table,
		TableRaw:    rule.Table,
		Column:      column,
		Predicate:   predicate,
		ResolveArgs: rule.ResolveArgs,
	}
	if predicate != "" {
		compiled.ParamCount = strings.Count(predicate, "{{param}}")
		compiled.HasAliasToken = strings.Contains(predicate, "{{alias}}")
		compiled.HasParamTokens = compiled.ParamCount > 0
		return compiled, true
	}
	compiled.ParamCount = 1
	compiled.HasParamTokens = true
	return compiled, true
}

func (h *ResourceScopeHook) SetStrictMode(strict bool) *ResourceScopeHook {
	h.strictMode = strict
	return h
}

func (h *ResourceScopeHook) SetStrictAllTables(strict bool) *ResourceScopeHook {
	h.strictAllTables = strict
	return h
}

func (h *ResourceScopeHook) SetRejectUnknownShapes(reject bool) *ResourceScopeHook {
	h.rejectUnknownShapes = reject
	return h
}

func (h *ResourceScopeHook) SetAuditSink(sink ScopeAuditSink) *ResourceScopeHook {
	h.auditSink = sink
	return h
}

func (h *ResourceScopeHook) SetAllowTrustedBypass(allow bool) *ResourceScopeHook {
	h.allowTrustedBypass = allow
	return h
}

func (h *ResourceScopeHook) SetRequireBypassToken(require bool) *ResourceScopeHook {
	h.requireBypassToken = require
	return h
}

func (h *ResourceScopeHook) SetBypassToken(token string) *ResourceScopeHook {
	token = strings.TrimSpace(token)
	if token != "" {
		h.bypassToken = token
	}
	return h
}

func (h *ResourceScopeHook) emitAudit(ctx context.Context, decision ScopeDecision) {
	if h.auditSink == nil {
		return
	}
	h.auditSink(ctx, decision)
}

func ArgsFromContextValue(key any) ScopeArgsResolver {
	return func(ctx context.Context) ([]any, error) {
		v := ctx.Value(key)
		if v == nil {
			return nil, scopeErr(ScopeDenyMissingContext, fmt.Sprintf("resource scope value missing in context for key %v", key))
		}
		return []any{v}, nil
	}
}

func (h *ResourceScopeHook) Before(ctx context.Context, query string, args ...any) (context.Context, string, []any, error) {
	containsBypassToken := h.bypassToken != "" && strings.Contains(strings.ToLower(query), strings.ToLower(h.bypassToken))
	if req, ok := ScopeBypassFromContext(ctx); ok {
		if !h.allowTrustedBypass || !req.Trusted {
			err := scopeErr(ScopeDenyBypassNotAllowed, "trusted bypass is not allowed")
			h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionRejected, ReasonCode: ScopeDenyBypassNotAllowed, Reason: err.Error(), Query: query})
			return ctx, query, args, err
		}
		if strings.TrimSpace(req.Reason) == "" {
			err := scopeErr(ScopeDenyBypassMissingReason, "trusted bypass reason is required")
			h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionRejected, ReasonCode: ScopeDenyBypassMissingReason, Reason: err.Error(), Query: query})
			return ctx, query, args, err
		}
		if h.requireBypassToken && !containsBypassToken {
			err := scopeErr(ScopeDenyBypassTokenRequired, "trusted bypass requires per-query bypass token")
			h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionRejected, ReasonCode: ScopeDenyBypassTokenRequired, Reason: err.Error(), Query: query})
			return ctx, query, args, err
		}
		h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionBypassed, Reason: req.Reason, Query: query})
		return ctx, query, args, nil
	}

	if containsBypassToken {
		err := scopeErr(ScopeDenyBypassNotAllowed, "per-query bypass token requires trusted bypass context")
		h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionRejected, ReasonCode: ScopeDenyBypassNotAllowed, Reason: err.Error(), Query: query})
		return ctx, query, args, err
	}

	if len(h.rules) == 0 {
		h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionPassthrough, Reason: "no rules configured", Query: query})
		return ctx, query, args, nil
	}
	newQuery, newArgs, err := h.rewrite(ctx, query, args)
	if err != nil {
		return ctx, query, args, err
	}
	return ctx, newQuery, newArgs, nil
}

func (h *ResourceScopeHook) rewrite(ctx context.Context, query string, args []any) (string, []any, error) {
	segments := splitTopLevelStatements(query)
	if len(segments) == 0 {
		return query, args, nil
	}

	currentQuery := query
	currentArgs := args
	for i := len(segments) - 1; i >= 0; i-- {
		seg := segments[i]
		argPrefix := countQuestionMarksBefore(currentQuery, seg.start)
		rewritten, updatedArgs, err := h.rewriteStatement(ctx, currentQuery[seg.start:seg.end], currentArgs, argPrefix)
		if err != nil {
			return query, args, err
		}
		currentQuery = currentQuery[:seg.start] + rewritten + currentQuery[seg.end:]
		currentArgs = updatedArgs
	}
	return currentQuery, currentArgs, nil
}

func (h *ResourceScopeHook) rewriteStatement(ctx context.Context, statement string, args []any, argPrefix int) (string, []any, error) {
	for {
		tokens := sqltoken.Tokenize(statement, fullSQLTokenizerConfig())
		infos := buildTokenInfos(tokens)
		ranges := deepestNestedStatementRanges(statement, infos)
		if len(ranges) == 0 {
			break
		}
		for i := len(ranges) - 1; i >= 0; i-- {
			rg := ranges[i]
			subPrefix := argPrefix + countQuestionMarksBefore(statement, rg.start)
			rewrittenSub, updatedArgs, err := h.rewriteStatement(ctx, statement[rg.start:rg.end], args, subPrefix)
			if err != nil {
				return statement, args, err
			}
			statement = statement[:rg.start] + rewrittenSub + statement[rg.end:]
			args = updatedArgs
		}
	}

	tokens := sqltoken.Tokenize(statement, fullSQLTokenizerConfig())
	infos := buildTokenInfos(tokens)
	if len(infos) == 0 {
		return statement, args, nil
	}

	firstWord := firstTopLevelWord(infos)
	if firstWord == "" {
		h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionPassthrough, Reason: "empty statement", Query: statement})
		return statement, args, nil
	}

	if firstWord == "WITH" {
		mainIdx := mainStatementStart(infos)
		if mainIdx < 0 {
			if h.strictMode || h.rejectUnknownShapes {
				err := scopeErr(ScopeDenyUnknownShape, "resource scope rejected WITH statement without main body")
				h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionRejected, StatementType: "WITH", ReasonCode: ScopeDenyUnknownShape, Reason: err.Error(), Query: statement})
				return statement, args, err
			}
			h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionPassthrough, StatementType: "WITH", Reason: "WITH without main body", Query: statement})
			return statement, args, nil
		}
		body := statement[mainIdx:]
		rewrittenBody, updatedArgs, err := h.rewriteStatement(ctx, body, args, argPrefix+countQuestionMarksBefore(statement, mainIdx))
		if err != nil {
			return statement, args, err
		}
		return statement[:mainIdx] + rewrittenBody, updatedArgs, nil
	}

	statementType := firstWord
	if statementType != "SELECT" && statementType != "UPDATE" && statementType != "DELETE" {
		if h.rejectUnknownShapes || (h.strictMode && h.strictAllTables) {
			err := scopeErr(ScopeDenyUnsupportedStmtType, fmt.Sprintf("resource scope rejected unsupported statement type %q", statementType))
			h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionRejected, StatementType: statementType, ReasonCode: ScopeDenyUnsupportedStmtType, Reason: err.Error(), Query: statement})
			return statement, args, err
		}
		h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionPassthrough, StatementType: statementType, Reason: "statement type not scoped", Query: statement})
		return statement, args, nil
	}

	var tableRefs []tableRef
	unknownShape := false
	switch statementType {
	case "SELECT":
		tableRefs, unknownShape = collectTopLevelTableRefs(infos)
	case "UPDATE":
		tableRefs = collectUpdateTableRefs(infos)
	case "DELETE":
		tableRefs = collectDeleteTableRefs(infos)
	}
	if unknownShape && h.rejectUnknownShapes {
		err := scopeErr(ScopeDenyUnknownShape, "resource scope rejected unknown SELECT shape")
		h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionRejected, StatementType: statementType, ReasonCode: ScopeDenyUnknownShape, Reason: err.Error(), MatchedTables: tableNames(tableRefs), Query: statement})
		return statement, args, err
	}
	if len(tableRefs) == 0 {
		if h.strictMode || h.rejectUnknownShapes {
			err := scopeErr(ScopeDenyUnscopedStatement, "resource scope rejected statement with no table refs")
			h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionRejected, StatementType: statementType, ReasonCode: ScopeDenyUnscopedStatement, Reason: err.Error(), Query: statement})
			return statement, args, err
		}
		h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionPassthrough, StatementType: statementType, Reason: "no table refs discovered", Query: statement})
		return statement, args, nil
	}

	wherePos, insertionPos := clausePositions(statementType, infos, len(statement))

	placeholder := newPlaceholderBuilder(ctx, statement, len(args))
	predicates := make([]string, 0, len(tableRefs))
	addedArgs := make([]any, 0, len(tableRefs))

	appliedRules := make([]string, 0, len(tableRefs))
	for _, ref := range tableRefs {
		rule, ok := h.rules[canonicalTableName(ref.table)]
		if !ok {
			if h.strictAllTables {
				err := scopeErr(ScopeDenyMissingRule, fmt.Sprintf("resource scope rule missing for table %q", ref.table))
				h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionRejected, StatementType: statementType, ReasonCode: ScopeDenyMissingRule, Reason: err.Error(), MatchedTables: tableNames(tableRefs), Query: statement})
				return statement, args, err
			}
			continue
		}
		predicate, params, err := h.buildPredicate(ctx, rule, ref.alias, placeholder)
		if err != nil {
			se, _ := err.(*ScopeError)
			decision := ScopeDecision{Action: ScopeDecisionRejected, StatementType: statementType, Reason: err.Error(), MatchedTables: tableNames(tableRefs), Query: statement}
			if se != nil {
				decision.ReasonCode = se.Code
			}
			h.emitAudit(ctx, decision)
			return statement, args, err
		}
		if predicate == "" {
			if h.strictMode {
				err := scopeErr(ScopeDenyUnscopedStatement, fmt.Sprintf("resource scope generated empty predicate for table %q", ref.table))
				h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionRejected, StatementType: statementType, ReasonCode: ScopeDenyUnscopedStatement, Reason: err.Error(), MatchedTables: tableNames(tableRefs), Query: statement})
				return statement, args, err
			}
			continue
		}
		predicates = append(predicates, predicate)
		appliedRules = append(appliedRules, rule.TableKey)
		addedArgs = append(addedArgs, params...)
	}
	if len(predicates) == 0 {
		if h.strictMode {
			err := scopeErr(ScopeDenyUnscopedStatement, "resource scope rejected unscoped statement")
			h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionRejected, StatementType: statementType, ReasonCode: ScopeDenyUnscopedStatement, Reason: err.Error(), MatchedTables: tableNames(tableRefs), Query: statement})
			return statement, args, err
		}
		h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionPassthrough, StatementType: statementType, Reason: "no matching rules for discovered tables", MatchedTables: tableNames(tableRefs), Query: statement})
		return statement, args, nil
	}

	joined := strings.Join(predicates, " AND ")
	insertAt := insertionPos
	if insertAt < 0 || insertAt > len(statement) {
		insertAt = len(statement)
	}
	if wherePos >= 0 && wherePos < insertionPos {
		statement = statement[:insertAt] + " AND (" + joined + ")" + statement[insertAt:]
	} else {
		statement = statement[:insertAt] + " WHERE (" + joined + ")" + statement[insertAt:]
	}

	updatedArgs := mergeArgsForInsertion(ctx, statement, args, addedArgs, insertAt, argPrefix)
	h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionScoped, StatementType: statementType, MatchedTables: tableNames(tableRefs), AppliedRules: appliedRules, AddedPredicate: len(predicates), Query: statement})
	return statement, updatedArgs, nil
}

func tableNames(refs []tableRef) []string {
	out := make([]string, 0, len(refs))
	for _, ref := range refs {
		if ref.table == "" {
			continue
		}
		out = append(out, ref.table)
	}
	return out
}

func clausePositions(statementType string, infos []tokenInfo, fallbackEnd int) (wherePos int, insertionPos int) {
	wherePos = -1
	insertionPos = fallbackEnd
	for _, info := range infos {
		if info.depth != 0 {
			continue
		}
		if info.token.Type == sqltoken.Semicolon && info.start < insertionPos {
			insertionPos = info.start
			continue
		}
		if !isWord(info.token) {
			continue
		}
		word := strings.ToUpper(strings.TrimSpace(info.token.Text))
		switch statementType {
		case "SELECT":
			switch word {
			case "WHERE":
				if wherePos < 0 {
					wherePos = info.start
				}
			case "GROUP", "ORDER", "LIMIT", "OFFSET", "FETCH", "FOR", "UNION", "EXCEPT", "INTERSECT":
				if info.start < insertionPos {
					insertionPos = info.start
				}
			}
		case "UPDATE":
			switch word {
			case "WHERE":
				if wherePos < 0 {
					wherePos = info.start
				}
			case "RETURNING", "ORDER", "LIMIT":
				if info.start < insertionPos {
					insertionPos = info.start
				}
			}
		case "DELETE":
			switch word {
			case "WHERE":
				if wherePos < 0 {
					wherePos = info.start
				}
			case "RETURNING", "ORDER", "LIMIT":
				if info.start < insertionPos {
					insertionPos = info.start
				}
			}
		}
	}
	return wherePos, insertionPos
}

func mergeArgsForInsertion(ctx context.Context, query string, args, added []any, insertionPos int, argPrefix int) []any {
	kind := detectPlaceholderKind(ctx, query)
	if kind != placeholderQuestion || len(added) == 0 {
		return append(args, added...)
	}
	before := argPrefix + countQuestionMarksBefore(query, insertionPos)
	if before < 0 {
		before = 0
	}
	if before > len(args) {
		before = len(args)
	}
	merged := make([]any, 0, len(args)+len(added))
	merged = append(merged, args[:before]...)
	merged = append(merged, added...)
	merged = append(merged, args[before:]...)
	return merged
}

func deepestNestedStatementRanges(statement string, infos []tokenInfo) []statementSegment {
	type frame struct {
		start      int
		hasStmtKid bool
	}
	stack := make([]frame, 0, 8)
	ranges := make([]statementSegment, 0, 4)

	for _, info := range infos {
		if info.token.Type != sqltoken.Punctuation {
			continue
		}
		txt := strings.TrimSpace(info.token.Text)
		switch txt {
		case "(":
			stack = append(stack, frame{start: info.end})
		case ")":
			if len(stack) == 0 {
				continue
			}
			current := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if current.start >= info.start {
				continue
			}
			inner := strings.TrimSpace(statement[current.start:info.start])
			isStmt := isScopeStatementWord(firstWordFromSQL(inner))
			if isStmt && !current.hasStmtKid {
				ranges = append(ranges, statementSegment{start: current.start, end: info.start})
			}
			if len(stack) > 0 && (current.hasStmtKid || isStmt) {
				stack[len(stack)-1].hasStmtKid = true
			}
		}
	}
	return ranges
}

func firstWordFromSQL(sql string) string {
	tokens := sqltoken.Tokenize(sql, fullSQLTokenizerConfig())
	infos := buildTokenInfos(tokens)
	return firstTopLevelWord(infos)
}

func isScopeStatementWord(word string) bool {
	switch strings.ToUpper(strings.TrimSpace(word)) {
	case "SELECT", "WITH", "UPDATE", "DELETE":
		return true
	default:
		return false
	}
}

func countQuestionMarksBefore(query string, pos int) int {
	if pos <= 0 {
		return 0
	}
	tokens := sqltoken.Tokenize(query, fullSQLTokenizerConfig())
	count := 0
	offset := 0
	for _, token := range tokens {
		end := offset + len(token.Text)
		if end > pos {
			break
		}
		if token.Type == sqltoken.QuestionMark {
			count++
		}
		offset = end
	}
	return count
}

type statementSegment struct {
	start int
	end   int
}

func splitTopLevelStatements(query string) []statementSegment {
	tokens := sqltoken.Tokenize(query, fullSQLTokenizerConfig())
	infos := buildTokenInfos(tokens)
	if len(infos) == 0 {
		return nil
	}
	segments := make([]statementSegment, 0, 2)
	start := 0
	for _, info := range infos {
		if info.depth == 0 && info.token.Type == sqltoken.Semicolon {
			segments = append(segments, statementSegment{start: start, end: info.end})
			start = info.end
		}
	}
	if start < len(query) {
		segments = append(segments, statementSegment{start: start, end: len(query)})
	}
	return segments
}

func mainStatementStart(infos []tokenInfo) int {
	seenWith := false
	for _, info := range infos {
		if info.depth != 0 || !isWord(info.token) {
			continue
		}
		word := strings.ToUpper(strings.TrimSpace(info.token.Text))
		if !seenWith {
			if word == "WITH" {
				seenWith = true
			}
			continue
		}
		switch word {
		case "SELECT", "UPDATE", "DELETE", "INSERT":
			return info.start
		}
	}
	return -1
}

func collectUpdateTableRefs(infos []tokenInfo) []tableRef {
	for i := 0; i < len(infos); i++ {
		info := infos[i]
		if info.depth != 0 || !isWord(info.token) {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(info.token.Text), "UPDATE") {
			continue
		}
		ref, _ := parseTableRef(infos, i+1)
		if ref.table != "" {
			return []tableRef{ref}
		}
		break
	}
	return nil
}

func collectDeleteTableRefs(infos []tokenInfo) []tableRef {
	for i := 0; i < len(infos); i++ {
		info := infos[i]
		if info.depth != 0 || !isWord(info.token) {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(info.token.Text), "DELETE") {
			continue
		}
		for j := i + 1; j < len(infos); j++ {
			next := infos[j]
			if next.depth != 0 || !isWord(next.token) {
				continue
			}
			if strings.EqualFold(strings.TrimSpace(next.token.Text), "FROM") {
				ref, _ := parseTableRef(infos, j+1)
				if ref.table != "" {
					return []tableRef{ref}
				}
				return nil
			}
		}
	}
	return nil
}

func (h *ResourceScopeHook) buildPredicate(ctx context.Context, rule compiledScopeRule, alias string, next func() string) (string, []any, error) {
	predicate := rule.Predicate
	alias = strings.TrimSpace(alias)
	if alias == "" {
		alias = normalizeIdentifier(rule.TableRaw)
	}
	if predicate == "" {
		column := rule.Column
		if column == "" {
			return "", nil, nil
		}
		predicate = alias + "." + column + " = {{param}}"
	} else if rule.HasAliasToken {
		predicate = strings.ReplaceAll(predicate, "{{alias}}", alias)
	}

	resolver := rule.ResolveArgs
	if resolver == nil {
		resolver = h.defaultResolver
	}
	paramCount := rule.ParamCount
	if rule.Predicate == "" {
		paramCount = 1
	}
	if paramCount == 0 {
		return predicate, nil, nil
	}
	if resolver == nil {
		return "", nil, scopeErr(ScopeDenyResolverRequired, fmt.Sprintf("resource scope rule for table %q requires a resolver", rule.TableRaw))
	}
	params, err := resolver(ctx)
	if err != nil {
		if _, ok := err.(*ScopeError); ok {
			return "", nil, err
		}
		return "", nil, scopeErr(ScopeDenyResolverFailed, err.Error())
	}
	if len(params) == 1 && paramCount > 1 {
		clone := make([]any, paramCount)
		for i := range clone {
			clone[i] = params[0]
		}
		params = clone
	}
	if len(params) != paramCount {
		return "", nil, scopeErr(ScopeDenyParamMismatch, fmt.Sprintf("resource scope rule for table %q expects %d params, got %d", rule.TableRaw, paramCount, len(params)))
	}
	for _, value := range params {
		predicate = strings.Replace(predicate, "{{param}}", next(), 1)
		_ = value
	}
	return predicate, params, nil
}

type tableRef struct {
	table string
	alias string
}

type tokenInfo struct {
	token sqltoken.Token
	start int
	end   int
	depth int
}

func buildTokenInfos(tokens []sqltoken.Token) []tokenInfo {
	infos := make([]tokenInfo, 0, len(tokens))
	depth := 0
	pos := 0
	for _, token := range tokens {
		start := pos
		end := pos + len(token.Text)
		infos = append(infos, tokenInfo{token: token, start: start, end: end, depth: depth})
		for _, char := range token.Text {
			switch char {
			case '(':
				depth++
			case ')':
				if depth > 0 {
					depth--
				}
			}
		}
		pos = end
	}
	return infos
}

func firstTopLevelWord(infos []tokenInfo) string {
	for _, info := range infos {
		if info.depth != 0 || !isWord(info.token) {
			continue
		}
		return strings.ToUpper(strings.TrimSpace(info.token.Text))
	}
	return ""
}

func collectTopLevelTableRefs(infos []tokenInfo) ([]tableRef, bool) {
	refs := make([]tableRef, 0, 4)
	unknown := false
	for i := 0; i < len(infos); i++ {
		info := infos[i]
		if info.depth != 0 || !isWord(info.token) {
			continue
		}
		word := strings.ToUpper(strings.TrimSpace(info.token.Text))
		if word != "FROM" && word != "JOIN" {
			continue
		}
		ref, next := parseTableRef(infos, i+1)
		i = next
		if ref.table == "" {
			unknown = true
			continue
		}
		refs = append(refs, ref)
	}
	return refs, unknown
}

func parseTableRef(infos []tokenInfo, idx int) (tableRef, int) {
	idx = nextSignificantIndex(infos, idx)
	if idx >= len(infos) {
		return tableRef{}, idx
	}
	if infos[idx].token.Type == sqltoken.Punctuation && strings.Contains(infos[idx].token.Text, "(") {
		return tableRef{}, idx
	}

	start := idx
	parts := make([]string, 0, 3)
	for idx < len(infos) {
		token := infos[idx].token
		if token.Type == sqltoken.Whitespace || token.Type == sqltoken.Comment {
			break
		}
		if token.Type == sqltoken.Punctuation {
			text := strings.TrimSpace(token.Text)
			if text == "." {
				parts = append(parts, text)
				idx++
				continue
			}
			break
		}
		if isIdentifierToken(token) {
			parts = append(parts, token.Text)
			idx++
			continue
		}
		break
	}
	if len(parts) == 0 {
		return tableRef{}, start
	}
	alias := ""
	aliasIdx := nextSignificantIndex(infos, idx)
	if aliasIdx < len(infos) && infos[aliasIdx].depth == infos[start].depth {
		if isWord(infos[aliasIdx].token) && strings.EqualFold(strings.TrimSpace(infos[aliasIdx].token.Text), "AS") {
			nameIdx := nextSignificantIndex(infos, aliasIdx+1)
			if nameIdx < len(infos) && isIdentifierToken(infos[nameIdx].token) {
				alias = infos[nameIdx].token.Text
				idx = nameIdx
			}
		} else if isIdentifierToken(infos[aliasIdx].token) && !isReservedAliasBoundary(infos[aliasIdx].token.Text) {
			alias = infos[aliasIdx].token.Text
			idx = aliasIdx
		}
	}

	table := normalizeIdentifier(strings.Join(parts, ""))
	if alias == "" {
		if dot := strings.LastIndex(table, "."); dot >= 0 {
			alias = table[dot+1:]
		} else {
			alias = table
		}
	}
	return tableRef{table: table, alias: alias}, idx
}

func isReservedAliasBoundary(word string) bool {
	switch strings.ToUpper(strings.TrimSpace(word)) {
	case "ON", "USING", "WHERE", "GROUP", "ORDER", "LIMIT", "OFFSET", "FETCH", "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "UNION", "EXCEPT", "INTERSECT":
		return true
	case "SET", "FROM", "RETURNING":
		return true
	default:
		return false
	}
}

func nextSignificantIndex(infos []tokenInfo, idx int) int {
	for idx < len(infos) {
		t := infos[idx].token.Type
		if t != sqltoken.Whitespace && t != sqltoken.Comment {
			break
		}
		idx++
	}
	return idx
}

func isWord(token sqltoken.Token) bool {
	return token.Type == sqltoken.Word
}

func isIdentifierToken(token sqltoken.Token) bool {
	switch token.Type {
	case sqltoken.Word, sqltoken.Identifier, sqltoken.AtWord, sqltoken.Literal:
		return true
	default:
		return false
	}
}

func normalizeIdentifier(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	value = strings.Trim(value, "`")
	value = strings.Trim(value, "\"")
	value = strings.TrimPrefix(value, "[")
	value = strings.TrimSuffix(value, "]")
	return strings.ToLower(value)
}

func canonicalTableName(value string) string {
	value = normalizeIdentifier(value)
	if dot := strings.LastIndex(value, "."); dot >= 0 {
		return value[dot+1:]
	}
	return value
}

func fullSQLTokenizerConfig() sqltoken.Config {
	return sqltoken.Config{
		NoticeQuestionMark:         true,
		NoticeDollarNumber:         true,
		NoticeColonWord:            true,
		ColonWordIncludesUnicode:   true,
		NoticeHashComment:          true,
		NoticeDollarQuotes:         true,
		NoticeHexNumbers:           true,
		NoticeBinaryNumbers:        true,
		NoticeUAmpPrefix:           true,
		NoticeCharsetLiteral:       true,
		NoticeNotionalStrings:      true,
		NoticeDeliminatedStrings:   true,
		NoticeTypedNumbers:         true,
		NoticeMoneyConstants:       true,
		NoticeAtWord:               true,
		NoticeIdentifiers:          true,
	}
}

var dollarParamRe = regexp.MustCompile(`\$(\d+)`)
var atParamRe = regexp.MustCompile(`(?i)@p(\d+)`)

type placeholderKind int

const (
	placeholderQuestion placeholderKind = iota
	placeholderDollar
	placeholderAt
)

func newPlaceholderBuilder(ctx context.Context, query string, argCount int) func() string {
	kind := detectPlaceholderKind(ctx, query)
	current := argCount
	if kind == placeholderDollar {
		if max := findMaxPlaceholder(dollarParamRe, query); max > current {
			current = max
		}
	}
	if kind == placeholderAt {
		if max := findMaxPlaceholder(atParamRe, query); max > current {
			current = max
		}
	}
	return func() string {
		switch kind {
		case placeholderDollar:
			current++
			return "$" + strconv.Itoa(current)
		case placeholderAt:
			current++
			return "@p" + strconv.Itoa(current)
		default:
			return "?"
		}
	}
}

func detectPlaceholderKind(ctx context.Context, query string) placeholderKind {
	if driverName, ok := squealx.DriverNameFromContext(ctx); ok {
		switch strings.ToLower(driverName) {
		case "pgx", "postgres", "cockroach":
			return placeholderDollar
		case "mssql", "sqlserver":
			return placeholderAt
		}
	}
	if dollarParamRe.MatchString(query) {
		return placeholderDollar
	}
	if atParamRe.MatchString(query) {
		return placeholderAt
	}
	return placeholderQuestion
}

func findMaxPlaceholder(re *regexp.Regexp, query string) int {
	matches := re.FindAllStringSubmatch(query, -1)
	max := 0
	for _, m := range matches {
		if len(m) < 2 {
			continue
		}
		num, err := strconv.Atoi(m[1])
		if err != nil {
			continue
		}
		if num > max {
			max = num
		}
	}
	return max
}
