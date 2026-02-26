package hooks

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

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
	ScopeDenyPassthroughBudget   ScopeDenyCode = "passthrough_budget_exceeded"
)

type ScopeReasonCategory string
type ScopeReasonSeverity string

const (
	ScopeReasonCategoryContext  ScopeReasonCategory = "context"
	ScopeReasonCategoryShape    ScopeReasonCategory = "shape"
	ScopeReasonCategoryRule     ScopeReasonCategory = "rule"
	ScopeReasonCategoryResolver ScopeReasonCategory = "resolver"
	ScopeReasonCategoryBypass   ScopeReasonCategory = "bypass"
	ScopeReasonCategoryBudget   ScopeReasonCategory = "budget"
	ScopeReasonCategoryUnknown  ScopeReasonCategory = "unknown"
)

const (
	ScopeReasonSeverityLow      ScopeReasonSeverity = "low"
	ScopeReasonSeverityMedium   ScopeReasonSeverity = "medium"
	ScopeReasonSeverityHigh     ScopeReasonSeverity = "high"
	ScopeReasonSeverityCritical ScopeReasonSeverity = "critical"
	ScopeReasonSeverityUnknown  ScopeReasonSeverity = "unknown"
)

type ScopeReasonTaxonomy struct {
	Code      ScopeDenyCode
	Category  ScopeReasonCategory
	Severity  ScopeReasonSeverity
	Retryable bool
}

var scopeReasonTaxonomy = map[ScopeDenyCode]ScopeReasonTaxonomy{
	ScopeDenyMissingContext:      {Code: ScopeDenyMissingContext, Category: ScopeReasonCategoryContext, Severity: ScopeReasonSeverityHigh, Retryable: true},
	ScopeDenyUnknownShape:        {Code: ScopeDenyUnknownShape, Category: ScopeReasonCategoryShape, Severity: ScopeReasonSeverityHigh, Retryable: false},
	ScopeDenyMissingRule:         {Code: ScopeDenyMissingRule, Category: ScopeReasonCategoryRule, Severity: ScopeReasonSeverityCritical, Retryable: false},
	ScopeDenyResolverRequired:    {Code: ScopeDenyResolverRequired, Category: ScopeReasonCategoryResolver, Severity: ScopeReasonSeverityHigh, Retryable: false},
	ScopeDenyResolverFailed:      {Code: ScopeDenyResolverFailed, Category: ScopeReasonCategoryResolver, Severity: ScopeReasonSeverityHigh, Retryable: true},
	ScopeDenyParamMismatch:       {Code: ScopeDenyParamMismatch, Category: ScopeReasonCategoryRule, Severity: ScopeReasonSeverityHigh, Retryable: false},
	ScopeDenyUnscopedStatement:   {Code: ScopeDenyUnscopedStatement, Category: ScopeReasonCategoryShape, Severity: ScopeReasonSeverityCritical, Retryable: false},
	ScopeDenyUnsupportedStmtType: {Code: ScopeDenyUnsupportedStmtType, Category: ScopeReasonCategoryShape, Severity: ScopeReasonSeverityMedium, Retryable: false},
	ScopeDenyBypassNotAllowed:    {Code: ScopeDenyBypassNotAllowed, Category: ScopeReasonCategoryBypass, Severity: ScopeReasonSeverityCritical, Retryable: false},
	ScopeDenyBypassMissingReason: {Code: ScopeDenyBypassMissingReason, Category: ScopeReasonCategoryBypass, Severity: ScopeReasonSeverityHigh, Retryable: true},
	ScopeDenyBypassTokenRequired: {Code: ScopeDenyBypassTokenRequired, Category: ScopeReasonCategoryBypass, Severity: ScopeReasonSeverityHigh, Retryable: true},
	ScopeDenyPassthroughBudget:   {Code: ScopeDenyPassthroughBudget, Category: ScopeReasonCategoryBudget, Severity: ScopeReasonSeverityCritical, Retryable: true},
}

func ScopeReasonTaxonomyForCode(code ScopeDenyCode) (ScopeReasonTaxonomy, bool) {
	tax, ok := scopeReasonTaxonomy[code]
	if !ok {
		return ScopeReasonTaxonomy{Code: code, Category: ScopeReasonCategoryUnknown, Severity: ScopeReasonSeverityUnknown}, false
	}
	return tax, true
}

func ScopeReasonTaxonomyFromError(err error) (ScopeReasonTaxonomy, bool) {
	code, ok := ScopeDenyCodeFromError(err)
	if !ok {
		return ScopeReasonTaxonomy{}, false
	}
	return ScopeReasonTaxonomyForCode(code)
}

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

type ScopeConfidence string

const (
	ScopeConfidenceHigh   ScopeConfidence = "high"
	ScopeConfidenceMedium ScopeConfidence = "medium"
	ScopeConfidenceLow    ScopeConfidence = "low"
)

type ScopeTableOrigin string

const (
	ScopeTableOriginBase    ScopeTableOrigin = "base_table"
	ScopeTableOriginCTE     ScopeTableOrigin = "cte"
	ScopeTableOriginUnknown ScopeTableOrigin = "unknown"
)

type ScopeLineage struct {
	Table  string
	Alias  string
	Origin ScopeTableOrigin
}

type ScopeDecision struct {
	Action         ScopeDecisionAction
	ReasonCode     ScopeDenyCode
	ReasonCategory ScopeReasonCategory
	ReasonSeverity ScopeReasonSeverity
	Reason         string
	StatementType  string
	Confidence     ScopeConfidence
	Coverage       []string
	Lineage        []ScopeLineage
	MatchedTables  []string
	AppliedRules   []string
	AddedPredicate int
	Query          string
}

type ScopeAuditSink func(ctx context.Context, decision ScopeDecision)

type ScopeBudgetSnapshot struct {
	Enabled        bool
	Threshold      float64
	MinSamples     int
	TotalDecisions int
	Passthroughs   int
	Ratio          float64
	Exceeded       bool
}

type ScopeBudgetSink func(ctx context.Context, snapshot ScopeBudgetSnapshot)

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
	Table       string
	Column      string
	Predicate   string
	ResolveArgs ScopeArgsResolver
}

type ResourceScopeHook struct {
	defaultResolver             ScopeArgsResolver
	rules                       map[string]compiledScopeRule
	strictMode                  bool
	strictAllTables             bool
	rejectUnknownShapes         bool
	compatibilityMode           bool
	auditSink                   ScopeAuditSink
	allowTrustedBypass          bool
	requireBypassToken          bool
	bypassToken                 string
	passthroughBudgetEnabled    bool
	passthroughBudgetThreshold  float64
	passthroughBudgetMinSamples int
	budgetDecisionCount         int
	budgetPassthroughCount      int
	budgetSink                  ScopeBudgetSink
	budgetMu                    sync.Mutex
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
		defaultResolver:    defaultResolver,
		rules:              index,
		requireBypassToken: true,
		bypassToken:        "/* scope:bypass */",
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

func (h *ResourceScopeHook) SetCompatibilityMode(compat bool) *ResourceScopeHook {
	h.compatibilityMode = compat
	return h
}

func (h *ResourceScopeHook) SetPassthroughBudget(threshold float64, minSamples int) *ResourceScopeHook {
	if threshold <= 0 || threshold > 1 {
		h.passthroughBudgetEnabled = false
		h.passthroughBudgetThreshold = 0
		h.passthroughBudgetMinSamples = 0
		return h
	}
	if minSamples < 1 {
		minSamples = 1
	}
	h.passthroughBudgetEnabled = true
	h.passthroughBudgetThreshold = threshold
	h.passthroughBudgetMinSamples = minSamples
	return h
}

func (h *ResourceScopeHook) SetBudgetSink(sink ScopeBudgetSink) *ResourceScopeHook {
	h.budgetSink = sink
	return h
}

func (h *ResourceScopeHook) BudgetSnapshot() ScopeBudgetSnapshot {
	h.budgetMu.Lock()
	defer h.budgetMu.Unlock()
	return h.currentBudgetSnapshotLocked()
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
	if decision.ReasonCode != "" && (decision.ReasonCategory == "" || decision.ReasonSeverity == "") {
		if tax, ok := ScopeReasonTaxonomyForCode(decision.ReasonCode); ok {
			if decision.ReasonCategory == "" {
				decision.ReasonCategory = tax.Category
			}
			if decision.ReasonSeverity == "" {
				decision.ReasonSeverity = tax.Severity
			}
		}
	}
	snapshot := h.recordBudgetDecision(decision)
	if h.auditSink != nil {
		h.auditSink(ctx, decision)
	}
	if h.budgetSink != nil {
		h.budgetSink(ctx, snapshot)
	}
}

func (h *ResourceScopeHook) recordBudgetDecision(decision ScopeDecision) ScopeBudgetSnapshot {
	h.budgetMu.Lock()
	defer h.budgetMu.Unlock()
	h.budgetDecisionCount++
	if decision.Action == ScopeDecisionPassthrough {
		h.budgetPassthroughCount++
	}
	return h.currentBudgetSnapshotLocked()
}

func (h *ResourceScopeHook) currentBudgetSnapshotLocked() ScopeBudgetSnapshot {
	ratio := 0.0
	if h.budgetDecisionCount > 0 {
		ratio = float64(h.budgetPassthroughCount) / float64(h.budgetDecisionCount)
	}
	exceeded := false
	if h.passthroughBudgetEnabled &&
		h.budgetDecisionCount >= h.passthroughBudgetMinSamples &&
		ratio > h.passthroughBudgetThreshold {
		exceeded = true
	}
	return ScopeBudgetSnapshot{
		Enabled:        h.passthroughBudgetEnabled,
		Threshold:      h.passthroughBudgetThreshold,
		MinSamples:     h.passthroughBudgetMinSamples,
		TotalDecisions: h.budgetDecisionCount,
		Passthroughs:   h.budgetPassthroughCount,
		Ratio:          ratio,
		Exceeded:       exceeded,
	}
}

func (h *ResourceScopeHook) enforcePassthroughBudget(ctx context.Context, decision ScopeDecision) error {
	if !h.passthroughBudgetEnabled || decision.Action != ScopeDecisionPassthrough {
		return nil
	}
	h.budgetMu.Lock()
	projectedTotal := h.budgetDecisionCount + 1
	projectedPassthrough := h.budgetPassthroughCount + 1
	ratio := float64(projectedPassthrough) / float64(projectedTotal)
	exceeds := projectedTotal >= h.passthroughBudgetMinSamples && ratio > h.passthroughBudgetThreshold
	h.budgetMu.Unlock()
	if !exceeds {
		return nil
	}
	err := scopeErr(ScopeDenyPassthroughBudget, fmt.Sprintf("resource scope passthrough budget exceeded: ratio %.4f > %.4f", ratio, h.passthroughBudgetThreshold))
	reject := decision
	reject.Action = ScopeDecisionRejected
	reject.ReasonCode = ScopeDenyPassthroughBudget
	reject.Reason = err.Error()
	reject.ReasonCategory = ScopeReasonCategoryBudget
	reject.ReasonSeverity = ScopeReasonSeverityCritical
	h.emitAudit(ctx, reject)
	return err
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
		rewritten, updatedArgs, err := h.rewriteStatement(ctx, currentQuery[seg.start:seg.end], currentArgs, argPrefix, nil)
		if err != nil {
			return query, args, err
		}
		currentQuery = currentQuery[:seg.start] + rewritten + currentQuery[seg.end:]
		currentArgs = updatedArgs
	}
	return currentQuery, currentArgs, nil
}

func (h *ResourceScopeHook) rewriteStatement(ctx context.Context, statement string, args []any, argPrefix int, inheritedCTEs map[string]struct{}) (string, []any, error) {
	tokens := sqltoken.Tokenize(statement, fullSQLTokenizerConfig())
	infos := buildTokenInfos(tokens)
	ranges := deepestNestedStatementRanges(statement, infos)
	for i := len(ranges) - 1; i >= 0; i-- {
		rg := ranges[i]
		if !shouldRewriteNestedRange(infos, rg) {
			continue
		}
		subPrefix := argPrefix + countQuestionMarksBefore(statement, rg.start)
		rewrittenSub, updatedArgs, err := h.rewriteStatement(ctx, statement[rg.start:rg.end], args, subPrefix, inheritedCTEs)
		if err != nil {
			return statement, args, err
		}
		statement = statement[:rg.start] + rewrittenSub + statement[rg.end:]
		args = updatedArgs
	}

	tokens = sqltoken.Tokenize(statement, fullSQLTokenizerConfig())
	infos = buildTokenInfos(tokens)
	coverage := collectCoverage(statement, infos)
	if len(infos) == 0 {
		return statement, args, nil
	}

	firstWord := firstTopLevelWord(infos)
	if firstWord == "" {
		h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionPassthrough, Reason: "empty statement", Confidence: ScopeConfidenceLow, Coverage: coverage, Query: statement})
		return statement, args, nil
	}

	if firstWord == "WITH" {
		mainIdx := mainStatementStart(infos)
		cteNames := mergeCTESets(inheritedCTEs, collectCTENames(infos))
		confidence := ScopeConfidenceLow
		if mainIdx < 0 {
			if h.compatibilityMode {
				decision := ScopeDecision{Action: ScopeDecisionPassthrough, StatementType: "WITH", Reason: "compatibility mode passthrough: WITH without main body", Confidence: confidence, Coverage: coverage, Query: statement}
				if err := h.enforcePassthroughBudget(ctx, decision); err != nil {
					return statement, args, err
				}
				h.emitAudit(ctx, decision)
				return statement, args, nil
			}
			if h.strictMode || h.rejectUnknownShapes {
				err := scopeErr(ScopeDenyUnknownShape, "resource scope rejected WITH statement without main body")
				h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionRejected, StatementType: "WITH", ReasonCode: ScopeDenyUnknownShape, Reason: err.Error(), Confidence: confidence, Coverage: coverage, Query: statement})
				return statement, args, err
			}
			h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionPassthrough, StatementType: "WITH", Reason: "WITH without main body", Confidence: confidence, Coverage: coverage, Query: statement})
			return statement, args, nil
		}
		body := statement[mainIdx:]
		rewrittenBody, updatedArgs, err := h.rewriteStatement(ctx, body, args, argPrefix+countQuestionMarksBefore(statement, mainIdx), cteNames)
		if err != nil {
			return statement, args, err
		}
		return statement[:mainIdx] + rewrittenBody, updatedArgs, nil
	}

	statementType := firstWord
	if statementType != "SELECT" && statementType != "UPDATE" && statementType != "DELETE" && statementType != "INSERT" && statementType != "MERGE" {
		confidence := ScopeConfidenceLow
		if h.compatibilityMode {
			decision := ScopeDecision{Action: ScopeDecisionPassthrough, StatementType: statementType, Reason: "compatibility mode passthrough: statement type not scoped", Confidence: confidence, Coverage: coverage, Query: statement}
			if err := h.enforcePassthroughBudget(ctx, decision); err != nil {
				return statement, args, err
			}
			h.emitAudit(ctx, decision)
			return statement, args, nil
		}
		if h.rejectUnknownShapes || (h.strictMode && h.strictAllTables) {
			err := scopeErr(ScopeDenyUnsupportedStmtType, fmt.Sprintf("resource scope rejected unsupported statement type %q", statementType))
			h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionRejected, StatementType: statementType, ReasonCode: ScopeDenyUnsupportedStmtType, Reason: err.Error(), Confidence: confidence, Coverage: coverage, Query: statement})
			return statement, args, err
		}
		h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionPassthrough, StatementType: statementType, Reason: "statement type not scoped", Confidence: confidence, Coverage: coverage, Query: statement})
		return statement, args, nil
	}
	if statementType == "INSERT" {
		sourceStart, sourceEnd := insertSourceQueryRange(statement, infos)
		confidence := ScopeConfidenceLow
		if sourceStart < 0 || sourceEnd <= sourceStart {
			if h.compatibilityMode {
				decision := ScopeDecision{Action: ScopeDecisionPassthrough, StatementType: statementType, Reason: "compatibility mode passthrough: INSERT without query source", Confidence: confidence, Coverage: coverage, Query: statement}
				if err := h.enforcePassthroughBudget(ctx, decision); err != nil {
					return statement, args, err
				}
				h.emitAudit(ctx, decision)
				return statement, args, nil
			}
			if h.rejectUnknownShapes || (h.strictMode && h.strictAllTables) {
				err := scopeErr(ScopeDenyUnknownShape, "resource scope rejected INSERT without query source")
				h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionRejected, StatementType: statementType, ReasonCode: ScopeDenyUnknownShape, Reason: err.Error(), Confidence: confidence, Coverage: coverage, Query: statement})
				return statement, args, err
			}
			h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionPassthrough, StatementType: statementType, Reason: "INSERT without query source not scoped", Confidence: confidence, Coverage: coverage, Query: statement})
			return statement, args, nil
		}
		source := statement[sourceStart:sourceEnd]
		rewrittenSource, updatedArgs, err := h.rewriteStatement(ctx, source, args, argPrefix+countQuestionMarksBefore(statement, sourceStart), inheritedCTEs)
		if err != nil {
			return statement, args, err
		}
		return statement[:sourceStart] + rewrittenSource + statement[sourceEnd:], updatedArgs, nil
	}
	if statementType == "SELECT" {
		setSegments := splitTopLevelSetOperands(statement, infos)
		if len(setSegments) > 1 {
			currentStatement := statement
			currentArgs := args
			for i := len(setSegments) - 1; i >= 0; i-- {
				seg := setSegments[i]
				subPrefix := argPrefix + countQuestionMarksBefore(currentStatement, seg.start)
				rewrittenSeg, updatedArgs, err := h.rewriteStatement(ctx, currentStatement[seg.start:seg.end], currentArgs, subPrefix, inheritedCTEs)
				if err != nil {
					return statement, args, err
				}
				currentStatement = currentStatement[:seg.start] + rewrittenSeg + currentStatement[seg.end:]
				currentArgs = updatedArgs
			}
			return currentStatement, currentArgs, nil
		}
	}

	var tableRefs []tableRef
	unknownShape := false
	switch statementType {
	case "SELECT":
		tableRefs, unknownShape = collectTopLevelTableRefs(infos, inheritedCTEs)
	case "UPDATE":
		tableRefs = collectUpdateTableRefs(infos)
	case "DELETE":
		tableRefs = collectDeleteTableRefs(infos)
	case "MERGE":
		tableRefs = collectMergeTableRefs(infos)
	}
	lineage := refsToLineage(tableRefs)
	confidence := evaluateStatementConfidence(statementType, unknownShape, tableRefs, coverage)
	if unknownShape && h.rejectUnknownShapes {
		if h.compatibilityMode && confidence == ScopeConfidenceLow {
			decision := ScopeDecision{Action: ScopeDecisionPassthrough, StatementType: statementType, Reason: "compatibility mode passthrough: low-confidence shape", Confidence: confidence, Coverage: coverage, Lineage: lineage, MatchedTables: tableNames(tableRefs), Query: statement}
			if err := h.enforcePassthroughBudget(ctx, decision); err != nil {
				return statement, args, err
			}
			h.emitAudit(ctx, decision)
			return statement, args, nil
		}
		err := scopeErr(ScopeDenyUnknownShape, fmt.Sprintf("resource scope rejected unknown %s shape", statementType))
		h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionRejected, StatementType: statementType, ReasonCode: ScopeDenyUnknownShape, Reason: err.Error(), Confidence: confidence, Coverage: coverage, Lineage: lineage, MatchedTables: tableNames(tableRefs), Query: statement})
		return statement, args, err
	}
	if len(tableRefs) == 0 {
		if h.compatibilityMode && confidence == ScopeConfidenceLow {
			decision := ScopeDecision{Action: ScopeDecisionPassthrough, StatementType: statementType, Reason: "compatibility mode passthrough: low-confidence shape", Confidence: confidence, Coverage: coverage, Query: statement}
			if err := h.enforcePassthroughBudget(ctx, decision); err != nil {
				return statement, args, err
			}
			h.emitAudit(ctx, decision)
			return statement, args, nil
		}
		if h.strictMode || h.rejectUnknownShapes {
			err := scopeErr(ScopeDenyUnscopedStatement, "resource scope rejected statement with no table refs")
			h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionRejected, StatementType: statementType, ReasonCode: ScopeDenyUnscopedStatement, Reason: err.Error(), Confidence: confidence, Coverage: coverage, Query: statement})
			return statement, args, err
		}
		h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionPassthrough, StatementType: statementType, Reason: "no table refs discovered", Confidence: confidence, Coverage: coverage, Query: statement})
		return statement, args, nil
	}

	wherePos, insertionPos := clausePositions(statementType, infos, len(statement))
	if statementType == "MERGE" && wherePos < 0 {
		if h.compatibilityMode {
			decision := ScopeDecision{Action: ScopeDecisionPassthrough, StatementType: statementType, Reason: "compatibility mode passthrough: MERGE without ON clause", Confidence: confidence, Coverage: coverage, Lineage: lineage, MatchedTables: tableNames(tableRefs), Query: statement}
			if err := h.enforcePassthroughBudget(ctx, decision); err != nil {
				return statement, args, err
			}
			h.emitAudit(ctx, decision)
			return statement, args, nil
		}
		err := scopeErr(ScopeDenyUnknownShape, "resource scope rejected MERGE without ON clause")
		h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionRejected, StatementType: statementType, ReasonCode: ScopeDenyUnknownShape, Reason: err.Error(), Confidence: confidence, Coverage: coverage, Lineage: lineage, MatchedTables: tableNames(tableRefs), Query: statement})
		return statement, args, err
	}

	placeholder := newPlaceholderBuilder(ctx, statement, len(args))
	predicates := make([]string, 0, len(tableRefs))
	addedArgs := make([]any, 0, len(tableRefs))

	appliedRules := make([]string, 0, len(tableRefs))
	for _, ref := range tableRefs {
		if ref.origin == ScopeTableOriginCTE {
			continue
		}
		rule, ok := h.rules[canonicalTableName(ref.table)]
		if !ok {
			if h.strictAllTables {
				err := scopeErr(ScopeDenyMissingRule, fmt.Sprintf("resource scope rule missing for table %q", ref.table))
				h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionRejected, StatementType: statementType, ReasonCode: ScopeDenyMissingRule, Reason: err.Error(), Confidence: confidence, Coverage: coverage, Lineage: lineage, MatchedTables: tableNames(tableRefs), Query: statement})
				return statement, args, err
			}
			continue
		}
		predicate, params, err := h.buildPredicate(ctx, rule, ref.alias, placeholder)
		if err != nil {
			se, _ := err.(*ScopeError)
			decision := ScopeDecision{Action: ScopeDecisionRejected, StatementType: statementType, Reason: err.Error(), Confidence: confidence, Coverage: coverage, Lineage: lineage, MatchedTables: tableNames(tableRefs), Query: statement}
			if se != nil {
				decision.ReasonCode = se.Code
			}
			h.emitAudit(ctx, decision)
			return statement, args, err
		}
		if predicate == "" {
			if h.strictMode {
				err := scopeErr(ScopeDenyUnscopedStatement, fmt.Sprintf("resource scope generated empty predicate for table %q", ref.table))
				h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionRejected, StatementType: statementType, ReasonCode: ScopeDenyUnscopedStatement, Reason: err.Error(), Confidence: confidence, Coverage: coverage, Lineage: lineage, MatchedTables: tableNames(tableRefs), Query: statement})
				return statement, args, err
			}
			continue
		}
		predicates = append(predicates, predicate)
		appliedRules = append(appliedRules, rule.TableKey)
		addedArgs = append(addedArgs, params...)
	}
	if len(predicates) == 0 {
		if statementType == "SELECT" && len(tableRefs) > 0 && len(appliedRules) == 0 {
			h.emitAudit(ctx, ScopeDecision{
				Action:        ScopeDecisionScoped,
				StatementType: statementType,
				Reason:        "no direct base-table predicates at this level; scoped by nested statements",
				Confidence:    confidence,
				Coverage:      coverage,
				Lineage:       lineage,
				MatchedTables: tableNames(tableRefs),
				Query:         statement,
			})
			return statement, args, nil
		}
		if h.strictMode {
			err := scopeErr(ScopeDenyUnscopedStatement, "resource scope rejected unscoped statement")
			h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionRejected, StatementType: statementType, ReasonCode: ScopeDenyUnscopedStatement, Reason: err.Error(), Confidence: confidence, Coverage: coverage, Lineage: lineage, MatchedTables: tableNames(tableRefs), Query: statement})
			return statement, args, err
		}
		h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionPassthrough, StatementType: statementType, Reason: "no matching rules for discovered tables", Confidence: confidence, Coverage: coverage, Lineage: lineage, MatchedTables: tableNames(tableRefs), Query: statement})
		return statement, args, nil
	}

	joined := strings.Join(predicates, " AND ")
	insertAt := insertionPos
	if insertAt < 0 || insertAt > len(statement) {
		insertAt = len(statement)
	}
	if wherePos >= 0 && wherePos < insertionPos {
		statement = statement[:insertAt] + " AND (" + joined + ") " + statement[insertAt:]
	} else if statementType == "MERGE" {
		if h.compatibilityMode {
			decision := ScopeDecision{Action: ScopeDecisionPassthrough, StatementType: statementType, Reason: "compatibility mode passthrough: MERGE rewrite boundary missing", Confidence: confidence, Coverage: coverage, Lineage: lineage, MatchedTables: tableNames(tableRefs), Query: statement}
			if err := h.enforcePassthroughBudget(ctx, decision); err != nil {
				return statement, args, err
			}
			h.emitAudit(ctx, decision)
			return statement, args, nil
		}
		err := scopeErr(ScopeDenyUnknownShape, "resource scope rejected MERGE rewrite boundary")
		h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionRejected, StatementType: statementType, ReasonCode: ScopeDenyUnknownShape, Reason: err.Error(), Confidence: confidence, Coverage: coverage, Lineage: lineage, MatchedTables: tableNames(tableRefs), Query: statement})
		return statement, args, err
	} else {
		statement = statement[:insertAt] + " WHERE (" + joined + ") " + statement[insertAt:]
	}

	updatedArgs := mergeArgsForInsertion(ctx, statement, args, addedArgs, insertAt, argPrefix)
	h.emitAudit(ctx, ScopeDecision{Action: ScopeDecisionScoped, StatementType: statementType, Confidence: confidence, Coverage: coverage, Lineage: lineage, MatchedTables: tableNames(tableRefs), AppliedRules: appliedRules, AddedPredicate: len(predicates), Query: statement})
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
			case "GROUP", "ORDER", "LIMIT", "OFFSET", "FETCH", "FOR", "UNION", "EXCEPT", "INTERSECT", "INTERSECTION", "MINUS":
				if info.start < insertionPos {
					insertionPos = info.start
				}
			}
		case "MERGE":
			switch word {
			case "ON":
				if wherePos < 0 {
					wherePos = info.start
				}
			case "WHEN", "OUTPUT", "RETURNING":
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

func splitTopLevelSetOperands(statement string, infos []tokenInfo) []statementSegment {
	segments := make([]statementSegment, 0, 2)
	start := 0
	for i := 0; i < len(infos); i++ {
		info := infos[i]
		if info.depth != 0 || !isWord(info.token) {
			continue
		}
		word := strings.ToUpper(strings.TrimSpace(info.token.Text))
		if !isSetOperatorWord(word) {
			continue
		}
		if start < info.start {
			segments = append(segments, statementSegment{start: start, end: info.start})
		}
		nextStart := info.end
		modIdx := nextSignificantIndex(infos, i+1)
		for modIdx < len(infos) && infos[modIdx].depth == 0 && isWord(infos[modIdx].token) {
			modWord := strings.ToUpper(strings.TrimSpace(infos[modIdx].token.Text))
			if !isSetOperatorModifierWord(modWord) {
				break
			}
			nextStart = infos[modIdx].end
			modIdx = nextSignificantIndex(infos, modIdx+1)
		}
		start = nextStart
	}
	if start < len(statement) {
		segments = append(segments, statementSegment{start: start, end: len(statement)})
	}
	if len(segments) <= 1 {
		return nil
	}
	filtered := make([]statementSegment, 0, len(segments))
	for _, seg := range segments {
		if seg.start >= seg.end {
			continue
		}
		if strings.TrimSpace(statement[seg.start:seg.end]) == "" {
			continue
		}
		filtered = append(filtered, seg)
	}
	if len(filtered) <= 1 {
		return nil
	}
	return filtered
}

func insertSourceQueryRange(statement string, infos []tokenInfo) (start int, end int) {
	start = -1
	end = len(statement)
	for i := 0; i < len(infos); i++ {
		info := infos[i]
		if info.depth != 0 || !isWord(info.token) {
			continue
		}
		word := strings.ToUpper(strings.TrimSpace(info.token.Text))
		if word == "SELECT" || word == "WITH" {
			start = info.start
			break
		}
	}
	if start < 0 {
		return -1, -1
	}
	started := false
	for i := 0; i < len(infos); i++ {
		info := infos[i]
		if info.depth != 0 || !isWord(info.token) {
			continue
		}
		if info.start == start {
			started = true
			continue
		}
		if !started {
			continue
		}
		word := strings.ToUpper(strings.TrimSpace(info.token.Text))
		switch word {
		case "RETURNING":
			return start, info.start
		case "ON":
			nextIdx := nextSignificantIndex(infos, i+1)
			if nextIdx < len(infos) && infos[nextIdx].depth == 0 && isWord(infos[nextIdx].token) {
				nextWord := strings.ToUpper(strings.TrimSpace(infos[nextIdx].token.Text))
				if nextWord == "CONFLICT" || nextWord == "DUPLICATE" {
					return start, info.start
				}
			}
		}
	}
	return start, end
}

func isSetOperatorWord(word string) bool {
	switch strings.ToUpper(strings.TrimSpace(word)) {
	case "UNION", "INTERSECT", "INTERSECTION", "EXCEPT", "MINUS":
		return true
	default:
		return false
	}
}

func isSetOperatorModifierWord(word string) bool {
	switch strings.ToUpper(strings.TrimSpace(word)) {
	case "ALL", "DISTINCT":
		return true
	default:
		return false
	}
}

func firstWordFromSQL(sql string) string {
	tokens := sqltoken.Tokenize(sql, fullSQLTokenizerConfig())
	infos := buildTokenInfos(tokens)
	return firstTopLevelWord(infos)
}

func isScopeStatementWord(word string) bool {
	switch strings.ToUpper(strings.TrimSpace(word)) {
	case "SELECT", "WITH", "UPDATE", "DELETE", "MERGE":
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
	updateIdx := -1
	for i := 0; i < len(infos); i++ {
		info := infos[i]
		if info.depth != 0 || !isWord(info.token) {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(info.token.Text), "UPDATE") {
			continue
		}
		updateIdx = i
		break
	}
	if updateIdx < 0 {
		return nil
	}

	targetIdx := nextSignificantIndex(infos, updateIdx+1)
	if targetIdx < len(infos) && isWord(infos[targetIdx].token) && strings.EqualFold(strings.TrimSpace(infos[targetIdx].token.Text), "ONLY") {
		targetIdx = nextSignificantIndex(infos, targetIdx+1)
	}
	refs := make([]tableRef, 0, 4)
	ref, _ := parseTableRef(infos, targetIdx)
	if ref.table != "" {
		ref.origin = ScopeTableOriginBase
		refs = append(refs, ref)
	}

	fromRefs := collectSourceTableRefs(infos, updateIdx+1, isUpdateSourceBoundaryWord)
	return appendUniqueTableRefs(refs, fromRefs)
}

func collectDeleteTableRefs(infos []tokenInfo) []tableRef {
	deleteIdx := -1
	for i := 0; i < len(infos); i++ {
		info := infos[i]
		if info.depth != 0 || !isWord(info.token) {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(info.token.Text), "DELETE") {
			continue
		}
		deleteIdx = i
		break
	}
	if deleteIdx < 0 {
		return nil
	}
	return appendUniqueTableRefs(nil, collectSourceTableRefs(infos, deleteIdx+1, isDeleteSourceBoundaryWord))
}

func collectMergeTableRefs(infos []tokenInfo) []tableRef {
	mergeIdx := -1
	for i := 0; i < len(infos); i++ {
		info := infos[i]
		if info.depth != 0 || !isWord(info.token) {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(info.token.Text), "MERGE") {
			mergeIdx = i
			break
		}
	}
	if mergeIdx < 0 {
		return nil
	}

	refs := make([]tableRef, 0, 4)
	targetIdx := nextSignificantIndex(infos, mergeIdx+1)
	if targetIdx < len(infos) && isWord(infos[targetIdx].token) && strings.EqualFold(strings.TrimSpace(infos[targetIdx].token.Text), "INTO") {
		targetIdx = nextSignificantIndex(infos, targetIdx+1)
	}
	targetRef, _ := parseTableRef(infos, targetIdx)
	if targetRef.table != "" {
		targetRef.origin = ScopeTableOriginBase
		refs = append(refs, targetRef)
	}

	usingRefs := collectMergeUsingTableRefs(infos, mergeIdx+1)
	return appendUniqueTableRefs(refs, usingRefs)
}

func collectMergeUsingTableRefs(infos []tokenInfo, start int) []tableRef {
	for i := start; i < len(infos); i++ {
		info := infos[i]
		if info.depth != 0 || !isWord(info.token) {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(info.token.Text), "USING") {
			idx := nextSignificantIndex(infos, i+1)
			if idx >= len(infos) {
				return nil
			}
			if infos[idx].token.Type == sqltoken.Punctuation && strings.TrimSpace(infos[idx].token.Text) == "(" {
				// USING (SELECT ...) aliases are scoped via nested statement rewrite.
				return nil
			}
			ref, _ := parseTableRef(infos, idx)
			if ref.table == "" {
				return nil
			}
			ref.origin = ScopeTableOriginBase
			return []tableRef{ref}
		}
		if strings.EqualFold(strings.TrimSpace(info.token.Text), "WHEN") {
			break
		}
	}
	return nil
}

func collectSourceTableRefs(infos []tokenInfo, start int, isBoundary func(string) bool) []tableRef {
	refs := make([]tableRef, 0, 4)
	inSourceClause := false
	expectTable := false
	for i := start; i < len(infos); i++ {
		info := infos[i]
		if info.depth != 0 {
			continue
		}
		switch info.token.Type {
		case sqltoken.Whitespace, sqltoken.Comment:
			continue
		}
		if isWord(info.token) {
			word := strings.ToUpper(strings.TrimSpace(info.token.Text))
			if inSourceClause && isBoundary(word) {
				break
			}
			if isJoinModifierWord(word) {
				continue
			}
			if word == "FROM" || word == "USING" || word == "JOIN" {
				inSourceClause = true
				expectTable = true
				continue
			}
			if expectTable {
				ref, next := parseTableRef(infos, i)
				if ref.table != "" {
					ref.origin = ScopeTableOriginBase
					refs = append(refs, ref)
					i = next
				}
				expectTable = false
			}
			continue
		}
		if info.token.Type == sqltoken.Punctuation && strings.TrimSpace(info.token.Text) == "," && inSourceClause {
			expectTable = true
		}
	}
	return refs
}

func isJoinModifierWord(word string) bool {
	switch strings.ToUpper(strings.TrimSpace(word)) {
	case "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "NATURAL", "OUTER", "LATERAL", "STRAIGHT_JOIN":
		return true
	default:
		return false
	}
}

func isUpdateSourceBoundaryWord(word string) bool {
	switch strings.ToUpper(strings.TrimSpace(word)) {
	case "WHERE", "RETURNING", "ORDER", "LIMIT":
		return true
	default:
		return false
	}
}

func isDeleteSourceBoundaryWord(word string) bool {
	switch strings.ToUpper(strings.TrimSpace(word)) {
	case "WHERE", "RETURNING", "ORDER", "LIMIT":
		return true
	default:
		return false
	}
}

func appendUniqueTableRefs(base []tableRef, extra []tableRef) []tableRef {
	seen := make(map[string]struct{}, len(base)+len(extra))
	for _, ref := range base {
		seen[tableRefIdentity(ref)] = struct{}{}
	}
	for _, ref := range extra {
		if ref.table == "" {
			continue
		}
		key := tableRefIdentity(ref)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		base = append(base, ref)
	}
	return base
}

func tableRefIdentity(ref tableRef) string {
	return canonicalTableName(ref.table) + "|" + normalizeIdentifier(ref.alias)
}

func isCTEName(table string, cteNames map[string]struct{}) bool {
	if len(cteNames) == 0 {
		return false
	}
	_, ok := cteNames[table]
	return ok
}

func collectCTENames(infos []tokenInfo) map[string]struct{} {
	names := make(map[string]struct{})
	if firstTopLevelWord(infos) != "WITH" {
		return names
	}
	seenWith := false
	expectName := false
	for _, info := range infos {
		if info.depth != 0 {
			continue
		}
		if !seenWith {
			if isWord(info.token) && strings.EqualFold(strings.TrimSpace(info.token.Text), "WITH") {
				seenWith = true
				expectName = true
			}
			continue
		}
		if isWord(info.token) {
			word := strings.ToUpper(strings.TrimSpace(info.token.Text))
			if word == "RECURSIVE" {
				continue
			}
			if word == "SELECT" || word == "UPDATE" || word == "DELETE" || word == "INSERT" {
				break
			}
		}
		if expectName && isIdentifierToken(info.token) {
			name := canonicalTableName(info.token.Text)
			if name != "" && !isReservedCTEKeyword(name) {
				names[name] = struct{}{}
				expectName = false
			}
			continue
		}
		if info.token.Type == sqltoken.Punctuation && strings.TrimSpace(info.token.Text) == "," {
			expectName = true
		}
	}
	return names
}

func isReservedCTEKeyword(name string) bool {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "AS", "MATERIALIZED", "NOT":
		return true
	default:
		return false
	}
}

func mergeCTESets(parent map[string]struct{}, current map[string]struct{}) map[string]struct{} {
	if len(parent) == 0 && len(current) == 0 {
		return nil
	}
	merged := make(map[string]struct{}, len(parent)+len(current))
	for k := range parent {
		merged[k] = struct{}{}
	}
	for k := range current {
		merged[k] = struct{}{}
	}
	return merged
}

func refsToLineage(refs []tableRef) []ScopeLineage {
	if len(refs) == 0 {
		return nil
	}
	out := make([]ScopeLineage, 0, len(refs))
	for _, ref := range refs {
		origin := ref.origin
		if origin == "" {
			origin = ScopeTableOriginUnknown
		}
		out = append(out, ScopeLineage{
			Table:  ref.table,
			Alias:  ref.alias,
			Origin: origin,
		})
	}
	return out
}

func collectCoverage(statement string, infos []tokenInfo) []string {
	if len(infos) == 0 {
		return nil
	}
	coverage := make(map[string]struct{}, 8)
	if len(deepestNestedStatementRanges(statement, infos)) > 0 {
		coverage["subquery"] = struct{}{}
	}
	first := firstTopLevelWord(infos)
	if first != "" {
		coverage[strings.ToLower(first)] = struct{}{}
	}
	for _, info := range infos {
		if !isWord(info.token) {
			continue
		}
		word := strings.ToUpper(strings.TrimSpace(info.token.Text))
		switch word {
		case "WITH":
			coverage["with"] = struct{}{}
		case "JOIN":
			coverage["join"] = struct{}{}
		case "UNION", "INTERSECT", "INTERSECTION", "EXCEPT", "MINUS":
			coverage["set_operation"] = struct{}{}
		case "OVER":
			coverage["window"] = struct{}{}
		case "TABLESAMPLE":
			coverage["tablesample"] = struct{}{}
		}
		if strings.HasPrefix(word, "JSON") {
			coverage["json"] = struct{}{}
		}
	}
	for i := 0; i < len(infos); i++ {
		info := infos[i]
		if info.depth != 0 || !isWord(info.token) {
			continue
		}
		word := strings.ToUpper(strings.TrimSpace(info.token.Text))
		if word != "FROM" && word != "JOIN" {
			continue
		}
		next := nextSignificantIndex(infos, i+1)
		if next < len(infos) && infos[next].token.Type == sqltoken.Punctuation && strings.TrimSpace(infos[next].token.Text) == "(" {
			coverage["derived_table"] = struct{}{}
		}
	}
	return sortedKeys(coverage)
}

func evaluateStatementConfidence(statementType string, unknownShape bool, refs []tableRef, coverage []string) ScopeConfidence {
	if unknownShape || len(refs) == 0 {
		return ScopeConfidenceLow
	}
	hasCTE := false
	hasBase := false
	for _, ref := range refs {
		switch ref.origin {
		case ScopeTableOriginCTE:
			hasCTE = true
		case ScopeTableOriginBase:
			hasBase = true
		}
	}
	if hasCTE && !hasBase {
		return ScopeConfidenceMedium
	}
	for _, c := range coverage {
		if c == "set_operation" || c == "window" {
			return ScopeConfidenceMedium
		}
	}
	if statementType == "SELECT" && hasCTE {
		return ScopeConfidenceMedium
	}
	return ScopeConfidenceHigh
}

func sortedKeys(set map[string]struct{}) []string {
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for key := range set {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
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
	table  string
	alias  string
	origin ScopeTableOrigin
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

func collectTopLevelTableRefs(infos []tokenInfo, cteNames map[string]struct{}) ([]tableRef, bool) {
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
		if isCTEName(canonicalTableName(ref.table), cteNames) {
			ref.origin = ScopeTableOriginCTE
		} else {
			ref.origin = ScopeTableOriginBase
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
	case "ON", "USING", "WHERE", "GROUP", "ORDER", "LIMIT", "OFFSET", "FETCH", "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "UNION", "EXCEPT", "INTERSECT", "INTERSECTION", "MINUS":
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
		NoticeQuestionMark:       true,
		NoticeDollarNumber:       true,
		NoticeColonWord:          true,
		ColonWordIncludesUnicode: true,
		NoticeHashComment:        true,
		NoticeDollarQuotes:       true,
		NoticeHexNumbers:         true,
		NoticeBinaryNumbers:      true,
		NoticeUAmpPrefix:         true,
		NoticeCharsetLiteral:     true,
		NoticeNotionalStrings:    true,
		NoticeDeliminatedStrings: true,
		NoticeTypedNumbers:       true,
		NoticeMoneyConstants:     true,
		NoticeAtWord:             true,
		NoticeIdentifiers:        true,
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

func shouldRewriteNestedRange(infos []tokenInfo, rg statementSegment) bool {
	openIdx := -1
	for i, info := range infos {
		if info.end != rg.start {
			continue
		}
		if info.token.Type == sqltoken.Punctuation && strings.TrimSpace(info.token.Text) == "(" {
			openIdx = i
			break
		}
	}
	if openIdx < 0 {
		return true
	}
	prev := openIdx - 1
	for prev >= 0 {
		t := infos[prev].token.Type
		if t != sqltoken.Whitespace && t != sqltoken.Comment {
			break
		}
		prev--
	}
	if prev < 0 || !isWord(infos[prev].token) {
		return false
	}
	switch strings.ToUpper(strings.TrimSpace(infos[prev].token.Text)) {
	case "IN", "EXISTS", "FROM", "USING", "JOIN", "AS", "WITH", "ON", "WHERE", "UNION", "INTERSECT", "INTERSECTION", "EXCEPT", "MINUS":
		return true
	default:
		return false
	}
}
