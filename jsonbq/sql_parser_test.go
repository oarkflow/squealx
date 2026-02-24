package jsonbq

import (
	"reflect"
	"regexp"
	"strings"
	"testing"
)

func TestParseSQLTemplateBasic(t *testing.T) {
	query := `
SELECT COALESCE(JSON_TEXT(p.data, 'provider_name'), JSON_TEXT(p.data, 'provider_id'), CAST(p.id AS text)) AS provider
FROM registry_module_records p
WHERE p.tenant_id = :tenant_id AND p.module_key = :module_key
`
	vars := map[string]any{
		"tenant_id":  int64(7),
		"module_key": "providers",
	}

	sql, args, err := ParseNormalSQL(query, vars)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if !strings.Contains(sql, `jsonb_extract_path_text(p.data, 'provider_name')`) {
		t.Fatalf("expected JSON_TEXT rewrite in sql: %s", sql)
	}
	if !strings.Contains(sql, `FROM registry_module_records p`) {
		t.Fatalf("expected normal table reference in sql: %s", sql)
	}
	wantArgs := []any{int64(7), "providers"}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("unexpected args:\nwant: %#v\ngot:  %#v", wantArgs, args)
	}
}

func TestParseSQLTemplateIgnoresPlaceholdersInLiteralsAndComments(t *testing.T) {
	query := `
SELECT ':tenant_id' AS literal_text, $$ :tenant_id $$ AS dollar_text
FROM t
-- :tenant_id
WHERE id = :tenant_id /* :tenant_id */
`
	sql, args, err := ParseNormalSQL(query, map[string]any{"tenant_id": 9})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if strings.Count(sql, "$1") != 1 {
		t.Fatalf("expected exactly one bound placeholder, got sql=%s", sql)
	}
	if len(args) != 1 || args[0] != 9 {
		t.Fatalf("unexpected args: %v", args)
	}
}

func TestParseSQLTemplateErrors(t *testing.T) {
	_, _, err := ParseSQLTemplate("SELECT {{missing}}", map[string]any{})
	if err == nil || !strings.Contains(err.Error(), "missing template variable") {
		t.Fatalf("expected missing variable error, got: %v", err)
	}

	_, _, err = ParseSQLTemplate("SELECT {{ident:bad}}", map[string]any{"bad": "x;DROP"})
	if err == nil || !strings.Contains(err.Error(), "invalid identifier part") {
		t.Fatalf("expected invalid identifier error, got: %v", err)
	}

	_, _, err = ParseSQLTemplate("SELECT {{", map[string]any{})
	if err == nil || !strings.Contains(err.Error(), "unterminated placeholder") {
		t.Fatalf("expected unterminated placeholder error, got: %v", err)
	}
}

func TestParseNormalSQLJSONPathHelpers(t *testing.T) {
	sql, args, err := ParseNormalSQL(
		`SELECT JSON_NUM(c.data, 'charge_amount') FROM t WHERE id = :id`,
		map[string]any{"id": 42},
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(sql, `(jsonb_extract_path_text(c.data, 'charge_amount'))::numeric`) {
		t.Fatalf("expected JSON_NUM rewrite, got: %s", sql)
	}
	if len(args) != 1 || args[0] != 42 {
		t.Fatalf("unexpected args: %v", args)
	}
}

func TestParseNormalSQLIfHelpers(t *testing.T) {
	sql, args, err := ParseNormalSQL(
		`SELECT IF(score >= 50, 'high', 'low') AS grade, IFNULL(name, 'unknown') AS n FROM t WHERE tenant_id = :tenant_id`,
		map[string]any{"tenant_id": 99},
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(sql, "CASE WHEN score >= 50 THEN 'high' ELSE 'low' END") {
		t.Fatalf("expected IF rewrite, got: %s", sql)
	}
	if !strings.Contains(sql, "COALESCE(name, 'unknown')") {
		t.Fatalf("expected IFNULL rewrite, got: %s", sql)
	}
	if len(args) != 1 || args[0] != 99 {
		t.Fatalf("unexpected args: %v", args)
	}
}

func TestParseNormalSQLDateBetweenAndLike(t *testing.T) {
	sql, args, err := ParseNormalSQL(
		`SELECT id FROM claims
		 WHERE JSON_DATE(data, 'service_date') BETWEEN :from_date AND :to_date
		   AND JSON_TEXT(data, 'status') LIKE :status_like
		   AND JSON_TSTZ(data, 'submitted_at') >= :submitted_after`,
		map[string]any{
			"from_date":       "2026-01-01",
			"to_date":         "2026-12-31",
			"status_like":     "app%",
			"submitted_after": "2026-01-01T00:00:00Z",
		},
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(sql, "(jsonb_extract_path_text(data, 'service_date'))::date BETWEEN $1 AND $2") {
		t.Fatalf("expected JSON_DATE + BETWEEN rewrite, got: %s", sql)
	}
	if !strings.Contains(sql, "jsonb_extract_path_text(data, 'status') LIKE $3") {
		t.Fatalf("expected LIKE with JSON_TEXT, got: %s", sql)
	}
	if !strings.Contains(sql, "(jsonb_extract_path_text(data, 'submitted_at'))::timestamptz >= $4") {
		t.Fatalf("expected JSON_TSTZ rewrite, got: %s", sql)
	}
	if len(args) != 4 {
		t.Fatalf("expected 4 args, got %d (%v)", len(args), args)
	}
}

func TestParseNormalSQLJSONNumEquivalentToJSONTextNumericCast(t *testing.T) {
	q1 := `SELECT JSON_NUM(c.data, 'amount') AS amount_num FROM claims c WHERE c.tenant_id = :tenant_id`
	q2 := `SELECT JSON_TEXT(c.data, 'amount')::numeric AS amount_num FROM claims c WHERE c.tenant_id = :tenant_id`
	vars := map[string]any{"tenant_id": 123}

	sql1, args1, err := ParseNormalSQL(q1, vars)
	if err != nil {
		t.Fatalf("unexpected err for q1: %v", err)
	}
	sql2, args2, err := ParseNormalSQL(q2, vars)
	if err != nil {
		t.Fatalf("unexpected err for q2: %v", err)
	}

	normalize := func(s string) string {
		return strings.TrimSpace(regexp.MustCompile(`\s+`).ReplaceAllString(s, " "))
	}
	n1 := normalize(sql1)
	n2 := normalize(sql2)

	if !strings.Contains(n1, "(jsonb_extract_path_text(c.data, 'amount'))::numeric") {
		t.Fatalf("expected JSON_NUM rewrite to numeric cast, got: %s", sql1)
	}
	if !strings.Contains(n2, "jsonb_extract_path_text(c.data, 'amount')::numeric") &&
		!strings.Contains(n2, "(jsonb_extract_path_text(c.data, 'amount'))::numeric") {
		t.Fatalf("expected JSON_TEXT::numeric rewrite, got: %s", sql2)
	}

	if !reflect.DeepEqual(args1, args2) || len(args1) != 1 || args1[0] != 123 {
		t.Fatalf("expected identical args, got q1=%v q2=%v", args1, args2)
	}
}

func TestParseNormalSQLDotNotationRewrite(t *testing.T) {
	sql, args, err := ParseNormalSQL(
		`SELECT pt.data.name AS patient_name, c.data.procedure.code AS proc_code
		 FROM claims c JOIN patients pt ON pt.id = c.patient_id
		 WHERE c.data.amount::numeric BETWEEN :min_amt AND :max_amt
		   AND pt.data.name LIKE :name_like`,
		map[string]any{
			"min_amt":   10,
			"max_amt":   200,
			"name_like": "J%",
		},
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(sql, "jsonb_extract_path_text(pt.data, 'name') AS patient_name") {
		t.Fatalf("expected pt.data.name rewrite, got: %s", sql)
	}
	if !strings.Contains(sql, "jsonb_extract_path_text(c.data, 'procedure', 'code') AS proc_code") {
		t.Fatalf("expected nested dot rewrite, got: %s", sql)
	}
	if !strings.Contains(sql, "jsonb_extract_path_text(c.data, 'amount')::numeric BETWEEN $1 AND $2") {
		t.Fatalf("expected numeric cast + between rewrite, got: %s", sql)
	}
	if !strings.Contains(sql, "jsonb_extract_path_text(pt.data, 'name') LIKE $3") {
		t.Fatalf("expected like rewrite, got: %s", sql)
	}
	if len(args) != 3 {
		t.Fatalf("expected 3 args, got %d (%v)", len(args), args)
	}
}

func TestParseNormalSQLDoesNotRewriteRegularThreePartIdentifier(t *testing.T) {
	sql, _, err := ParseNormalSQL(`SELECT public.claims.id FROM public.claims WHERE public.claims.id = :id`, map[string]any{"id": 1})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if strings.Contains(sql, "jsonb_extract_path_text(") {
		t.Fatalf("did not expect json dot rewrite for regular identifiers, got: %s", sql)
	}
}

func TestParseNormalSQLImplicitNumericCastsForDotNotation(t *testing.T) {
	sql, _, err := ParseNormalSQL(
		`SELECT COALESCE(SUM(c.data.amount), 0) AS total
		 FROM claims c
		 WHERE :paid_total >= c.data.amount
		   AND c.data.status = 'approved'`,
		map[string]any{"paid_total": 100},
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(sql, "SUM((jsonb_extract_path_text(c.data, 'amount'))::numeric)") {
		t.Fatalf("expected implicit numeric cast in SUM, got: %s", sql)
	}
	if !strings.Contains(sql, "$1 >= (jsonb_extract_path_text(c.data, 'amount'))::numeric") {
		t.Fatalf("expected implicit numeric cast in comparison, got: %s", sql)
	}
	if !strings.Contains(sql, "jsonb_extract_path_text(c.data, 'status') = 'approved'") {
		t.Fatalf("expected text equality to remain text, got: %s", sql)
	}
}

func TestParseNormalSQLImplicitNumericCastsForAliases(t *testing.T) {
	sql, _, err := ParseNormalSQL(
		`WITH x AS (
			SELECT c.data.amount AS claim_amount
			FROM claims c
		)
		SELECT COALESCE(SUM(x.claim_amount), 0) AS billed_total
		FROM x`,
		map[string]any{},
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(sql, "SUM((x.claim_amount)::numeric)") {
		t.Fatalf("expected alias numeric cast inference, got: %s", sql)
	}
}

func TestParseNormalSQLEncryptedSearchRewriteNamedParam(t *testing.T) {
	email := "john.carter@demo.test"
	idx, err := blindIndexWithKey(email, "demo-hmac-key")
	if err != nil {
		t.Fatalf("unexpected blind-index err: %v", err)
	}

	sql, args, err := ParseNormalSQLWithOptions(
		`SELECT id FROM patients p WHERE p.tenant_id = :tenant_id AND p.data.email = :email`,
		map[string]any{
			"tenant_id": int64(77),
			"email":     email,
		},
		SQLParseOptions{
			JSONColumns:          []string{"data"},
			EncryptedSearchPaths: map[string]bool{"email": true},
			EncryptedHMACKey:     "demo-hmac-key",
		},
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(sql, "jsonb_extract_path_text(p.data, '_secure_idx', 'email') = '"+idx+"'") {
		t.Fatalf("expected encrypted blind-index rewrite, got: %s", sql)
	}
	if len(args) != 1 || args[0] != int64(77) {
		t.Fatalf("unexpected args, expected only tenant_id: %v", args)
	}
}

func TestParseNormalSQLEncryptedSearchRewriteReversedEquality(t *testing.T) {
	email := "john.carter@demo.test"
	idx, err := blindIndexWithKey(email, "demo-hmac-key")
	if err != nil {
		t.Fatalf("unexpected blind-index err: %v", err)
	}

	sql, args, err := ParseNormalSQLWithOptions(
		`SELECT id FROM patients p WHERE :email = p.data.email AND p.tenant_id = :tenant_id`,
		map[string]any{
			"tenant_id": int64(77),
			"email":     email,
		},
		SQLParseOptions{
			JSONColumns:          []string{"data"},
			EncryptedSearchPaths: map[string]bool{"email": true},
			EncryptedHMACKey:     "demo-hmac-key",
		},
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(sql, "'"+idx+"' = jsonb_extract_path_text(p.data, '_secure_idx', 'email')") {
		t.Fatalf("expected encrypted blind-index rewrite in reversed equality, got: %s", sql)
	}
	if len(args) != 1 || args[0] != int64(77) {
		t.Fatalf("unexpected args, expected only tenant_id: %v", args)
	}
}
