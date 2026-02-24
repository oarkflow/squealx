package jsonbq

import "testing"

func TestLexSQLPlaceholderAndStrings(t *testing.T) {
	query := "SELECT '{{x}}' a, {{x}} b, $$ {{x}} $$ c"
	tokens, err := LexSQL(query)
	if err != nil {
		t.Fatalf("lex err: %v", err)
	}
	placeholders := 0
	for _, tok := range tokens {
		if tok.Type == SQLTokenPlaceholder {
			placeholders++
		}
	}
	if placeholders != 1 {
		t.Fatalf("expected 1 placeholder token, got %d", placeholders)
	}
}

func TestParseSQLSelectClauses(t *testing.T) {
	stmt, err := ParseSQL(`SELECT a FROM t WHERE x = 1 GROUP BY a HAVING COUNT(*) > 1 ORDER BY a LIMIT 10 OFFSET 5`)
	if err != nil {
		t.Fatalf("parse err: %v", err)
	}
	if stmt.Kind != StatementSelect {
		t.Fatalf("expected select, got %s", stmt.Kind)
	}
	if len(stmt.Clauses) < 6 {
		t.Fatalf("expected multiple clauses, got %d", len(stmt.Clauses))
	}
	if stmt.AST == nil {
		t.Fatalf("expected AST to be populated")
	}
	if len(stmt.AST.SelectItems) != 1 {
		t.Fatalf("expected 1 select item, got %d", len(stmt.AST.SelectItems))
	}
	if stmt.AST.Where == nil || stmt.AST.Having == nil {
		t.Fatalf("expected where/having expressions in AST")
	}
	if len(stmt.AST.GroupBy) != 1 || len(stmt.AST.OrderBy) != 1 {
		t.Fatalf("expected group/order expressions in AST")
	}
	if stmt.AST.Limit == nil || stmt.AST.Offset == nil {
		t.Fatalf("expected limit/offset expressions in AST")
	}
}

func TestParseSQLKinds(t *testing.T) {
	cases := map[string]StatementKind{
		"INSERT INTO t(a) VALUES(1)":  StatementInsert,
		"UPDATE t SET a=1 WHERE id=1": StatementUpdate,
		"DELETE FROM t WHERE id=1":    StatementDelete,
	}
	for sql, want := range cases {
		stmt, err := ParseSQL(sql)
		if err != nil {
			t.Fatalf("parse err: %v", err)
		}
		if stmt.Kind != want {
			t.Fatalf("sql=%q want=%s got=%s", sql, want, stmt.Kind)
		}
	}
}

func TestParseSQLUpdateASTAssignments(t *testing.T) {
	stmt, err := ParseSQL(`UPDATE t SET a = b + 1, c = COALESCE(d, 0) WHERE id = 1 RETURNING a`)
	if err != nil {
		t.Fatalf("parse err: %v", err)
	}
	if stmt.AST == nil {
		t.Fatalf("expected ast")
	}
	if len(stmt.AST.Assignments) != 2 {
		t.Fatalf("expected 2 assignments, got %d", len(stmt.AST.Assignments))
	}
	if stmt.AST.Where == nil {
		t.Fatalf("expected where expression")
	}
	if len(stmt.AST.Returning) != 1 {
		t.Fatalf("expected 1 returning expression, got %d", len(stmt.AST.Returning))
	}
}

func TestParseSQLInsertValuesAST(t *testing.T) {
	stmt, err := ParseSQL(`INSERT INTO t(a,b) VALUES (1, 2), (3, 4) RETURNING a`)
	if err != nil {
		t.Fatalf("parse err: %v", err)
	}
	if stmt.AST == nil {
		t.Fatalf("expected ast")
	}
	if len(stmt.AST.Values) != 2 {
		t.Fatalf("expected 2 value rows, got %d", len(stmt.AST.Values))
	}
	if len(stmt.AST.Values[0]) != 2 || len(stmt.AST.Values[1]) != 2 {
		t.Fatalf("expected 2 values per row")
	}
	if len(stmt.AST.Returning) != 1 {
		t.Fatalf("expected returning expression")
	}
}
