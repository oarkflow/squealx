package jsonbq

import (
	"reflect"
	"testing"
	"time"
)

func TestExprHelpersSQL(t *testing.T) {
	expr := Coalesce(
		Col("p.data").TextAt("provider_name"),
		Col("p.id").Cast("text"),
	).As("provider")

	sql, args := SQL(expr, "data")
	wantSQL := "COALESCE((p.data)->>'provider_name', (p.id)::text) AS provider"
	if sql != wantSQL {
		t.Fatalf("unexpected SQL:\nwant: %s\ngot:  %s", wantSQL, sql)
	}
	if len(args) != 0 {
		t.Fatalf("expected no args, got %v", args)
	}
}

func TestCaseWhenAggregateSQL(t *testing.T) {
	chargeText := Col("c.data").TextAt("charge_amount")
	expr := Coalesce(
		Sum(CaseWhen(
			chargeText.Regex("^-?[0-9]+(\\.[0-9]+)?$"),
			chargeText.Cast("numeric"),
			Val(0),
		)),
		Val(0),
	).Cast("numeric(12,2)")

	sql, args := SQL(expr, "data")
	wantSQL := "(COALESCE(SUM(CASE WHEN (c.data)->>'charge_amount' ~ $1 THEN ((c.data)->>'charge_amount')::numeric ELSE $2 END), $3))::numeric(12,2)"
	if sql != wantSQL {
		t.Fatalf("unexpected SQL:\nwant: %s\ngot:  %s", wantSQL, sql)
	}
	wantArgs := []any{"^-?[0-9]+(\\.[0-9]+)?$", 0, 0}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("unexpected args:\nwant: %#v\ngot:  %#v", wantArgs, args)
	}
}

func TestSelectQueryWithStructuredJoinsAndExprs(t *testing.T) {
	createdAfter := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	totalExpr := Sum(Val(1))

	q := (&SelectQuery{columnName: "data"}).
		SelectExpr(
			CountAll().As("cnt"),
			totalExpr.As("total"),
		).
		From("a").
		InnerJoin("b",
			Col("b.tenant_id").EqExpr(Col("a.tenant_id")),
			Col("b.registry_item_id").EqExpr(Col("a.registry_item_id")),
		).
		LeftJoin("entries e",
			Col("e.tenant_id").EqExpr(Col("b.tenant_id")),
		).
		Where(
			Col("a.tenant_id").Eq(int64(1)),
			Col("e.created_at").Gte(createdAfter),
		).
		GroupByExpr(Col("a.tenant_id")).
		OrderByExprDesc(totalExpr).
		Limit(10)

	sql, args := q.Build()
	wantSQL := "SELECT COUNT(*) AS cnt, SUM($1) AS total FROM a JOIN b ON b.tenant_id = a.tenant_id AND b.registry_item_id = a.registry_item_id LEFT JOIN entries e ON e.tenant_id = b.tenant_id WHERE a.tenant_id = $2 AND e.created_at >= $3 GROUP BY a.tenant_id ORDER BY SUM($4) DESC LIMIT $5"
	if sql != wantSQL {
		t.Fatalf("unexpected SQL:\nwant: %s\ngot:  %s", wantSQL, sql)
	}
	if len(args) != 5 {
		t.Fatalf("unexpected args len: got %d args=%v", len(args), args)
	}
}

func TestCaseWhenNilConditionAndNilExprs(t *testing.T) {
	expr := CaseWhen(nil, Expr{}, Expr{})
	sql, args := SQL(expr, "data")
	wantSQL := "CASE WHEN FALSE THEN NULL ELSE NULL END"
	if sql != wantSQL {
		t.Fatalf("unexpected SQL:\nwant: %s\ngot:  %s", wantSQL, sql)
	}
	if len(args) != 0 {
		t.Fatalf("expected no args, got %v", args)
	}
}

func TestCoalesceEmptyExprs(t *testing.T) {
	sql, args := SQL(Coalesce(), "data")
	if sql != "NULL" {
		t.Fatalf("unexpected SQL: %s", sql)
	}
	if len(args) != 0 {
		t.Fatalf("expected no args, got %v", args)
	}
}

func TestSelectQueryIgnoresNilConditions(t *testing.T) {
	q := (&SelectQuery{columnName: "data"}).
		SelectExpr(CountAll().As("cnt")).
		From("a").
		InnerJoin("b",
			nil,
			Col("b.id").EqExpr(Col("a.id")),
		).
		Where(
			nil,
			Col("a.tenant_id").Eq(int64(7)),
		).
		Having(nil)

	sql, args := q.Build()
	wantSQL := "SELECT COUNT(*) AS cnt FROM a JOIN b ON b.id = a.id WHERE a.tenant_id = $1"
	if sql != wantSQL {
		t.Fatalf("unexpected SQL:\nwant: %s\ngot:  %s", wantSQL, sql)
	}
	wantArgs := []any{int64(7)}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("unexpected args:\nwant: %#v\ngot:  %#v", wantArgs, args)
	}
}
