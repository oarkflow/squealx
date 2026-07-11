package squealx

import "testing"

func TestLimitQueryLexicalSafety(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  string
	}{
		{"simple", "SELECT * FROM users;", "SELECT * FROM users LIMIT 1"},
		{"replace outer", "SELECT * FROM users LIMIT 20 OFFSET 5", "SELECT * FROM users LIMIT 1"},
		{"ignore string", "SELECT ' limit 99 ' AS note FROM users", "SELECT ' limit 99 ' AS note FROM users LIMIT 1"},
		{"ignore subquery", "SELECT * FROM (SELECT * FROM users LIMIT 9) u", "SELECT * FROM (SELECT * FROM users LIMIT 9) u LIMIT 1"},
		{"ignore comment", "SELECT * FROM users /* LIMIT 9 */", "SELECT * FROM users /* LIMIT 9 */ LIMIT 1"},
		{"ignore dollar quote", "SELECT $$ LIMIT 9 $$ AS body", "SELECT $$ LIMIT 9 $$ AS body LIMIT 1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := LimitQuery(tt.query); got != tt.want {
				t.Fatalf("got %q want %q", got, tt.want)
			}
		})
	}
}

func TestLimitQueryForDriver(t *testing.T) {
	if got := LimitQueryForDriver("mssql", "SELECT DISTINCT id FROM users"); got != "SELECT DISTINCT TOP (1) id FROM users" {
		t.Fatalf("mssql got %q", got)
	}
	if got := LimitQueryForDriver("oracle", "SELECT id FROM users"); got != "SELECT id FROM users FETCH FIRST 1 ROWS ONLY" {
		t.Fatalf("oracle got %q", got)
	}
}

func TestWithReturningLexicalSafety(t *testing.T) {
	if got := WithReturning("INSERT INTO logs(note) VALUES ('returning id');"); got != "INSERT INTO logs(note) VALUES ('returning id') RETURNING *" {
		t.Fatalf("got %q", got)
	}
	if got := WithReturning("UPDATE users SET active=true RETURNING id"); got != "UPDATE users SET active=true RETURNING *" {
		t.Fatalf("got %q", got)
	}
}

func TestSanitizeQueryReturnsTemplateError(t *testing.T) {
	if _, err := SanitizeQuery("SELECT {{ missing }}", map[string]any{}); err == nil {
		t.Fatal("expected template render error")
	}
}

func TestInIgnoresQuestionMarksInSQLText(t *testing.T) {
	query := "SELECT '?' AS literal, id FROM users /* ? */ WHERE id IN (?) -- ?\n AND active=?"
	got, args, err := In(query, []int{1, 2, 3}, true)
	if err != nil {
		t.Fatal(err)
	}
	want := "SELECT '?' AS literal, id FROM users /* ? */ WHERE id IN (?, ?, ?) -- ?\n AND active=?"
	if got != want {
		t.Fatalf("query:\n got: %s\nwant: %s", got, want)
	}
	if len(args) != 4 || args[0] != 1 || args[3] != true {
		t.Fatalf("args=%#v", args)
	}
}
