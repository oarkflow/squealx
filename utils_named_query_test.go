package squealx

import "testing"

func TestIsNamedQueryTrueForNamedParams(t *testing.T) {
	if !IsNamedQuery(`SELECT * FROM patients WHERE tenant_id = :tenant_id AND data.email = :email`) {
		t.Fatalf("expected named query detection to be true")
	}
}

func TestIsNamedQueryFalseForPostgresCasts(t *testing.T) {
	if IsNamedQuery(`SELECT p.data::jsonb FROM patients p WHERE p.id = $1`) {
		t.Fatalf("expected named query detection to ignore postgres casts")
	}
	if IsNamedQuery(`SELECT ARRAY['a']::text[]`) {
		t.Fatalf("expected named query detection to ignore array casts")
	}
}

func TestIsNamedQueryIgnoresCommentsAndLiterals(t *testing.T) {
	query := `
SELECT ':tenant_id' AS literal_text
-- :tenant_id
/* :tenant_id */
FROM t
WHERE id = $1
`
	if IsNamedQuery(query) {
		t.Fatalf("expected named query detection to ignore comment/literal placeholders")
	}
}
