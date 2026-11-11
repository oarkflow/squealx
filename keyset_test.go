package squealx

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

type keysetCursorMarshaler struct{ called *bool }

func (v keysetCursorMarshaler) MarshalJSON() ([]byte, error) {
	*v.called = true
	return []byte(`"value"`), nil
}

func TestKeysetCursorRoundTrip(t *testing.T) {
	in := []any{int64(42), "héllo", 1.5, true}
	enc, err := EncodeKeysetCursor(in)
	if err != nil {
		t.Fatal(err)
	}
	if strings.ContainsAny(enc, "+/=") {
		t.Fatalf("not base64url: %s", enc)
	}
	out, err := DecodeKeysetCursor(enc)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 4 || out[0] != int64(42) || out[1] != "héllo" || out[2] != 1.5 || out[3] != true {
		t.Fatalf("unexpected %#v", out)
	}
}

func TestKeysetCursorMalformed(t *testing.T) {
	for _, bad := range []string{"!!!", "e30", "W3t9XQ", "W1sxXV0", "WzFdIHg"} {
		if _, err := DecodeKeysetCursor(bad); err == nil {
			t.Errorf("expected error for %q", bad)
		}
	}
	for _, raw := range []string{"[1] [2]", "[1] trailing"} {
		bad := base64.RawURLEncoding.EncodeToString([]byte(raw))
		if _, err := DecodeKeysetCursor(bad); !errors.Is(err, ErrInvalidKeysetCursor) {
			t.Errorf("expected invalid cursor for %q, got %v", raw, err)
		}
	}
	valid := base64.RawURLEncoding.EncodeToString([]byte("[1]  \n\t"))
	if got, err := DecodeKeysetCursor(valid); err != nil || len(got) != 1 || got[0] != int64(1) {
		t.Errorf("valid trailing whitespace rejected: %#v, %v", got, err)
	}
}

func TestKeysetCursorSizeLimit(t *testing.T) {
	oversized := strings.Repeat("A", maxKeysetCursorEncodedSize+1)
	if _, err := DecodeKeysetCursor(oversized); !errors.Is(err, ErrInvalidKeysetCursor) {
		t.Fatalf("oversized cursor decode error = %v", err)
	}
	if _, err := EncodeKeysetCursor([]any{strings.Repeat("x", maxKeysetCursorEncodedSize)}); !errors.Is(err, ErrInvalidKeysetCursor) {
		t.Fatalf("oversized cursor encode error = %v", err)
	}
}

func TestKeysetCursorEncodePreflightsPayload(t *testing.T) {
	maxString := strings.Repeat("x", maxKeysetCursorJSONSize-4)
	cursor, err := EncodeKeysetCursor([]any{maxString})
	if err != nil {
		t.Fatalf("maximum-sized scalar cursor rejected: %v", err)
	}
	if len(cursor) != maxKeysetCursorEncodedSize {
		t.Fatalf("encoded size = %d, want %d", len(cursor), maxKeysetCursorEncodedSize)
	}
	if _, err := DecodeKeysetCursor(cursor); err != nil {
		t.Fatalf("maximum-sized cursor did not decode: %v", err)
	}

	for _, value := range []any{
		strings.Repeat("x", maxKeysetCursorJSONSize),
		[]byte(strings.Repeat("x", maxKeysetCursorJSONSize)),
		map[string]any{"key": strings.Repeat("x", maxKeysetCursorJSONSize)},
	} {
		if _, err := EncodeKeysetCursor([]any{value}); !errors.Is(err, ErrInvalidKeysetCursor) {
			t.Errorf("oversized or composite value %T error = %v, want ErrInvalidKeysetCursor", value, err)
		}
	}
	marshalerCalled := false
	if _, err := EncodeKeysetCursor([]any{keysetCursorMarshaler{called: &marshalerCalled}}); !errors.Is(err, ErrInvalidKeysetCursor) {
		t.Fatalf("custom marshaler error = %v, want ErrInvalidKeysetCursor", err)
	}
	if marshalerCalled {
		t.Fatal("custom marshaler ran before payload size and type validation")
	}

	for _, value := range []string{"<>&", "quote: \" slash: \\ newline:\n", "\u2028\u2029", string([]byte{0xff, 'x'})} {
		got := keysetJSONStringSize(value)
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if got != len(encoded) {
			t.Errorf("escaped size for %q = %d, json.Marshal size = %d", value, got, len(encoded))
		}
	}
}

func FuzzDecodeKeysetCursor(f *testing.F) {
	f.Add("")
	f.Add("###")
	f.Add(base64.RawURLEncoding.EncodeToString([]byte(`[1,"item",true]`)))
	f.Add(strings.Repeat("A", maxKeysetCursorEncodedSize+1))
	f.Fuzz(func(t *testing.T, cursor string) {
		values, err := DecodeKeysetCursor(cursor)
		if err != nil {
			return
		}
		encoded, err := EncodeKeysetCursor(values)
		if err != nil {
			t.Fatalf("decoded cursor could not be encoded: %v", err)
		}
		if _, err := DecodeKeysetCursor(encoded); err != nil {
			t.Fatalf("encoded decoded cursor could not be decoded: %v", err)
		}
	})
}

func TestBuildKeysetSQL(t *testing.T) {
	asc := []KeyColumn{{Name: "a"}, {Name: "b"}}
	mixed := []KeyColumn{{Name: "a"}, {Name: "t.b", Desc: true}}
	cases := []struct {
		driver string
		keys   []KeyColumn
		cursor bool
		want   string
	}{
		{"postgres", asc, true, "SELECT * FROM (SELECT * FROM t) AS keyset_q WHERE (a, b) > (:ks_after_0, :ks_after_1) ORDER BY a ASC, b ASC LIMIT 11"},
		{"sqlite3", asc, false, "SELECT * FROM (SELECT * FROM t) AS keyset_q ORDER BY a ASC, b ASC LIMIT 11"},
		{"mysql", []KeyColumn{{Name: "a", Desc: true}}, true, "SELECT * FROM (SELECT * FROM t) AS keyset_q WHERE a < :ks_after_0 ORDER BY a DESC LIMIT 11"},
		{"sqlserver", asc, true, "SELECT TOP (11) * FROM (SELECT * FROM t) AS keyset_q WHERE ((a > :ks_after_0) OR (a = :ks_after_0 AND b > :ks_after_1)) ORDER BY a ASC, b ASC"},
		{"postgres", mixed, true, "SELECT * FROM (SELECT * FROM t) AS keyset_q WHERE ((a > :ks_after_0) OR (a = :ks_after_0 AND b < :ks_after_1)) ORDER BY a ASC, b DESC LIMIT 11"},
		{"oracle", asc, false, "SELECT * FROM (SELECT * FROM t) keyset_q ORDER BY a ASC, b ASC FETCH FIRST 11 ROWS ONLY"},
	}
	for _, c := range cases {
		got, err := buildKeysetSQL(c.driver, "SELECT * FROM t ORDER BY x LIMIT 5;", c.keys, 10, c.cursor)
		if err != nil {
			t.Fatal(err)
		}
		if got != c.want {
			t.Errorf("%s:\n got %s\nwant %s", c.driver, got, c.want)
		}
	}
}

func TestBuildKeysetSQLRejectsUnsafe(t *testing.T) {
	if _, err := buildKeysetSQL("postgres", "SELECT 1", []KeyColumn{{Name: "a; DROP TABLE x"}}, 1, false); err == nil {
		t.Fatal("expected error")
	}
	if _, err := buildKeysetSQL("postgres", "SELECT 1", nil, 1, false); err == nil {
		t.Fatal("expected error for no keys")
	}
}

func TestPrepareRawQueryLimitStripping(t *testing.T) {
	cases := []struct {
		driver, in, want string
	}{
		{"sqlite3", "select * from t limit 5 offset 2", "select * from t LIMIT :limit OFFSET :offset"},
		{"sqlite3", "SELECT limit_col, 'LIMIT 3' FROM t WHERE id IN (SELECT id FROM u LIMIT 2)", "SELECT limit_col, 'LIMIT 3' FROM t WHERE id IN (SELECT id FROM u LIMIT 2) LIMIT :limit OFFSET :offset"},
		{"mysql", "SELECT * FROM t;", "SELECT * FROM t LIMIT :offset, :limit"},
		{"sqlserver", "SELECT * FROM t", "SELECT * FROM t ORDER BY (SELECT NULL) OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY"},
		{"sqlserver", "SELECT * FROM t ORDER BY id", "SELECT * FROM t ORDER BY id OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY"},
	}
	for _, c := range cases {
		got, err := prepareRawQuery(&DB{driverName: c.driver}, c.in, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got != c.want {
			t.Errorf("%s:\n got %s\nwant %s", c.driver, got, c.want)
		}
	}
	got, _ := prepareRawQuery(&DB{driverName: "postgres"}, "SELECT * FROM t", &Paging{OrderBy: []string{"id desc"}})
	if got != "SELECT * FROM t ORDER BY id desc LIMIT :limit OFFSET :offset" {
		t.Errorf("got %s", got)
	}
}

func TestPagingNormalize(t *testing.T) {
	p := &Paging{Page: 3, Limit: 500, MaxPageLimit: 50}
	page, limit, off, err := p.normalize()
	if err != nil || page != 3 || limit != 50 || off != 100 {
		t.Fatalf("%d %d %d %v", page, limit, off, err)
	}
	if p.Limit != 500 {
		t.Fatal("caller mutated")
	}
	if _, _, _, err := (&Paging{Limit: -1}).normalize(); err == nil {
		t.Fatal("expected error")
	}
	if _, _, _, err := (&Paging{Page: -1}).normalize(); err == nil {
		t.Fatal("expected error")
	}
}
