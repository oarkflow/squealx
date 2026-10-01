package squealx

import (
	"errors"
	"testing"
	"time"
)

func TestAppendJSONValue(t *testing.T) {
	ts := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	cases := []struct {
		in   any
		want string
	}{
		{nil, "null"},
		{[]byte("hi"), `"hi"`},
		{[]byte{0xff, 0xfe}, `"//4="`},
		{int64(7), "7"},
		{1.5, "1.5"},
		{true, "true"},
		{ts, `"2024-01-02T03:04:05Z"`},
		{`a"b`, `"a\"b"`},
	}
	for _, c := range cases {
		b, err := appendJSONValue(nil, c.in)
		if err != nil || string(b) != c.want {
			t.Errorf("%v: got %s err %v want %s", c.in, b, err, c.want)
		}
	}
}

func TestCSVField(t *testing.T) {
	if s, _ := csvField(nil); s != "" {
		t.Fatal("nil should be empty")
	}
	if s, _ := csvField([]byte{0xff}); s != "/w==" {
		t.Fatalf("got %q", s)
	}
	if s, _ := csvField(int64(3)); s != "3" {
		t.Fatalf("got %q", s)
	}
}

func TestLimitedBytes(t *testing.T) {
	l := LimitedBytes{Max: 3}
	if err := l.Scan([]byte("abc")); err != nil || string(l.Data) != "abc" {
		t.Fatal(err)
	}
	if err := l.Scan([]byte("abcd")); !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("got %v", err)
	}
	if err := l.Scan("abcd"); !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("got %v", err)
	}
	if err := l.Scan(nil); err != nil || l.Data != nil {
		t.Fatal("nil")
	}
	u := LimitedBytes{}
	if err := u.Scan([]byte("abcdef")); err != nil {
		t.Fatal(err)
	}
}
