package squealx

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
)

var namedCacheQueries = []string{
	"",
	"SELECT 1",
	"SELECT * FROM t WHERE a = :a AND b = :b",
	"SELECT :a::int, :b_c.d",
	"SELECT 'x :no' , :yes",
	"SELECT 1 -- :no\n, :yes",
	"/* :no /* nested :no */ :no */ SELECT :yes",
	"SELECT $$ :no $$, :yes",
	`SELECT "a:b", :yes`,
	"SELECT :é, :名前",
	"SELECT :a",
	"UPDATE t SET a=:a, b=:b, c=:c WHERE id=:id",
	"SELECT x::text FROM t",
	"SELECT :a:b",
}

var namedCacheBinds = []int{QUESTION, DOLLAR, NAMED, AT, UNKNOWN}

func TestCompileNamedCachedMatchesUncached(t *testing.T) {
	for _, q := range namedCacheQueries {
		for _, b := range namedCacheBinds {
			wb, wn, werr := compileNamedQuery(q, b)
			for i := 0; i < 2; i++ { // miss then hit
				gb, gn, gerr := compileNamedQueryCached(q, b)
				if gb != wb || !reflect.DeepEqual(gn, wn) || (gerr == nil) != (werr == nil) {
					t.Fatalf("q=%q bind=%d: got (%q,%v,%v) want (%q,%v,%v)", q, b, gb, gn, gerr, wb, wn, werr)
				}
			}
		}
	}
}

func TestCompileNamedKnownOutputs(t *testing.T) {
	b, n, _ := compileNamedQuery("SELECT :a::int, ':x', :b.c", DOLLAR)
	if b != "SELECT $1::int, ':x', $2" || !reflect.DeepEqual(n, []string{"a", "b.c"}) {
		t.Fatalf("got %q %v", b, n)
	}
	b, n, _ = compileNamedQuery("x=:é AND y=:z", AT)
	if b != "x=@p1 AND y=@p2" || !reflect.DeepEqual(n, []string{"é", "z"}) {
		t.Fatalf("got %q %v", b, n)
	}
}

func TestCompileNamedCachedConcurrent(t *testing.T) {
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				q := fmt.Sprintf("SELECT :a, :b -- %d", (i+g)%50)
				b, n, err := compileNamedQueryCached(q, DOLLAR)
				if err != nil || !strings.HasPrefix(b, "SELECT $1, $2") || len(n) != 2 {
					t.Errorf("bad result %q %v %v", b, n, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}

func TestCompileNamedCacheBounded(t *testing.T) {
	for i := 0; i < namedCacheMaxEntries*3; i++ {
		compileNamedQueryCached(fmt.Sprintf("SELECT :a /* %d */", i), QUESTION)
	}
	namedCache.RLock()
	n := len(namedCache.m)
	namedCache.RUnlock()
	if n > namedCacheMaxEntries {
		t.Fatalf("cache size %d exceeds cap", n)
	}
	long := "SELECT :a /*" + strings.Repeat("x", namedCacheMaxQueryLen) + "*/"
	compileNamedQueryCached(long, QUESTION)
	namedCache.RLock()
	_, ok := namedCache.m[namedCacheKey{long, QUESTION}]
	namedCache.RUnlock()
	if ok {
		t.Fatal("long query should not be cached")
	}
}

func TestIsNamedQueryTricky(t *testing.T) {
	cases := map[string]bool{
		"":                  false,
		"SELECT 1":          false,
		"SELECT x::int":     false,
		"SELECT :a":         true,
		"SELECT :_a":        true,
		"SELECT 'a :b'":     false,
		"SELECT 1 -- :b":    false,
		"SELECT /* :b */ 1": false,
		"SELECT $$ :b $$":   false,
		`SELECT "a:b"`:      false,
		"SELECT a::int, :b": true,
		"SELECT :1":         false,
		"SELECT 1:":         false,
		"SELECT :é":         true,
		"SELECT '12:30:45'": false,
	}
	for q, want := range cases {
		if got := IsNamedQuery(q); got != want {
			t.Errorf("IsNamedQuery(%q)=%v want %v", q, got, want)
		}
	}
}

const benchNamedQuery = "SELECT id, name FROM users WHERE id = :id AND name = :name AND created > :created -- :c\n"

func BenchmarkCompileNamedUncached(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		compileNamedQuery(benchNamedQuery, QUESTION)
	}
}

func BenchmarkCompileNamedCached(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		compileNamedQueryCached(benchNamedQuery, QUESTION)
	}
}

func BenchmarkIsNamedQuery(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		IsNamedQuery(benchNamedQuery)
		IsNamedQuery("SELECT * FROM users WHERE id = 1")
	}
}
