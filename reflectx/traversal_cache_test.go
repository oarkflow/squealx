package reflectx

import (
	"reflect"
	"strconv"
	"sync"
	"testing"
)

type tcInner struct {
	City string `db:"city"`
}

type tcRow struct {
	ID    int64   `db:"id"`
	Name  string  `db:"name"`
	Home  tcInner `db:"home"`
	Other *tcInner
}

func TestTraversalsByNameCachedMatchesUncached(t *testing.T) {
	m := NewMapperFunc("db", func(s string) string { return s })
	typ := reflect.TypeOf(tcRow{})
	sets := [][]string{
		{"id", "name"},
		{"name", "id"},
		{"id", "home.city", "missing"},
		{},
		{"id"},
	}
	for round := 0; round < 3; round++ { // miss, then hits
		for _, cols := range sets {
			got := m.TraversalsByNameCached(typ, cols)
			want := m.TraversalsByName(typ, cols)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("round %d cols %v: got %v want %v", round, cols, got, want)
			}
		}
	}
	// distinct column order must not collide
	a := m.TraversalsByNameCached(typ, []string{"id", "name"})
	b := m.TraversalsByNameCached(typ, []string{"name", "id"})
	if reflect.DeepEqual(a, b) {
		t.Fatal("different column orders returned the same traversals")
	}
}

func TestTraversalsByNameCachedDoesNotAliasCallerSlice(t *testing.T) {
	m := NewMapperFunc("db", func(s string) string { return s })
	typ := reflect.TypeOf(tcRow{})
	cols := []string{"id", "name"}
	first := m.TraversalsByNameCached(typ, cols)
	cols[0], cols[1] = "name", "id" // caller reuses/mutates its slice
	second := m.TraversalsByNameCached(typ, cols)
	if reflect.DeepEqual(first, second) {
		t.Fatal("cache returned stale entry after caller mutated its column slice")
	}
}

func TestTraversalsByNameCachedBoundedAndConcurrent(t *testing.T) {
	m := NewMapperFunc("db", func(s string) string { return s })
	typ := reflect.TypeOf(tcRow{})
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 600; i++ {
				cols := []string{"id", "col" + strconv.Itoa(g*1000+i)}
				got := m.TraversalsByNameCached(typ, cols)
				if len(got) != 2 || len(got[0]) == 0 || len(got[1]) != 0 {
					t.Errorf("bad traversals %v", got)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	if n := m.tcount.Load(); n > maxCachedTraversals+8 {
		t.Fatalf("cache not bounded: %d", n)
	}
}
