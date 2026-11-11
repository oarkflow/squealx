package reflectx

import (
	"reflect"
	"strconv"
	"strings"
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

func TestTraversalsByNameCachedSkipsOversizedShapes(t *testing.T) {
	m := NewMapperFunc("db", func(s string) string { return s })
	typ := reflect.TypeOf(tcRow{})
	cases := [][]string{
		make([]string, maxCachedTraversalColumns+1),
		{strings.Repeat("x", maxCachedTraversalNameBytes+1)},
	}
	for _, names := range cases {
		before := m.tcount.Load()
		got := m.TraversalsByNameCached(typ, names)
		want := m.TraversalsByName(typ, names)
		if !reflect.DeepEqual(got, want) {
			t.Fatal("uncached traversal result differs from TraversalsByName")
		}
		if after := m.tcount.Load(); after != before {
			t.Fatalf("oversized shape entered cache: count changed from %d to %d", before, after)
		}
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
	if n := m.tcount.Load(); n > maxCachedTraversals {
		t.Fatalf("cache not bounded: %d", n)
	}
}

func TestTraversalsByNameCacheAggregateByteBudget(t *testing.T) {
	m := NewMapperFunc("db", func(s string) string { return s })
	typ := reflect.TypeOf(tcRow{})
	const shapes = 400
	for shape := 0; shape < shapes; shape++ {
		names := make([]string, 400)
		for column := range names {
			names[column] = "column_" + strconv.Itoa(shape) + "_" + strconv.Itoa(column)
		}
		m.TraversalsByNameCached(typ, names)
	}
	if got := m.tbytes; got > maxCachedTraversalBytes {
		t.Fatalf("traversal cache retained estimated %d bytes, limit %d", got, maxCachedTraversalBytes)
	}
	if got := m.tcount.Load(); got >= shapes {
		t.Fatalf("aggregate byte limit did not evict entries: retained %d of %d", got, shapes)
	}
}

func TestTypeMapCacheBoundedForDynamicStructTypes(t *testing.T) {
	m := NewMapper("db")
	var firstMapping *StructMap
	for i := 0; i < maxCachedStructTypes+8; i++ {
		typ := reflect.StructOf([]reflect.StructField{{
			Name: "Field" + strconv.Itoa(i),
			Type: reflect.TypeOf(int(0)),
		}})
		mapping := m.TypeMap(typ)
		if i == 0 {
			firstMapping = mapping
		}
	}
	if len(m.cache) > maxCachedStructTypes {
		t.Fatalf("struct type cache retained %d entries, limit %d", len(m.cache), maxCachedStructTypes)
	}
	if firstMapping == nil || firstMapping.Names["Field0"] == nil || firstMapping.Names["Field0"].Field.Type != reflect.TypeOf(int(0)) {
		t.Fatal("clearing the cache invalidated a mapping already returned to a caller")
	}
}

func TestTypeMapCacheAggregateByteBudget(t *testing.T) {
	m := NewMapper("db")
	const types = 300
	for n := 0; n < types; n++ {
		fields := make([]reflect.StructField, 128)
		for i := range fields {
			fields[i] = reflect.StructField{
				Name: "Field" + strconv.Itoa(i),
				Type: reflect.TypeOf(int(0)),
			}
		}
		typ := reflect.StructOf(fields)
		m.TypeMap(typ)
	}
	if m.mapBytes > maxCachedStructMapBytes {
		t.Fatalf("type map cache retained estimated %d bytes, limit %d", m.mapBytes, maxCachedStructMapBytes)
	}
	if len(m.cache) >= types {
		t.Fatalf("aggregate byte limit did not evict mappings: retained %d of %d", len(m.cache), types)
	}
}

func TestTypeMapSkipsOversizedStructShapes(t *testing.T) {
	fields := make([]reflect.StructField, maxCachedStructMapFields+1)
	for i := range fields {
		fields[i] = reflect.StructField{Name: "Field" + strconv.Itoa(i), Type: reflect.TypeOf(int(0))}
	}
	typ := reflect.StructOf(fields)
	m := NewMapper("db")
	first := m.TypeMap(typ)
	second := m.TypeMap(typ)
	if len(m.cache) != 0 {
		t.Fatalf("oversized struct mapping entered cache: %d entries", len(m.cache))
	}
	if first == nil || second == nil || first == second {
		t.Fatal("oversized struct mapping was not returned without memoization")
	}
}
