package reflectx

import (
	"reflect"
	"testing"
)

type typeMapBenchRow struct {
	ID      int64  `db:"id"`
	Name    string `db:"name"`
	Payload []byte `db:"payload"`
}

func BenchmarkTypeMapCachedLookupParallel(b *testing.B) {
	mapper := NewMapper("db")
	typ := reflect.TypeOf(typeMapBenchRow{})
	mapper.TypeMap(typ)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = mapper.TypeMap(typ)
		}
	})
}
