package squealx

import "testing"

type dirtyFieldsBenchCase struct {
	ID     int
	Values [256]int
	Data   []byte
}

func BenchmarkMapperLookup(b *testing.B) {
	_ = mapper()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = mapper()
	}
}

func BenchmarkDirtyFieldsLargeStruct(b *testing.B) {
	value := dirtyFieldsBenchCase{ID: 1}
	value.Values[len(value.Values)-1] = 2
	b.ReportAllocs()
	for b.Loop() {
		if _, err := DirtyFields(value); err != nil {
			b.Fatal(err)
		}
	}
}
