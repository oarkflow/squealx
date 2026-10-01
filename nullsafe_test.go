package squealx

import (
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"
)

type fakeUUID [4]byte

func (u *fakeUUID) Scan(src any) error {
	switch v := src.(type) {
	case []byte:
		copy(u[:], v)
	case string:
		copy(u[:], v)
	default:
		return errors.New("bad src")
	}
	return nil
}

type namedInt int32

func scanInto(t *testing.T, dest, src any) {
	t.Helper()
	if err := (&nullSafe{dest: dest}).Scan(src); err != nil {
		t.Fatal(err)
	}
}

func TestNullSafeNull(t *testing.T) {
	s, i, f, b := "x", int64(5), 1.5, true
	tm := time.Now()
	bs := []byte("a")
	p := new(string)
	for _, d := range []any{&s, &i, &f, &b, &tm, &bs, &p} {
		scanInto(t, d, nil)
	}
	if s != "" || i != 0 || f != 0 || b || !tm.IsZero() || bs != nil || p != nil {
		t.Fatal("NULL did not zero values")
	}
}

func TestNullSafeInts(t *testing.T) {
	var i int
	var i8 int8
	var i16 int16
	var i32 int32
	var i64 int64
	var u uint
	var u8 uint8
	var u16 uint16
	var u32 uint32
	var u64 uint64
	var ni namedInt
	for _, d := range []any{&i, &i8, &i16, &i32, &i64, &u, &u8, &u16, &u32, &u64, &ni} {
		scanInto(t, d, int64(42))
	}
	if i != 42 || i8 != 42 || i16 != 42 || i32 != 42 || i64 != 42 || u != 42 || u8 != 42 || u16 != 42 || u32 != 42 || u64 != 42 || ni != 42 {
		t.Fatal("int scan mismatch")
	}
	scanInto(t, &i8, int64(300))
	if i8 != int8(44) {
		t.Fatalf("expected wrap, got %d", i8)
	}
	scanInto(t, &i, "17")
	if i != 17 {
		t.Fatal("string parse")
	}
	scanInto(t, &i, "abc")
	if i != 0 {
		t.Fatal("zero on failure")
	}
}

func TestNullSafeScalars(t *testing.T) {
	var s string
	var f64 float64
	var f32 float32
	var b bool
	var bs []byte
	var tm time.Time
	scanInto(t, &s, "hi")
	scanInto(t, &f64, 2.5)
	scanInto(t, &f32, 2.5)
	scanInto(t, &b, true)
	src := []byte("raw")
	scanInto(t, &bs, src)
	now := time.Now()
	scanInto(t, &tm, now)
	if s != "hi" || f64 != 2.5 || f32 != 2.5 || !b || string(bs) != "raw" || !tm.Equal(now) {
		t.Fatal("scalar mismatch")
	}
	scanInto(t, &s, []byte("bytes"))
	if s != "bytes" {
		t.Fatal("[]byte -> string")
	}
	scanInto(t, &tm, "2024-01-02 03:04:05")
	if tm.Year() != 2024 {
		t.Fatal("time parse")
	}
	var j json.RawMessage
	scanInto(t, &j, []byte(`{"a":1}`))
	if string(j) != `{"a":1}` {
		t.Fatal("raw message")
	}
}

func TestNullSafePointersAndScanners(t *testing.T) {
	var ps *string
	var pi *int64
	scanInto(t, &ps, "v")
	scanInto(t, &pi, int64(9))
	if ps == nil || *ps != "v" || pi == nil || *pi != 9 {
		t.Fatal("pointer alloc")
	}
	var u fakeUUID
	scanInto(t, &u, []byte("abcd"))
	if string(u[:]) != "abcd" {
		t.Fatal("scanner")
	}
	var pu *fakeUUID
	scanInto(t, &pu, []byte("wxyz"))
	if pu == nil || string(pu[:]) != "wxyz" {
		t.Fatal("pointer scanner")
	}
	scanInto(t, &pu, nil)
	if pu != nil {
		t.Fatal("pointer scanner NULL")
	}
	var ns sql.NullString
	scanInto(t, &ns, "z")
	if !ns.Valid || ns.String != "z" {
		t.Fatal("NullString")
	}
}

func TestNativeNullHandling(t *testing.T) {
	yes := []any{sql.NullString{}, sql.NullInt64{}, sql.NullTime{}, fakeUUID{}, (*fakeUUID)(nil), []byte(nil), sql.RawBytes(nil)}
	no := []any{"", int64(0), 0, 1.5, true, time.Time{}, json.RawMessage(nil), namedInt(0), (*namedInt)(nil), (*string)(nil), (*int64)(nil), (*time.Time)(nil)}
	for _, v := range yes {
		if !nativeNullHandling(reflect.TypeOf(v)) {
			t.Errorf("%T should skip wrapper", v)
		}
	}
	for _, v := range no {
		if nativeNullHandling(reflect.TypeOf(v)) {
			t.Errorf("%T should keep wrapper", v)
		}
	}
}

// Pointer-to-scalar fields stay lenient: NULL -> nil, unparseable -> zero
// value (non-nil pointer), strings parse into numbers and times, and a reused
// destination never aliases a previously returned value.
func TestNullSafePointerScalarsLenient(t *testing.T) {
	var pi *int64
	scanInto(t, &pi, nil)
	if pi != nil {
		t.Fatal("NULL must leave nil pointer")
	}
	scanInto(t, &pi, "42")
	if pi == nil || *pi != 42 {
		t.Fatalf("string->*int64 parse: %v", pi)
	}
	scanInto(t, &pi, "abc")
	if pi == nil || *pi != 0 {
		t.Fatalf("unparseable must give zero, not error: %v", pi)
	}
	first := pi
	scanInto(t, &pi, int64(7))
	if pi == first || *pi != 7 || *first != 0 {
		t.Fatal("each scan must allocate fresh so retained rows never alias")
	}

	var pt *time.Time
	scanInto(t, &pt, "2024-03-05 10:11:12")
	if pt == nil || pt.Year() != 2024 {
		t.Fatalf("string->*time.Time: %v", pt)
	}
	now := time.Now()
	scanInto(t, &pt, now)
	if pt == nil || !pt.Equal(now) {
		t.Fatal("time source")
	}
	scanInto(t, &pt, nil)
	if pt != nil {
		t.Fatal("NULL *time.Time")
	}

	var pf *float64
	scanInto(t, &pf, int64(3))
	if pf == nil || *pf != 3 {
		t.Fatal("int64->*float64")
	}
	var pb *bool
	scanInto(t, &pb, true)
	if pb == nil || !*pb {
		t.Fatal("*bool")
	}
	var ps *string
	scanInto(t, &ps, []byte("hi"))
	if ps == nil || *ps != "hi" {
		t.Fatal("[]byte->*string")
	}
}

func BenchmarkNullSafePointerInt64(b *testing.B) {
	var p *int64
	ns := &nullSafe{dest: &p}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = ns.Scan(int64(i))
	}
}
