package squealx

import (
	"reflect"
	"testing"
	"time"
)

func TestConvertMapValueToStringFormatsCommonValues(t *testing.T) {
	type text string
	timestamp := time.Date(2026, time.November, 11, 12, 13, 14, 150000000, time.UTC)
	cases := []struct {
		name string
		raw  any
		want string
	}{
		{name: "integer", raw: int64(42), want: "42"},
		{name: "boolean", raw: true, want: "true"},
		{name: "float", raw: float64(1.25), want: "1.25"},
		{name: "bytes", raw: []byte("value"), want: "value"},
		{name: "time", raw: timestamp, want: timestamp.Format(time.RFC3339Nano)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := convertMapValue(tc.raw, reflect.TypeOf(text("")))
			if err != nil {
				t.Fatal(err)
			}
			if value := got.Interface().(text); string(value) != tc.want {
				t.Fatalf("converted value = %q, want %q", value, tc.want)
			}
		})
	}
}

func TestTypedBinaryMapValuePreservesOwnedBytes(t *testing.T) {
	input := []byte{0x00, 0xff, 0x80}
	got := mapScanRawValue(input, " blob ", reflect.TypeOf([]byte(nil)))
	value, ok := got.([]byte)
	if !ok {
		t.Fatalf("binary value type = %T, want []byte", got)
	}
	if !reflect.DeepEqual(value, input) {
		t.Fatalf("binary value = %v, want %v", value, input)
	}
	input[0] = 0x01
	if value[0] != 0x00 {
		t.Fatal("returned binary value aliases driver buffer")
	}
	if text := bytesToAny(input, "BLOB"); text != string([]byte{0x01, 0xff, 0x80}) {
		t.Fatalf("interface map conversion = %#v, want compatibility string", text)
	}
}
