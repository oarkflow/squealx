package sqliteutil

import (
	"errors"
	"testing"
	"time"
)

func TestIsPrivateMemoryDSN(t *testing.T) {
	private := []string{
		":memory:",
		":memory:?_pragma=busy_timeout(5000)",
		"file::memory:",
		"file:session?mode=memory&cache=private",
	}
	for _, dsn := range private {
		if !IsPrivateMemoryDSN(dsn) {
			t.Errorf("IsPrivateMemoryDSN(%q) = false, want true", dsn)
		}
	}
	shared := []string{
		"file::memory:?cache=shared",
		"file:session?mode=memory&cache=shared",
		"./app.db",
	}
	for _, dsn := range shared {
		if IsPrivateMemoryDSN(dsn) {
			t.Errorf("IsPrivateMemoryDSN(%q) = true, want false", dsn)
		}
	}
}

func TestValidatePrivateMemoryPool(t *testing.T) {
	if err := ValidatePrivateMemoryPool(":memory:", 1, 1, 0, 0); err != nil {
		t.Fatalf("valid private-memory pool: %v", err)
	}
	if err := ValidatePrivateMemoryPool("file:shared?mode=memory&cache=shared", 8, 4, time.Minute, 0); err != nil {
		t.Fatalf("shared-memory pool: %v", err)
	}
	invalid := [][4]int64{
		{0, 1, 0, 0},
		{2, 1, 0, 0},
		{1, 0, 0, 0},
		{1, 1, int64(time.Minute), 0},
		{1, 1, 0, int64(time.Minute)},
	}
	for _, config := range invalid {
		err := ValidatePrivateMemoryPool(":memory:", int(config[0]), int(config[1]), time.Duration(config[2]), time.Duration(config[3]))
		if !errors.Is(err, ErrPrivateMemoryPool) {
			t.Errorf("config %v error = %v, want ErrPrivateMemoryPool", config, err)
		}
	}
}

func TestDefaultPoolLimits(t *testing.T) {
	cases := []struct {
		dsn                string
		wantOpen, wantIdle int
	}{
		{dsn: ":memory:", wantOpen: 1, wantIdle: 1},
		{dsn: "file:shared?mode=memory&cache=shared", wantOpen: 4, wantIdle: 2},
		{dsn: "./database.sqlite", wantOpen: 4, wantIdle: 2},
	}
	for _, test := range cases {
		open, idle := DefaultPoolLimits(test.dsn)
		if open != test.wantOpen || idle != test.wantIdle {
			t.Errorf("DefaultPoolLimits(%q) = (%d,%d), want (%d,%d)", test.dsn, open, idle, test.wantOpen, test.wantIdle)
		}
	}
}

func TestWithDefaultBusyTimeout(t *testing.T) {
	tests := []struct {
		dsn  string
		want string
	}{
		{dsn: ":memory:", want: ":memory:?_busy_timeout=5000"},
		{dsn: "file:app.db", want: "file:app.db?_busy_timeout=5000"},
		{dsn: "file:app.db?mode=memory&cache=shared", want: "file:app.db?mode=memory&cache=shared&_busy_timeout=5000"},
		{dsn: "file:app.db?_busy_timeout=250", want: "file:app.db?_busy_timeout=250"},
		{dsn: "file:app.db?_timeout=0", want: "file:app.db?_timeout=0"},
		{dsn: "file:app.db?bad=%zz", want: "file:app.db?bad=%zz"},
	}
	for _, test := range tests {
		if got := WithDefaultBusyTimeout(test.dsn, 5000); got != test.want {
			t.Errorf("WithDefaultBusyTimeout(%q) = %q, want %q", test.dsn, got, test.want)
		}
	}
}
