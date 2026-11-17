package squealx

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

type poolConfigTestDriver struct {
	opens  atomic.Int32
	closes atomic.Int32
	pings  atomic.Int32
	ping   func(context.Context) error
}

func (d *poolConfigTestDriver) Open(string) (driver.Conn, error) {
	d.opens.Add(1)
	return &poolConfigTestConn{driver: d}, nil
}

type poolConfigTestConn struct{ driver *poolConfigTestDriver }

func (c *poolConfigTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("pool config test driver does not prepare statements")
}

func (c *poolConfigTestConn) Close() error {
	c.driver.closes.Add(1)
	return nil
}

func (c *poolConfigTestConn) Begin() (driver.Tx, error) {
	return nil, errors.New("pool config test driver does not support transactions")
}

func (c *poolConfigTestConn) Ping(ctx context.Context) error {
	c.driver.pings.Add(1)
	if c.driver.ping != nil {
		return c.driver.ping(ctx)
	}
	return nil
}

var poolConfigTestDriverID atomic.Uint64

func registerPoolConfigTestDriver(t *testing.T, ping func(context.Context) error) (*poolConfigTestDriver, string) {
	t.Helper()
	d := &poolConfigTestDriver{ping: ping}
	name := fmt.Sprintf("squealx-pool-test-%d", poolConfigTestDriverID.Add(1))
	sql.Register(name, d)
	return d, name
}

func TestConnectWithPoolConfigAppliesLimitsAndCloses(t *testing.T) {
	driver, name := registerPoolConfigTestDriver(t, nil)
	db, err := ConnectWithPoolConfig(context.Background(), name, "", "pool-config", PoolConfig{
		MaxOpenConns:    3,
		MaxIdleConns:    1,
		ConnMaxLifetime: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats := db.Stats(); stats.MaxOpenConnections != 3 {
		t.Fatalf("max open connections = %d, want 3", stats.MaxOpenConnections)
	}
	if driver.pings.Load() != 1 || driver.opens.Load() != 1 {
		t.Fatalf("driver opens=%d pings=%d, want one each", driver.opens.Load(), driver.pings.Load())
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if driver.closes.Load() != 1 {
		t.Fatalf("driver closes=%d, want 1", driver.closes.Load())
	}
}

func TestWarmPoolOpensRequestedConnectionCount(t *testing.T) {
	driver, name := registerPoolConfigTestDriver(t, nil)
	db, err := Open(name, "", "warm-pool")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(5)

	if err := db.WarmPool(context.Background(), 3); err != nil {
		t.Fatal(err)
	}
	if got := driver.opens.Load(); got != 3 {
		t.Fatalf("driver opens = %d, want 3", got)
	}
	if stats := db.Stats(); stats.Idle != 3 {
		t.Fatalf("idle connections = %d, want 3 (warmed connections should be released back to the pool)", stats.Idle)
	}
}

func TestApplyPoolConfigWarmsConnections(t *testing.T) {
	driver, name := registerPoolConfigTestDriver(t, nil)
	db, err := Open(name, "", "apply-pool-warm")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.ApplyPoolConfig(PoolConfig{MaxOpenConns: 4, MaxIdleConns: 4, WarmConnections: 2}); err != nil {
		t.Fatal(err)
	}
	if got := driver.opens.Load(); got != 2 {
		t.Fatalf("driver opens = %d, want 2", got)
	}
}

func TestPoolConfigRejectsWarmConnectionsAboveMaxOpen(t *testing.T) {
	config := PoolConfig{MaxOpenConns: 2, WarmConnections: 3}
	if err := config.Validate(); !errors.Is(err, ErrInvalidPool) {
		t.Fatalf("Validate() error = %v, want ErrInvalidPool", err)
	}
}

func TestOpenWithInvalidPoolDoesNotOpenDriver(t *testing.T) {
	driver, name := registerPoolConfigTestDriver(t, nil)
	_, err := OpenWithPoolConfig(name, "", "invalid-pool", PoolConfig{MaxOpenConns: 1, MaxIdleConns: 2})
	if !errors.Is(err, ErrInvalidPool) {
		t.Fatalf("OpenWithPoolConfig error = %v, want ErrInvalidPool", err)
	}
	if got := driver.opens.Load(); got != 0 {
		t.Fatalf("driver opened %d connections for invalid pool config", got)
	}
}

func TestConnectWithPoolConfigClosesAfterCanceledPing(t *testing.T) {
	pingStarted := make(chan struct{})
	driver, name := registerPoolConfigTestDriver(t, func(ctx context.Context) error {
		close(pingStarted)
		<-ctx.Done()
		return ctx.Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := ConnectWithPoolConfig(ctx, name, "", "canceled-ping", PoolConfig{MaxOpenConns: 1, MaxIdleConns: 1})
		done <- err
	}()
	select {
	case <-pingStarted:
		cancel()
	case <-time.After(time.Second):
		cancel()
		t.Fatal("startup ping did not begin")
	}
	var err error
	select {
	case err = <-done:
	case <-time.After(time.Second):
		t.Fatal("startup ping did not stop after cancellation")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ConnectWithPoolConfig error = %v, want context canceled", err)
	}
	if driver.opens.Load() != 1 || driver.pings.Load() != 1 || driver.closes.Load() != 1 {
		t.Fatalf("driver opens=%d pings=%d closes=%d, want one each", driver.opens.Load(), driver.pings.Load(), driver.closes.Load())
	}
}
