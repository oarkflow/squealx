package dbresolver

import (
	"context"
	"sync/atomic"
	"time"
)

// LoadBalancerPolicy define the loadbalancer policy data type
type LoadBalancerPolicy string

// Supported Loadbalancer policy
const (
	RoundRobinLB         LoadBalancerPolicy = "ROUND_ROBIN"
	RandomLB             LoadBalancerPolicy = "RANDOM"
	InjectedLoadBalancer LoadBalancerPolicy = "INJECTED_LOAD_BALANCER"
)

// LoadBalancer chooses a database from the given databases.
type LoadBalancer interface {
	Select(ctx context.Context, dbs []string) string
	Name() LoadBalancerPolicy
}

// RandomLoadBalancer is a load balancer that chooses a database randomly.
type RandomLoadBalancer struct {
	state uint64
}

var _ LoadBalancer = (*RandomLoadBalancer)(nil)

func NewRandomLoadBalancer() *RandomLoadBalancer {
	seed := uint64(time.Now().UnixNano())
	if seed == 0 {
		seed = 0x9e3779b97f4a7c15
	}
	return &RandomLoadBalancer{state: seed}
}

// Select returns the database to use for the given operation.
// If there are no databases, it returns nil. but it should not happen.
func (b *RandomLoadBalancer) Select(_ context.Context, dbs []string) string {
	n := len(dbs)
	if n == 0 {
		return ""
	}
	if n == 1 {
		return dbs[0]
	}
	// SplitMix64 gives a fast, lock-free, statistically sound choice without
	// contending on math/rand's package-global source.
	x := atomic.AddUint64(&b.state, 0x9e3779b97f4a7c15)
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	x ^= x >> 31
	return dbs[int(x%uint64(n))]
}

func (b *RandomLoadBalancer) Name() LoadBalancerPolicy {
	return RandomLB
}

// injectedLoadBalancer is a load balancer that always chooses the given database.
// It is used for testing.
type injectedLoadBalancer struct {
	db string
}

var _ LoadBalancer = (*injectedLoadBalancer)(nil)

func (b *injectedLoadBalancer) Select(_ context.Context, _ []string) string {
	return b.db
}

func (b *injectedLoadBalancer) Name() LoadBalancerPolicy {
	return InjectedLoadBalancer
}

func NewRoundRobinLoadBalancer() *RoundRobinLoadBalancer {
	return &RoundRobinLoadBalancer{}
}

type RoundRobinLoadBalancer struct {
	next uint32
}

func (b *RoundRobinLoadBalancer) Select(_ context.Context, dbs []string) string {
	if len(dbs) == 0 {
		return ""
	}
	n := atomic.AddUint32(&b.next, 1)
	return dbs[(int(n)-1)%len(dbs)]
}

func (b *RoundRobinLoadBalancer) Name() LoadBalancerPolicy {
	return RoundRobinLB
}
