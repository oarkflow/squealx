package dbresolver

import (
	"context"
	"math/rand"
	"sync/atomic"

	"github.com/oarkflow/squealx"
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
	Select(ctx context.Context, dbs []*squealx.DB) *squealx.DB
	Name() LoadBalancerPolicy
}

// RandomLoadBalancer is a load balancer that chooses a database randomly.
type RandomLoadBalancer struct{}

var _ LoadBalancer = (*RandomLoadBalancer)(nil)

func NewRandomLoadBalancer() *RandomLoadBalancer {
	return &RandomLoadBalancer{}
}

// Select returns the database to use for the given operation.
// If there are no databases, it returns nil. but it should not happen.
func (b *RandomLoadBalancer) Select(_ context.Context, dbs []*squealx.DB) *squealx.DB {
	n := len(dbs)
	if n == 0 {
		return nil
	}
	if n == 1 {
		return dbs[0]
	}
	return dbs[rand.Intn(n)]
}

func (b *RandomLoadBalancer) Name() LoadBalancerPolicy {
	return RandomLB
}

// injectedLoadBalancer is a load balancer that always chooses the given database.
// It is used for testing.
type injectedLoadBalancer struct {
	db *squealx.DB
}

var _ LoadBalancer = (*injectedLoadBalancer)(nil)

func (b *injectedLoadBalancer) Select(_ context.Context, _ []*squealx.DB) *squealx.DB {
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

func (b *RoundRobinLoadBalancer) Select(_ context.Context, dbs []*squealx.DB) *squealx.DB {
	n := atomic.AddUint32(&b.next, 1)
	return dbs[(int(n)-1)%len(dbs)]
}

func (b *RoundRobinLoadBalancer) Name() LoadBalancerPolicy {
	return RoundRobinLB
}
