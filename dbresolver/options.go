package dbresolver

import (
	"github.com/oarkflow/squealx"
)

// Options is the config for dbResolver.
type Options struct {
	ReplicaDBs   []*squealx.DB
	LoadBalancer LoadBalancer
}

// OptionFunc is a function that configures a Options.
type OptionFunc func(*Options)

// WithReplicaDBs sets the secondary databases.
func WithReplicaDBs(dbs ...*squealx.DB) OptionFunc {
	return func(opt *Options) {
		opt.ReplicaDBs = dbs
	}
}

// WithLoadBalancer sets the load balancer.
func WithLoadBalancer(loadBalancer LoadBalancer) OptionFunc {
	return func(opt *Options) {
		opt.LoadBalancer = loadBalancer
	}
}
