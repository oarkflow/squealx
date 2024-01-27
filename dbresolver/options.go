package dbresolver

import (
	"github.com/oarkflow/squealx"
)

// Options is the config for dbResolver.
type Options struct {
	SecondaryDBs []*squealx.DB
	LoadBalancer LoadBalancer
}

// OptionFunc is a function that configures a Options.
type OptionFunc func(*Options)

// WithSecondaryDBs sets the secondary databases.
func WithSecondaryDBs(dbs ...*squealx.DB) OptionFunc {
	return func(opt *Options) {
		opt.SecondaryDBs = dbs
	}
}

// WithLoadBalancer sets the load balancer.
func WithLoadBalancer(loadBalancer LoadBalancer) OptionFunc {
	return func(opt *Options) {
		opt.LoadBalancer = loadBalancer
	}
}
