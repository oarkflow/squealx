package dbresolver

import (
	"github.com/oarkflow/squealx"
)

// Options is the config for dbResolver.
type Options struct {
	masterDBs       []*squealx.DB
	replicaDBs      []*squealx.DB
	defaultDB       *squealx.DB
	loadBalancer    LoadBalancer
	fileLoader      *squealx.FileLoader
	readWritePolicy ReadWritePolicy
}

// OptionFunc is a function that configures a Options.
type OptionFunc func(*Options)

// WithReplicaDBs sets the secondary databases.
func WithReplicaDBs(dbs ...*squealx.DB) OptionFunc {
	return func(opt *Options) {
		opt.replicaDBs = dbs
	}
}

// WithMasterDBs sets the secondary databases.
func WithMasterDBs(dbs ...*squealx.DB) OptionFunc {
	return func(opt *Options) {
		opt.masterDBs = dbs
	}
}

// WithDefaultDB sets the secondary databases.
func WithDefaultDB(dbs *squealx.DB) OptionFunc {
	return func(opt *Options) {
		opt.defaultDB = dbs
	}
}

// WithReadWritePolicy sets the secondary databases.
func WithReadWritePolicy(dbs ReadWritePolicy) OptionFunc {
	return func(opt *Options) {
		opt.readWritePolicy = dbs
	}
}

// WithLoadBalancer sets the load balancer.
func WithLoadBalancer(loadBalancer LoadBalancer) OptionFunc {
	return func(opt *Options) {
		opt.loadBalancer = loadBalancer
	}
}

// WithFileLoader sets the load balancer.
func WithFileLoader(fileLoader *squealx.FileLoader) OptionFunc {
	return func(opt *Options) {
		opt.fileLoader = fileLoader
	}
}
