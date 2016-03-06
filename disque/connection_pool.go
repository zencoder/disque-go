package disque

import (
	"time"

	"github.com/youtube/vitess/go/pools"
	"golang.org/x/net/context"
)

// Pool represents a pool of Disque connections
type Pool struct {
	servers []string
	cycle   int
	pool    *pools.ResourcePool
}

// NewPool creates a new pool of Disque connections.
// capacity is the number of active resources in the pool:
// there can be up to 'capacity' of these at a given time.
// maxCapacity specifies the extent to which the pool can be resized
// in the future through the SetCapacity function.
// You cannot resize the pool beyond maxCapacity.
// If a resource is unused beyond idleTimeout, it's discarded.
// An idleTimeout of 0 means that there is no timeout.
func NewPool(servers []string, cycle int, capacity int, maxCapacity int, idleTimeout time.Duration) (p *Pool) {
	p = &Pool{
		servers: servers,
		cycle:   cycle,
	}
	p.pool = pools.NewResourcePool(p.poolFactory, capacity, maxCapacity, idleTimeout)
	return
}

// SetCapacity changes the capacity of the pool.
// You can use it to shrink or expand, but not beyond
// the max capacity. If the change requires the pool
// to be shrunk, SetCapacity waits till the necessary
// number of resources are returned to the pool.
// A SetCapacity of 0 is equivalent to closing the ResourcePool.
func (p *Pool) SetCapacity(capacity int) {
	p.pool.SetCapacity(capacity)
}

// Get will return the next available resource. If capacity
// has not been reached, it will create a new one using the factory. Otherwise,
// it will wait until the supplied context expires.
func (p *Pool) Get(ctx context.Context) (conn *Disque, err error) {
	var r pools.Resource
	if r, err = p.pool.Get(ctx); err == nil && r != nil {
		conn = r.(*Disque)
	}
	return conn, err
}

// Put will return a resource to the pool. For every successful Get,
// a corresponding Put is required. If you no longer need a resource,
// you will need to call Put(nil) instead of returning the closed resource.
// This will eventually cause a new resource to be created in its place.
func (p *Pool) Put(conn *Disque) {
	if conn == nil {
		// Converting a concrete value (*Disque) into an interface value
		// (pools.Resource) produces an interface value that is != nil, even if
		// the concrete value was nil.
		//
		// Instead, we create a new nil value of type pools.Resource which is
		// == nil, because it doesn't have a concrete type.
		p.pool.Put(nil)
	} else {
		p.pool.Put(conn)
	}
}

// Close empties the pool calling Close on all its resources.
// You can call Close while there are outstanding resources.
// It waits for all resources to be returned (Put).
// After a Close, Get is not allowed.
func (p *Pool) Close() {
	p.pool.Close()
}

// IsClosed returns true if the resource pool is closed.
func (p *Pool) IsClosed() (closed bool) {
	return p.pool.IsClosed()
}

func (p *Pool) poolFactory() (r pools.Resource, err error) {
	conn := NewDisque(p.servers, p.cycle)
	err = conn.Initialize()

	return conn, err
}
