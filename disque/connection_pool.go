package disque

import (
	"time"

	"github.com/youtube/vitess/go/pools"
	"golang.org/x/net/context"
)

type DisquePool struct {
	servers []string
	cycle   int
	pool    *pools.ResourcePool
}

// NewDisquePool creates a new pool of Disque connections.
// capacity is the number of active resources in the pool:
// there can be up to 'capacity' of these at a given time.
// maxCapacity specifies the extent to which the pool can be resized
// in the future through the SetCapacity function.
// You cannot resize the pool beyond maxCapacity.
// If a resource is unused beyond idleTimeout, it's discarded.
// An idleTimeout of 0 means that there is no timeout.
func NewDisquePool(servers []string, cycle int, capacity int, maxCapacity int, idleTimeout time.Duration) (d *DisquePool) {
	d = &DisquePool{
		servers: servers,
		cycle:   cycle,
	}
	d.pool = pools.NewResourcePool(d.poolFactory, capacity, maxCapacity, idleTimeout)
	return
}

// SetCapacity changes the capacity of the pool.
// You can use it to shrink or expand, but not beyond
// the max capacity. If the change requires the pool
// to be shrunk, SetCapacity waits till the necessary
// number of resources are returned to the pool.
// A SetCapacity of 0 is equivalent to closing the ResourcePool.
func (d *DisquePool) SetCapacity(capacity int) {
	d.pool.SetCapacity(capacity)
}

// Get will return the next available resource. If capacity
// has not been reached, it will create a new one using the factory. Otherwise,
// it will wait until the supplied context expires.
func (d *DisquePool) Get(ctx context.Context) (conn *Disque, err error) {
	var r pools.Resource
	if r, err = d.pool.Get(ctx); err == nil && r != nil {
		conn = r.(*Disque)
	}
	return conn, err
}

// TryGet will return the next available resource. If none is available, and capacity
// has not been reached, it will create a new one using the factory. Otherwise,
// it will return nil with no error.
func (d *DisquePool) TryGet() (conn *Disque, err error) {
	var r pools.Resource
	if r, err = d.pool.TryGet(); err == nil && r != nil {
		conn = r.(*Disque)
	}
	return conn, err
}

// Put will return a resource to the pool. For every successful Get,
// a corresponding Put is required. If you no longer need a resource,
// you will need to call Put(nil) instead of returning the closed resource.
// The will eventually cause a new resource to be created in its place.
func (d *DisquePool) Put(conn *Disque) {
	d.pool.Put(conn)
}

// Close empties the pool calling Close on all its resources.
// You can call Close while there are outstanding resources.
// It waits for all resources to be returned (Put).
// After a Close, Get and TryGet are not allowed.
func (d *DisquePool) Close() {
	d.pool.Close()
}

// IsClosed returns true if the resource pool is closed.
func (d *DisquePool) IsClosed() (closed bool) {
	return d.pool.IsClosed()
}

func (d *DisquePool) poolFactory() (r pools.Resource, err error) {
	conn := NewDisque(d.servers, d.cycle)
	err = conn.Initialize()

	return conn, err
}
