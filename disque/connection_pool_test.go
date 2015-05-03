package disque

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type DisquePoolSuite struct {
	suite.Suite
}

func TestDisquePoolSuite(t *testing.T) {
	suite.Run(t, new(DisquePoolSuite))
}

func (s *DisquePoolSuite) SetupTest() {
}

func (s *DisquePoolSuite) SetupSuite() {
}

func (s *DisquePoolSuite) TestWithPoolOfOne() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisquePool(hosts, 1000, 1, 1, time.Hour)

	c, err := d.Get()
	assert.Nil(s.T(), err)
	assert.NotNil(s.T(), c)

	// return the connection and close the pool
	d.Put(c)
	d.Close()

	// try getting a connection from the closed pool
	c, err = d.Get()
	assert.NotNil(s.T(), err)
}

func (s *DisquePoolSuite) TestWithResizedPool() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisquePool(hosts, 1000, 1, 2, time.Hour)

	c1, err := d.Get()
	assert.Nil(s.T(), err)
	assert.NotNil(s.T(), c1)

	var c2 *Disque
	c2, err = d.TryGet()
	assert.Nil(s.T(), c2)
	assert.Nil(s.T(), err)

	// return the connection
	d.Put(c1)

	// resize the pool
	d.SetCapacity(2)
	c1, err = d.TryGet()
	assert.Nil(s.T(), err)
	c2, err = d.TryGet()
	assert.Nil(s.T(), err)
	d.Put(c1)
	d.Put(c2)

	d.Close()
	assert.True(s.T(), d.IsClosed())
}
