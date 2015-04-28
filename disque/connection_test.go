package disque

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type DisqueSuite struct {
	suite.Suite
}

func TestDisqueSuite(t *testing.T) {
	suite.Run(t, new(DisqueSuite))
}

func (s *DisqueSuite) SetupTest() {
}

func (s *DisqueSuite) SetupSuite() {
}

func (s *DisqueSuite) TestInitWithOneNode() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	assert.NotNil(s.T(), d)

	d.Initialize()
	assert.EqualValues(s.T(), 1, len(d.nodes))
}

func (s *DisqueSuite) TestInitWithMultipleHostsOneNode() {
	hosts := []string{"127.0.0.1:7711", "127.0.0.1:8800"}
	d := NewDisque(hosts, 1000)
	assert.NotNil(s.T(), d)

	d.Initialize()
	assert.EqualValues(s.T(), 1, len(d.nodes))
}

func (s *DisqueSuite) TestInitWithZeroNodes() {
	hosts := []string{"127.0.0.1:8800"}
	d := NewDisque(hosts, 1000)
	assert.NotNil(s.T(), d)

	assert.NotNil(s.T(), d.Initialize())
	assert.EqualValues(s.T(), 0, len(d.nodes))
}

func (s *DisqueSuite) TestInitWithZeroHosts() {
	hosts := []string{}
	d := NewDisque(hosts, 1000)
	assert.NotNil(s.T(), d)

	assert.NotNil(s.T(), d.Initialize())
	assert.EqualValues(s.T(), 0, len(d.nodes))
}
