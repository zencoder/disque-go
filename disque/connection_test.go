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

	d.Initialize()
	assert.EqualValues(s.T(), 1, len(d.nodes))
}

func (s *DisqueSuite) TestInitWithMultipleHostsOneNode() {
	hosts := []string{"127.0.0.1:7711", "127.0.0.1:8800"}
	d := NewDisque(hosts, 1000)

	d.Initialize()
	assert.EqualValues(s.T(), 1, len(d.nodes))
}

func (s *DisqueSuite) TestInitWithZeroNodes() {
	hosts := []string{"127.0.0.1:8800"}
	d := NewDisque(hosts, 1000)
	assert.NotNil(s.T(), d)

	err := d.Initialize()

	assert.NotNil(s.T(), err)
	assert.EqualValues(s.T(), 0, len(d.nodes))
}

func (s *DisqueSuite) TestInitWithZeroHosts() {
	hosts := []string{}
	d := NewDisque(hosts, 1000)

	err := d.Initialize()

	assert.NotNil(s.T(), err)
	assert.EqualValues(s.T(), 0, len(d.nodes))
}

func (s *DisqueSuite) TestPickClientBelowClientSelectionCount() {
	hosts := []string{}
	d := NewDisque(hosts, 1000)
	d.prefix = "host1"
	d.count = 999

	d.pickClient()

	assert.Equal(s.T(), "host1", d.prefix)
}
func (s *DisqueSuite) TestPickClientWithEmptyStats() {
	hosts := []string{}
	d := NewDisque(hosts, 1000)
	d.prefix = "host1"
	d.count = 1000

	d.pickClient()

	assert.Equal(s.T(), "host1", d.prefix)
}

func (s *DisqueSuite) TestPickClientWithTwoHostStatsDifferentOptimalHost() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.nodes["host1"] = "example.com:7711"
	d.nodes["host2"] = "127.0.0.1:7711"
	d.stats["host1"] = 500
	d.stats["host2"] = 600
	d.prefix = "host1"
	d.count = 1000

	d.pickClient()

	assert.Equal(s.T(), "host2", d.prefix)
	assert.Equal(s.T(), 0, len(d.stats))
}

func (s *DisqueSuite) TestPickClientWithTwoHostStatsSameOptimalHost() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.nodes["host1"] = "example.com:7711"
	d.nodes["host2"] = "127.0.0.1:7711"
	d.stats["host1"] = 600
	d.stats["host2"] = 500
	d.prefix = "host1"
	d.count = 1000

	d.pickClient()

	assert.Equal(s.T(), "host1", d.prefix)
	assert.Equal(s.T(), 2, len(d.stats))
}

func (s *DisqueSuite) TestPickClientWithTwoHostStatsUnrecognizedOptimalHost() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.nodes["host1"] = "example.com:7711"
	d.stats["host1"] = 500
	d.stats["host2"] = 600
	d.prefix = "host1"
	d.count = 1000

	d.pickClient()

	assert.Equal(s.T(), "host1", d.prefix)
	assert.Equal(s.T(), 2, len(d.stats))
}

func (s *DisqueSuite) TestPickClientWithTwoHostStatsUnreachableOptimalHost() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.nodes["host1"] = "example.com:7711"
	d.nodes["host2"] = "127.0.0.1:7712"
	d.stats["host1"] = 500
	d.stats["host2"] = 600
	d.prefix = "host1"
	d.host = "example.com:7711"
	d.count = 1000

	d.pickClient()

	assert.Equal(s.T(), "host1", d.prefix)
	assert.Equal(s.T(), "example.com:7711", d.host)
	assert.Equal(s.T(), 2, len(d.stats))
}
