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

func (s *DisqueSuite) TestInitAndCloseWithOneNode() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)

	d.Initialize()

	assert.Nil(s.T(), d.Close())
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
	d.nodes["host2"] = "127.0.0.1:7722"
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

func (s *DisqueSuite) TestPushWithEmptyOptions() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	options := make(map[string]string)

	err := d.PushWithOptions("queue1", "asdf", 100, options)

	assert.Nil(s.T(), err)
}

func (s *DisqueSuite) TestPushWithOptions() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	options := make(map[string]string)
	options["TTL"] = "60"
	options["ASYNC"] = "true"

	err := d.PushWithOptions("queue1", "asdf", 100, options)

	assert.Nil(s.T(), err)
}

func (s *DisqueSuite) TestPushWithOptionsOnClosedConnection() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	d.Close()
	options := make(map[string]string)
	options["TTL"] = "60"
	options["ASYNC"] = "true"

	err := d.PushWithOptions("queue1", "asdf", 100, options)

	assert.Nil(s.T(), err)
}

func (s *DisqueSuite) TestPush() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()

	err := d.Push("queue1", "asdf", 100)

	assert.Nil(s.T(), err)
}

func (s *DisqueSuite) TestPushToClosedConnection() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	d.Close()

	err := d.Push("queue1", "asdf", 100)

	assert.Nil(s.T(), err)
}

func (s *DisqueSuite) TestPushToUnreachableNode() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	d.Close()
	d.servers = []string{"127.0.0.1:7722"}

	err := d.Push("queue1", "asdf", 100)

	assert.NotNil(s.T(), err)
}

func (s *DisqueSuite) TestQueueLength() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	err := d.Push("queue3", "asdf", 100)
	assert.Nil(s.T(), err)
	assert.Equal(s.T(), 0, d.stats[d.prefix])

	var queueLength int
	queueLength, err = d.QueueLength("queue3")
	assert.Nil(s.T(), err)
	assert.Equal(s.T(), 1, queueLength)

	var jobs []*Job
	jobs, err = d.Fetch("queue3", 1, 1)
	err = d.Ack(jobs[0].MessageId)
}

func (s *DisqueSuite) TestQueueLengthOnClosedConnection() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	err := d.Push("queue3", "asdf", 100)
	assert.Nil(s.T(), err)
	assert.Equal(s.T(), 0, d.stats[d.prefix])
	d.Close()

	var queueLength int
	queueLength, err = d.QueueLength("queue3")
	assert.Nil(s.T(), err)
	assert.Equal(s.T(), 1, queueLength)

	var jobs []*Job
	jobs, err = d.Fetch("queue3", 1, 1)
	err = d.Ack(jobs[0].MessageId)
}

func (s *DisqueSuite) TestFetch() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	err := d.Push("queue2", "asdf", 100)
	assert.Nil(s.T(), err)
	assert.Equal(s.T(), 0, d.stats[d.prefix])

	jobs, err := d.Fetch("queue2", 1, 0)
	assert.Nil(s.T(), err)
	assert.Equal(s.T(), 1, len(jobs))
	assert.Equal(s.T(), "queue2", jobs[0].QueueName)
	assert.Equal(s.T(), "asdf", jobs[0].Message)
	assert.Equal(s.T(), jobs[0].MessageId[2:10], d.prefix)
	assert.Equal(s.T(), 1, d.stats[d.prefix])
}

func (s *DisqueSuite) TestFetchWithMultipleJobs() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	err := d.Push("queue2", "msg1", 100)
	err = d.Push("queue2", "msg2", 100)
	err = d.Push("queue2", "msg3", 100)

	jobs, err := d.Fetch("queue2", 2, 2)
	assert.Nil(s.T(), err)
	assert.Equal(s.T(), 2, len(jobs))

	jobs, err = d.Fetch("queue2", 2, 2)
	assert.Nil(s.T(), err)
	assert.Equal(s.T(), 1, len(jobs))
}

func (s *DisqueSuite) TestAck() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	err := d.Push("queue2", "asdf", 100)
	assert.Nil(s.T(), err)

	jobs, err := d.Fetch("queue2", 1, 0)
	assert.Nil(s.T(), err)

	err = d.Ack(jobs[0].MessageId)
	assert.Nil(s.T(), err)
}

func (s *DisqueSuite) TestAckWithMalformedMessageId() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()

	err := d.Ack("foobaz")
	assert.NotNil(s.T(), err)
}

func BenchmarkPush(b *testing.B) {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()

	for i := 0; i < b.N; i++ {
		d.Push("queueBenchPush", "asdf", 100)
	}
}

func BenchmarkPushAsync(b *testing.B) {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	options := make(map[string]string)
	options["ASYNC"] = "true"

	for i := 0; i < b.N; i++ {
		d.PushWithOptions("queueBenchPushAsync", "asdf", 100, options)
	}
}

func BenchmarkFetch(b *testing.B) {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	for i := 0; i < b.N; i++ {
		d.Push("queueBenchFetch", "asdf", 100)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.Fetch("queueBenchFetch", 1, 100)
	}
}
