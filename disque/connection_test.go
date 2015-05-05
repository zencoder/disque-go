package disque

import (
	"testing"
	"time"

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
	d.Initialize()
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

	jobId, err := d.PushWithOptions("queue1", "asdf", time.Second, options)

	assert.NotNil(s.T(), jobId)
	assert.NotEqual(s.T(), "", jobId)
	assert.Nil(s.T(), err)
}

func (s *DisqueSuite) TestPushWithOptions() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	options := make(map[string]string)
	options["TTL"] = "60"
	options["ASYNC"] = "true"

	jobId, err := d.PushWithOptions("queue1", "asdf", time.Second, options)

	assert.NotNil(s.T(), jobId)
	assert.NotEqual(s.T(), "", jobId)
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

	// should explore Disque cluster again, reconnecting automatically
	jobId, err := d.PushWithOptions("queue1", "asdf", time.Second, options)

	assert.NotNil(s.T(), jobId)
	assert.NotEqual(s.T(), "", jobId)
	assert.Nil(s.T(), err)
}

func (s *DisqueSuite) TestGetJobDetailsWithInvalidJobId() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()

	jobDetails, err := d.GetJobDetails("asdfasdfasdfbogus")
	assert.NotNil(s.T(), err)
	assert.Nil(s.T(), jobDetails)
}

func (s *DisqueSuite) TestGetJobDetailsWithAckdJobId() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()

	jobId, err := d.Push("queue5000", "asdf", time.Second)

	var jobDetails *JobDetails
	jobDetails, err = d.GetJobDetails(jobId)
	assert.NotNil(s.T(), jobDetails)
	assert.Nil(s.T(), err)

	var job *Job
	job, err = d.Fetch("queue5000", time.Second)
	assert.Equal(s.T(), jobId, job.JobId)
	assert.NotNil(s.T(), job)
	assert.Nil(s.T(), err)
	d.Ack(job.JobId)

	jobDetails, err = d.GetJobDetails(job.JobId)
	assert.NotNil(s.T(), err)
	assert.Nil(s.T(), jobDetails)
}

func (s *DisqueSuite) TestGetJobDetails() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()

	jobId, err := d.Push("queue1", "asdf", time.Second)

	assert.NotNil(s.T(), jobId)
	assert.NotEqual(s.T(), "", jobId)
	assert.Nil(s.T(), err)

	var jobDetails *JobDetails
	jobDetails, err = d.GetJobDetails(jobId)
	assert.Nil(s.T(), err)
	assert.NotNil(s.T(), jobDetails)

	assert.NotNil(s.T(), jobDetails.CreatedAt)
	assert.True(s.T(), time.Now().After(jobDetails.CreatedAt))
	assert.Equal(s.T(), "asdf", jobDetails.Message)
	assert.Equal(s.T(), jobId, jobDetails.JobId)
}

func (s *DisqueSuite) TestPush() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()

	jobId, err := d.Push("queue1", "asdf", time.Second)

	assert.NotNil(s.T(), jobId)
	assert.NotEqual(s.T(), "", jobId)
	assert.Nil(s.T(), err)
}

func (s *DisqueSuite) TestPushToClosedConnection() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	d.Close()

	_, err := d.Push("queue1", "asdf", time.Second)

	assert.Nil(s.T(), err)
}

func (s *DisqueSuite) TestPushToUnreachableNode() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	d.Close()
	d.servers = []string{"127.0.0.1:7722"}

	_, err := d.Push("queue1", "asdf", time.Second)

	assert.NotNil(s.T(), err)
}

func (s *DisqueSuite) TestQueueLength() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	_, err := d.Push("queue3", "asdf", time.Second)
	assert.Nil(s.T(), err)
	assert.Equal(s.T(), 0, d.stats[d.prefix])

	var queueLength int
	queueLength, err = d.QueueLength("queue3")
	assert.Nil(s.T(), err)
	assert.Equal(s.T(), 1, queueLength)

	var job *Job
	job, err = d.Fetch("queue3", time.Second)
	err = d.Ack(job.JobId)
}

func (s *DisqueSuite) TestQueueLengthOnClosedConnection() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	_, err := d.Push("queue3", "asdf", time.Second)
	assert.Nil(s.T(), err)
	assert.Equal(s.T(), 0, d.stats[d.prefix])
	d.Close()

	var queueLength int
	queueLength, err = d.QueueLength("queue3")
	assert.Nil(s.T(), err)
	assert.Equal(s.T(), 1, queueLength)

	var job *Job
	job, err = d.Fetch("queue3", time.Second)
	err = d.Ack(job.JobId)
}

func (s *DisqueSuite) TestFetch() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	_, err := d.Push("queue4", "asdf", time.Second)
	assert.Nil(s.T(), err)
	assert.Equal(s.T(), 0, d.stats[d.prefix])

	job, err := d.Fetch("queue4", time.Second)
	assert.Nil(s.T(), err)
	assert.NotNil(s.T(), job)
	assert.Equal(s.T(), "queue4", job.QueueName)
	assert.Equal(s.T(), "asdf", job.Message)
	assert.Equal(s.T(), job.JobId[2:10], d.prefix)
	assert.Equal(s.T(), 1, d.stats[d.prefix])
}

func (s *DisqueSuite) TestFetchWithNoJobs() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	_, err := d.Push("queue4", "asdf", time.Second)
	assert.Nil(s.T(), err)
	assert.Equal(s.T(), 0, d.stats[d.prefix])

	job, err := d.Fetch("emptyqueue", time.Second)
	assert.Nil(s.T(), err)
	assert.Nil(s.T(), job)
	assert.Equal(s.T(), 0, d.stats[d.prefix])
}

func (s *DisqueSuite) TestFetchWithMultipleJobs() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	_, err := d.Push("queue5", "msg1", time.Second)
	_, err = d.Push("queue5", "msg2", time.Second)
	_, err = d.Push("queue5", "msg3", time.Second)

	jobs, err := d.FetchMultiple("queue5", 2, time.Second)
	assert.Nil(s.T(), err)
	assert.Equal(s.T(), 2, len(jobs))

	jobs, err = d.FetchMultiple("queue5", 2, time.Second)
	assert.Nil(s.T(), err)
	assert.Equal(s.T(), 1, len(jobs))
}

func (s *DisqueSuite) TestFetchMultipleWithNoJobs() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	_, err := d.Push("queue4", "asdf", time.Second)
	assert.Nil(s.T(), err)
	assert.Equal(s.T(), 0, d.stats[d.prefix])

	jobs, err := d.FetchMultiple("emptyqueue", 1, time.Second)
	assert.Nil(s.T(), err)
	assert.NotNil(s.T(), jobs)
	assert.Equal(s.T(), 0, len(jobs))
	assert.Equal(s.T(), 0, d.stats[d.prefix])
}

func (s *DisqueSuite) TestAck() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	_, err := d.Push("queue2", "asdf", time.Second)
	assert.Nil(s.T(), err)

	job, err := d.Fetch("queue2", time.Second)
	assert.Nil(s.T(), err)

	err = d.Ack(job.JobId)
	assert.Nil(s.T(), err)
}

func (s *DisqueSuite) TestAckWithMalformedJobId() {
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
		d.Push("queueBenchPush", "asdf", time.Second)
	}
}

func BenchmarkPushAsync(b *testing.B) {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	options := make(map[string]string)
	options["ASYNC"] = "true"

	for i := 0; i < b.N; i++ {
		d.PushWithOptions("queueBenchPushAsync", "asdf", time.Second, options)
	}
}

func BenchmarkFetch(b *testing.B) {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	for i := 0; i < b.N; i++ {
		d.Push("queueBenchFetch", "asdf", time.Second)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.Fetch("queueBenchFetch", time.Second)
	}
}
