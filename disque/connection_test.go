package disque

import (
	"log"
	"testing"
	"time"

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
	s.EqualValues(1, len(d.nodes))
}

func (s *DisqueSuite) TestInitWithMultipleHostsOneNode() {
	hosts := []string{"127.0.0.1:7711", "127.0.0.1:8800"}
	d := NewDisque(hosts, 1000)

	d.Initialize()
	s.EqualValues(1, len(d.nodes))
}

func (s *DisqueSuite) TestInitWithZeroNodes() {
	hosts := []string{"127.0.0.1:8800"}
	d := NewDisque(hosts, 1000)
	s.NotNil(d)

	err := d.Initialize()

	s.NotNil(err)
	s.EqualValues(0, len(d.nodes))
}

func (s *DisqueSuite) TestInitWithZeroHosts() {
	hosts := []string{}
	d := NewDisque(hosts, 1000)

	err := d.Initialize()

	s.NotNil(err)
	s.EqualValues(0, len(d.nodes))
}

func (s *DisqueSuite) TestPickClientBelowClientSelectionCount() {
	hosts := []string{}
	d := NewDisque(hosts, 1000)
	d.prefix = "host1"
	d.count = 999

	d.pickClient()

	s.Equal("host1", d.prefix)
}
func (s *DisqueSuite) TestPickClientWithEmptyStats() {
	hosts := []string{}
	d := NewDisque(hosts, 1000)
	d.prefix = "host1"
	d.count = 1000

	d.pickClient()

	s.Equal("host1", d.prefix)
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

	s.Equal("host2", d.prefix)
	s.Equal(0, len(d.stats))
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

	s.Equal("host1", d.prefix)
	s.Equal(2, len(d.stats))
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

	s.Equal("host1", d.prefix)
	s.Equal(2, len(d.stats))
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

	s.Equal("host1", d.prefix)
	s.Equal("example.com:7711", d.host)
	s.Equal(2, len(d.stats))
}

func (s *DisqueSuite) TestPushWithEmptyOptions() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	options := make(map[string]string)

	jobID, err := d.PushWithOptions("queue1", "asdf", time.Second, options)

	s.NotNil(jobID)
	s.NotEqual("", jobID)
	s.Nil(err)
}

func (s *DisqueSuite) TestPushWithOptions() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	options := make(map[string]string)
	options["TTL"] = "60"
	options["ASYNC"] = "true"

	jobID, err := d.PushWithOptions("queue1", "asdf", time.Second, options)

	s.NotNil(jobID)
	s.NotEqual("", jobID)
	s.Nil(err)
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
	jobID, err := d.PushWithOptions("queue1", "asdf", time.Second, options)

	s.NotNil(jobID)
	s.NotEqual("", jobID)
	s.Nil(err)
}

func (s *DisqueSuite) TestGetJobDetailsWithInvalidJobID() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()

	jobDetails, err := d.GetJobDetails("asdfasdfasdfbogus")
	s.NotNil(err)
	s.Nil(jobDetails)
}

func (s *DisqueSuite) TestGetJobDetailsWithAckdJobID() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()

	jobID, err := d.Push("queue5000", "asdf", time.Second)

	var jobDetails *JobDetails
	jobDetails, err = d.GetJobDetails(jobID)
	s.NotNil(jobDetails)
	s.Nil(err)

	var job *Job
	job, err = d.Fetch("queue5000", time.Second)
	s.Equal(jobID, job.JobID)
	s.NotNil(job)
	s.Nil(err)
	d.Ack(job.JobID)

	jobDetails, err = d.GetJobDetails(job.JobID)
	s.NotNil(err)
	s.Nil(jobDetails)
}

func (s *DisqueSuite) TestGetJobDetails() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()

	jobID, err := d.Push("queue1", "asdf", time.Second)

	s.NotNil(jobID)
	s.NotEqual("", jobID)
	s.Nil(err)

	var jobDetails *JobDetails
	jobDetails, err = d.GetJobDetails(jobID)
	s.Nil(err)
	s.NotNil(jobDetails)

	s.NotNil(jobDetails.CreatedAt)
	s.True(time.Now().After(jobDetails.CreatedAt))
	s.Equal("asdf", jobDetails.Message)
	s.Equal(jobID, jobDetails.JobID)
}

func (s *DisqueSuite) TestPush() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()

	jobID, err := d.Push("queue1", "asdf", time.Second)

	s.NotNil(jobID)
	s.NotEqual("", jobID)
	s.Nil(err)
}

func (s *DisqueSuite) TestPushToClosedConnection() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	d.Close()

	_, err := d.Push("queue1", "asdf", time.Second)

	s.Nil(err)
}

func (s *DisqueSuite) TestPushToUnreachableNode() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	d.Close()
	d.servers = []string{"127.0.0.1:7722"}

	_, err := d.Push("queue1", "asdf", time.Second)

	s.NotNil(err)
}

func (s *DisqueSuite) TestQueueLength() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	_, err := d.Push("queue3", "asdf", time.Second)
	s.Nil(err)
	s.Equal(0, d.stats[d.prefix])

	var queueLength int
	queueLength, err = d.QueueLength("queue3")
	s.Nil(err)
	s.Equal(1, queueLength)

	var job *Job
	job, err = d.Fetch("queue3", time.Second)
	err = d.Ack(job.JobID)
}

func (s *DisqueSuite) TestQueueLengthOnClosedConnection() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	_, err := d.Push("queue3", "asdf", time.Second)
	s.Nil(err)
	s.Equal(0, d.stats[d.prefix])
	d.Close()

	var queueLength int
	queueLength, err = d.QueueLength("queue3")
	s.Nil(err)
	s.Equal(1, queueLength)

	var job *Job
	job, err = d.Fetch("queue3", time.Second)
	err = d.Ack(job.JobID)
}

func (s *DisqueSuite) TestFetch() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	_, err := d.Push("queue4", "asdf", time.Second)
	s.Nil(err)
	s.Equal(0, d.stats[d.prefix])

	job, err := d.Fetch("queue4", time.Second)
	s.Nil(err)
	s.NotNil(job)
	s.Equal("queue4", job.QueueName)
	s.Equal("asdf", job.Message)
	s.Equal(int64(0), job.Nacks)
	s.Equal(int64(0), job.AdditionalDeliveries)
	s.Equal(job.JobID[2:10], d.prefix)
	s.Equal(1, d.stats[d.prefix])
	s.Equal(1, d.count)

	// verify the NACK count in job details
	var jobDetails *JobDetails
	jobDetails, err = d.GetJobDetails(job.JobID)
	s.Equal(int64(0), jobDetails.Nacks)
}

func (s *DisqueSuite) TestFetchAndNack() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	_, err := d.Push("queue6", "asdf", time.Second)
	s.Nil(err)
	s.Equal(0, d.stats[d.prefix])

	// fetch an item
	job, err := d.Fetch("queue6", time.Second)
	s.Nil(err)
	s.NotNil(job)
	s.Equal(int64(0), job.Nacks)

	// send a NACK for the job, putting it back on the queue
	err = d.Nack(job.JobID)
	s.Nil(err)

	// FETCH 2
	job, err = d.Fetch("queue6", time.Second)
	s.Nil(err)
	s.NotNil(job)
	s.Equal("queue6", job.QueueName)
	s.Equal("asdf", job.Message)
	s.Equal(job.JobID[2:10], d.prefix)
	s.Equal(2, d.stats[d.prefix])
	s.Equal(int64(1), job.Nacks)

	// verify the NACK count in job details
	var jobDetails *JobDetails
	jobDetails, err = d.GetJobDetails(job.JobID)
	s.Equal(int64(1), jobDetails.Nacks)

	// send a NACK for the job, putting it back on the queue
	err = d.Nack(job.JobID)
	s.Nil(err)

	// FETCH 3
	job, err = d.Fetch("queue6", time.Second)
	s.Nil(err)
	s.NotNil(job)
	s.Equal("queue6", job.QueueName)
	s.Equal("asdf", job.Message)
	s.Equal(job.JobID[2:10], d.prefix)
	s.Equal(3, d.stats[d.prefix])
	s.EqualValues(2, job.Nacks)

	// verify the NACK count in job details
	jobDetails, err = d.GetJobDetails(job.JobID)
	s.EqualValues(2, jobDetails.Nacks)

	err = d.Ack(job.JobID)
}

func (s *DisqueSuite) TestFetchWithNoJobs() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	_, err := d.Push("queue4", "asdf", time.Second)
	s.Nil(err)
	s.Equal(0, d.stats[d.prefix])

	job, err := d.Fetch("emptyqueue", time.Second)
	s.Nil(err)
	s.Nil(job)
	s.Equal(0, d.stats[d.prefix])
}

func (s *DisqueSuite) TestFetchWithNoJobsWithNoHang() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	_, err := d.Push("queue4", "asdf", time.Second)
	s.Nil(err)
	s.Equal(0, d.stats[d.prefix])

	ticker := time.NewTicker(3 * time.Second)
	jobChan := make(chan bool)
	rtChan := make(chan bool)
	go func() {
		for {
			select {
			case t := <-ticker.C:
				log.Printf("ticker at %v", t.Second())
				rtChan <- false
				return
			case <-jobChan:
				log.Println("Sent rtChan")
				rtChan <- true
				return
			}
		}
	}()

	job, err := d.FetchMultipleNoHang("emptyqueue", 1, 3*time.Second)
	log.Println("Sent JobChan")
	s.Nil(err)
	// job is the empty job, but not nil
	s.NotNil(job)
	jobChan <- true
	ticker.Stop()
	r := <-rtChan
	if !r {
		return
	} else {
		return
	}
}

func (s *DisqueSuite) TestFetchWithMultipleJobs() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	_, err := d.Push("queue5", "msg1", time.Second)
	_, err = d.Push("queue5", "msg2", time.Second)
	_, err = d.Push("queue5", "msg3", time.Second)

	jobs, err := d.FetchMultiple("queue5", 2, time.Second)
	s.Nil(err)
	s.Equal(2, len(jobs))
	s.Equal(2, d.count)

	jobs, err = d.FetchMultiple("queue5", 2, time.Second)
	s.Nil(err)
	s.EqualValues(1, len(jobs))
	s.Equal(4, d.count)
}

func (s *DisqueSuite) TestFetchMultipleWithNoJobs() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	_, err := d.Push("queue4", "asdf", time.Second)
	s.Nil(err)
	s.Equal(0, d.stats[d.prefix])

	jobs, err := d.FetchMultiple("emptyqueue", 1, time.Second)
	s.Nil(err)
	s.NotNil(jobs)
	s.Equal(0, len(jobs))
	s.Equal(0, d.stats[d.prefix])
}

func (s *DisqueSuite) TestAck() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()
	_, err := d.Push("queue2", "asdf", time.Second)
	s.Nil(err)

	job, err := d.Fetch("queue2", time.Second)
	s.Nil(err)

	err = d.Ack(job.JobID)
	s.Nil(err)
}

func (s *DisqueSuite) TestAckWithMalformedJobID() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()

	err := d.Ack("foobaz")
	s.NotNil(err)
}

func (s *DisqueSuite) TestDeleteJob() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()

	jobID, err := d.Push("queue3", "todelete", time.Second)
	s.Nil(err)

	err = d.Delete(jobID)
	s.Nil(err)

	// verify that the job details are unretrievable after deleting the job
	var jobDetails *JobDetails
	jobDetails, err = d.GetJobDetails(jobID)
	s.Nil(jobDetails)
	s.NotNil(err)
}

func (s *DisqueSuite) TestDeleteJobWithUnknownJobID() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()

	err := d.Delete("badId")
	s.NotNil(err)
}

func BenchmarkPush(b *testing.B) {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()

	b.ResetTimer()
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

	b.ResetTimer()
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

func BenchmarkGetJobDetails(b *testing.B) {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts, 1000)
	d.Initialize()

	jobID, _ := d.Push("queueGetJobDetailsBench", "asdf", time.Second)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.GetJobDetails(jobID)
	}
}
