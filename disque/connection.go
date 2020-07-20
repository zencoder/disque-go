package disque

import (
	"errors"
	"log"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
)

// Disque connection type
type Disque struct {
	servers []string
	cycle   int

	count int

	nodes  map[string]string
	stats  map[string]int
	prefix string
	client redis.Conn
	host   string
}

// Job represents a Disque job
type Job struct {
	QueueName            string
	JobID                string
	Message              string
	Nacks                int64
	AdditionalDeliveries int64
}

// JobDetails contains details for a specific Disque job
type JobDetails struct {
	JobID                string
	QueueName            string
	State                string
	ReplicationFactor    int
	TTL                  time.Duration
	CreatedAt            time.Time
	Delay                time.Duration
	Retry                time.Duration
	Nacks                int64
	AdditionalDeliveries int64
	NodesDelivered       []string
	NodesConfirmed       []string
	NextRequeueWithin    time.Duration
	NextAwakeWithin      time.Duration
	Message              string
}

// NewDisque instantiates a new Disque connection
func NewDisque(servers []string, cycle int) *Disque {
	return &Disque{
		servers: servers,
		cycle:   cycle,
		nodes:   make(map[string]string),
		stats:   make(map[string]int),
	}
}

// Initialize the connection, including the exploration of nodes
// participating in the cluster.
func (d *Disque) Initialize() (err error) {
	return d.explore()
}

// Close the main connection maintained by this Disque instance
func (d *Disque) Close() {
	d.client.Close()
}

// Push job onto a Disque queue with the default set of options
func (d *Disque) Push(queueName string, job string, timeout time.Duration) (jobID string, err error) {
	args := redis.Args{}.
		Add(queueName).
		Add(job).
		Add(int64(timeout.Seconds() * 1000))
	jobID, err = redis.String(d.call("ADDJOB", args))
	return
}

// PushWithOptions pushes a job onto a Disque queue with options given in the options map
//
//     ADDJOB queue_name job <ms-timeout>
//       [REPLICATE <count>]
//       [DELAY <sec>]
//       [RETRY <sec>]
//       [TTL <sec>]
//       [MAXLEN <count>]
//       [ASYNC]
//
// Example:
//     options := make(map[string]string)
//     options["DELAY"] = 30
//     options["ASYNC"] = true
//     d.PushWithOptions("queue_name", "job", 1*time.Second, options)
func (d *Disque) PushWithOptions(queueName string, job string, timeout time.Duration, options map[string]string) (jobID string, err error) {
	if len(options) == 0 {
		jobID, err = d.Push(queueName, job, timeout)
	} else {
		args := redis.Args{}.
			Add(queueName).
			Add(job).
			Add(int64(timeout.Seconds() * 1000)).
			AddFlat(optionsToArguments(options))
		jobID, err = redis.String(d.call("ADDJOB", args))
	}
	return
}

// Ack will acknowledge receipt and processing of a message
func (d *Disque) Ack(jobID string) (err error) {
	_, err = d.call("ACKJOB", redis.Args{}.Add(jobID))
	return
}

// Nack instructs Disque to put back the job in the queue ASAP.
func (d *Disque) Nack(jobID string) (err error) {
	_, err = d.call("NACK", redis.Args{}.Add(jobID))
	return
}

// Delete a job that was enqueued on the cluster
func (d *Disque) Delete(jobID string) (err error) {
	_, err = d.call("DELJOB", redis.Args{}.Add(jobID))
	return
}

// GetJobDetails will retrieve details for an existing job
func (d *Disque) GetJobDetails(jobID string) (jobDetails *JobDetails, err error) {
	var jobDetailsMap []interface{}
	if jobDetailsMap, err = redis.Values(d.call("SHOW", redis.Args{}.Add(jobID))); err == nil {
		var repl, ttl, delay, retry, nextRequeueWithin, nextAwakeWithin int
		var ctime, nacks, additionalDeliveries int64
		var nodesDelivered, nodesConfirmed []string

		repl, err = redis.Int(jobDetailsMap[7], err)
		ttl, err = redis.Int(jobDetailsMap[9], err)
		ctime, err = redis.Int64(jobDetailsMap[11], err)
		delay, err = redis.Int(jobDetailsMap[13], err)
		retry, err = redis.Int(jobDetailsMap[15], err)
		nacks, err = redis.Int64(jobDetailsMap[17], err)
		additionalDeliveries, err = redis.Int64(jobDetailsMap[19], err)
		nodesDelivered, err = redis.Strings(jobDetailsMap[21], err)
		nodesConfirmed, err = redis.Strings(jobDetailsMap[23], err)
		nextRequeueWithin, err = redis.Int(jobDetailsMap[25], err)
		nextAwakeWithin, err = redis.Int(jobDetailsMap[27], err)

		if err == nil {
			jobDetails = &JobDetails{
				JobID:                string(jobDetailsMap[1].([]byte)),
				QueueName:            string(jobDetailsMap[3].([]byte)),
				State:                string(jobDetailsMap[5].([]byte)),
				ReplicationFactor:    repl,
				TTL:                  time.Duration(ttl) * time.Second,
				CreatedAt:            time.Unix(0, ctime),
				Delay:                time.Duration(delay) * time.Second,
				Retry:                time.Duration(retry) * time.Second,
				Nacks:                nacks,
				AdditionalDeliveries: additionalDeliveries,
				NodesDelivered:       nodesDelivered,
				NodesConfirmed:       nodesConfirmed,
				NextRequeueWithin:    time.Duration(nextRequeueWithin/1000) * time.Second,
				NextAwakeWithin:      time.Duration(nextAwakeWithin/1000) * time.Second,
				Message:              string(jobDetailsMap[29].([]byte)),
			}
		}
	}
	return
}

// QueueLength will retrieve length of queue
func (d *Disque) QueueLength(queueName string) (queueLength int, err error) {
	return redis.Int(d.call("QLEN", redis.Args{}.Add(queueName)))
}

// Fetch a single job from a Disque queue.
func (d *Disque) Fetch(queueName string, timeout time.Duration) (job *Job, err error) {
	var jobs []*Job
	if jobs, err = d.FetchMultiple(queueName, 1, timeout); err == nil {
		if len(jobs) > 0 {
			job = jobs[0]
		}
	}
	return
}

// FetchMultiple will retrieve multiple jobs from a Disque queue.
func (d *Disque) FetchMultiple(queueName string, count int, timeout time.Duration) (jobs []*Job, err error) {
	return d.fetch(queueName, count, timeout)
}

// FetchMultipleNoHang will retrieve multiple jobs from a Disque queue.
// But it will work with NOHANG option.
// Check disque doc for more details: https://github.com/antirez/disque#getjob-nohang-timeout-ms-timeout-count-count-withcounters-from-queue1-queue2--queuen
func (d *Disque) FetchMultipleNoHang(queueName string, count int, timeout time.Duration) (jobs []*Job, err error) {
	return d.fetch(queueName, count, timeout, noHangOption{})
}

func (d *Disque) fetch(queueName string, count int, timeout time.Duration, opts ...fetchOption) (jobs []*Job, err error) {
	jobs = make([]*Job, 0)
	if err = d.pickClient(); err == nil {

		args := redis.Args{}
		for _, opt := range withFetchOption(queueName, timeout, count, opts...) {
			args = append(args, opt.Name())
			if len(opt.Args()) == 0 {
				continue
			}
			args = append(args, opt.Args()...)
		}
		if values, err := redis.Values(d.call("GETJOB", args)); err == nil {
			for _, job := range values {
				if jobValues, err := redis.Values(job, err); err == nil {
					var nacks, additionalDeliveries int64
					nacks, err = redis.Int64(jobValues[4], err)
					// TODO: we should handle this redis type conversion error
					additionalDeliveries, err = redis.Int64(jobValues[6], err)
					// TODO: and this error
					j := &Job{
						QueueName:            string(jobValues[0].([]byte)),
						JobID:                string(jobValues[1].([]byte)),
						Message:              string(jobValues[2].([]byte)),
						Nacks:                nacks,
						AdditionalDeliveries: additionalDeliveries,
					}
					jobs = append(jobs, j)

					// update stats using fragment of the job-id
					statsKey := j.JobID[2:10]
					d.stats[statsKey] = d.stats[statsKey] + 1
				}
			}
		}
	}
	d.count += count
	return jobs, err
}

func (d *Disque) call(command string, args redis.Args) (reply interface{}, err error) {
	if reply, err = d.client.Do(command, args...); err != nil {
		if err = d.explore(); err == nil {
			reply, err = d.client.Do(command, args...)
		}
	}
	return
}

func optionsToArguments(options map[string]string) (arguments []string) {
	arguments = make([]string, 0)
	for key, value := range options {
		if value == "true" {
			arguments = append(arguments, key)
		} else {
			arguments = append(arguments, key, value)
		}
	}
	return
}

func (d *Disque) pickClient() (err error) {
	if d.count == d.cycle {
		d.count = 0
		sortedHosts := reverseSortMapByValue(d.stats)

		if len(sortedHosts) > 0 {
			optimalHostID := sortedHosts[0].Key
			if optimalHostID != d.prefix {
				// a different optimal host has been discovered
				if val, ok := d.nodes[optimalHostID]; ok {
					// close old main client connection if it exists
					if d.client != nil {
						d.client.Close()
					}

					// configure main client
					if d.client, err = redis.Dial("tcp", d.nodes[optimalHostID]); err == nil {
						// keep track of selected node
						d.prefix = optimalHostID
						d.host = val

						// clear stats
						d.stats = make(map[string]int)
					}
				}
			}
		}
	}
	return
}

func (d *Disque) explore() (err error) {
	// clear nodes
	d.nodes = map[string]string{}

	for _, host := range d.servers {
		var scout redis.Conn
		if scout, err = redis.Dial("tcp", host); err == nil {
			defer scout.Close()

			if lines, err := redis.String(scout.Do("CLUSTER", "NODES")); err == nil {
				for _, line := range strings.Split(lines, "\n") {
					if strings.TrimSpace(line) != "" {
						fields := strings.Fields(line)

						id := fields[0]
						clusterHost := fields[1]
						flag := fields[2]
						prefix := id[0:8]

						if flag == "myself" {
							// close main client if it exists
							if d.client != nil {
								d.client.Close()
							}

							// configure main client
							if d.client, err = redis.Dial("tcp", host); err == nil {
								// keep track of selected node
								d.prefix = prefix
							}
						}

						d.nodes[prefix] = clusterHost
					}
				}
				return err
			}
			log.Printf("Error returned when querying for cluster nodes on host: %s, exception: %s", host, err)
		} else {
			log.Printf("Error while exploring connection to host: %s, exception: %s", host, err)
		}
	}

	if len(d.nodes) == 0 {
		err = errors.New("Nodes unavailable")
	}
	return err
}
