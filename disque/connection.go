package disque

import (
	"errors"
	"log"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
)

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

type Job struct {
	QueueName string
	JobId     string
	Message   string
}

type JobDetails struct {
	JobId             string
	QueueName         string
	State             string
	ReplicationFactor int
	TTL               time.Duration
	CreatedAt         time.Time
	Delay             time.Duration
	Retry             time.Duration
	NodesDelivered    []string
	NodesConfirmed    []string
	NextRequeueWithin time.Duration
	NextAwakeWithin   time.Duration
	Message           string
}

// Instantiate a new Disque connection
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
func (d *Disque) Push(queueName string, job string, timeout time.Duration) (jobId string, err error) {
	args := redis.Args{}.
		Add(queueName).
		Add(job).
		Add(int64(timeout.Seconds() * 1000))
	jobId, err = redis.String(d.call("ADDJOB", args))
	return
}

// Push job onto a Disque queue with options given in the options map
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
func (d *Disque) PushWithOptions(queueName string, job string, timeout time.Duration, options map[string]string) (jobId string, err error) {
	if len(options) == 0 {
		jobId, err = d.Push(queueName, job, timeout)
	} else {
		args := redis.Args{}.
			Add(queueName).
			Add(job).
			Add(int64(timeout.Seconds() * 1000)).
			AddFlat(optionsToArguments(options))
		jobId, err = redis.String(d.call("ADDJOB", args))
	}
	return
}

// Acknowledge receipt and processing of a message
func (d *Disque) Ack(jobId string) (err error) {
	_, err = d.call("ACKJOB", redis.Args{}.Add(jobId))
	return
}

// Retrieve details for an existing job
func (d *Disque) GetJobDetails(jobId string) (jobDetails *JobDetails, err error) {
	var jobDetailsMap []interface{}
	if jobDetailsMap, err = redis.Values(d.call("SHOW", redis.Args{}.Add(jobId))); err == nil {
		var repl, ttl, delay, retry, nextRequeueWithin, nextAwakeWithin int
		var ctime int64
		var nodesDelivered, nodesConfirmed []string

		repl, err = redis.Int(jobDetailsMap[7], err)
		ttl, err = redis.Int(jobDetailsMap[9], err)
		ctime, err = redis.Int64(jobDetailsMap[11], err)
		delay, err = redis.Int(jobDetailsMap[13], err)
		retry, err = redis.Int(jobDetailsMap[15], err)
		nodesDelivered, err = redis.Strings(jobDetailsMap[17], err)
		nodesConfirmed, err = redis.Strings(jobDetailsMap[19], err)
		nextRequeueWithin, err = redis.Int(jobDetailsMap[21], err)
		nextAwakeWithin, err = redis.Int(jobDetailsMap[23], err)

		if err == nil {
			jobDetails = &JobDetails{
				JobId:             string(jobDetailsMap[1].([]byte)),
				QueueName:         string(jobDetailsMap[3].([]byte)),
				State:             string(jobDetailsMap[5].([]byte)),
				ReplicationFactor: repl,
				TTL:               time.Duration(ttl) * time.Second,
				CreatedAt:         time.Unix(0, ctime),
				Delay:             time.Duration(delay) * time.Second,
				Retry:             time.Duration(retry) * time.Second,
				NodesDelivered:    nodesDelivered,
				NodesConfirmed:    nodesConfirmed,
				NextRequeueWithin: time.Duration(nextRequeueWithin/1000) * time.Second,
				NextAwakeWithin:   time.Duration(nextAwakeWithin/1000) * time.Second,
				Message:           string(jobDetailsMap[25].([]byte)),
			}
		}
	}
	return
}

// Retrieve length of queue
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

// Fetch jobs from a Disque queue.
func (d *Disque) FetchMultiple(queueName string, count int, timeout time.Duration) (jobs []*Job, err error) {
	jobs = make([]*Job, 0)
	if err = d.pickClient(); err == nil {
		if values, err := redis.Values(d.client.Do("GETJOB", "TIMEOUT", int64(timeout.Seconds()*1000), "COUNT", count, "FROM", queueName)); err == nil {
			for _, job := range values {
				if jobValues, err := redis.Strings(job, err); err == nil {
					jobs = append(jobs, &Job{QueueName: jobValues[0], JobId: jobValues[1], Message: jobValues[2]})

					// update stats using fragment of the job-id
					statsKey := jobValues[1][2:10]
					d.stats[statsKey] = d.stats[statsKey] + 1
				}
			}
		}
	}
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
			optimalHostId := sortedHosts[0].Key
			if optimalHostId != d.prefix {
				// a different optimal host has been discovered
				if val, ok := d.nodes[optimalHostId]; ok {
					// close old main client connection if it exists
					if d.client != nil {
						d.client.Close()
					}

					// configure main client
					if d.client, err = redis.Dial("tcp", d.nodes[optimalHostId]); err == nil {
						// keep track of selected node
						d.prefix = optimalHostId
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
			} else {
				log.Printf("Error returned when querying for cluster nodes on host: %s, exception: %s", host, err)
			}
		} else {
			log.Printf("Error while exploring connection to host: %s, exception: %s", host, err)
		}
	}

	if len(d.nodes) == 0 {
		err = errors.New("Nodes unavailable")
	}
	return err
}
