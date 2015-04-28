package disque

import (
	"errors"
	"log"
	"strings"

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
	MessageId string
	Message   string
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
func (d *Disque) Close() error {
	return d.client.Close()
}

// Push job onto a Disque queue with the default set of options
func (d *Disque) Push(queueName string, job string, timeout int64) (err error) {
	if _, err = d.client.Do("ADDJOB", queueName, job, timeout); err != nil {
		if err = d.explore(); err == nil {
			_, err = d.client.Do("ADDJOB", queueName, job, timeout)
		}
	}
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
//     d.PushWithOptions("queue_name", "job", 0, options)
func (d *Disque) PushWithOptions(queueName string, job string, timeout int64, options map[string]string) (err error) {
	if len(options) == 0 {
		err = d.Push(queueName, job, timeout)
	} else {
		args := redis.Args{}.Add(queueName).Add(job).Add(timeout).AddFlat(optionsToArguments(options))
		if _, err = d.client.Do("ADDJOB", args...); err != nil {
			if err = d.explore(); err == nil {
				_, err = d.client.Do("ADDJOB", args...)
			}
		}
	}
	return
}

// Acknowledge receipt and processing of a message
func (d *Disque) Ack(messageId string) (err error) {
	if _, err = d.client.Do("ACKJOB", messageId); err != nil {
		if err = d.explore(); err == nil {
			_, err = d.client.Do("ACKJOB", messageId)
		}
	}
	return
}

// Retrieve length of queue
func (d *Disque) QueueLength(queueName string) (queueLength int, err error) {
	if queueLength, err = redis.Int(d.client.Do("QLEN", queueName)); err != nil {
		if err = d.explore(); err == nil {
			queueLength, err = redis.Int(d.client.Do("QLEN", queueName))
		}
	}
	return
}

// Fetch jobs from a Disque queue.
func (d *Disque) Fetch(queueName string, count int, timeout int64) (jobs []*Job, err error) {
	jobs = make([]*Job, 0)
	if err = d.pickClient(); err == nil {
		if values, err := redis.Values(d.client.Do("GETJOB", "TIMEOUT", timeout, "COUNT", count, "FROM", queueName)); err == nil {
			for _, job := range values {
				if jobValues, err := redis.Strings(job, err); err == nil {
					jobs = append(jobs, &Job{QueueName: jobValues[0], MessageId: jobValues[1], Message: jobValues[2]})

					// update stats using fragment of the message-id
					statsKey := jobValues[1][2:10]
					d.stats[statsKey] = d.stats[statsKey] + 1
				}
			}
		}
	}
	return jobs, err
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
