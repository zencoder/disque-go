package disque

import (
	"errors"
	"log"
	"strings"

	"github.com/garyburd/redigo/redis"
)

type Disque struct {
	servers []string
	cycle   int32

	nodes  map[string]string
	prefix string
	client redis.Conn
	scout  redis.Conn
}

const (
	NEWLINE     = "\n"
	MYSELF_FLAG = "myself"
)

// Instantiate a new Disque connection
func NewDisque(servers []string, cycle int32) *Disque {
	return &Disque{
		servers: servers,
		cycle:   cycle,
	}
}

// Initialize the connection, including the exploration of nodes
// participating in the cluster.
func (d *Disque) Initialize() (err error) {
	return d.explore()
}

func (d *Disque) explore() error {
	// clear nodes
	d.nodes = map[string]string{}

	var err error
	for _, host := range d.servers {
		log.Printf("Evaluating host: %s", host)
		hostURL := host

		if d.scout, err = redis.Dial("tcp", hostURL); err == nil {
			log.Println("Connected to host, finding nodes")
			defer d.scout.Close()

			if lines, err := redis.String(d.scout.Do("CLUSTER", "NODES")); err == nil {
				for _, line := range strings.Split(lines, NEWLINE) {
					if strings.TrimSpace(line) != "" {
						log.Printf("Identifying fields for cluster line: %s", line)
						fields := strings.Fields(line)

						id := fields[0]
						clusterHost := fields[1]
						flag := fields[2]
						prefix := id[0:8]

						if flag == MYSELF_FLAG {
							// configure main client
							if d.client, err = redis.Dial("tcp", hostURL); err == nil {
								// keep track of selected node
								d.prefix = prefix
							}
						}

						d.nodes[prefix] = clusterHost
					}
				}
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
