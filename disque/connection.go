package disque

import (
	"errors"
	"log"
	"strings"

	"github.com/garyburd/redigo/redis"
)

type Disque struct {
	servers []string
	nodes   map[string]string
	prefix  string
	client  redis.Conn
	scout   redis.Conn
}

var (
	NEWLINE = "\n"
)

func NewDisque(servers []string) *Disque {
	return &Disque{
		servers: servers,
	}
}

// Identify Disque nodes in the cluster
func (d *Disque) Initialize() (err error) {
	err = d.explore()
	return err
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

						if flag == "myself" {
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
