package disque

import (
	"fmt"
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

func NewDisque(servers []string) *Disque {
	return &Disque{
		servers: servers,
		nodes:   map[string]string{},
	}
}

// Identify Disque nodes in the cluster
func (d *Disque) Initialize() (err error) {
	d.explore()
	return
}

func (d *Disque) explore() {
	// clear nodes
	d.nodes = map[string]string{}

	var err error
	for _, host := range d.servers {
		hostURL := url(host)
		if d.scout, err = redis.Dial("tcp", hostURL); err == nil {
			defer d.scout.Close()

			var lines []string
			if lines, err = redis.Strings(d.scout.Do("CLUSTER", "NODES")); err == nil {
				var line string
				for _, line = range lines {
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
		}

	}
}

func url(host string) string {
	return fmt.Sprintf("disque://%s", host)
}
