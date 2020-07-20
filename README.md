# disque-go

[![codecov](https://codecov.io/gh/ezbuy/disque-go/branch/master/graph/badge.svg)](https://codecov.io/gh/ezbuy/disque-go)

**NOTICE**: This repo is forked from [zencoder/disque-go](github.com/zencoder/disque-go), and move the disque with Go forward.

[Go](https://www.golang.org) client for the [Disque server](https://github.com/antirez/disque)

### Documentation
 * [API Reference](https://pkg.go.dev/github.com/ezbuy/disque-go/disque)

### Installation
```shell
go get github.com/ezbuy/disque-go/disque
```

### Usage
#### Connection Pool
Instantiate the pool as follows:
```go
import (
  "github.com/ezbuy/disque-go/disque"
  "golang.org/x/net/context"
)

...

hosts := []string{"127.0.0.1:7711"} // array of 1 or more Disque servers
cycle := 1000                       // check connection stats every 1000 Fetch's
capacity := 5                       // initial capacity of the pool
maxCapacity := 10                   // max capacity that the pool can be resized to
idleTimeout := 15 * time.Minute     // timeout for idle connections
var p *disque.Pool
p = disque.NewPool(hosts, cycle, capacity, maxCapacity, idleTimeout)
```

Next, get a handle to a connection from the pool, specifying a [context](https://godoc.org/golang.org/x/net/context) that controls how long to wait for a connection to be retrieved:
```go
var d *disque.Disque
var err error
d, err = p.Get(context.Background())   // get a connection from the pool
defer p.Put(d)                         // return a connection to the pool

... (use the connection to interact with Disque)...

```

To shutdown the connection pool, such as when the application is exiting, invoke the `Close` function:
```go
p.Close()           // close the pool, waits for all connections to be returned
```

#### Single Connection
Begin by instantiating and initializing a Disque client connection:
```go
import (
  "github.com/ezbuy/disque-go/disque"
)

...

hosts := []string{"127.0.0.1:7711"} // array of 1 or more Disque servers
cycle := 1000                       // check connection stats every 1000 Fetch's
var d *disque.Disque
var err error
d = disque.NewDisque(hosts, cycle)
err = d.Initialize()
```
This will yield a Disque client instance `d` that is configured to use the Disque server at 127.0.0.1:7711 and its cluster members, if any.


Close the Disque client connection when finished:
```go
err = d.Close()
```

#### Disque Operations
You can push a job to a Disque queue by invoking the `Push` or `PushWithOptions` methods.
```go
// Push with default settings
queueName := "queue_name"
jobDetails := "job"
timeout := time.Second          // take no long than 1 second to enqueue the message
var jobID string
jobID, err = d.Push(queueName, jobDetails, timeout)

// Push with custom options
options = make(map[string]string)
options["TTL"] = "60"            // 60 second TTL on the job message
options["ASYNC"] = "true"        // push the message asynchronously
jobID, err = d.PushWithOptions(queueName, jobDetails, timeout, options)
```

Find the length of a queue using the `QueueLength` function:
```go
var queueLength int
queueLength, err = d.QueueLength(queueName)
```

Fetch a single job using the `Fetch` function:
```go
var job *disque.Job
job, err = d.Fetch(queueName, timeout)   // retrieve a single job, taking no longer than timeout (1 second) to return
```

Fetch multiple jobs using the `FetchMultiple` function:
```go
count := 5
var jobs []*disque.Job
jobs, err = d.FetchMultiple(queueName, count, timeout)   // retrieve up to 5 Jobs, taking no longer than timeout (1 second) to return
```

Retrieve details for an enqueued job before it has been acknowledged:
```go
var jobDetails *disque.JobDetails
jobDetails, err = d.GetJobDetails(jobID)
```

Enqueued messages can be deleted using their Job-Id:
```go
err = d.Delete(jobID)
```

Acknowledge receipt and processing of a message by invoking the `Ack` function:
```go
err = d.Ack(job.JobID)
```

That's it (for now)!

### License
`disque-go` is available under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html).
