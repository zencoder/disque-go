# disque-go [![Circle CI](https://circleci.com/gh/zencoder/disque-go.svg?style=svg)](https://circleci.com/gh/zencoder/disque-go)

[Go](https://www.golang.org) client for the [Disque server](https://github.com/antirez/disque)

###Dependencies
The [Redigo](https://github.com/garyburd/redigo) Redis client is the only dependency used by `disque-go`. Dependencies are managed with [Godep](https://github.com/tools/godep).

###Documentation
 * [API Reference](http://godoc.org/github.com/zencoder/disque-go/disque)

###Installation
```shell
go get github.com/zencoder/disque-go/disque
```

###Usage
Begin by instantiating and initializing a Disque client:
```go
import (
  "github.com/zencoder/disque-go/disque"
)

...

hosts := []string{"127.0.0.1:7711"} // array of 1 or more Disque servers
cycle := 1000                       // check connection stats every 1000 Fetch's
d := disque.NewDisque(hosts, cycle)
err := d.Initialize()
```
This will yield a Disque client instance `d` that is configured to use the Disque server at 127.0.0.1:7711 and its cluster members, if any.

Next, you can push a job to a Disque queue by invoking the `Push` or `PushWithOptions` methods.
```go
// Push with default settings
queueName := "queue_name"
jobDetails := "job"
timeout := 1*time.Second          // take no long than 1 second to enqueue the message
err = d.Push(queueName, jobDetails, timeout)

// Push with custom options
options = make(map[string]string)
options["TTL"] = "60"            // 60 second TTL on the job message
options["ASYNC"] = "true"        // push the message asynchronously
err = d.PushWithOptions(queueName, jobDetails, timeout, options)
```

Find the length of a queue using the `QueueLength` function:
```go
var queueLength int
queueLength, err = d.QueueLength(queueName)
```

Fetch a single job using the `Fetch` function:
```go
var job *Job
job, err = d.Fetch(queueName, timeout)   // retrieve a single job, taking no longer than timeout (1 second) to return
```

Fetch multiple jobs using the `FetchMultiple` function:
```go
count := 5
var jobs []*Job
jobs, err = d.FetchMultiple(queueName, count, timeout)   // retrieve up to 5 Jobs, taking no longer than timeout (1 second) to return
```

Acknowledge receipt and processing of a message by invoking the `Ack` function:
```go
err = d.Ack(job.MessageId)
```

Lastly, close the Disque client connection when finished:
```go
err = d.Close()
```

That's it (for now)!

###License
`disque-go` is available under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html).
