package disque

import (
	"time"
)

type fetchOption interface {
	Name() string
	Args() []interface{}
}

type timeoutOption struct {
	duration time.Duration
}

func (to timeoutOption) Name() string {
	return "TIMEOUT"
}

func (to timeoutOption) Args() []interface{} {
	return []interface{}{int64(to.duration.Seconds()) * 1000}
}

type countOption struct {
	max int
}

func (c countOption) Name() string {
	return "COUNT"
}

func (c countOption) Args() []interface{} {
	return []interface{}{c.max}
}

type withCounterOption struct {
}

func (c withCounterOption) Name() string {
	return "WITHCOUNTERS"
}

func (c withCounterOption) Args() []interface{} {
	return []interface{}{}
}

type fromOption struct {
	queue string
}

func (f fromOption) Name() string {
	return "FROM"
}

func (f fromOption) Args() []interface{} {
	return []interface{}{f.queue}
}

// noHangOption asks the command to not block even if there are no jobs in all the specified queues. This way the caller can just check if there are available jobs without blocking at all.
// SCOPE: GETJOB
// See more at https://github.com/antirez/disque#getjob-nohang-timeout-ms-timeout-count-count-withcounters-from-queue1-queue2--queuen
type noHangOption struct{}

func (nh noHangOption) Name() string {
	return "NOHANG"
}

func (nh noHangOption) Args() []interface{} {
	return []interface{}{}
}

// withFetchOption defines a set of options when fetch the JOB from disque.
// COMMAND: GETJOB [TIMEOUT <ms-timeout>] [COUNT <count>] [WITHCOUNTERS] FROM [queueName]
func withFetchOption(queueName string, timeout time.Duration, count int, options ...fetchOption) []fetchOption {
	var defaultOptions [5]fetchOption
	for _, opt := range options {
		switch opt.Name() {
		case "NOHANG":
			defaultOptions[0] = opt
		case "TIMEOUT":
			defaultOptions[1] = opt
		case "COUNT":
			defaultOptions[2] = opt
		case "WITHCOUNTERS":
			defaultOptions[3] = opt
		case "FROM":
			defaultOptions[4] = opt
		}
	}
	var sortOptions []fetchOption
	for i := 0; i < 5; i++ {
		// use the user-defined option first
		if defaultOptions[i] != nil {
			sortOptions = append(sortOptions, defaultOptions[i])
			continue
		}
		// fallback to default option
		switch i {
		case 0:
			continue
		case 1:
			sortOptions = append(sortOptions, timeoutOption{timeout})
		case 2:
			sortOptions = append(sortOptions, countOption{count})
		case 3:
			sortOptions = append(sortOptions, withCounterOption{})
		case 4:
			sortOptions = append(sortOptions, fromOption{queueName})
		}
	}
	return sortOptions
}
