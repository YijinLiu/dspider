package dspider

import (
	"time"
)

type Retrier interface {
	RunWithRetry(job func() error) error
}

type SimpleRetrier struct {
	Times    int
	Interval time.Duration
}

func (r *SimpleRetrier) RunWithRetry(job func() error) (err error) {
	for i := 0; i <= r.Times; i++ {
		if err = job(); err == nil {
			break
		}
		time.Sleep(r.Interval)
	}
	return
}
