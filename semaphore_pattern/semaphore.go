package main

import (
	"context"
	"sync"
	"sync/atomic"
)

type SemaphoreRunner struct {
	limit int
}

func NewSemaphoreRunner(limit int) *SemaphoreRunner {
	return &SemaphoreRunner{
		limit: limit,
	}
}

func (r *SemaphoreRunner) Run(ctx context.Context, jobs []int, process func(int)) {
	sem := make(chan struct{}, r.limit)

	var wg sync.WaitGroup

	for _, job := range jobs {
		wg.Add(1)

		go func(job int) {
			defer wg.Done()

			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}

			defer func() {
				<-sem
			}()

			process(job)
		}(job)
	}

	wg.Wait()
}

// RunWithMetrics is useful for comparing the actual concurrency.
func (r *SemaphoreRunner) RunWithMetrics(
	ctx context.Context,
	jobs []int,
	process func(int),
) Metrics {
	sem := make(chan struct{}, r.limit)

	var wg sync.WaitGroup
	var active atomic.Int64
	var maxActive atomic.Int64

	for _, job := range jobs {
		wg.Add(1)

		go func(job int) {
			defer wg.Done()

			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}

			defer func() {
				<-sem
			}()

			current := active.Add(1)
			updateMax(&maxActive, current)

			defer active.Add(-1)

			process(job)
		}(job)
	}

	wg.Wait()

	return Metrics{
		MaxActive: maxActive.Load(),
	}
}
