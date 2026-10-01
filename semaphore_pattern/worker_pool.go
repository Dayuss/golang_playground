package main

import (
	"context"
	"sync"
	"sync/atomic"
)

type WorkerPool struct {
	workers int
}

func NewWorkerPool(workers int) *WorkerPool {
	return &WorkerPool{
		workers: workers,
	}
}

func (p *WorkerPool) Run(
	ctx context.Context,
	jobs []int,
	process func(int),
) {
	jobCh := make(chan int)

	var wg sync.WaitGroup

	for i := 0; i < p.workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				select {
				case job, ok := <-jobCh:
					if !ok {
						return
					}

					process(job)

				case <-ctx.Done():
					return
				}
			}
		}()
	}

	for _, job := range jobs {
		select {
		case jobCh <- job:
		case <-ctx.Done():
			close(jobCh)
			wg.Wait()
			return
		}
	}

	close(jobCh)
	wg.Wait()
}

func (p *WorkerPool) RunWithMetrics(
	ctx context.Context,
	jobs []int,
	process func(int),
) Metrics {
	jobCh := make(chan int)

	var wg sync.WaitGroup
	var active atomic.Int64
	var maxActive atomic.Int64

	for i := 0; i < p.workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				select {
				case job, ok := <-jobCh:
					if !ok {
						return
					}

					current := active.Add(1)
					updateMax(&maxActive, current)

					process(job)

					active.Add(-1)

				case <-ctx.Done():
					return
				}
			}
		}()
	}

	for _, job := range jobs {
		select {
		case jobCh <- job:
		case <-ctx.Done():
			close(jobCh)
			wg.Wait()
			return Metrics{
				MaxActive: maxActive.Load(),
			}
		}
	}

	close(jobCh)
	wg.Wait()

	return Metrics{
		MaxActive: maxActive.Load(),
	}
}
