package main

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"time"
)

const (
	totalJobs   = 1000
	concurrency = 10
	jobDuration = 10 * time.Millisecond
)

type Metrics struct {
	MaxActive int64
}

func updateMax(max *atomic.Int64, current int64) {
	for {
		old := max.Load()

		if current <= old {
			return
		}

		if max.CompareAndSwap(old, current) {
			return
		}
	}
}

func createJobs(n int) []int {
	jobs := make([]int, n)

	for i := range jobs {
		jobs[i] = i
	}

	return jobs
}

func processJob(_ int) {
	time.Sleep(jobDuration)
}

func runSemaphore(jobs []int) {
	ctx := context.Background()

	runner := NewSemaphoreRunner(concurrency)

	before := runtime.NumGoroutine()
	start := time.Now()

	metrics := runner.RunWithMetrics(
		ctx,
		jobs,
		processJob,
	)

	duration := time.Since(start)
	after := runtime.NumGoroutine()

	printResult(
		"Semaphore",
		duration,
		before,
		after,
		metrics,
	)
}

func runWorkerPool(jobs []int) {
	ctx := context.Background()

	pool := NewWorkerPool(concurrency)

	before := runtime.NumGoroutine()
	start := time.Now()

	metrics := pool.RunWithMetrics(
		ctx,
		jobs,
		processJob,
	)

	duration := time.Since(start)
	after := runtime.NumGoroutine()

	printResult(
		"Worker Pool",
		duration,
		before,
		after,
		metrics,
	)
}

func printResult(
	name string,
	duration time.Duration,
	before int,
	after int,
	metrics Metrics,
) {
	throughput := float64(totalJobs) / duration.Seconds()

	fmt.Printf("\n%s\n", name)
	fmt.Printf("  jobs:           %d\n", totalJobs)
	fmt.Printf("  concurrency:   %d\n", concurrency)
	fmt.Printf("  duration:       %v\n", duration)
	fmt.Printf("  throughput:     %.2f jobs/sec\n", throughput)
	fmt.Printf("  max active:     %d\n", metrics.MaxActive)
	fmt.Printf("  goroutines:     %d -> %d\n", before, after)
}

func main() {
	jobs := createJobs(totalJobs)

	fmt.Println("Semaphore vs Worker Pool")
	fmt.Println("========================")

	runSemaphore(jobs)
	runWorkerPool(jobs)
}
