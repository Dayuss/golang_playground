package main

import (
	"context"
	"sync/atomic"
	"testing"
)

func TestNewWorkerPool_StoresWorkerCount(t *testing.T) {
	tests := []struct {
		name    string
		workers int
	}{
		{name: "single_worker", workers: 1},
		{name: "five_workers", workers: 5},
		{name: "ten_workers", workers: 10},
		{name: "zero_workers", workers: 0},
		{name: "negative_workers", workers: -1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pool := NewWorkerPool(tc.workers)

			if pool == nil {
				t.Fatal("NewWorkerPool returned nil")
			}

			if pool.workers != tc.workers {
				t.Fatalf("workers = %d, want %d", pool.workers, tc.workers)
			}
		})
	}
}

func TestNewWorkerPool_NonPositiveWorkerCount_Behaviour(t *testing.T) {
	// Design gap, documented rather than "fixed": NewWorkerPool accepts any
	// int without validation.
	//
	//   - workers < 1: no worker goroutine is started (the loop bound is never
	//     reached), and because the job channel is unbuffered the feeding loop
	//     blocks forever on a non-empty job list with a live context. Nothing is
	//     processed, so this behaves exactly like a deadlock.
	//   - workers == 0 with an empty job list: the feeding loop body never
	//     runs, so Run returns immediately and safely.
	//
	// The blocked path is intentionally not exercised: asserting that
	// something never finishes requires a timeout, which this suite avoids.
	t.Run("non_positive_workers_with_empty_jobs", func(t *testing.T) {
		for _, workers := range []int{0, -1} {
			t.Run(workerCountName(workers), func(t *testing.T) {
				pool := NewWorkerPool(workers)

				var called atomic.Bool

				done := runAsync(false, pool, context.Background(), nil, func(int) {
					called.Store(true)
				}, nil)

				waitOrFail(t, done, "Run with no workers and no jobs")

				if called.Load() {
					t.Fatal("process was called even though no workers exist")
				}
			})
		}
	})

	t.Run("non_positive_workers_cancelled_context", func(t *testing.T) {
		for _, workers := range []int{0, -1} {
			t.Run(workerCountName(workers), func(t *testing.T) {
				pool := NewWorkerPool(workers)

				recorder := newJobRecorder()

				ctx, cancel := context.WithCancel(context.Background())

				cancel()

				done := runAsync(false, pool, ctx, []int{1, 2, 3}, recorder.record, nil)

				waitOrFail(t, done, "Run with no workers and a cancelled context")

				if got := recorder.total(); got != 0 {
					t.Fatalf("processed %d jobs, want 0: no worker can pick a job up", got)
				}
			})
		}
	})
}

func workerCountName(workers int) string {
	if workers < 0 {
		return "negative_workers"
	}

	return "zero_workers"
}

func TestWorkerPool_Run_ProcessesAllJobs(t *testing.T) {
	const jobsN = 100

	pool := NewWorkerPool(5)

	jobs := createJobs(jobsN)
	recorder := newJobRecorder()

	done := runAsync(false, pool, context.Background(), jobs, recorder.record, nil)

	waitOrFail(t, done, "Run")

	assertExactlyOnce(t, recorder, jobs)
}

func TestWorkerPool_Run_EmptyJobs(t *testing.T) {
	tests := []struct {
		name string
		jobs []int
	}{
		{name: "empty_slice", jobs: []int{}},
		{name: "nil_slice", jobs: nil},
	}

	for _, tc := range tests {
		for _, withMetrics := range []bool{false, true} {
			method := "Run"
			if withMetrics {
				method = "RunWithMetrics"
			}

			t.Run(tc.name+"/"+method, func(t *testing.T) {
				pool := NewWorkerPool(5)

				var called atomic.Bool

				process := func(int) { called.Store(true) }

				var metrics Metrics

				done := runAsync(withMetrics, pool, context.Background(), tc.jobs, process, func(m Metrics) {
					metrics = m
				})

				waitOrFail(t, done, method)

				if called.Load() {
					t.Fatal("process was called for an empty job list")
				}

				if withMetrics && metrics.MaxActive != 0 {
					t.Fatalf("MaxActive = %d, want 0", metrics.MaxActive)
				}
			})
		}
	}
}

func TestWorkerPool_Run_EmptyJobs_IgnoresNilProcess(t *testing.T) {
	pool := NewWorkerPool(5)

	done := runAsync(false, pool, context.Background(), nil, nil, nil)

	waitOrFail(t, done, "Run with a nil callback and no jobs")
}

func TestWorkerPool_Run_SingleJob(t *testing.T) {
	pool := NewWorkerPool(5)

	recorder := newJobRecorder()

	done := runAsync(false, pool, context.Background(), []int{42}, recorder.record, nil)

	waitOrFail(t, done, "Run")

	assertExactlyOnce(t, recorder, []int{42})
}

func TestWorkerPool_RunWithMetrics_SingleJob_MaxActiveIsOne(t *testing.T) {
	pool := NewWorkerPool(5)

	recorder := newJobRecorder()

	var metrics Metrics

	done := runAsync(true, pool, context.Background(), []int{42}, recorder.record, func(m Metrics) {
		metrics = m
	})

	waitOrFail(t, done, "RunWithMetrics")

	assertExactlyOnce(t, recorder, []int{42})

	if metrics.MaxActive != 1 {
		t.Fatalf("MaxActive = %d, want 1 for a single job", metrics.MaxActive)
	}
}

func TestWorkerPool_RunWithMetrics_SingleWorker_IsStrictlySequential(t *testing.T) {
	const jobsN = 50

	pool := NewWorkerPool(1)

	jobs := createJobs(jobsN)
	recorder := newJobRecorder()

	var (
		live atomic.Int64
		peak atomic.Int64
	)

	var metrics Metrics

	process := func(job int) {
		recordPeak(&peak, live.Add(1))
		recorder.record(job)
		live.Add(-1)
	}

	done := runAsync(true, pool, context.Background(), jobs, process, func(m Metrics) {
		metrics = m
	})

	waitOrFail(t, done, "RunWithMetrics")

	assertExactlyOnce(t, recorder, jobs)

	if got := peak.Load(); got != 1 {
		t.Fatalf("peak concurrency = %d, want 1 with a single worker", got)
	}

	if metrics.MaxActive != 1 {
		t.Fatalf("MaxActive = %d, want 1 with a single worker", metrics.MaxActive)
	}
}

func TestWorkerPool_Run_MoreWorkersThanJobs(t *testing.T) {
	pool := NewWorkerPool(10)

	jobs := []int{1, 2, 3}
	recorder := newJobRecorder()

	done := runAsync(false, pool, context.Background(), jobs, recorder.record, nil)

	waitOrFail(t, done, "Run")

	assertExactlyOnce(t, recorder, jobs)
}

func TestWorkerPool_Run_DuplicateInputIDsAreNotDeduplicated(t *testing.T) {
	pool := NewWorkerPool(2)

	recorder := newJobRecorder()

	done := runAsync(false, pool, context.Background(), []int{7, 7, 9}, recorder.record, nil)

	waitOrFail(t, done, "Run")

	if got := recorder.count(7); got != 2 {
		t.Fatalf("job 7 processed %d times, want 2 (input listed twice)", got)
	}

	if got := recorder.count(9); got != 1 {
		t.Fatalf("job 9 processed %d times, want 1", got)
	}

	if got := recorder.total(); got != 3 {
		t.Fatalf("total processed = %d, want 3", got)
	}
}

func TestWorkerPool_Run_RecoversWhenTheCallbackPanics(t *testing.T) {
	// The pool's deferred `wg.Done()` and its `active.Add(-1)` both run during
	// panic unwinding, so recovering inside the callback must not leak a worker
	// or desynchronise the active counter. RunWithMetrics is used so the
	// counter itself is observable afterwards.
	pool := NewWorkerPool(2)

	recorder := newJobRecorder()

	var recovered atomic.Int64

	process := func(job int) {
		defer func() {
			if r := recover(); r != nil {
				recovered.Add(1)
			}
		}()

		recorder.record(job)

		if job%2 == 0 {
			panic("callback failure")
		}
	}

	// A second, non-panicking run afterwards proves the counter recovered to a
	// sane value: a leaked increment would make MaxActive exceed the worker
	// count on the follow-up run.
	var metrics Metrics

	done := runAsync(true, pool, context.Background(), createJobs(8), process, func(m Metrics) {
		metrics = m
	})

	waitOrFail(t, done, "RunWithMetrics")

	assertExactlyOnce(t, recorder, createJobs(8))

	if got := recovered.Load(); got != 4 {
		t.Fatalf("recovered %d panics, want 4", got)
	}

	if got := metrics.MaxActive; got > 2 {
		t.Fatalf("MaxActive = %d, want <= 2", got)
	}

	clean := newJobRecorder()

	var second Metrics

	secondDone := runAsync(true, pool, context.Background(), []int{1, 2, 3, 4}, clean.record, func(m Metrics) {
		second = m
	})

	waitOrFail(t, secondDone, "follow-up RunWithMetrics")

	assertExactlyOnce(t, clean, []int{1, 2, 3, 4})

	if got := second.MaxActive; got > 2 {
		t.Fatalf("MaxActive on the follow-up run = %d, want <= 2: the active counter leaked", got)
	}
}

func TestWorkerPool_Run_EmptyJobsReleasesEveryWorker(t *testing.T) {
	// A worker must observe the closed job channel and return; if it did not,
	// the run would still complete but the goroutine would leak. A second run
	// on the same pool must therefore behave identically to the first.
	pool := NewWorkerPool(4)

	for round := 1; round <= 10; round++ {
		recorder := newJobRecorder()

		done := runAsync(false, pool, context.Background(), createJobs(15), recorder.record, nil)

		waitOrFail(t, done, "Run")

		if got := recorder.total(); got != 15 {
			t.Fatalf("round %d: processed %d jobs, want 15", round, got)
		}
	}
}
