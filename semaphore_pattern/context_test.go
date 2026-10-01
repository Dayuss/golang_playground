package main

import (
	"context"
	"sync/atomic"
	"testing"
)

func TestSemaphoreRunner_Run_ContextCancelledWhileJobsWait(t *testing.T) {
	const (
		limit = 5
		jobsN = 100
	)

	runner := NewSemaphoreRunner(limit)

	jobs := createJobs(jobsN)

	entered := make(chan int, jobsN)
	release := make(chan struct{})

	var (
		live, peak atomic.Int64
		recorder   = newJobRecorder()
		exited     atomic.Int64
	)

	process := func(job int) {
		recordPeak(&peak, live.Add(1))
		recorder.record(job)
		entered <- job
		<-release
		exited.Add(1)
		live.Add(-1)
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := runAsync(false, runner, ctx, jobs, process, nil)

	// Exactly `limit` jobs hold a permit; the rest are parked waiting for one.
	awaitStarts(t, entered, limit, "run should fill every slot")

	cancel()

	close(release)

	waitOrFail(t, done, "Run after cancellation")

	// Finding, not an expectation: the exact number of processed jobs is NOT
	// deterministic here. The runner's `select` has two ready cases once the
	// permits are freed - a free permit and a cancelled context - and Go picks
	// uniformly at random, so a waiting job may still be admitted after
	// cancellation. It may also be skipped. Both outcomes are legal for this
	// implementation, so only invariants are asserted:
	//
	//   - Run returned (no deadlock, no leaked permit),
	//   - every started callback also finished,
	//   - no job ran twice and no unknown job ran,
	//   - concurrency never exceeded the limit.
	//
	// This is arguably a design gap: a cancelled run is not guaranteed to stop
	// promptly. Production code was left unchanged, since making cancellation
	// win the select is a behaviour change, not a test fix.
	if got := recorder.total(); got < limit {
		t.Fatalf("processed %d jobs after cancellation, want at least %d (the ones already running)", got, limit)
	}

	if got := live.Load(); got != 0 {
		t.Fatalf("%d callbacks still running after Run returned, want 0", got)
	}

	if got, want := exited.Load(), int64(recorder.total()); got != want {
		t.Fatalf("%d callbacks finished, %d were started: a job goroutine was left behind", got, want)
	}

	if got := peak.Load(); got > limit {
		t.Fatalf("peak concurrency = %d, want <= %d", got, limit)
	}

	assertExactlyOnceSubset(t, recorder, jobs)
}

func TestSemaphoreRunner_RunWithMetrics_ContextCancelledWhileJobsWait(t *testing.T) {
	const (
		limit = 5
		jobsN = 100
	)

	runner := NewSemaphoreRunner(limit)

	entered := make(chan int, jobsN)
	release := make(chan struct{})

	var live, peak atomic.Int64

	process := blockingProcess(entered, &live, &peak, release)

	ctx, cancel := context.WithCancel(context.Background())

	var metrics Metrics

	done := runAsync(true, runner, ctx, createJobs(jobsN), process, func(m Metrics) {
		metrics = m
	})

	awaitStarts(t, entered, limit, "run should fill every slot")

	cancel()

	close(release)

	waitOrFail(t, done, "RunWithMetrics after cancellation")

	if got := metrics.MaxActive; got > int64(limit) {
		t.Fatalf("MaxActive = %d, want <= %d", got, limit)
	}

	if got := live.Load(); got != 0 {
		t.Fatalf("%d callbacks still running after RunWithMetrics returned, want 0", got)
	}
}

func TestSemaphoreRunner_Run_ContextAlreadyCancelled(t *testing.T) {
	// With a cancelled context and a usable limit, select may still pick the
	// permit branch, so the number of processed jobs is scheduling dependent.
	// Only "returns, bounded by the job count" is deterministic here.
	t.Run("live_limit", func(t *testing.T) {
		jobs := []int{1, 2, 3}

		runner := NewSemaphoreRunner(5)

		recorder := newJobRecorder()

		ctx, cancel := context.WithCancel(context.Background())

		cancel()

		done := runAsync(false, runner, ctx, jobs, recorder.record, nil)

		waitOrFail(t, done, "Run with an already cancelled context")

		if got := recorder.total(); got > len(jobs) {
			t.Fatalf("processed %d jobs, want <= %d", got, len(jobs))
		}
	})

	// With limit 0 the permit branch can never proceed, so ctx.Done() is the
	// only ready case and processing is deterministically skipped.
	t.Run("zero_limit_skips_every_job", func(t *testing.T) {
		runner := NewSemaphoreRunner(0)

		recorder := newJobRecorder()

		ctx, cancel := context.WithCancel(context.Background())

		cancel()

		done := runAsync(false, runner, ctx, []int{1, 2, 3}, recorder.record, nil)

		waitOrFail(t, done, "Run with limit 0 and an already cancelled context")

		if got := recorder.total(); got != 0 {
			t.Fatalf("processed %d jobs, want 0", got)
		}
	})

	// Cancellation before any job runs must not prevent the run from
	// returning promptly, and MaxActive must stay within the limit.
	t.Run("RunWithMetrics", func(t *testing.T) {
		runner := NewSemaphoreRunner(5)

		recorder := newJobRecorder()

		ctx, cancel := context.WithCancel(context.Background())

		cancel()

		var metrics Metrics

		done := runAsync(true, runner, ctx, []int{1, 2, 3, 4, 5, 6, 7, 8}, recorder.record, func(m Metrics) {
			metrics = m
		})

		waitOrFail(t, done, "RunWithMetrics with an already cancelled context")

		if got := recorder.total(); got > 8 {
			t.Fatalf("processed %d jobs, want <= 8", got)
		}

		if metrics.MaxActive > 5 {
			t.Fatalf("MaxActive = %d, want <= 5", metrics.MaxActive)
		}
	})
}

func TestSemaphoreRunner_Run_ContextCancelledAfterCompletionHasNoEffect(t *testing.T) {
	// A late cancellation must not corrupt an already finished run.
	runner := NewSemaphoreRunner(3)

	jobs := createJobs(30)
	recorder := newJobRecorder()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := runAsync(false, runner, ctx, jobs, recorder.record, nil)

	waitOrFail(t, done, "Run")

	cancel()

	assertExactlyOnce(t, recorder, jobs)
}

func TestWorkerPool_Run_ContextCancelledWhileJobsWait(t *testing.T) {
	const (
		workers = 5
		jobsN   = 100
	)

	pool := NewWorkerPool(workers)

	jobs := createJobs(jobsN)

	entered := make(chan int, jobsN)
	release := make(chan struct{})

	var (
		live, peak atomic.Int64
		recorder   = newJobRecorder()
		exited     atomic.Int64
	)

	process := func(job int) {
		recordPeak(&peak, live.Add(1))
		recorder.record(job)
		entered <- job
		<-release
		exited.Add(1)
		live.Add(-1)
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := runAsync(false, pool, ctx, jobs, process, nil)

	// Every worker has taken one job and is parked inside the callback; the
	// feeding loop is blocked trying to hand over job number workers+1.
	awaitStarts(t, entered, workers, "every worker should pick up a job")

	cancel()

	close(release)

	waitOrFail(t, done, "Run after cancellation")

	// Unlike the semaphore case this count IS deterministic. Every worker is
	// parked inside the callback, so the feeding loop's only ready case is
	// ctx.Done(): it closes jobCh and returns. The workers then find a closed
	// channel (ok == false) or a cancelled context on their next select, and
	// either way they return without taking another job.
	if got := recorder.total(); got != workers {
		t.Fatalf("processed %d jobs after cancellation, want %d (one per worker)", got, workers)
	}

	if got := live.Load(); got != 0 {
		t.Fatalf("%d callbacks still running after Run returned, want 0", got)
	}

	if got, want := exited.Load(), int64(recorder.total()); got != want {
		t.Fatalf("%d callbacks finished, %d were started: a worker was left behind", got, want)
	}

	if got := peak.Load(); got > workers {
		t.Fatalf("peak concurrency = %d, want <= %d", got, workers)
	}

	assertExactlyOnceSubset(t, recorder, jobs)
}

func TestWorkerPool_RunWithMetrics_ContextCancelledWhileJobsWait(t *testing.T) {
	const (
		workers = 5
		jobsN   = 100
	)

	pool := NewWorkerPool(workers)

	entered := make(chan int, jobsN)
	release := make(chan struct{})

	var live, peak atomic.Int64

	process := blockingProcess(entered, &live, &peak, release)

	ctx, cancel := context.WithCancel(context.Background())

	var metrics Metrics

	done := runAsync(true, pool, ctx, createJobs(jobsN), process, func(m Metrics) {
		metrics = m
	})

	awaitStarts(t, entered, workers, "every worker should pick up a job")

	cancel()

	close(release)

	waitOrFail(t, done, "RunWithMetrics after cancellation")

	if got := metrics.MaxActive; got > int64(workers) {
		t.Fatalf("MaxActive = %d, want <= %d", got, workers)
	}

	if got := live.Load(); got != 0 {
		t.Fatalf("%d callbacks still running after RunWithMetrics returned, want 0", got)
	}
}

func TestWorkerPool_Run_ContextAlreadyCancelled(t *testing.T) {
	// Both the worker and the feeding loop select on a ready ctx.Done() and a
	// ready channel operation, so a cancelled context can still let some jobs
	// through. Only an upper bound is deterministic.
	jobs := []int{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("Run", func(t *testing.T) {
		pool := NewWorkerPool(5)

		recorder := newJobRecorder()

		ctx, cancel := context.WithCancel(context.Background())

		cancel()

		done := runAsync(false, pool, ctx, jobs, recorder.record, nil)

		waitOrFail(t, done, "Run with an already cancelled context")

		if got := recorder.total(); got > len(jobs) {
			t.Fatalf("processed %d jobs, want <= %d", got, len(jobs))
		}

		assertExactlyOnceSubset(t, recorder, jobs)
	})

	t.Run("RunWithMetrics", func(t *testing.T) {
		pool := NewWorkerPool(5)

		recorder := newJobRecorder()

		ctx, cancel := context.WithCancel(context.Background())

		cancel()

		var metrics Metrics

		done := runAsync(true, pool, ctx, jobs, recorder.record, func(m Metrics) {
			metrics = m
		})

		waitOrFail(t, done, "RunWithMetrics with an already cancelled context")

		if got := recorder.total(); got > len(jobs) {
			t.Fatalf("processed %d jobs, want <= %d", got, len(jobs))
		}

		if metrics.MaxActive > 5 {
			t.Fatalf("MaxActive = %d, want <= 5", metrics.MaxActive)
		}
	})
}

func TestWorkerPool_Run_ContextCancelledDuringScheduling(t *testing.T) {
	// Cancellation while the feeding loop is still handing jobs over (no
	// callback ever blocks) must still return, and must not process any job
	// twice. The exact count is scheduler dependent, so only invariants are
	// asserted.
	pool := NewWorkerPool(4)

	jobs := createJobs(200)
	recorder := newJobRecorder()

	ctx, cancel := context.WithCancel(context.Background())

	done := runAsync(false, pool, ctx, jobs, recorder.record, nil)

	cancel()

	waitOrFail(t, done, "Run cancelled while scheduling")

	assertExactlyOnceSubset(t, recorder, jobs)
}

func TestSemaphoreRunner_Run_ContextCancelledDuringScheduling(t *testing.T) {
	runner := NewSemaphoreRunner(4)

	jobs := createJobs(200)
	recorder := newJobRecorder()

	ctx, cancel := context.WithCancel(context.Background())

	done := runAsync(false, runner, ctx, jobs, recorder.record, nil)

	cancel()

	waitOrFail(t, done, "Run cancelled while scheduling")

	assertExactlyOnceSubset(t, recorder, jobs)
}

func TestRunners_ContextCancelled_RecoversForSubsequentRuns(t *testing.T) {
	// A cancelled run must not leave the implementation unusable.
	eachImplementation(t, func(t *testing.T, name string, run runner) {
		jobs := createJobs(50)

		recorder := newJobRecorder()

		ctx, cancel := context.WithCancel(context.Background())

		cancelledDone := runAsync(false, run, ctx, jobs, recorder.record, nil)

		cancel()

		waitOrFail(t, cancelledDone, "cancelled Run")

		// Same runner, fresh context: everything must be processed now.
		second := newJobRecorder()

		done := runAsync(false, run, context.Background(), jobs, second.record, nil)

		waitOrFail(t, done, "Run after a cancelled run")

		assertExactlyOnce(t, second, jobs)
	})
}

// assertExactlyOnceSubset checks that no recorded ID was processed more than
// once and that no ID outside expected was processed. It deliberately does not
// require every expected ID to appear, because a cancelled run may skip jobs.
func assertExactlyOnceSubset(t *testing.T, recorder *jobRecorder, expected []int) {
	t.Helper()

	allowed := make(map[int]struct{}, len(expected))

	for _, job := range expected {
		allowed[job] = struct{}{}
	}

	for job, count := range recorder.snapshot() {
		if count > 1 {
			t.Errorf("job %d processed %d times, want at most 1", job, count)
		}

		if _, ok := allowed[job]; !ok {
			t.Errorf("unexpected job %d processed %d times", job, count)
		}
	}
}
