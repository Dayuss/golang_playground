package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// deadlockTimeout is a deadlock safety net only. It is never used to decide
// what the correct behaviour of a run is: a run that is supposed to finish
// finishes as fast as its callbacks allow, so a generous timeout only ever
// catches a genuinely stuck implementation instead of a slow machine.
const deadlockTimeout = 10 * time.Second

// runner is the behaviour both implementations expose. Keeping the tests
// table-driven over this interface means the shared concurrency invariants are
// asserted once and therefore hold for every implementation in this package.
type runner interface {
	Run(ctx context.Context, jobs []int, process func(int))
	RunWithMetrics(ctx context.Context, jobs []int, process func(int)) Metrics
}

// implementations returns one runner per implementation under test, all
// configured with the same concurrency limit.
func implementations(limit int) []struct {
	name string
	run  runner
} {
	return []struct {
		name string
		run  runner
	}{
		{name: "Semaphore", run: NewSemaphoreRunner(limit)},
		{name: "WorkerPool", run: NewWorkerPool(limit)},
	}
}

func eachImplementation(t *testing.T, fn func(t *testing.T, name string, run runner)) {
	t.Helper()

	for _, impl := range implementations(5) {
		t.Run(impl.name, func(t *testing.T) {
			fn(t, impl.name, impl.run)
		})
	}
}

// jobRecorder records how often each job ID was processed. The map is
// mutex-guarded because callbacks run on many goroutines concurrently.
type jobRecorder struct {
	mu     sync.Mutex
	counts map[int]int
}

func newJobRecorder() *jobRecorder {
	return &jobRecorder{counts: make(map[int]int)}
}

func (r *jobRecorder) record(job int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.counts[job]++
}

func (r *jobRecorder) count(job int) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.counts[job]
}

func (r *jobRecorder) total() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	total := 0

	for _, n := range r.counts {
		total += n
	}

	return total
}

func (r *jobRecorder) snapshot() map[int]int {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make(map[int]int, len(r.counts))

	for job, n := range r.counts {
		out[job] = n
	}

	return out
}

// recordPeak stores v in peak if it is the highest value seen so far. This is
// an independent implementation of main.go's updateMax on purpose: sharing the
// production helper would let a bug in that helper hide a bug in a runner.
func recordPeak(peak *atomic.Int64, v int64) {
	for {
		old := peak.Load()

		if v <= old {
			return
		}

		if peak.CompareAndSwap(old, v) {
			return
		}
	}
}

// blockingProcess returns a callback that
//
//   - records the job ID on entered (buffered, so the send never blocks),
//   - tracks live/peak concurrency with atomics, and
//   - parks until release is closed.
//
// Because every started callback is parked, the test - not the scheduler -
// decides when overlapping work ends, which is what makes overlap assertions
// deterministic without sleeps.
func blockingProcess(
	entered chan<- int,
	live *atomic.Int64,
	peak *atomic.Int64,
	release <-chan struct{},
) func(int) {
	return func(job int) {
		recordPeak(peak, live.Add(1))

		entered <- job

		<-release

		live.Add(-1)
	}
}

// awaitStarts reads exactly n job IDs from entered, failing the test if the
// implementation cannot reach n concurrent executions (i.e. it is stuck or
// over-serialised). It never asserts an upper bound; use requireNoExtraStart
// for that.
func awaitStarts(t *testing.T, entered <-chan int, n int, what string) []int {
	t.Helper()

	starts := make([]int, 0, n)

	for len(starts) < n {
		select {
		case job := <-entered:
			starts = append(starts, job)
		case <-time.After(deadlockTimeout):
			t.Fatalf("only %d of %d expected concurrent executions started: %s", len(starts), n, what)
		}
	}

	return starts
}

// requireNoExtraStart asserts that no further execution has started. It is
// sound because all callers of this helper have parked every started
// execution, so no new start can appear while the check runs.
func requireNoExtraStart(t *testing.T, entered <-chan int, what string) {
	t.Helper()

	select {
	case job := <-entered:
		t.Fatalf("execution for job %d started although every slot is already in use: %s", job, what)
	default:
	}
}

// batchingProcess returns a callback that releases work in batches of limit.
//
// Every arriving job parks until `limit` jobs have arrived, which can only
// happen if the implementation lets exactly `limit` jobs run at once. The
// returned counter reports how many batches were released, so a run that
// serialises too much shows up as fewer batches rather than as a hang.
//
// The caller must schedule exactly limit*expectedBatches jobs.
func batchingProcess(limit, total int) (batches func() int, process func(int)) {
	var (
		mu      sync.Mutex
		cond    = sync.NewCond(&mu)
		arrived int
		release int
	)

	return func() int {
			mu.Lock()
			defer mu.Unlock()

			return release / limit
		}, func(int) {
			mu.Lock()
			defer mu.Unlock()

			arrived++

			target := ((arrived-1)/limit + 1) * limit

			for arrived < target {
				cond.Wait()
			}

			if arrived == target {
				release = arrived

				cond.Broadcast()
			}
		}
}

// waitOrFail blocks until done is closed and fails the test on timeout. The
// timeout is a deadlock safety net, never a behavioural expectation.
func waitOrFail(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()

	select {
	case <-done:
	case <-time.After(deadlockTimeout):
		t.Fatalf("%s did not return: implementation appears deadlocked", what)
	}
}

// runAsync starts run(...) in a goroutine and returns a channel closed once
// the run has returned. onReturn is invoked with the metrics (when withMetrics
// is set) before the channel is closed.
func runAsync(
	withMetrics bool,
	run runner,
	ctx context.Context,
	jobs []int,
	process func(int),
	onReturn func(Metrics),
) <-chan struct{} {
	done := make(chan struct{})

	go func() {
		defer close(done)

		if withMetrics {
			onReturn(run.RunWithMetrics(ctx, jobs, process))

			return
		}

		run.Run(ctx, jobs, process)
	}()

	return done
}

func TestRunners_RespectsConcurrencyLimit(t *testing.T) {
	const (
		limit = 5
		jobsN = 100
	)

	eachImplementation(t, func(t *testing.T, name string, run runner) {
		jobs := createJobs(jobsN)

		entered := make(chan int, jobsN)
		release := make(chan struct{})

		var live, peak atomic.Int64

		process := blockingProcess(entered, &live, &peak, release)

		done := runAsync(false, run, context.Background(), jobs, process, nil)

		// The limit executions must be able to overlap: wait until all limit
		// slots are busy, then prove no additional execution can start.
		awaitStarts(t, entered, limit, "run should fill every slot")
		requireNoExtraStart(t, entered, "concurrency limit exceeded")

		close(release)

		waitOrFail(t, done, "Run")

		if got := peak.Load(); got > limit {
			t.Fatalf("peak concurrency = %d, want <= %d", got, limit)
		}

		if got := live.Load(); got != 0 {
			t.Fatalf("live callbacks after Run returned = %d, want 0", got)
		}
	})
}

func TestRunners_MaxActiveEqualsLimitWhenOverlapIsGuaranteed(t *testing.T) {
	const limit = 5

	eachImplementation(t, func(t *testing.T, name string, run runner) {
		jobs := createJobs(100)

		entered := make(chan int, len(jobs))
		release := make(chan struct{})

		var live, peak atomic.Int64

		process := blockingProcess(entered, &live, &peak, release)

		var metrics Metrics

		done := runAsync(true, run, context.Background(), jobs, process, func(m Metrics) {
			metrics = m
		})

		// Every started execution is parked, so reaching limit simultaneous
		// executions guarantees MaxActive was at least limit.
		awaitStarts(t, entered, limit, "run should reach the configured concurrency")

		close(release)

		waitOrFail(t, done, "RunWithMetrics")

		if got := metrics.MaxActive; got > limit {
			t.Fatalf("MaxActive = %d, want <= %d", got, limit)
		} else if got := metrics.MaxActive; got != int64(limit) {
			t.Fatalf("MaxActive = %d, want %d: %d executions were provably overlapping", got, limit, limit)
		}
	})
}

func TestRunners_MaxActiveIsBoundedWhenOverlapIsNotGuaranteed(t *testing.T) {
	const limit = 5

	eachImplementation(t, func(t *testing.T, name string, run runner) {
		jobs := createJobs(50)

		var metrics Metrics

		// Non-blocking callbacks: the run may serialise completely, so only
		// the upper bound is a guaranteed property here.
		done := runAsync(true, run, context.Background(), jobs, func(int) {}, func(m Metrics) {
			metrics = m
		})

		waitOrFail(t, done, "RunWithMetrics")

		if metrics.MaxActive < 1 {
			t.Fatalf("MaxActive = %d, want >= 1 (at least one callback ran)", metrics.MaxActive)
		}

		if metrics.MaxActive > limit {
			t.Fatalf("MaxActive = %d, want <= %d", metrics.MaxActive, limit)
		}
	})
}

func TestRunners_MaxActiveEqualsJobCountWhenLimitExceedsJobs(t *testing.T) {
	const limit = 10

	eachImplementation(t, func(t *testing.T, name string, run runner) {
		jobs := []int{1, 2, 3}

		entered := make(chan int, len(jobs))
		release := make(chan struct{})

		var live, peak atomic.Int64

		process := blockingProcess(entered, &live, &peak, release)

		var metrics Metrics

		done := runAsync(true, run, context.Background(), jobs, process, func(m Metrics) {
			metrics = m
		})

		// With a limit above the job count every job can run at once, and the
		// barrier proves it rather than assuming it.
		awaitStarts(t, entered, len(jobs), "all jobs should fit within the limit")

		close(release)

		waitOrFail(t, done, "RunWithMetrics")

		if got := metrics.MaxActive; got != int64(len(jobs)) {
			t.Fatalf("MaxActive = %d, want %d", got, len(jobs))
		}
	})
}

func TestRunners_ConcurrentRunsAreIndependent(t *testing.T) {
	// Each Run must own its own concurrency state. If the limiter were shared
	// across runs, the two concurrent runs below could not reach
	// limit*2 simultaneous callbacks and one would starve the other.
	//
	// Both runs share one `entered` channel and one `release` channel, so the
	// test observes them as a single pool of concurrent callbacks while still
	// measuring each run's peak separately.
	const (
		limit = 4
		jobsN = 32
	)

	tests := []struct {
		name  string
		build func() (runner, runner)
	}{
		{
			name: "two_semaphore_runs",
			build: func() (runner, runner) {
				return NewSemaphoreRunner(limit), NewSemaphoreRunner(limit)
			},
		},
		{
			name: "two_worker_pools",
			build: func() (runner, runner) {
				return NewWorkerPool(limit), NewWorkerPool(limit)
			},
		},
		{
			name: "one_semaphore_runner_reused",
			build: func() (runner, runner) {
				shared := NewSemaphoreRunner(limit)

				return shared, shared
			},
		},
		{
			name: "one_worker_pool_reused",
			build: func() (runner, runner) {
				shared := NewWorkerPool(limit)

				return shared, shared
			},
		},
		{
			name: "mixed_implementations",
			build: func() (runner, runner) {
				return NewSemaphoreRunner(limit), NewWorkerPool(limit)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			first, second := tc.build()

			// Shared across both runs so the test can observe their callbacks as
			// one set, while each run keeps its own peak counter.
			entered := make(chan int, 2*jobsN)
			release := make(chan struct{})

			// live and peak must be separate counters: sharing one would make
			// the decrement on callback return also lower the recorded peak.
			runs := []struct {
				name     string
				run      runner
				live     *atomic.Int64
				peak     *atomic.Int64
				recorder *jobRecorder
				done     <-chan struct{}
			}{
				{
					name: "a", run: first,
					live: &atomic.Int64{}, peak: &atomic.Int64{},
					recorder: newJobRecorder(),
				},
				{
					name: "b", run: second,
					live: &atomic.Int64{}, peak: &atomic.Int64{},
					recorder: newJobRecorder(),
				},
			}

			for i := range runs {
				r := &runs[i]

				process := func(job int) {
					r.recorder.record(job)
					blockingProcess(entered, r.live, r.peak, release)(job)
				}

				r.done = runAsync(false, r.run, context.Background(), createJobs(jobsN), process, nil)
			}

			// Each run is capped at `limit`, so reaching 2*limit simultaneous
			// callbacks proves the two runs are limited independently.
			awaitStarts(t, entered, 2*limit, "concurrent runs should overlap")

			close(release)

			for _, r := range runs {
				waitOrFail(t, r.done, "run "+r.name)

				if got := r.recorder.total(); got != jobsN {
					t.Errorf("run %s processed %d jobs, want %d", r.name, got, jobsN)
				}

				if got := r.peak.Load(); got > limit {
					t.Errorf("run %s peak concurrency = %d, want <= %d", r.name, got, limit)
				}
			}
		})
	}
}

func TestRunners_MaxActiveCountsConcurrencyNotCumulativeWork(t *testing.T) {
	// MaxActive must track simultaneous callbacks, not how many callbacks have
	// run in total. 200 trivial jobs on 5 slots must therefore report a MaxActive
	// far below the job count; a cumulative counter would report 200. The upper
	// bound is deterministic, the lower bound only needs one callback to have
	// run.
	const (
		limit = 5
		jobsN = 200
	)

	eachImplementation(t, func(t *testing.T, name string, run runner) {
		var metrics Metrics

		done := runAsync(true, run, context.Background(), createJobs(jobsN), func(int) {}, func(m Metrics) {
			metrics = m
		})

		waitOrFail(t, done, "RunWithMetrics")

		if metrics.MaxActive < 1 {
			t.Fatalf("MaxActive = %d, want >= 1 (at least one callback ran)", metrics.MaxActive)
		}

		if metrics.MaxActive > limit {
			t.Fatalf("MaxActive = %d, want <= %d", metrics.MaxActive, limit)
		}

		if metrics.MaxActive >= jobsN {
			t.Fatalf("MaxActive = %d, want < %d: MaxActive must not count cumulative callbacks", metrics.MaxActive, jobsN)
		}
	})
}

func TestRunners_RunsAreRepeatableAndIsolated(t *testing.T) {
	// Reusing a runner must not carry state between runs: a run that leaks a
	// permit or a worker would eventually stall the next one.
	eachImplementation(t, func(t *testing.T, name string, run runner) {
		jobs := createJobs(20)

		for round := 1; round <= 25; round++ {
			recorder := newJobRecorder()

			done := runAsync(false, run, context.Background(), jobs, recorder.record, nil)

			waitOrFail(t, done, fmt.Sprintf("Run round %d", round))

			if got := recorder.total(); got != len(jobs) {
				t.Fatalf("round %d: processed %d jobs, want %d", round, got, len(jobs))
			}

			for job := range jobs {
				if got := recorder.count(job); got != 1 {
					t.Fatalf("round %d: job %d processed %d times, want 1", round, job, got)
				}
			}
		}
	})
}

const panicChildEnv = "SEMAPHORE_PATTERN_TEST_PANIC_CHILD"

// TestRunners_PanicInProcess_PropagatesAndCrashesProcess documents the
// documented-by-absence behaviour: neither implementation recovers a panic
// raised by the process callback, and a panic on any job goroutine terminates
// the whole program.
//
// A panic can only be observed from a separate process, so the child branch
// below runs the real implementation and must never return normally. If an
// implementation ever added recovery to production code, the child would exit
// with status 0 and this test would fail - which is the point.
func TestRunners_PanicInProcess_PropagatesAndCrashesProcess(t *testing.T) {
	const panicMessage = "boom-from-process-callback"

	if child := os.Getenv(panicChildEnv); child != "" {
		var run runner

		switch child {
		case "Semaphore":
			run = NewSemaphoreRunner(2)
		case "WorkerPool":
			run = NewWorkerPool(2)
		default:
			fmt.Fprintf(os.Stderr, "unknown child %q\n", child)

			os.Exit(3)
		}

		// Two jobs, two slots: a panicking callback can only be masked by
		// recovery, never by the panic simply not happening.
		run.Run(context.Background(), []int{1, 2}, func(int) {
			panic(panicMessage)
		})

		// Run returned, which is itself a finding: the runners use
		// `defer wg.Done()` and `defer func() { <-sem }()`, and those defers
		// run *during* panic unwinding. So Run's WaitGroup is released and Run
		// can return while a job goroutine is still unwinding. Exiting here
		// would race that goroutine and make the observation non-deterministic,
		// so the child parks instead. A recovered panic can never terminate a
		// parked process, so the child always ends via the runtime's panic
		// handler, which is what the parent asserts below.
		select {}
	}

	for _, impl := range implementations(2) {
		t.Run(impl.name, func(t *testing.T) {
			cmd := exec.Command(
				os.Args[0],
				"-test.run=^TestRunners_PanicInProcess_PropagatesAndCrashesProcess$",
				// Without this the child would hang forever if the panic were
				// ever swallowed, turning a failed assertion into a hung suite.
				"-test.timeout=60s",
			)
			cmd.Env = append(os.Environ(), panicChildEnv+"="+impl.name)

			out, err := cmd.CombinedOutput()

			if err == nil {
				t.Fatalf("child exited cleanly: the panic did not propagate. output:\n%s", out)
			}

			if !strings.Contains(string(out), "panic: "+panicMessage) {
				t.Fatalf("expected %q at the top level, got output:\n%s", "panic: "+panicMessage, out)
			}

			// A Go panic on any goroutine is fatal for the whole process, so
			// this also documents the blast radius: one bad callback kills
			// everything, not just the offending job.
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) {
				t.Fatalf("expected a non-zero exit from the panic, got %v", err)
			}
		})
	}
}

func TestRunners_PanicRecoveredByCallback_StillReleasesResources(t *testing.T) {
	// When the callback itself recovers the panic, the runner's own deferred
	// cleanup must still run: no deadlock, no leaked permit, no leaked worker.
	// limit=1 forces strict serialisation, so every job must still run after
	// the panicking one released its slot.
	for _, impl := range implementations(1) {
		t.Run(impl.name, func(t *testing.T) {
			jobs := []int{10, 20, 30}

			var (
				mu        sync.Mutex
				recovered []string
				panics    int
			)

			done := runAsync(false, impl.run, context.Background(), jobs, func(job int) {
				defer func() {
					if r := recover(); r != nil {
						mu.Lock()
						defer mu.Unlock()

						panics++
						recovered = append(recovered, fmt.Sprint(r))
					}
				}()

				mu.Lock()
				defer mu.Unlock()

				panic("callback failure")
			}, nil)

			waitOrFail(t, done, "Run")

			mu.Lock()
			defer mu.Unlock()

			if panics != len(jobs) {
				t.Fatalf("recovered %d panics, want %d", panics, len(jobs))
			}

			if len(recovered) != len(jobs) {
				t.Fatalf("recovered %v, want %d entries", recovered, len(jobs))
			}

			for _, r := range recovered {
				if r != "callback failure" {
					t.Fatalf("recovered %q, want %q", r, "callback failure")
				}
			}
		})
	}
}
