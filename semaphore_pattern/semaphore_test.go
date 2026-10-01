package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
)

func TestNewSemaphoreRunner_StoresLimit(t *testing.T) {
	tests := []struct {
		name  string
		limit int
	}{
		{name: "single_slot", limit: 1},
		{name: "five_slots", limit: 5},
		{name: "ten_slots", limit: 10},
		{name: "zero_slots", limit: 0},
		{name: "negative_slots", limit: -1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runner := NewSemaphoreRunner(tc.limit)

			if runner == nil {
				t.Fatal("NewSemaphoreRunner returned nil")
			}

			if runner.limit != tc.limit {
				t.Fatalf("limit = %d, want %d", runner.limit, tc.limit)
			}
		})
	}
}

func TestNewSemaphoreRunner_NonPositiveLimit_Behaviour(t *testing.T) {
	// Design gap, documented rather than "fixed": NewSemaphoreRunner accepts
	// any int without validation, and the consequences are inconsistent.
	//
	//   - limit < 0: Run/RunWithMetrics panic inside make(chan struct{}, n)
	//     with "makechan: size out of range". It happens on the caller's
	//     goroutine, so the caller can recover from it.
	//   - limit == 0: the channel is unbuffered, so no job can ever acquire a
	//     permit and Run blocks forever unless the context is cancelled. That
	//     path is intentionally not exercised here - proving a hang requires a
	//     timeout-based assertion, which this suite avoids by design. The
	//     cancelled-context path below is the same configuration and is
	//     deterministic.
	t.Run("negative_limit_panics", func(t *testing.T) {
		runner := NewSemaphoreRunner(-1)

		var processed atomic.Int64

		process := func(int) { processed.Add(1) }

		assertPanicsWithMakechan(t, "Run", func() {
			runner.Run(context.Background(), []int{1, 2, 3}, process)
		})

		assertPanicsWithMakechan(t, "RunWithMetrics", func() {
			runner.RunWithMetrics(context.Background(), []int{1, 2, 3}, process)
		})

		if got := processed.Load(); got != 0 {
			t.Fatalf("processed %d jobs, want 0: the channel is created before any job runs", got)
		}
	})

	t.Run("zero_limit_cancelled_context", func(t *testing.T) {
		runner := NewSemaphoreRunner(0)

		recorder := newJobRecorder()

		ctx, cancel := context.WithCancel(context.Background())

		cancel()

		done := runAsync(false, runner, ctx, []int{1, 2, 3}, recorder.record, nil)

		waitOrFail(t, done, "Run with limit 0 and a cancelled context")

		if got := recorder.total(); got != 0 {
			t.Fatalf("processed %d jobs, want 0: no permit can ever be acquired with limit 0", got)
		}
	})
}

func assertPanicsWithMakechan(t *testing.T, name string, fn func()) {
	t.Helper()

	var recovered any

	func() {
		defer func() { recovered = recover() }()

		fn()
	}()

	if recovered == nil {
		t.Fatalf("%s did not panic", name)
	}

	// runtime.plainError, not string, is what makechan's panic carries.
	if msg := fmt.Sprint(recovered); !strings.Contains(msg, "size out of range") {
		t.Fatalf("%s panicked with %q, want a panic mentioning %q", name, msg, "size out of range")
	}
}

func TestSemaphoreRunner_Run_ProcessesAllJobs(t *testing.T) {
	const jobsN = 100

	runner := NewSemaphoreRunner(5)

	jobs := createJobs(jobsN)
	recorder := newJobRecorder()

	done := runAsync(false, runner, context.Background(), jobs, recorder.record, nil)

	waitOrFail(t, done, "Run")

	assertExactlyOnce(t, recorder, jobs)
}

func TestSemaphoreRunner_Run_EmptyJobs(t *testing.T) {
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
				runner := NewSemaphoreRunner(5)

				var called atomic.Bool

				process := func(int) { called.Store(true) }

				var metrics Metrics

				done := runAsync(withMetrics, runner, context.Background(), tc.jobs, process, func(m Metrics) {
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

func TestSemaphoreRunner_Run_EmptyJobs_IgnoresNilProcess(t *testing.T) {
	// Documented edge case: with no jobs the callback is never invoked, so a
	// nil callback is never dereferenced. With jobs it would panic - the
	// production code performs no nil check.
	runner := NewSemaphoreRunner(5)

	done := runAsync(false, runner, context.Background(), nil, nil, nil)

	waitOrFail(t, done, "Run with a nil callback and no jobs")
}

func TestSemaphoreRunner_Run_SingleJob(t *testing.T) {
	runner := NewSemaphoreRunner(5)

	recorder := newJobRecorder()

	done := runAsync(false, runner, context.Background(), []int{42}, recorder.record, nil)

	waitOrFail(t, done, "Run")

	assertExactlyOnce(t, recorder, []int{42})
}

func TestSemaphoreRunner_RunWithMetrics_SingleJob_MaxActiveIsOne(t *testing.T) {
	runner := NewSemaphoreRunner(5)

	recorder := newJobRecorder()

	var metrics Metrics

	done := runAsync(true, runner, context.Background(), []int{42}, recorder.record, func(m Metrics) {
		metrics = m
	})

	waitOrFail(t, done, "RunWithMetrics")

	assertExactlyOnce(t, recorder, []int{42})

	if metrics.MaxActive != 1 {
		t.Fatalf("MaxActive = %d, want 1 for a single job", metrics.MaxActive)
	}
}

func TestSemaphoreRunner_RunWithMetrics_LimitOne_IsStrictlySequential(t *testing.T) {
	const jobsN = 50

	runner := NewSemaphoreRunner(1)

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

	done := runAsync(true, runner, context.Background(), jobs, process, func(m Metrics) {
		metrics = m
	})

	waitOrFail(t, done, "RunWithMetrics")

	assertExactlyOnce(t, recorder, jobs)

	if got := peak.Load(); got != 1 {
		t.Fatalf("peak concurrency = %d, want 1 with limit 1", got)
	}

	if metrics.MaxActive != 1 {
		t.Fatalf("MaxActive = %d, want 1 with limit 1", metrics.MaxActive)
	}
}

func TestSemaphoreRunner_Run_LimitLargerThanJobCount(t *testing.T) {
	runner := NewSemaphoreRunner(10)

	jobs := []int{1, 2, 3}
	recorder := newJobRecorder()

	done := runAsync(false, runner, context.Background(), jobs, recorder.record, nil)

	waitOrFail(t, done, "Run")

	assertExactlyOnce(t, recorder, jobs)
}

func TestSemaphoreRunner_Run_DuplicateInputIDsAreNotDeduplicated(t *testing.T) {
	// The at-most-once guarantee is per slice element, not per value: the
	// runner does not deduplicate its input.
	runner := NewSemaphoreRunner(2)

	recorder := newJobRecorder()

	done := runAsync(false, runner, context.Background(), []int{7, 7, 9}, recorder.record, nil)

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

func TestSemaphoreRunner_Run_PermitsAreReleasedOnCallbackReturn(t *testing.T) {
	// Jobs run in batches of `limit`. A batch only completes once every job in
	// it has arrived, and the next batch can only start once the previous batch
	// has returned and released its permits. So the run completing at all
	// proves permits are released on callback return, and every batch reaching
	// full size proves the limit is actually filled rather than approximated.
	// The timeout in waitOrFail is a deadlock safety net, not an expectation.
	const (
		limit   = 3
		batches = 4
	)

	jobsN := limit * batches

	runner := NewSemaphoreRunner(limit)

	jobs := createJobs(jobsN)
	recorder := newJobRecorder()

	batchesSeen, process := batchingProcess(limit, jobsN)

	done := runAsync(false, runner, context.Background(), jobs, func(job int) {
		recorder.record(job)
		process(job)
	}, nil)

	waitOrFail(t, done, "Run")

	if got := batchesSeen(); got != batches {
		t.Fatalf("completed %d full batches, want %d: a batch did not fill all %d slots", got, batches, limit)
	}

	assertExactlyOnce(t, recorder, jobs)
}

func TestRunners_ExactlyOnceCheckDetectsBadRuns(t *testing.T) {
	// Meta-test: the shared assertion must actually be able to fail, otherwise
	// every test relying on it could pass for the wrong reason.
	recorder := newJobRecorder()

	for _, job := range []int{1, 2, 3} {
		recorder.record(job)
	}

	if msg := checkExactlyOnce(recorder, []int{1, 2, 3}); msg != "" {
		t.Fatalf("checkExactlyOnce reported %q for a correct run", msg)
	}

	tests := []struct {
		name     string
		mutate   func(*jobRecorder)
		expected []int
	}{
		{
			name:     "missing_job",
			expected: []int{1, 2, 4},
		},
		{
			name:     "unexpected_job",
			expected: []int{1, 2, 3, 4},
		},
		{
			name:     "duplicate_job",
			mutate:   func(r *jobRecorder) { r.record(2) },
			expected: []int{1, 2, 3},
		},
		{
			name:     "nothing_processed",
			expected: []int{1, 2, 3, 4, 5},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fresh := newJobRecorder()

			for _, job := range []int{1, 2, 3} {
				fresh.record(job)
			}

			if tc.mutate != nil {
				tc.mutate(fresh)
			}

			if msg := checkExactlyOnce(fresh, tc.expected); msg == "" {
				t.Fatalf("checkExactlyOnce accepted a %s run", tc.name)
			}
		})
	}
}

// assertExactlyOnce verifies every expected ID was processed exactly once and
// that no unexpected ID was processed.
func assertExactlyOnce(t *testing.T, recorder *jobRecorder, expected []int) {
	t.Helper()

	if msg := checkExactlyOnce(recorder, expected); msg != "" {
		t.Error(msg)
	}
}

// checkExactlyOnce returns a human-readable description of every discrepancy
// between the expected job IDs and what was recorded, or "" when the run
// processed exactly the expected IDs exactly once.
func checkExactlyOnce(recorder *jobRecorder, expected []int) string {
	want := make(map[int]int, len(expected))

	for _, job := range expected {
		want[job]++
	}

	got := recorder.snapshot()

	var problems []string

	missing := make([]int, 0)

	for job, wantN := range want {
		gotN := got[job]

		switch {
		case gotN == 0:
			missing = append(missing, job)
		case gotN != wantN:
			problems = append(problems, fmt.Sprintf("job %d processed %d times, want %d", job, gotN, wantN))
		}
	}

	sort.Ints(missing)

	if len(missing) > 0 {
		problems = append(problems, fmt.Sprintf("jobs never processed: %v", missing))
	}

	unexpected := make([]int, 0)

	for job := range got {
		if _, ok := want[job]; !ok {
			unexpected = append(unexpected, job)
		}
	}

	sort.Ints(unexpected)

	if len(unexpected) > 0 {
		problems = append(problems, fmt.Sprintf("unexpected jobs processed: %v", unexpected))
	}

	if total, wantTotal := recorder.total(), len(expected); total != wantTotal {
		problems = append(problems, fmt.Sprintf("total processed = %d, want %d", total, wantTotal))
	}

	return strings.Join(problems, "; ")
}
