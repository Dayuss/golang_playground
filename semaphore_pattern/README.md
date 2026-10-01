# Semaphore vs Worker Pool on Golang

## What this compares

Two ways to bound concurrency in Go, implemented side by side over the same API:

- **Semaphore** (`semaphore.go`) — spawn one goroutine per job, and use a buffered channel as a permit counter. A job must acquire a token before it may run.
- **Worker Pool** (`worker_pool.go`) — spawn a fixed number of worker goroutines up front and feed them jobs over a channel.

Both expose the same surface, so they can be compared directly:

```go
func (r *SemaphoreRunner) Run(ctx context.Context, jobs []int, process func(int))
func (r *SemaphoreRunner) RunWithMetrics(ctx context.Context, jobs []int, process func(int)) Metrics

func (p *WorkerPool) Run(ctx context.Context, jobs []int, process func(int))
func (p *WorkerPool) RunWithMetrics(ctx context.Context, jobs []int, process func(int)) Metrics
```

`RunWithMetrics` additionally returns `Metrics.MaxActive` — the highest number of callbacks that were executing simultaneously. This is the number that actually answers "did the limit hold?", rather than a wall-clock guess.

## How each one works

### Semaphore

```go
sem := make(chan struct{}, r.limit)  // buffered channel = `limit` permits

for _, job := range jobs {
    wg.Add(1)
    go func(job int) {
        defer wg.Done()

        select {
        case sem <- struct{}{}:   // acquire a permit
        case <-ctx.Done():        // or give up if cancelled
            return
        }
        defer func() { <-sem }()  // release on return

        process(job)
    }(job)
}

wg.Wait()
```

Every job gets a goroutine immediately. A job that can't get a permit simply parks inside `select` until one frees up. Concurrency is limited by the channel's buffer size.

### Worker Pool

```go
jobCh := make(chan int)

for i := 0; i < p.workers; i++ {   // fixed pool, started once
    wg.Add(1)
    go func() {
        defer wg.Done()
        for {
            select {
            case job, ok := <-jobCh:
                if !ok {
                    return       // channel closed = shut down
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
```

Only `workers` goroutines ever exist. Each loops: take a job, process it, repeat. The unbuffered `jobCh` means the feeding loop blocks until a worker is actually free to take the next job — the channel *is* the queue.

## Results

Measured by `go run .` — 1000 jobs, concurrency 10, 10ms of simulated work per job (so the theoretical floor is `1000/10 x 10ms = 1.0s`).

| Metric | Semaphore | Worker Pool |
|---|---|---|
| **Throughput** (3 runs, jobs/sec) | 949.1 / 950.8 / 845.1 | 957.9 / 963.7 / 959.5 |
| Duration | ~1.04s | ~1.04s |
| `MaxActive` | 10 | 10 |
| **Peak goroutines** | **1003** | **13** |
| Memory allocated | ~0.9 MB | ~0.2 MB |

### Analysis

**Throughput is a tie, and that's the expected result.** Both honour the limit of 10 and both sit at the 1.0s floor imposed by the workload. Concurrency limiting only buys you something when the resource is actually scarce; here the "resource" is a `time.Sleep`, so once both are running 10 jobs at a time there is nothing left to optimise. The ~1s figure is arithmetic, not a measurement of either pattern's efficiency.

**Goroutine count is where they genuinely differ — by ~77x.** This is the real result:

- The semaphore spawns **one goroutine per job**, so 1000 jobs means 1000 goroutines regardless of the limit. Only 10 do useful work at any moment; the other 990 are blocked in `select`, each holding a stack (a few KB minimum, more as it grows). With 1M jobs you'd attempt 1M goroutines.
- The worker pool spawns exactly `workers` goroutines and reuses them, so the count is 10 workers + the caller, no matter whether you feed it 10 jobs or 10 million.

Neither leaks — `runtime.NumGoroutine()` returns to baseline after every run in both cases. The difference is peak allocation, not a leak.

**Cancellation behaves differently, and only the pool is prompt.** Once a semaphore run is cancelled and permits free up, a waiting job's `select` has *two* ready cases (a free permit, and `ctx.Done()`), and Go picks uniformly at random. So a cancelled semaphore run can keep processing jobs. The pool is deterministic here: the feeding loop's only ready case is `ctx.Done()`, so it closes the channel and returns, and each worker exits having processed at most one job. This is covered by `TestSemaphoreRunner_Run_ContextCancelledWhileJobsWait` (invariants only) and `TestWorkerPool_Run_ContextCancelledWhileJobsWait` (exact count).

**Fairness.** A worker pool is implicitly FIFO, because jobs leave an ordered channel and workers pull in order. The semaphore makes no ordering promise at all — which goroutine wins a freed permit is up to the scheduler. Neither is documented as ordered, so don't build on it.

## When to use which

**Worker pool** — the default choice. Bounded goroutines, predictable memory, prompt cancellation, natural backpressure from the unbuffered channel. Use it unless you have a reason not to.

**Semaphore** — when you want the shape of "spawn everything now, let it self-throttle", and the job count is small enough that one goroutine each is genuinely free. It is also the more flexible pattern: the limit can be derived per job, and a semaphore composes naturally with other resources (a second `select` case on a shared token, rate limiting, a priority scheme) in ways a fixed worker count does not.

Rule of thumb: if `len(jobs)` is unbounded or the jobs arrive as a stream, the pool is the safer structure. The semaphore's cost scales with the job count, which is exactly the thing you usually can't predict.

## Tests

43 top-level tests / 101 cases with subtests, all in-package, no external dependencies:

| File | Covers |
|---|---|
| `semaphore_test.go` | `NewSemaphoreRunner`, `Run`, `RunWithMetrics`: all jobs processed, empty and nil job lists, single job, limit 1 sequencing, limit > job count, permit release on callback return, duplicate input IDs, invalid configuration |
| `worker_pool_test.go` | The same shape for the pool, plus worker release on empty job lists and the `active` counter after a recovered panic |
| `concurrency_test.go` | Shared invariants for both: limit enforcement, `MaxActive`, independent concurrent runs, repeatability, panic propagation |
| `context_test.go` | Cancellation mid-flight, pre-cancelled, cancellation during scheduling, and recovery for subsequent runs |

Every function under test is at **100% statement coverage**. (`go test -cover` reports 74.1% overall; the gap is `main.go`'s demo path, which only runs from `main()`.)

Three details worth knowing if you read the tests:

- **Overlap is created with channels, not sleeps.** Each callback reports itself on a buffered `entered` channel and then parks on `release`. The test decides when overlapping work ends, so assertions about peak concurrency are deterministic rather than scheduler-dependent. There is no `time.Sleep` anywhere in the suite; the 10s timeouts exist only to turn a hang into a failure.
- **The exactly-once assertion is itself tested.** `TestRunners_ExactlyOnceCheckDetectsBadRuns` feeds the shared checker a missing job, a duplicate, an unexpected job, and an empty run, and requires it to reject all four — otherwise the other tests could pass for the wrong reason.
- **`recordPeak` deliberately reimplements `updateMax` from `main.go`.** Reusing the production helper would let a bug in it hide a bug in a runner.

## Known issues in the implementations

Documented in test comments rather than fixed — changing these would be a behaviour change, not a test fix.

1. **Constructors accept any `int`.** `NewSemaphoreRunner(-1)` panics inside `make(chan struct{}, -1)` with `makechan: size out of range`. `NewSemaphoreRunner(0)` and `NewWorkerPool(0)`/`(-1)` block forever on a non-empty job list with a live context. The same class of bad input produces three different failure modes; a `limit < 1` guard at construction would make them consistent.
2. **Semaphore cancellation is not prompt** (see Analysis above). Not a deadlock — the run always returns — but a cancelled run may still process jobs.
3. **A panicking callback kills the process.** Neither implementation recovers. Note that `defer wg.Done()` and `defer func() { <-sem }()` run *during* panic unwinding, so `Run` can return while a job goroutine is still unwinding: a caller that recovers inside its own callback can get `Run` back with work silently abandoned. `TestRunners_PanicInProcess_PropagatesAndCrashesProcess` documents the propagation in a subprocess; `TestRunners_PanicRecoveredByCallback_StillReleasesResources` covers the recover-in-callback case and asserts no permit or worker is leaked.

## Running it yourself

```bash
make run      # compare both patterns on 1000 jobs
make test     # go test ./...
make race     # go test -race ./...
make cover    # coverage profile + per-function breakdown
make check    # test + race + cover
```

Each folder is an independent Go module (Go 1.24) — `cd` into it before running `go` commands.
