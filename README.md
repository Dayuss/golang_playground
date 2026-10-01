# go_playground

Small, self-contained Go experiments — one folder per topic, each with its own `go.mod`.

## Topics

| Folder | Article | What it explores |
|---|---|---|
| [`singleflight/`](singleflight/) |  [Golang singleflight: One Line to Kill Duplicate DB Calls](https://medium.com/@dayuss/golang-singleflight-one-line-to-kill-duplicate-db-calls-5c9e96b9ac71?postPublishedType=repub)  | `golang.org/x/sync/singleflight` — deduplicating concurrent identical calls, with a load-test comparison (Fiber server, Go/vegeta load tester) showing its effect under a bottlenecked backend |
| [`least_recently_used/`](least_recently_used/) | | An LRU cache built on `map` + `container/list` for O(1) get/put/eviction, guarded with `sync.Mutex` for concurrent access — includes a documented limitation where `Put` on an existing key refreshes recency but not the stored value |
| [`semaphore_pattern/`](semaphore_pattern/) | | Two ways to bound concurrency in Go — a channel-based **semaphore** (one goroutine per job) vs a fixed **worker pool** (goroutine reuse) — benchmarked against each other, with a 43-test deterministic suite |


## Headline results

Each experiment answers one question, and the answer is in its own README:

- **singleflight** — collapses concurrent duplicate calls to the same key into one. Under a 5-slot backend limit, 600 requests at 200 req/s went from **0% success** (all timing out) to **100%**.
- **least_recently_used** — O(1) get/put/eviction via `map` + `container/list`. Carries a documented limitation: `Put` on an existing key refreshes recency but does not overwrite the value.
- **semaphore_pattern** — semaphore vs worker pool. Throughput is a **tie** (both hit the workload's 1.0s floor), but peak goroutines differ ~77x: **1003 vs 13** for 1000 jobs at concurrency 10.

## Conventions

- Each folder is an independent Go module (`go.mod` per topic) — cd into it before running `go` commands.
- Each folder has its own `README.md` with what it does, how it works, and (where relevant) load-test results.
- Run things via each folder's `Makefile` where present (`make run`, `make loadtest-*`, etc).
- Concurrency work is tested for determinism, not just coverage — channels and atomics for synchronization, never `time.Sleep`, with timeouts used only to turn a hang into a failure. `semaphore_pattern/` runs clean under `go test -race`.
