# Singleflight on golang

## What is `singleflight`

`golang.org/x/sync/singleflight` is a package from the Go team for **deduplicating identical, concurrent calls**. It exposes one core type, `singleflight.Group`, with one main method:

```go
func (g *Group) Do(key string, fn func() (any, error)) (v any, err error, shared bool)
```

How it works:
1. Each call to `Do` is identified by a `key` (any string — in this project, the user `id`).
2. If **no other call** is currently in flight for that `key`, `fn()` runs normally.
3. If **another call is already in flight** for the same `key`, the new caller does **not** run `fn()` again — it just attaches as a waiter to the in-flight call and blocks until that call finishes.
4. Once the original call finishes, its result (`v`, `err`) is broadcast to every waiter attached to it — including the caller that originally triggered it.
5. `shared` is `true` when the result was shared with more than one caller.

Usage in `main.go`:

```go
var group singleflight.Group

user, err, _ := group.Do(id, func() (any, error) {
    return getUserDB(id)
})
```

Effect: if 100 requests hit `id=123` at the same time, `getUserDB("123")` actually runs **once** — not 100 times. The other 99 requests wait and receive the same result without each hitting the DB themselves.

**When this matters**: preventing *cache stampede* / *thundering herd* — the classic scenario where a cache entry expires and thousands of requests arrive at once, all falling through to the DB in parallel. With singleflight, only one of them actually hits the DB; the rest ride along on that result.

**Limitations**:
- Dedup only happens for requests **concurrent with the same key** — if requests arrive sequentially (the first one finishes before the second arrives), nothing gets deduped, because there's no in-flight call to attach to.
- It's not a cache — once all calls for a key finish, the `Group` forgets the result. The next request (even 1ms later) triggers a brand-new `fn()` call from scratch.
- It's not a substitute for a connection pool or rate limiter — it only reduces *how many* calls need to reach the resource behind it, not how many are allowed to run concurrently.

## Setup

- Server: `main.go` (Fiber v3), `go run .` on `:3000`
- Simulated DB call: `getUserDB` sleeps 2s per call, and is gated by a **5-slot semaphore** (`dbSem`) that simulates a limited DB connection pool — only 5 calls can actually run at once, everyone else queues.
- Load tool: a small Go CLI at `loadtest/`, built on [`vegeta`](https://github.com/tsenart/vegeta) (`vegeta.Attacker`), run via `make loadtest-*`
- Endpoints compared:
  - `/sf/user/:id` — wrapped in `singleflight.Group.Do(id, ...)`
  - `/user/:id` — calls `getUserDB` directly, no dedup
- Scenarios (both hit the same hot key `id=123`, so singleflight has something to dedup):
  - `burst` (`make loadtest-sf` / `make loadtest-plain`) — fires at 200 req/s for 3s (600 requests), fast enough that concurrent requests overlap while the 2s simulated DB call is in flight
  - `ramp` (`make loadtest-sf-ramp` / `make loadtest-plain-ramp`) — ramps 0→50 req/s over 10s, holds 50 req/s for 20s, ramps back down over 10s (1500 requests), mirroring a sustained traffic spike

## Results

| Metric | `/sf/user/:id` (singleflight) | `/user/:id` (plain) |
|---|---|---|
| **Burst** (600 req @ 200 req/s, 3s) | | |
| Success rate | **100%** | **0%** — all 600 timed out (30s) |
| Latency avg | 1.17s | 30.00s (timeout) |
| Latency p95 | 1.93s | 30.00s (timeout) |
| **Ramp** (1500 req, 0→50→0 req/s over 40s) | | |
| Success rate | **100%** | **0%** — all 1500 timed out (30s) |
| Latency avg | 1.01s | 30.00s (timeout) |
| Latency p95 | 1.92s | 30.00s (timeout) |
| Throughput — higher is better | 37.07 req/s | 0 req/s |

## Analysis

With the 5-slot semaphore in place, the plain endpoint effectively has a max sustained throughput of `5 slots / 2s per call` = **2.5 req/s**. Both test scenarios fire well above that (200 req/s burst, up to 50 req/s sustained ramp), so every plain request queues behind an ever-growing backlog and eventually hits vegeta's 30s client timeout before a connection slot ever frees up. Result: **0% success**, effectively a self-inflicted denial of service from redundant duplicate work.

The singleflight endpoint sidesteps this entirely: concurrent requests for the same `id` collapse into a single in-flight call before they ever reach the semaphore. Regardless of how many requests arrive per second, only a handful of them actually touch the 5-slot pool — the rest just wait for a result that's already being computed. Latency stays pinned near the DB's own 2s floor, and every request succeeds.

**Caveat**: this test used one single hot key (`id=123`) for every request — the best case for singleflight. Dedup only happens for concurrent requests to the *same* key; if traffic were spread across many distinct IDs, `/sf/user/:id` and `/user/:id` would show identical (and identically bad) results, since there's nothing to collapse.

## Conclusion

- Singleflight is not a latency optimization for the request that triggers the work — it's a **load-shedding mechanism for duplicate concurrent requests to the same key**.
- Its payoff is invisible under an unconstrained backend (no queuing to avoid, since there's nothing to bottleneck on) — it only shows up as fewer backend calls.
- Under a backend with real, limited capacity (a connection pool, rate limit, or any shared resource), that same call reduction becomes the difference between the endpoint staying responsive and it collapsing entirely under duplicate traffic — as seen above, 0% vs 100% success rate at the same request rate.
- Moral: to load-test a dedup/cache layer meaningfully, the downstream dependency must model real scarcity (connection limits, rate limits, CPU), not just fixed latency — otherwise the benefit only shows up in call-count metrics you have to measure separately, not in the load tool's own numbers.

## Running it yourself

```bash
make run                  # run the server
make loadtest-sf          # burst, singleflight endpoint
make loadtest-plain       # burst, plain endpoint
make loadtest-sf-ramp     # ramp, singleflight endpoint
make loadtest-plain-ramp  # ramp, plain endpoint
```
