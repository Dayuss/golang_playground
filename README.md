# go_playground

Small, self-contained Go experiments — one folder per topic, each with its own `go.mod`.

## Topics

| Folder | Article | What it explores |
|---|---|---|
| [`singleflight/`](singleflight/) |  [Golang singleflight: One Line to Kill Duplicate DB Calls](https://medium.com/@dayuss/golang-singleflight-one-line-to-kill-duplicate-db-calls-5c9e96b9ac71?postPublishedType=repub)  | `golang.org/x/sync/singleflight` — deduplicating concurrent identical calls, with a load-test comparison (Fiber server, Go/vegeta load tester) showing its effect under a bottlenecked backend |
| [`least_recently_used/`](least_recently_used/) | | An LRU cache built on `map` + `container/list` for O(1) get/put/eviction, guarded with `sync.Mutex` for concurrent access — includes a documented limitation where `Put` on an existing key refreshes recency but not the stored value |


## Conventions

- Each folder is an independent Go module (`go.mod` per topic) — cd into it before running `go` commands.
- Each folder has its own `README.md` with what it does, how it works, and (where relevant) load-test results.
- Run things via each folder's `Makefile` where present (`make run`, `make loadtest-*`, etc).
