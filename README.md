# go_playground

Small, self-contained Go experiments — one folder per topic, each with its own `go.mod`.

## Topics

| Folder | What it explores |
|---|---|
| [`singleflight/`](singleflight/) | `golang.org/x/sync/singleflight` — deduplicating concurrent identical calls, with a load-test comparison (Fiber server, Go/vegeta load tester) showing its effect under a bottlenecked backend |

## Conventions

- Each folder is an independent Go module (`go.mod` per topic) — cd into it before running `go` commands.
- Each folder has its own `README.md` with what it does, how it works, and (where relevant) load-test results.
- Run things via each folder's `Makefile` where present (`make run`, `make loadtest-*`, etc).
