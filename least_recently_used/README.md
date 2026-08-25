# LRU Cache on Golang

## What is an LRU cache

An LRU (Least Recently Used) cache is a fixed-capacity key-value store that evicts the entry that hasn't been accessed the longest whenever it's full and a new key needs to be inserted. It's the standard cache eviction policy when you want bounded memory but still want recently/frequently touched data to stay hot.

This implementation (`lru.go`) combines two structures for O(1) `Get`/`Put`:
- `map[string]*list.Element` — O(1) lookup from key to its position in the list.
- `container/list.List` (doubly linked list) — tracks recency order. Front = least recently used, back = most recently used.

```go
type LRUCache struct {
	capacity int
	items    map[string]*list.Element
	list     *list.List
	mu       sync.Mutex
}
```

How it works:
1. `Get(key)` — looks up the element in the map, moves it to the back of the list (marks it most recently used), and returns its value.
2. `Put(key, value)` — if the key already exists, it moves the element to the back (see **Known limitation** below). Otherwise it pushes a new entry to the back and inserts it into the map. If this pushes the list over `capacity`, `RemoveLRU` evicts the front element (the least recently used one) from both the list and the map.
3. `mu sync.Mutex` guards every method, since `map` and `container/list` are not safe for concurrent access — a `Get`/`Put` from multiple goroutines at once without a lock can corrupt the map or the list's internal pointers.

## Known limitation

`Put` on an **existing** key currently only refreshes recency — it does not overwrite the stored value:

```go
if item, ok := l.items[key]; ok {
	// move to back as recently use
	l.list.MoveToBack(item)
	return
}
```

So `Put("A", "Apple")` followed by `Put("A", "Avocado")` leaves `Get("A")` returning `"Apple"`, not `"Avocado"`. This is covered explicitly by `TestLRUCache_PutExistingKeyRefreshesRecencyButNotValue` in `lru_test.go`, which documents the current behavior rather than silently relying on it.

## Setup

- `lru.go` — the cache implementation (`NewLRUCache`, `Get`, `Put`, `RemoveLRU`, `GetSize`)
- `main.go` — a small runnable demo showing eviction in action
- `lru_test.go` — unit tests covering basic get/put, cache miss, eviction order, size tracking, the existing-key `Put` limitation above, and the `capacity <= 0` panic

## Running it yourself

```bash
go run .     # runs the demo in main.go
go test ./.  # runs the unit tests
```
