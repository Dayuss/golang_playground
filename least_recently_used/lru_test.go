package main

import "testing"

func TestLRUCache_GetPutBasic(t *testing.T) {
	cache := NewLRUCache(2)

	cache.Put("A", "Apple")
	cache.Put("B", "Banana")

	if v, ok := cache.Get("A"); !ok || v != "Apple" {
		t.Fatalf("Get(A) = %q, %v; want Apple, true", v, ok)
	}
	if v, ok := cache.Get("B"); !ok || v != "Banana" {
		t.Fatalf("Get(B) = %q, %v; want Banana, true", v, ok)
	}
}

func TestLRUCache_GetMissingKey(t *testing.T) {
	cache := NewLRUCache(2)

	if v, ok := cache.Get("X"); ok || v != "" {
		t.Fatalf("Get(X) = %q, %v; want \"\", false", v, ok)
	}
}

func TestLRUCache_EvictsLeastRecentlyUsed(t *testing.T) {
	cache := NewLRUCache(3)

	cache.Put("A", "Apple")
	cache.Put("B", "Banana")
	cache.Put("C", "Cherry")

	// touch A so it becomes most recently used
	cache.Get("A")

	// D pushes cache over capacity, B (least recently used) should be evicted
	cache.Put("D", "Durian")

	if _, ok := cache.Get("B"); ok {
		t.Fatalf("Get(B) = ok; want evicted")
	}
	if v, ok := cache.Get("C"); !ok || v != "Cherry" {
		t.Fatalf("Get(C) = %q, %v; want Cherry, true", v, ok)
	}
	if v, ok := cache.Get("A"); !ok || v != "Apple" {
		t.Fatalf("Get(A) = %q, %v; want Apple, true", v, ok)
	}
	if v, ok := cache.Get("D"); !ok || v != "Durian" {
		t.Fatalf("Get(D) = %q, %v; want Durian, true", v, ok)
	}
}

func TestLRUCache_GetSize(t *testing.T) {
	cache := NewLRUCache(2)

	if got := cache.GetSize(); got != 0 {
		t.Fatalf("GetSize() = %d; want 0", got)
	}

	cache.Put("A", "Apple")
	if got := cache.GetSize(); got != 1 {
		t.Fatalf("GetSize() = %d; want 1", got)
	}

	cache.Put("B", "Banana")
	cache.Put("C", "Cherry") // evicts one entry, size stays capped

	if got := cache.GetSize(); got != 2 {
		t.Fatalf("GetSize() = %d; want 2", got)
	}
}

func TestLRUCache_PutExistingKeyRefreshesRecencyButNotValue(t *testing.T) {
	cache := NewLRUCache(2)

	cache.Put("A", "Apple")
	cache.Put("B", "Banana")

	// A is least recently used at this point; re-Put A should move it
	// to the back (most recently used) instead of inserting a new entry.
	cache.Put("A", "Avocado")

	if got := cache.GetSize(); got != 2 {
		t.Fatalf("GetSize() = %d; want 2 (no new entry should be added)", got)
	}

	// Current implementation only moves the existing entry to the back
	// and does not overwrite its value, so the original value is kept.
	if v, ok := cache.Get("A"); !ok || v != "Apple" {
		t.Fatalf("Get(A) = %q, %v; want Apple, true (Put on existing key does not update value)", v, ok)
	}

	// B is now the least recently used and should be evicted first.
	cache.Put("C", "Cherry")

	if _, ok := cache.Get("B"); ok {
		t.Fatalf("Get(B) = ok; want evicted after re-Put(A) refreshed A's recency")
	}
	if _, ok := cache.Get("A"); !ok {
		t.Fatalf("Get(A) = not ok; want present, since re-Put(A) should have protected it from eviction")
	}
}

func TestNewLRUCache_PanicsOnInvalidCapacity(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for capacity <= 0")
		}
	}()
	NewLRUCache(0)
}
