package main

import (
	"container/list"
	"sync"
)

type Entry struct {
	Key   string
	Value string
}

type LRUCache struct {
	capacity int
	items    map[string]*list.Element
	list     *list.List
	mu       sync.Mutex
}

func NewLRUCache(capacity int) *LRUCache {
	if capacity <= 0 {
		panic("capacity cache must be greater than 0")
	}

	return &LRUCache{
		capacity: capacity,
		items:    map[string]*list.Element{},
		list:     list.New(),
	}
}

func (l *LRUCache) Get(key string) (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// get from hashmap
	item, ok := l.items[key]
	if !ok {
		return "", false
	}

	// move to back for recently use
	l.list.MoveToBack(item)

	return item.Value.(Entry).Value, true
}

func (l *LRUCache) RemoveLRU() {
	// get longest unused
	el := l.list.Front()
	l.list.Remove(el)
	// romve from hash map
	delete(l.items, el.Value.(Entry).Key)
}

func (l *LRUCache) Put(key, value string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if item, ok := l.items[key]; ok {
		// move to back as recently use
		l.list.MoveToBack(item)
		return
	}

	// put the value
	entry := Entry{
		Key:   key,
		Value: value,
	}

	// set hashmap
	element := l.list.PushBack(entry)

	l.items[key] = element

	// check cache capacity
	if l.list.Len() > l.capacity {
		l.RemoveLRU()
	}

}

func (l *LRUCache) GetSize() int {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.list.Len()
}
