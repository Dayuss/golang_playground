package main

import (
	"sync"
	"sync/atomic"
	"time"
)

const clientBuffer = 16 // events buffered per client before we start dropping

// Event is the unit of broadcast. It is rendered to clients as one SSE frame:
//
//	id: 42
//	event: price
//	data: {"id":42,"topic":"stocks",...}
type Event struct {
	ID      uint64    `json:"id"`
	Topic   string    `json:"topic"`
	Type    string    `json:"type"`
	Payload any       `json:"payload"`
	At      time.Time `json:"at"`
}

// client is one SSE subscriber: an inbox channel plus the topics it cares about.
type client struct {
	ch     chan Event
	topics map[string]struct{}
}

func (c *client) interested(topic string) bool {
	if _, all := c.topics["*"]; all {
		return true
	}
	_, ok := c.topics[topic]
	return ok
}

// Broker fans out events to every subscribed client. Publishing never blocks:
// a slow client simply gets its events dropped (counted in stats) instead of
// stalling every other subscriber.
type Broker struct {
	mu        sync.RWMutex
	clients   map[*client]struct{}
	seq       atomic.Uint64
	published atomic.Uint64
	dropped   atomic.Uint64
}

func NewBroker() *Broker {
	return &Broker{clients: make(map[*client]struct{})}
}

// Subscribe registers a client for the given topics ("*" means all).
func (b *Broker) Subscribe(topics ...string) *client {
	if len(topics) == 0 {
		topics = []string{"*"}
	}
	set := make(map[string]struct{}, len(topics))
	for _, t := range topics {
		set[t] = struct{}{}
	}
	c := &client{ch: make(chan Event, clientBuffer), topics: set}

	b.mu.Lock()
	b.clients[c] = struct{}{}
	b.mu.Unlock()
	return c
}

// Unsubscribe removes a client. Holding the write lock while closing the
// channel guarantees no Publish is mid-send (Publish only sends under RLock).
func (b *Broker) Unsubscribe(c *client) {
	b.mu.Lock()
	if _, ok := b.clients[c]; ok {
		delete(b.clients, c)
		close(c.ch)
	}
	b.mu.Unlock()
}

// Publish broadcasts one event and returns it (with its assigned ID).
func (b *Broker) Publish(topic, typ string, payload any) Event {
	ev := Event{
		ID:      b.seq.Add(1),
		Topic:   topic,
		Type:    typ,
		Payload: payload,
		At:      time.Now().UTC(),
	}
	b.published.Add(1)

	b.mu.RLock()
	defer b.mu.RUnlock()
	for c := range b.clients {
		if !c.interested(topic) {
			continue
		}
		select {
		case c.ch <- ev:
		default:
			b.dropped.Add(1) // client inbox full → it is too slow, skip it
		}
	}
	return ev
}

// Stats is a point-in-time snapshot of broker activity.
type Stats struct {
	Clients   int    `json:"clients"`
	Published uint64 `json:"published"`
	Dropped   uint64 `json:"dropped"`
}

func (b *Broker) Stats() Stats {
	b.mu.RLock()
	n := len(b.clients)
	b.mu.RUnlock()
	return Stats{Clients: n, Published: b.published.Load(), Dropped: b.dropped.Load()}
}

// NextID hands out a stream-global sequence number (used for the hello frame).
func (b *Broker) NextID() uint64 { return b.seq.Add(1) }
