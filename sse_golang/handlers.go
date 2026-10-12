package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const (
	writeTimeout   = 10 * time.Second // per-write deadline; a stuck client must not block us forever
	heartbeatEvery = 15 * time.Second // SSE comment frames to keep proxies from idling out
)

// handleEvents is the SSE endpoint.
func (s *server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	var topics []string
	for _, t := range strings.Split(r.URL.Query().Get("topics"), ",") {
		if t = strings.TrimSpace(t); t != "" {
			topics = append(topics, t)
		}
	}

	cl := s.broker.Subscribe(topics...)
	defer s.broker.Unsubscribe(cl)

	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no") // tell reverse proxies not to buffer
	w.WriteHeader(http.StatusOK)

	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Now().Add(writeTimeout))

	// Ask the browser to reconnect after 3s if the stream ever breaks.
	fmt.Fprint(w, "retry: 3000\n\n")

	// EventSource sends this back automatically on reconnect so the server
	// knows what the client last saw (we just acknowledge it in a comment).
	if last := r.Header.Get("Last-Event-ID"); last != "" {
		fmt.Fprintf(w, ": resuming after event %s\n\n", last)
	}
	flusher.Flush()

	// Say hello directly to THIS client only (no broadcast, otherwise the
	// frame would arrive twice: once from the fan-out, once from this write).
	hello := Event{
		ID:    s.broker.NextID(),
		Topic: "__local__",
		Type:  "hello",
		Payload: map[string]any{
			"client_id":     fmt.Sprintf("%p", cl),
			"topics":        topicsOf(cl),
			"last_event_id": r.Header.Get("Last-Event-ID"),
		},
		At: time.Now().UTC(),
	}
	if !writeEvent(rc, w, hello) {
		return
	}

	heartbeat := time.NewTicker(heartbeatEvery)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done(): // client disconnected or server shutting down
			return
		case ev, ok := <-cl.ch:
			if !ok { // broker closed our inbox
				return
			}
			if !writeEvent(rc, w, ev) {
				return
			}
		case <-heartbeat.C:
			_ = rc.SetWriteDeadline(time.Now().Add(writeTimeout))
			if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
				return
			}
		}
		flusher.Flush()
	}
}

// writeEvent renders one event as an SSE frame. Returns false when the client
// is gone (write deadline exceeded / broken pipe).
func writeEvent(rc *http.ResponseController, w http.ResponseWriter, ev Event) bool {
	data, err := json.Marshal(ev)
	if err != nil {
		return false
	}
	_ = rc.SetWriteDeadline(time.Now().Add(writeTimeout))
	_, err = fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", ev.ID, ev.Type, data)
	return err == nil
}

func topicsOf(c *client) []string {
	out := make([]string, 0, len(c.topics))
	for t := range c.topics {
		out = append(out, t)
	}
	return out
}

func (s *server) handlePublish(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Topic   string          `json:"topic"`
		Type    string          `json:"type"`
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.Topic == "" {
		req.Topic = "custom"
	}
	if req.Type == "" {
		req.Type = "message"
	}
	// SSE event names are line-delimited; keep them one line.
	req.Type = strings.ReplaceAll(req.Type, "\n", "")

	var payload any
	if len(req.Payload) > 0 {
		if err := json.Unmarshal(req.Payload, &payload); err != nil {
			http.Error(w, "payload must be valid JSON", http.StatusBadRequest)
			return
		}
	}

	ev := s.broker.Publish(req.Topic, req.Type, payload)
	respondJSON(w, http.StatusAccepted, map[string]any{
		"id": ev.ID, "topic": ev.Topic, "type": ev.Type,
	})
}

func (s *server) handleStats(w http.ResponseWriter, _ *http.Request) {
	st := s.broker.Stats()
	respondJSON(w, http.StatusOK, map[string]any{
		"clients":   st.Clients,
		"published": st.Published,
		"dropped":   st.Dropped,
		"uptime":    time.Since(s.started).Round(time.Second).String(),
	})
}

func (s *server) handleIndex(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, indexHTML)
}

func respondJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
