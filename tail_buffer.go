package main

import (
	"encoding/json"
	"sync"
	"time"
)

// TailBuffer stores spans for trace reconstruction
type TailBuffer struct {
	mu        sync.Mutex
	data      map[string][]json.RawMessage
	cap       int
	ttl       time.Duration
	lastSeen  map[string]time.Time
	keepTrace map[string]bool
}

// NewTailBuffer creates a new tail buffer
func NewTailBuffer(cap int, ttl time.Duration) *TailBuffer {
	tb := &TailBuffer{
		data:      make(map[string][]json.RawMessage),
		cap:       cap,
		ttl:       ttl,
		lastSeen:  make(map[string]time.Time),
		keepTrace: make(map[string]bool),
	}
	go tb.gc()
	return tb
}

// Add adds a span to the buffer
func (t *TailBuffer) Add(traceID string, raw json.RawMessage) {
	t.mu.Lock()
	defer t.mu.Unlock()
	arr := t.data[traceID]
	if len(arr) >= t.cap {
		arr = arr[1:]
	}
	arr = append(arr, raw)
	t.data[traceID] = arr
	t.lastSeen[traceID] = time.Now()
}

// Get retrieves all spans for a trace without removing them
func (t *TailBuffer) Get(traceID string) []json.RawMessage {
	t.mu.Lock()
	defer t.mu.Unlock()
	arr := t.data[traceID]
	// Return a copy to avoid race conditions
	result := make([]json.RawMessage, len(arr))
	copy(result, arr)
	return result
}

// GetAndClear retrieves and removes all spans for a trace
func (t *TailBuffer) GetAndClear(traceID string) []json.RawMessage {
	t.mu.Lock()
	defer t.mu.Unlock()
	arr := t.data[traceID]
	delete(t.data, traceID)
	delete(t.lastSeen, traceID)
	delete(t.keepTrace, traceID)
	return arr
}

// MarkKeep marks a trace to be kept (not garbage collected)
func (t *TailBuffer) MarkKeep(traceID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.keepTrace[traceID] = true
}

// ShouldKeep checks if a trace should be kept
func (t *TailBuffer) ShouldKeep(traceID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.keepTrace[traceID]
}

// ClearAll clears all traces from the buffer
func (t *TailBuffer) ClearAll() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.data = make(map[string][]json.RawMessage)
	t.lastSeen = make(map[string]time.Time)
	t.keepTrace = make(map[string]bool)
}

// gc runs garbage collection to remove old traces
func (t *TailBuffer) gc() {
	for {
		time.Sleep(time.Minute)
		t.mu.Lock()
		now := time.Now()
		for k, v := range t.lastSeen {
			if !t.keepTrace[k] && now.Sub(v) > t.ttl {
				delete(t.data, k)
				delete(t.lastSeen, k)
			}
		}
		t.mu.Unlock()
	}
}

