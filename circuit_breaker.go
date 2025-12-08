package main

import (
	"sync"
	"time"
)

// CircuitBreaker provides backpressure protection
type CircuitBreaker struct {
	mu          sync.Mutex
	failures    int
	lastFailure time.Time
	open        bool
	threshold   int
	timeout     time.Duration
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(threshold int, timeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		threshold: threshold,
		timeout:   timeout,
	}
}

// Allow checks if the circuit breaker allows the operation
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	
	if cb.open {
		if time.Since(cb.lastFailure) > cb.timeout {
			cb.open = false
			cb.failures = 0
			LogInfo("Circuit breaker closed - service recovered", nil)
			return true
		}
		return false
	}
	return true
}

// RecordSuccess records a successful operation
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures = 0
}

// RecordFailure records a failed operation
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures++
	cb.lastFailure = time.Now()
	if cb.failures >= cb.threshold && !cb.open {
		cb.open = true
		LogWarn("Circuit breaker opened", map[string]interface{}{
			"failures": cb.failures,
			"threshold": cb.threshold,
		})
	}
}

