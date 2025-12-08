package main

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	incomingCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "apm_agent_incoming_total",
		Help: "Incoming ND-JSON messages",
	})
	droppedCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "apm_agent_dropped_total",
		Help: "Dropped messages (backpressure)",
	})
	queueSizeGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "apm_agent_queue_size",
		Help: "Current queue size",
	})
	processingDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "apm_agent_processing_duration_seconds",
		Help:    "Processing duration in seconds",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 10),
	})
	spansTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "apm_spans_total",
		Help: "Total spans processed",
	}, []string{"service", "status"})
	tracesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "apm_traces_total",
		Help: "Total traces processed",
	}, []string{"service"})
	durationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "apm_duration_seconds",
		Help:    "Span duration in seconds",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
	}, []string{"service", "name"})
	networkBytesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "apm_network_bytes_total",
		Help: "Network bytes total",
	}, []string{"direction"})
	sqlQueriesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "apm_sql_queries_total",
		Help: "SQL queries total",
	}, []string{"fingerprint"})
	httpRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "apm_http_requests_total",
		Help: "HTTP requests total",
	}, []string{"method", "status"})
	cacheOperationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "apm_cache_operations_total",
		Help: "Cache operations total",
	}, []string{"cache_type", "operation", "hit_miss"})
	redisOperationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "apm_redis_operations_total",
		Help: "Redis operations total",
	}, []string{"command", "hit_miss"})
)

func init() {
	prometheus.MustRegister(
		incomingCounter, droppedCounter, queueSizeGauge, processingDuration,
		spansTotal, tracesTotal, durationHistogram, networkBytesTotal, sqlQueriesTotal,
		httpRequestsTotal, cacheOperationsTotal, redisOperationsTotal,
	)
}

