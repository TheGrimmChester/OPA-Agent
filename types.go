package main

import "encoding/json"

// Incoming represents raw span data from PHP extension
type Incoming struct {
	Type            string          `json:"type"`
	TraceID         string          `json:"trace_id"`
	SpanID          string          `json:"span_id"`
	ParentID        *string         `json:"parent_id"`
	Service         string          `json:"service"`
	Name            string          `json:"name"`
	URLScheme       *string         `json:"url_scheme,omitempty"`
	URLHost         *string         `json:"url_host,omitempty"`
	URLPath         *string         `json:"url_path,omitempty"`
	StartTS         int64           `json:"start_ts"`
	EndTS           int64           `json:"end_ts"`
	Duration        float64         `json:"duration_ms"`
	CPUms           float64         `json:"cpu_ms"`
	Status          string          `json:"status"`
	Net             json.RawMessage `json:"net,omitempty"`
	Sql             json.RawMessage `json:"sql,omitempty"`
	Http            json.RawMessage `json:"http,omitempty"`
	Cache           json.RawMessage `json:"cache,omitempty"`
	Redis           json.RawMessage `json:"redis,omitempty"`
	Stack           []interface{}   `json:"stack,omitempty"`
	Tags            json.RawMessage `json:"tags,omitempty"`
	Dumps           json.RawMessage `json:"dumps,omitempty"`
	ChunkID         *string         `json:"chunk_id,omitempty"`
	ChunkSeq        *int            `json:"chunk_seq,omitempty"`
	ChunkDone       *bool           `json:"chunk_done,omitempty"`
	Raw             json.RawMessage `json:"raw,omitempty"`
	Language        *string         `json:"language,omitempty"`
	LanguageVersion *string         `json:"language_version,omitempty"`
	Framework       *string         `json:"framework,omitempty"`
	FrameworkVersion *string        `json:"framework_version,omitempty"`
}

// CallNode represents a function call in the stack trace
type CallNode struct {
	CallID              string                 `json:"call_id"`
	Function            string                 `json:"function"`
	Class               string                 `json:"class,omitempty"`
	File                string                 `json:"file,omitempty"`
	Line                int                    `json:"line,omitempty"`
	DurationMs          float64                `json:"duration_ms"`
	CPUMs               float64                `json:"cpu_ms"`
	MemoryDelta         int64                  `json:"memory_delta"`
	NetworkBytesSent    int64                  `json:"network_bytes_sent"`
	NetworkBytesReceived int64                 `json:"network_bytes_received"`
	ParentID            string                 `json:"parent_id"`
	Depth               int                    `json:"depth"`
	FunctionType        int                    `json:"function_type"`
	SQLQueries          []interface{}          `json:"sql_queries,omitempty"`
	HttpRequests        []interface{}          `json:"http_requests,omitempty"`
	CacheOperations     []interface{}          `json:"cache_operations,omitempty"`
	RedisOperations     []interface{}          `json:"redis_operations,omitempty"`
	Children            []*CallNode            `json:"children,omitempty"`
}

// Span represents a processed span with reconstructed relationships
type Span struct {
	TraceID         string                 `json:"trace_id"`
	SpanID          string                 `json:"span_id"`
	ParentID        *string                `json:"parent_id"`
	Service         string                 `json:"service"`
	Name            string                 `json:"name"`
	StartTS         int64                  `json:"start_ts"`
	EndTS           int64                  `json:"end_ts"`
	Duration        float64                `json:"duration_ms"`
	CPUms           float64                `json:"cpu_ms"`
	Status          string                 `json:"status"`
	Net             map[string]interface{} `json:"net,omitempty"`
	Sql             []interface{}          `json:"sql,omitempty"`
	Http            []interface{}          `json:"http,omitempty"`
	Cache           []interface{}          `json:"cache,omitempty"`
	Redis           []interface{}          `json:"redis,omitempty"`
	Stack           []*CallNode            `json:"stack,omitempty"`
	StackFlat       []*CallNode            `json:"stack_flat,omitempty"`
	Tags            map[string]interface{} `json:"tags,omitempty"`
	Dumps           []interface{}          `json:"dumps,omitempty"`
	Children        []*Span                `json:"children,omitempty"`
	Language        *string                `json:"language,omitempty"`
	LanguageVersion *string                `json:"language_version,omitempty"`
	Framework       *string                `json:"framework,omitempty"`
	FrameworkVersion *string               `json:"framework_version,omitempty"`
}

// Trace represents a complete trace with all spans
type Trace struct {
	TraceID string  `json:"trace_id"`
	Spans   []*Span `json:"spans"`
	Root    *Span   `json:"root"`
}

