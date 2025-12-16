# W3C Trace Context - Agent

## Overview

The agent receives W3C Trace Context data from the PHP extension and stores it in ClickHouse.

## Data Flow

1. PHP extension sends span JSON with `w3c_traceparent` and `w3c_tracestate` fields
2. Agent unmarshals JSON into `Incoming` struct
3. Agent extracts W3C fields and includes in `Span` struct
4. Agent writes to ClickHouse `spans_full` table

## Data Structures

### Incoming Struct

```go
type Incoming struct {
    // ... other fields ...
    W3CTraceParent  *string `json:"w3c_traceparent,omitempty"`
    W3CTraceState   *string `json:"w3c_tracestate,omitempty"`
}
```

### Span Struct

```go
type Span struct {
    // ... other fields ...
    W3CTraceParent   *string `json:"w3c_traceparent,omitempty"`
    W3CTraceState    *string `json:"w3c_tracestate,omitempty"`
}
```

## Storage

### ClickHouse Schema

```sql
CREATE TABLE opa.spans_full (
    -- ... other columns ...
    w3c_traceparent Nullable(String) DEFAULT NULL,
    w3c_tracestate Nullable(String) DEFAULT NULL
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDateTime(start_ts / 1000))
ORDER BY (start_ts, trace_id);
```

### Writing Data

W3C fields are included in the `full` map when writing to ClickHouse:

```go
if inc.W3CTraceParent != nil && *inc.W3CTraceParent != "" {
    full["w3c_traceparent"] = *inc.W3CTraceParent
}
if inc.W3CTraceState != nil && *inc.W3CTraceState != "" {
    full["w3c_tracestate"] = *inc.W3CTraceState
}
writer.AddFull(full)
```

## Querying

### Find Spans with W3C Data

```sql
SELECT 
    trace_id,
    span_id,
    name,
    w3c_traceparent,
    w3c_tracestate
FROM opa.spans_full
WHERE w3c_traceparent IS NOT NULL
ORDER BY start_ts DESC
LIMIT 10;
```

### Count Spans with W3C Data

```sql
SELECT 
    COUNT(*) as total,
    COUNT(w3c_traceparent) as with_traceparent,
    COUNT(w3c_tracestate) as with_tracestate
FROM opa.spans_full
WHERE start_ts > now() - INTERVAL 1 HOUR;
```

## API Endpoints

W3C fields are included in trace API responses:

- `GET /api/traces/{trace_id}`: Returns trace with W3C fields in spans
- `GET /api/traces`: Returns traces, W3C fields included in span data

## Implementation Details

### JSON Unmarshaling

The agent uses `json.Unmarshal()` to parse incoming span data. W3C fields are automatically extracted if present in the JSON.

### Null Handling

W3C fields are optional (`omitempty` tag), so they may be `nil`:
- `nil` values are not written to ClickHouse (stored as `NULL`)
- Only non-empty strings are stored

### Data Validation

The agent does not validate W3C header format. Validation is done by the PHP extension. The agent simply stores the string values as received.

## Troubleshooting

### W3C Fields Not Appearing in ClickHouse

1. **Check if PHP extension is sending data:**
   - Verify span JSON includes `w3c_traceparent` and `w3c_tracestate` fields
   - Check agent logs for span processing

2. **Check ClickHouse schema:**
   ```sql
   DESCRIBE opa.spans_full;
   ```
   Verify `w3c_traceparent` and `w3c_tracestate` columns exist

3. **Check agent logs:**
   - Look for errors during span processing
   - Verify ClickHouse INSERT operations succeed

## References

- Main documentation: `docs/W3C_TRACE_CONTEXT.md`
- [W3C Trace Context Specification](https://www.w3.org/TR/trace-context/)

