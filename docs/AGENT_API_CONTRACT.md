# OpenProfilingAgent API Contract

**Version**: 1.0  
**Last Updated**: 2025-12-12

## Table of Contents

1. [Introduction](#introduction)
2. [Contract Overview](#contract-overview)
3. [Transport Protocol](#transport-protocol)
4. [Message Types](#message-types)
5. [Span Message Specification](#span-message-specification)
6. [Error Message Specification](#error-message-specification)
7. [Log Message Specification](#log-message-specification)
8. [Nested Structure Specifications](#nested-structure-specifications)
9. [Data Types and Validation](#data-types-and-validation)
10. [Implementation Guide](#implementation-guide)
11. [Examples](#examples)
12. [HTTP REST API](#http-rest-api)
13. [Authentication and Authorization](#authentication-and-authorization)
14. [WebSocket API](#websocket-api)
15. [Versioning and Compatibility](#versioning-and-compatibility)
16. [Troubleshooting](#troubleshooting)

## Introduction

This document defines the **API Contract** for communication between profiling tools and the OpenProfilingAgent. This contract is:

- **Language-agnostic**: Can be implemented in any programming language
- **Implementation-independent**: Not tied to any specific profiling tool
- **Complete**: Every field, type, and constraint is specified
- **Authoritative**: This is the definitive specification for agent communication

### Purpose

This contract enables any developer to implement a profiling tool that can send trace data, errors, and logs to OpenProfilingAgent. The contract specifies:

- Exact message formats and structures
- Transport layer protocols (Unix socket and TCP/IP)
- Data type requirements and validation rules
- Error handling and compatibility guarantees

### Scope

This contract covers:

- **Data Ingestion Protocol**: Three message types (`span`, `error`, and `log`) via Unix socket and TCP/IP
- **HTTP REST API**: Complete REST API for querying traces, services, metrics, errors, logs, and managing configurations
- **WebSocket API**: Real-time updates for traces, metrics, and errors
- **Authentication**: JWT-based authentication and API key management
- **Multi-Tenancy**: Organization and project-based data isolation
- Message serialization: ND-JSON format with optional LZ4 compression
- All nested data structures (SQL queries, HTTP requests, cache operations, Redis operations, call stacks, etc.)

## Contract Overview

### Protocol Version

**Current Version**: 1.0

The protocol version is not explicitly specified in messages. The agent accepts messages conforming to this specification. Future protocol versions will be documented separately with migration guides.

### Language Independence

This specification is implementation-agnostic. Implementations in any programming language MUST conform to:

- JSON serialization (RFC 7159)
- UTF-8 string encoding
- Transport protocols as specified
- Message formats as defined

### Contract Terms

This document uses RFC 2119 terminology:

- **MUST**: Required for compliance
- **SHOULD**: Recommended but not required
- **MAY**: Optional
- **MUST NOT**: Prohibited

## Transport Protocol

### Overview

OpenProfilingAgent accepts messages via two transport methods:

1. **Unix Socket** (AF_UNIX): For local communication (same machine or shared volume)
2. **TCP/IP** (AF_INET): For remote communication (network)

Both transports use the same message format: **ND-JSON** (newline-delimited JSON) with optional LZ4 compression.

### Message Format

**ND-JSON Format**:
- Each message is a single JSON object
- Messages are separated by newline characters (`\n`)
- Each message MUST be on a single line (no embedded newlines in JSON)
- Messages are independent (no ordering requirements)

**Example**:
```
{"type":"span","trace_id":"abc123",...}\n
{"type":"span","trace_id":"def456",...}\n
{"type":"error","trace_id":"ghi789",...}\n
```

### Compression (Optional)

Messages MAY be compressed using LZ4 compression. The compression format is:

```
[4 bytes: "LZ4" magic string]
[8 bytes: original size (little-endian uint64)]
[compressed data]
```

**Compression Header**:
- Magic string: `"LZ4"` (4 bytes, ASCII)
- Original size: 8 bytes, little-endian unsigned 64-bit integer
- Compressed data: LZ4-compressed payload

**When to Compress**:
- Compression is optional
- Recommended for messages larger than 1KB
- Agent automatically detects and decompresses

### Encoding

- **String Encoding**: All strings MUST be UTF-8 encoded
- **JSON Encoding**: Messages MUST conform to RFC 7159
- **Number Formats**: Integers and floats as per JSON specification

### Connection Management

**Connection Behavior**:
- Clients SHOULD maintain persistent connections
- Multiple messages can be sent over a single connection
- Connections MAY be closed after sending messages (not recommended for high throughput)
- Clients SHOULD implement reconnection logic with exponential backoff

**Connection Lifecycle**:
1. Establish connection (Unix socket or TCP/IP)
2. Send one or more ND-JSON messages
3. Keep connection open for subsequent messages (recommended)
4. Handle connection errors and reconnect as needed

## Transport Implementation Guide

### Unix Socket Transport

**Protocol**: AF_UNIX with SOCK_STREAM

**Address Format**: Absolute file path (e.g., `/var/run/opa.sock`)

**Implementation Steps**:

1. **Create Socket**:
   ```c
   int sock = socket(AF_UNIX, SOCK_STREAM, 0);
   ```

2. **Set Up Address Structure**:
   ```c
   struct sockaddr_un addr;
   memset(&addr, 0, sizeof(addr));
   addr.sun_family = AF_UNIX;
   strncpy(addr.sun_path, socket_path, sizeof(addr.sun_path)-1);
   ```

3. **Connect**:
   ```c
   int result = connect(sock, (struct sockaddr*)&addr, sizeof(addr));
   ```

4. **Send Messages**:
   - Serialize message to JSON string
   - Append newline (`\n`)
   - Write to socket (handle partial writes)
   - Flush if buffered

5. **Error Handling**:
   - Handle `ENOENT` (socket file not found)
   - Handle `EACCES` (permission denied)
   - Handle `ECONNREFUSED` (connection refused)
   - Implement retry with exponential backoff

**Permissions**:
- Socket file MUST be readable and writable by the client process
- Socket file typically owned by agent process or shared group

**Code Example (Pseudocode)**:
```python
import socket
import json

def send_via_unix_socket(socket_path, messages):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.connect(socket_path)
        for message in messages:
            json_str = json.dumps(message) + "\n"
            sock.sendall(json_str.encode('utf-8'))
    finally:
        sock.close()
```

### TCP/IP Transport

**Protocol**: AF_INET with SOCK_STREAM (IPv4)

**Address Format**: `host:port` (e.g., `agent:9090`, `127.0.0.1:9090`, `:9090`)

**Implementation Steps**:

1. **Parse Address**:
   - Split `host:port` format
   - If no host specified (starts with `:`), use `127.0.0.1`
   - Validate port (1-65535)

2. **Resolve Hostname** (if not IP address):
   - Use `getaddrinfo()` or equivalent DNS resolution
   - Support IPv4 addresses
   - Handle DNS resolution errors

3. **Create Socket**:
   ```c
   int sock = socket(AF_INET, SOCK_STREAM, 0);
   ```

4. **Set Up Address Structure**:
   ```c
   struct sockaddr_in addr;
   memset(&addr, 0, sizeof(addr));
   addr.sin_family = AF_INET;
   addr.sin_port = htons(port);
   addr.sin_addr.s_addr = inet_addr(ip_address); // or from getaddrinfo
   ```

5. **Connect**:
   ```c
   int result = connect(sock, (struct sockaddr*)&addr, sizeof(addr));
   ```

6. **Send Messages**:
   - Serialize message to JSON string
   - Append newline (`\n`)
   - Write to socket (handle partial writes)
   - Flush if buffered

7. **Error Handling**:
   - Handle `ECONNREFUSED` (connection refused)
   - Handle `ETIMEDOUT` (connection timeout)
   - Handle DNS resolution errors
   - Implement retry with exponential backoff

**Code Example (Pseudocode)**:
```python
import socket
import json

def send_via_tcp(host, port, messages):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((host, port))
        for message in messages:
            json_str = json.dumps(message) + "\n"
            sock.sendall(json_str.encode('utf-8'))
    finally:
        sock.close()
```

### Auto-Detection

Clients MUST detect transport type from address format:

- **Unix Socket**: Address starts with `/` (absolute path)
- **TCP/IP**: Address contains `:` (host:port format)

**Implementation**:
```python
def detect_transport(address):
    if address.startswith('/'):
        return 'unix'
    elif ':' in address:
        return 'tcp'
    else:
        raise ValueError(f"Invalid address format: {address}")
```

### Sending Data - Step by Step

**Step 1: Choose Transport**
- Parse address to determine transport type
- Unix socket: `/path/to/socket`
- TCP/IP: `host:port` or `:port`

**Step 2: Establish Connection**
- Unix socket: Connect to socket file
- TCP/IP: Resolve hostname, connect to IP:port
- Handle connection errors gracefully

**Step 3: Serialize Message**
- Convert message object to JSON string
- Ensure valid JSON (RFC 7159 compliant)
- UTF-8 encode all strings
- Escape special characters properly

**Step 4: Format as ND-JSON**
- Append newline character (`\n`) to JSON string
- Each message MUST be on a single line
- No embedded newlines in JSON

**Step 5: Optional Compression**
- If using compression, apply LZ4 compression
- Add LZ4 header (4 bytes magic + 8 bytes size)
- Send compressed payload

**Step 6: Send Data**
- Write complete message (with newline) to socket
- Handle partial writes (may need to retry)
- Flush socket if buffered
- Monitor for write errors

**Step 7: Connection Management**
- Keep connection open for multiple messages (recommended)
- Monitor connection health
- Reconnect on errors with exponential backoff
- Close connection gracefully when done

## Message Types

The agent accepts three message types:

1. **`span`**: Performance trace data (function calls, SQL queries, HTTP requests, etc.)
2. **`error`**: Error tracking data (exceptions, PHP errors, etc.)
3. **`log`**: Application log messages

Each message MUST have a `type` field indicating the message type.

## Span Message Specification

### Overview

Span messages represent performance traces of application execution. They contain timing information, call stacks, SQL queries, HTTP requests, and other profiling data.

### Required Fields

| Field | Type | Description |
|-------|------|-------------|
| `type` | string | MUST be `"span"` |
| `trace_id` | string | Unique identifier for the trace (hex string, UUID, or similar) |
| `span_id` | string | Unique identifier for this span (hex string, UUID, or similar) |
| `service` | string | Service name (e.g., `"my-service"`, `"php-fpm"`) |
| `name` | string | Span name (e.g., `"GET /users"`, `"UserRepository::find"`) |
| `start_ts` | integer | Start timestamp in milliseconds since Unix epoch |
| `end_ts` | integer | End timestamp in milliseconds since Unix epoch |
| `duration_ms` | float | Duration in milliseconds (calculated as `end_ts - start_ts`) |
| `status` | string | Span status: `"ok"` or `"error"` |

### Optional Fields

| Field | Type | Description |
|-------|------|-------------|
| `parent_id` | string (nullable) | Parent span ID (for span hierarchy) |
| `url_scheme` | string (nullable) | URL scheme (e.g., `"http"`, `"https"`) |
| `url_host` | string (nullable) | URL host (e.g., `"example.com"`) |
| `url_path` | string (nullable) | URL path (e.g., `"/api/users"`) |
| `cpu_ms` | float | CPU time in milliseconds |
| `language` | string (nullable) | Programming language (e.g., `"php"`) |
| `language_version` | string (nullable) | Language version (e.g., `"8.4"`) |
| `framework` | string (nullable) | Framework name (e.g., `"symfony"`) |
| `framework_version` | string (nullable) | Framework version (e.g., `"7.0"`) |
| `net` | object | Network metrics (see [Network Object](#network-object)) |
| `sql` | array | SQL queries (see [SQL Query Object](#sql-query-object)) |
| `http` | array | HTTP requests (see [HTTP Request Object](#http-request-object)) |
| `cache` | array | Cache operations (see [Cache Operation Object](#cache-operation-object)) |
| `redis` | array | Redis operations (see [Redis Operation Object](#redis-operation-object)) |
| `stack` | array | Call stack (see [CallNode Object](#callnode-object)) |
| `tags` | object | Metadata tags (see [Tags Object](#tags-object)) |
| `dumps` | array | Variable dumps (see [Dump Object](#dump-object)) |
| `chunk_id` | string (nullable) | Chunk identifier (for chunked spans) |
| `chunk_seq` | integer (nullable) | Chunk sequence number |
| `chunk_done` | boolean (nullable) | Whether this is the last chunk |
| `raw` | object | Additional raw data |

### Field Details

#### `trace_id` and `span_id`

- **Format**: String (hex string, UUID, or similar)
- **Uniqueness**: MUST be unique within a trace for `span_id`, globally unique for `trace_id`
- **Length**: Typically 16-64 characters
- **Example**: `"a1b2c3d4e5f6"`, `"550e8400-e29b-41d4-a716-446655440000"`

#### `start_ts` and `end_ts`

- **Format**: Integer (milliseconds since Unix epoch)
- **Range**: Positive integers representing valid timestamps
- **Precision**: Millisecond precision
- **Example**: `1704067200000` (2024-01-01 00:00:00 UTC)

#### `duration_ms`

- **Format**: Float (milliseconds)
- **Calculation**: `end_ts - start_ts`
- **Range**: Non-negative
- **Precision**: At least 3 decimal places recommended
- **Example**: `125.456`

#### `status`

- **Values**: `"ok"` or `"error"`
- **Default**: `"ok"`
- **Agent Behavior**: Agent may override based on HTTP status codes or error indicators

### Network Object

The `net` object contains network I/O metrics:

```json
{
  "bytes_sent": 1024,
  "bytes_received": 2048
}
```

| Field | Type | Description |
|-------|------|-------------|
| `bytes_sent` | integer | Total bytes sent (non-negative) |
| `bytes_received` | integer | Total bytes received (non-negative) |

### Tags Object

The `tags` object contains metadata about the span:

```json
{
  "organization_id": "org-123",
  "project_id": "proj-456",
  "http_request": {
    "scheme": "https",
    "method": "GET",
    "uri": "/api/users",
    "host": "api.example.com",
    "query_string": "page=1",
    "ip": "192.168.1.1"
  },
  "http_response": {
    "status_code": 200
  },
  "cli": {
    "script": "/path/to/script.php",
    "args": ["arg1", "arg2"]
  },
  "expand_spans": true
}
```

| Field | Type | Description |
|-------|------|-------------|
| `organization_id` | string (optional) | Organization identifier |
| `project_id` | string (optional) | Project identifier |
| `http_request` | object | HTTP request details (always present, may be empty `{}`) |
| `http_response` | object (optional) | HTTP response details |
| `cli` | object (optional) | CLI arguments (for CLI scripts) |
| `expand_spans` | boolean | Whether to expand child spans (default: `true`) |

**HTTP Request Object** (in tags):
- `scheme`: URL scheme (`"http"` or `"https"`)
- `method`: HTTP method (`"GET"`, `"POST"`, etc.)
- `uri`: Request URI path
- `host`: Host header value
- `query_string`: Query string (without `?`)
- `ip`: Client IP address

**HTTP Response Object** (in tags):
- `status_code`: HTTP status code (integer)

**CLI Object** (in tags):
- `script`: Script path
- `args`: Array of command-line arguments

## Error Message Specification

### Overview

Error messages represent application errors, exceptions, and PHP errors. They contain error details, stack traces, and context information.

### Required Fields

| Field | Type | Description |
|-------|------|-------------|
| `type` | string | MUST be `"error"` |
| `trace_id` | string | Trace ID associated with the error |
| `span_id` | string | Span ID where error occurred |
| `instance_id` | string | Unique identifier for this error instance |
| `group_id` | string | Group identifier for error grouping |
| `fingerprint` | string | Error fingerprint for deduplication |
| `error_type` | string | Error type (e.g., `"Error"`, `"Exception"`, `"Warning"`) |
| `error_message` | string | Error message text |
| `file` | string | File where error occurred |
| `line` | integer | Line number where error occurred |
| `organization_id` | string | Organization identifier |
| `project_id` | string | Project identifier |
| `service` | string | Service name |
| `occurred_at_ms` | integer | Timestamp in milliseconds when error occurred |

### Optional Fields

| Field | Type | Description |
|-------|------|-------------|
| `stack_trace` | array or string | Stack trace (array of frames or JSON string) |
| `http_request` | object | HTTP request context (same format as span tags) |
| `tags` | object | Additional tags |
| `sql_queries` | array | SQL queries executed before error |
| `http_requests` | array | HTTP requests made before error |
| `exception_code` | integer (nullable) | Exception code (for exceptions) |
| `environment` | string | Environment name (e.g., `"production"`, `"staging"`) |
| `release` | string | Release version or identifier |
| `user_context` | object | User context information |

### Field Details

#### `instance_id` and `group_id`

- **Format**: String (hex string, UUID, or similar)
- **Purpose**: `instance_id` uniquely identifies this error occurrence; `group_id` groups similar errors
- **Example**: `"err-instance-123"`, `"err-group-456"`

#### `fingerprint`

- **Format**: String
- **Purpose**: Used for error deduplication and grouping
- **Generation**: Typically based on error type, message, file, and line
- **Example**: `"Error:Division by zero@Calculator.php:42"`

#### `stack_trace`

- **Format**: Array of frame objects OR JSON string
- **Frame Object**:
  ```json
  {
    "file": "/path/to/file.php",
    "line": 42,
    "function": "functionName",
    "class": "ClassName"
  }
  ```

## Log Message Specification

### Overview

Log messages represent application log entries. They contain log level, message text, and optional context.

### Required Fields

| Field | Type | Description |
|-------|------|-------------|
| `type` | string | MUST be `"log"` |
| `id` | string | Unique identifier for this log entry |
| `trace_id` | string | Trace ID associated with the log |
| `level` | string | Log level: `"ERROR"`, `"WARN"`, `"INFO"`, `"DEBUG"`, etc. |
| `message` | string | Log message text |
| `service` | string | Service name |
| `timestamp_ms` | integer | Timestamp in milliseconds when log was created |

### Optional Fields

| Field | Type | Description |
|-------|------|-------------|
| `span_id` | string (nullable) | Span ID associated with the log (null if not associated) |
| `fields` | object | Additional fields (e.g., `file`, `line`, custom fields) |

### Field Details

#### `level`

- **Values**: `"ERROR"`, `"WARN"`, `"INFO"`, `"DEBUG"`, `"CRITICAL"`, etc.
- **Case**: Uppercase recommended
- **Normalization**: Agent may normalize (e.g., `"warn"` → `"WARNING"`)

#### `fields`

- **Common Fields**:
  - `file`: Source file path
  - `line`: Line number
- **Custom Fields**: Any additional key-value pairs

## Nested Structure Specifications

### SQL Query Object

SQL query objects represent database queries executed during span execution.

**Location**: In `sql` array (span-level) or `sql_queries` array (CallNode-level)

```json
{
  "query": "SELECT * FROM users WHERE id = ?",
  "duration": 0.0105,
  "duration_ms": 10.5,
  "timestamp": 1704067200.0,
  "type": "query",
  "query_type": "SELECT",
  "rows_affected": 1,
  "rows_returned": 1,
  "db_system": "mysql",
  "db_host": "db.example.com",
  "db_dsn": "mysql:host=db.example.com;dbname=mydb"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `query` | string | Yes | SQL query text |
| `duration` | float | No | Duration in seconds |
| `duration_ms` | float | No | Duration in milliseconds |
| `timestamp` | float | No | Query execution timestamp (Unix seconds) |
| `type` | string | No | Query type identifier |
| `query_type` | string | No | SQL operation type: `"SELECT"`, `"INSERT"`, `"UPDATE"`, `"DELETE"` |
| `rows_affected` | integer | No | Rows affected (-1 if unknown) |
| `rows_returned` | integer | No | Rows returned (for SELECT queries) |
| `db_system` | string | No | Database system: `"mysql"`, `"postgresql"`, etc. |
| `db_host` | string | No | Database hostname |
| `db_dsn` | string | No | Database DSN (MUST NOT include password) |

### HTTP Request Object

HTTP request objects represent outgoing HTTP requests (e.g., cURL requests).

**Location**: In `http` array (span-level) or `http_requests` array (CallNode-level)

```json
{
  "url": "https://api.example.com/users",
  "method": "GET",
  "status_code": 200,
  "bytes_sent": 256,
  "bytes_received": 1024,
  "duration": 0.125,
  "duration_ms": 125.0,
  "timestamp": 1704067200.0,
  "type": "curl",
  "uri": "/users",
  "query_string": "page=1",
  "request_headers_raw": "Host: api.example.com\r\nUser-Agent: MyApp/1.0",
  "response_headers_raw": "Content-Type: application/json\r\nContent-Length: 1024",
  "response_size": 1024,
  "request_size": 256,
  "dns_time": 0.001,
  "dns_time_ms": 1.0,
  "connect_time": 0.005,
  "connect_time_ms": 5.0,
  "network_time": 0.125,
  "network_time_ms": 125.0,
  "error": null
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `url` | string | Yes | Full request URL |
| `method` | string | Yes | HTTP method (`"GET"`, `"POST"`, etc.) |
| `status_code` | integer | No | HTTP status code |
| `bytes_sent` | integer | No | Bytes sent in request |
| `bytes_received` | integer | No | Bytes received in response |
| `duration` | float | No | Total duration in seconds |
| `duration_ms` | float | No | Total duration in milliseconds |
| `timestamp` | float | No | Request timestamp (Unix seconds) |
| `type` | string | No | Request type (e.g., `"curl"`) |
| `uri` | string | No | URI path |
| `query_string` | string | No | Query string |
| `request_headers_raw` | string | No | Raw request headers |
| `response_headers_raw` | string | No | Raw response headers |
| `response_size` | integer | No | Response body size |
| `request_size` | integer | No | Request body size |
| `dns_time` | float | No | DNS lookup time in seconds |
| `dns_time_ms` | float | No | DNS lookup time in milliseconds |
| `connect_time` | float | No | Connection time in seconds |
| `connect_time_ms` | float | No | Connection time in milliseconds |
| `network_time` | float | No | Total network time in seconds |
| `network_time_ms` | float | No | Total network time in milliseconds |
| `error` | string (nullable) | No | Error message if request failed |

### Cache Operation Object

Cache operation objects represent cache operations (e.g., APCu, Symfony Cache).

**Location**: In `cache` array (span-level) or `cache_operations` array (CallNode-level)

```json
{
  "key": "user:123",
  "operation": "get",
  "hit": true,
  "duration": 0.001,
  "duration_ms": 1.0,
  "timestamp": 1704067200.0,
  "data_size": 1024,
  "cache_type": "apcu"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `key` | string | No | Cache key |
| `operation` | string | No | Operation type: `"get"`, `"set"`, `"delete"`, etc. |
| `hit` | boolean | No | Whether operation was a cache hit |
| `duration` | float | No | Duration in seconds |
| `duration_ms` | float | No | Duration in milliseconds |
| `timestamp` | float | No | Operation timestamp (Unix seconds) |
| `data_size` | integer | No | Data size in bytes |
| `cache_type` | string | No | Cache type: `"apcu"`, `"symfony"`, etc. |

### Redis Operation Object

Redis operation objects represent Redis commands.

**Location**: In `redis` array (span-level) or `redis_operations` array (CallNode-level)

```json
{
  "command": "GET",
  "key": "user:123",
  "hit": true,
  "duration": 0.002,
  "duration_ms": 2.0,
  "timestamp": 1704067200.0,
  "type": "redis",
  "error": null
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `command` | string | No | Redis command (e.g., `"GET"`, `"SET"`, `"DEL"`) |
| `key` | string | No | Redis key |
| `hit` | boolean | No | Whether operation was a cache hit (for GET operations) |
| `duration` | float | No | Duration in seconds |
| `duration_ms` | float | No | Duration in milliseconds |
| `timestamp` | float | No | Operation timestamp (Unix seconds) |
| `type` | string | No | Operation type (e.g., `"redis"`) |
| `error` | string (nullable) | No | Error message if operation failed |

### CallNode Object

CallNode objects represent function calls in the execution stack.

**Location**: In `stack` array (span-level)

```json
{
  "call_id": "call-123",
  "function": "getUser",
  "class": "UserRepository",
  "file": "/app/src/Repository/UserRepository.php",
  "line": 42,
  "duration_ms": 10.5,
  "cpu_ms": 5.2,
  "memory_delta": 1024,
  "network_bytes_sent": 256,
  "network_bytes_received": 512,
  "parent_id": "call-122",
  "depth": 3,
  "function_type": 1,
  "sql_queries": [],
  "http_requests": [],
  "cache_operations": [],
  "redis_operations": [],
  "children": []
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `call_id` | string | Yes | Unique identifier for this call |
| `function` | string | Yes | Function name |
| `class` | string | No | Class name (for methods) |
| `file` | string | No | Source file path |
| `line` | integer | No | Line number |
| `duration_ms` | float | Yes | Call duration in milliseconds |
| `cpu_ms` | float | No | CPU time in milliseconds |
| `memory_delta` | integer | No | Memory change in bytes |
| `network_bytes_sent` | integer | No | Network bytes sent during call |
| `network_bytes_received` | integer | No | Network bytes received during call |
| `parent_id` | string | No | Parent call ID (empty string for root calls) |
| `depth` | integer | No | Call depth in stack (0 for root) |
| `function_type` | integer | No | Function type code (implementation-specific) |
| `sql_queries` | array | No | SQL queries executed in this call |
| `http_requests` | array | No | HTTP requests made in this call |
| `cache_operations` | array | No | Cache operations in this call |
| `redis_operations` | array | No | Redis operations in this call |
| `children` | array | No | Child CallNodes (usually empty, agent rebuilds tree) |

**Note**: The agent rebuilds the call tree from `parent_id` relationships. The `children` array in the message is typically empty.

### Dump Object

Dump objects represent variable dumps or debug output.

**Location**: In `dumps` array (span-level)

Structure is implementation-specific. Typically contains variable names and values.

## Data Types and Validation

### String Types

- **Encoding**: MUST be UTF-8
- **JSON Escaping**: MUST escape special characters per RFC 7159:
  - `"` → `\"`
  - `\` → `\\`
  - `/` → `\/` (optional)
  - Control characters → `\uXXXX`
  - Newlines → `\n`
  - Tabs → `\t`
  - Carriage returns → `\r`

### Number Types

- **Integers**: JSON number (no decimal point)
- **Floats**: JSON number (with decimal point)
- **Precision**: At least 3 decimal places for milliseconds
- **Range**: Valid for the data type (e.g., timestamps must be positive)

### Boolean Types

- **Values**: `true` or `false` (JSON boolean, not strings)

### Null Values

- **Representation**: `null` (JSON null, not string `"null"`)
- **Nullable Fields**: Fields that can be `null` are marked as `(nullable)`

### Timestamps

- **Format**: Milliseconds since Unix epoch (January 1, 1970 00:00:00 UTC)
- **Type**: Integer (for `start_ts`, `end_ts`, `occurred_at_ms`, `timestamp_ms`)
- **Type**: Float (for `timestamp` in nested objects, Unix seconds with decimals)
- **Example**: `1704067200000` (milliseconds), `1704067200.123` (seconds)

### Arrays

- **Format**: JSON array `[]`
- **Empty Arrays**: Use `[]` (not `null`)
- **Order**: Order may be significant (e.g., call stack order)

### Objects

- **Format**: JSON object `{}`
- **Empty Objects**: Use `{}` (not `null`)
- **Key-Value Pairs**: Keys are strings, values are any JSON type

### Validation Rules

**Required Fields**:
- Missing required fields result in message rejection
- Agent logs validation errors

**Field Constraints**:
- `trace_id` and `span_id`: Non-empty strings
- `start_ts` and `end_ts`: Positive integers, `end_ts >= start_ts`
- `duration_ms`: Non-negative float
- `status`: Must be `"ok"` or `"error"`
- Port numbers: 1-65535

**Size Limits**:
- Maximum message size: 10MB (recommended)
- Maximum field length: Implementation-dependent
- Agent may reject oversized messages

**JSON Compliance**:
- Messages MUST be valid JSON (RFC 7159)
- Invalid JSON results in message rejection

## Implementation Guide

### Step 1: Choose Your Language and Libraries

Select a programming language and JSON library:

- **Python**: `json` (standard library)
- **Go**: `encoding/json` (standard library)
- **Node.js**: `JSON` (built-in)
- **Java**: `com.google.gson` or `org.json`
- **C/C++**: `json-c` or `rapidjson`
- **Rust**: `serde_json`

### Step 2: Implement Transport Layer

Choose transport method and implement connection:

1. **Unix Socket**:
   - Use system socket APIs
   - Handle file permissions
   - Implement reconnection logic

2. **TCP/IP**:
   - Use system socket APIs
   - Implement DNS resolution
   - Handle network errors

### Step 3: Implement Message Serialization

Create message objects and serialize to JSON:

1. Define data structures matching the specification
2. Serialize to JSON string
3. Validate JSON output
4. Append newline character

### Step 4: Implement Optional Compression

If using compression:

1. Apply LZ4 compression to JSON string
2. Add compression header (magic + size)
3. Send compressed payload

### Step 5: Send Messages

1. Establish connection
2. Serialize message
3. Format as ND-JSON (add newline)
4. Optionally compress
5. Write to socket
6. Handle errors and retries

### Step 6: Test Implementation

1. Send test messages to agent
2. Verify agent receives messages
3. Check agent logs for validation errors
4. Verify data appears in ClickHouse (if applicable)

## Examples

### Minimal Span Message

```json
{
  "type": "span",
  "trace_id": "abc123",
  "span_id": "def456",
  "service": "my-service",
  "name": "GET /users",
  "start_ts": 1704067200000,
  "end_ts": 1704067200125,
  "duration_ms": 125.0,
  "status": "ok"
}
```

### Complete Span Message

```json
{
  "type": "span",
  "trace_id": "550e8400-e29b-41d4-a716-446655440000",
  "span_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
  "parent_id": "6ba7b811-9dad-11d1-80b4-00c04fd430c8",
  "service": "api-service",
  "name": "GET /api/users",
  "url_scheme": "https",
  "url_host": "api.example.com",
  "url_path": "/api/users",
  "start_ts": 1704067200000,
  "end_ts": 1704067200125,
  "duration_ms": 125.456,
  "cpu_ms": 50.2,
  "status": "ok",
  "language": "php",
  "language_version": "8.4",
  "framework": "symfony",
  "framework_version": "7.0",
  "net": {
    "bytes_sent": 1024,
    "bytes_received": 2048
  },
  "sql": [
    {
      "query": "SELECT * FROM users WHERE id = ?",
      "duration_ms": 10.5,
      "query_type": "SELECT",
      "rows_returned": 1,
      "db_system": "mysql",
      "db_host": "db.example.com"
    }
  ],
  "http": [
    {
      "url": "https://external-api.com/data",
      "method": "GET",
      "status_code": 200,
      "duration_ms": 50.0,
      "bytes_sent": 256,
      "bytes_received": 512,
      "type": "curl"
    }
  ],
  "cache": [
    {
      "key": "user:123",
      "operation": "get",
      "hit": true,
      "duration_ms": 1.0,
      "cache_type": "apcu"
    }
  ],
  "redis": [
    {
      "command": "GET",
      "key": "session:abc",
      "hit": true,
      "duration_ms": 2.0,
      "type": "redis"
    }
  ],
  "stack": [
    {
      "call_id": "call-1",
      "function": "getUser",
      "class": "UserRepository",
      "file": "/app/src/Repository/UserRepository.php",
      "line": 42,
      "duration_ms": 10.5,
      "cpu_ms": 5.2,
      "parent_id": "",
      "depth": 0
    }
  ],
  "tags": {
    "organization_id": "org-123",
    "project_id": "proj-456",
    "http_request": {
      "scheme": "https",
      "method": "GET",
      "uri": "/api/users",
      "host": "api.example.com"
    },
    "http_response": {
      "status_code": 200
    },
    "expand_spans": true
  },
  "dumps": []
}
```

### Error Message Example

```json
{
  "type": "error",
  "trace_id": "abc123",
  "span_id": "def456",
  "instance_id": "err-instance-789",
  "group_id": "err-group-101",
  "fingerprint": "Error:Division by zero@Calculator.php:42",
  "error_type": "Error",
  "error_message": "Division by zero",
  "file": "/app/src/Calculator.php",
  "line": 42,
  "stack_trace": [
    {
      "file": "/app/src/Calculator.php",
      "line": 42,
      "function": "divide",
      "class": "Calculator"
    },
    {
      "file": "/app/src/Controller.php",
      "line": 10,
      "function": "calculate",
      "class": "Controller"
    }
  ],
  "organization_id": "org-123",
  "project_id": "proj-456",
  "service": "api-service",
  "occurred_at_ms": 1704067200000,
  "environment": "production",
  "release": "v1.2.3"
}
```

### Log Message Example

```json
{
  "type": "log",
  "id": "log-123",
  "trace_id": "abc123",
  "span_id": "def456",
  "level": "ERROR",
  "message": "Failed to connect to database",
  "service": "api-service",
  "timestamp_ms": 1704067200000,
  "fields": {
    "file": "/app/src/Database.php",
    "line": 50,
    "error_code": "DB_CONNECTION_FAILED"
  }
}
```

## HTTP REST API

### Overview

The OpenProfilingAgent provides a comprehensive HTTP REST API for querying traces, services, metrics, errors, logs, and managing configurations. The API is available at the agent's HTTP endpoint (default: `:8080`).

### Base URL

The API base URL depends on your agent configuration:
- Default: `http://localhost:8080`
- Production: `https://agent.example.com`

All API endpoints are prefixed with `/api`.

### Authentication

Most endpoints require authentication. See the [Authentication and Authorization](#authentication-and-authorization) section for details.

### Multi-Tenancy

Most endpoints support multi-tenancy through:
- **Headers**: `X-Organization-ID` and `X-Project-ID`
- **Query Parameters**: `organization_id` and `project_id`
- Use `"all"` as value to query across all tenants

### Response Format

All responses are JSON unless otherwise specified. Error responses follow this format:

```json
{
  "error": "Error message"
}
```

### Endpoints

#### Health and System

**GET /api/health**
- **Description**: Health check endpoint
- **Authentication**: None required
- **Response**: `{"status": "ok"}`

**GET /api/stats**
- **Description**: Get system statistics
- **Authentication**: Required
- **Response**:
  ```json
  {
    "queue_size": 42
  }
  ```

#### Authentication

**POST /api/auth/login**
- **Description**: User login
- **Authentication**: None required
- **Request Body**:
  ```json
  {
    "username": "admin",
    "password": "password123"
  }
  ```
- **Response**:
  ```json
  {
    "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
    "username": "admin",
    "role": "admin",
    "expires": 1704067200
  }
  ```

**POST /api/auth/register**
- **Description**: User registration
- **Authentication**: None required
- **Request Body**:
  ```json
  {
    "username": "newuser",
    "email": "user@example.com",
    "password": "securepassword",
    "role": "viewer"
  }
  ```
- **Response**:
  ```json
  {
    "id": "user-123",
    "username": "newuser",
    "email": "user@example.com",
    "role": "viewer"
  }
  ```

#### API Keys

**GET /api/api-keys**
- **Description**: List all API keys for the authenticated user
- **Authentication**: JWT Bearer token required
- **Response**:
  ```json
  {
    "api_keys": [
      {
        "key_id": "key-123",
        "name": "Production API Key",
        "org_id": "org-123",
        "project_id": "proj-456",
        "created_at": "2024-01-01T00:00:00Z"
      }
    ]
  }
  ```

**POST /api/api-keys**
- **Description**: Create a new API key
- **Authentication**: JWT Bearer token required
- **Request Body**:
  ```json
  {
    "name": "Production API Key",
    "org_id": "org-123",
    "project_id": "proj-456"
  }
  ```
- **Response**: API key object with `key_hash` (only shown on creation)

**DELETE /api/api-keys/{key_id}**
- **Description**: Delete an API key
- **Authentication**: JWT Bearer token required

#### Organizations

**GET /api/organizations**
- **Description**: List all organizations
- **Authentication**: Required
- **Response**:
  ```json
  {
    "organizations": [
      {
        "org_id": "org-123",
        "name": "My Organization",
        "settings": "{}",
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z"
      }
    ]
  }
  ```

**POST /api/organizations**
- **Description**: Create a new organization
- **Authentication**: JWT Bearer token required
- **Request Body**:
  ```json
  {
    "name": "My Organization",
    "settings": "{}"
  }
  ```

#### Projects

**GET /api/projects**
- **Description**: List projects, optionally filtered by organization
- **Authentication**: Required
- **Query Parameters**:
  - `organization_id` (optional): Filter by organization
- **Response**:
  ```json
  {
    "projects": [
      {
        "project_id": "proj-456",
        "name": "My Project",
        "org_id": "org-123",
        "dsn": "https://agent.example.com",
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z"
      }
    ]
  }
  ```

**POST /api/projects**
- **Description**: Create a new project
- **Authentication**: JWT Bearer token required
- **Request Body**:
  ```json
  {
    "name": "My Project",
    "org_id": "org-123",
    "dsn": "https://agent.example.com"
  }
  ```

**DELETE /api/projects/{project_id}**
- **Description**: Delete a project
- **Authentication**: JWT Bearer token required

#### Traces

**GET /api/traces**
- **Description**: List traces with filtering, pagination, and sorting
- **Authentication**: Required
- **Query Parameters**:
  - `service` (optional): Filter by service name
  - `status` (optional): Filter by status (`ok` or `error`)
  - `language` (optional): Filter by programming language
  - `framework` (optional): Filter by framework
  - `from` (optional): Start time (ISO 8601 or ClickHouse format)
  - `to` (optional): End time
  - `min_duration` (optional): Minimum duration in milliseconds
  - `max_duration` (optional): Maximum duration in milliseconds
  - `scheme` (optional): Filter by URL scheme
  - `host` (optional): Filter by host
  - `uri` (optional): Filter by URI path
  - `sort` (optional): Sort field (`time`, `duration`, `service`), default: `time`
  - `order` (optional): Sort order (`asc`, `desc`), default: `desc`
  - `limit` (optional): Maximum number of traces (default: 50, max: 1000)
  - `offset` (optional): Offset for pagination (default: 0)
- **Response**:
  ```json
  {
    "traces": [
      {
        "trace_id": "trace-123",
        "service": "api-service",
        "name": "GET /api/users",
        "start_ts": "2024-01-01T00:00:00Z",
        "end_ts": "2024-01-01T00:00:00.125Z",
        "duration_ms": 125.456,
        "status": "ok",
        "span_count": 5,
        "language": "php",
        "framework": "symfony"
      }
    ],
    "total": 100,
    "has_more": true
  }
  ```

**GET /api/traces/{trace_id}**
- **Description**: Get a single trace by ID
- **Authentication**: Required
- **Response**: Trace object with spans

**GET /api/traces/{trace_id}/full**
- **Description**: Get complete trace with all spans and detailed information
- **Authentication**: Required
- **Response**: Full trace object with all spans

**GET /api/traces/{trace_id}/logs**
- **Description**: Get all logs associated with a trace
- **Authentication**: Required
- **Query Parameters**:
  - `limit` (optional): Maximum number of logs (default: 100)
- **Response**:
  ```json
  {
    "logs": [
      {
        "id": "log-123",
        "trace_id": "trace-123",
        "span_id": "span-456",
        "level": "ERROR",
        "message": "Failed to connect to database",
        "service": "api-service",
        "timestamp_ms": 1704067200000,
        "fields": {
          "file": "/app/src/Database.php",
          "line": 50
        }
      }
    ],
    "count": 5
  }
  ```

**DELETE /api/traces/{trace_id}**
- **Description**: Delete a single trace
- **Authentication**: Required
- **Response**:
  ```json
  {
    "status": "deleted",
    "trace_id": "trace-123"
  }
  ```

**POST /api/traces/batch-delete**
- **Description**: Delete multiple traces
- **Authentication**: Required
- **Request Body**:
  ```json
  {
    "trace_ids": ["trace-1", "trace-2", "trace-3"]
  }
  ```
- **Response**:
  ```json
  {
    "status": "completed",
    "deleted": ["trace-1", "trace-2"],
    "failed": [
      {
        "id": "trace-3",
        "error": "Trace not found"
      }
    ],
    "deleted_count": 2,
    "failed_count": 1
  }
  ```

#### Services

**GET /api/services**
- **Description**: Get overview of all services with metrics
- **Authentication**: Required
- **Query Parameters**:
  - `from` (optional): Start time filter
  - `to` (optional): End time filter
- **Response**:
  ```json
  {
    "services": [
      {
        "service": "api-service",
        "language": "php",
        "language_version": "8.4",
        "framework": "symfony",
        "framework_version": "7.0",
        "total_traces": 1000,
        "total_spans": 5000,
        "error_count": 50,
        "error_rate": 1.0,
        "avg_duration": 125.5,
        "p95_duration": 250.0,
        "p99_duration": 500.0,
        "top_sql_queries": [],
        "top_endpoints": []
      }
    ],
    "totals": {
      "total_traces": 10000,
      "total_spans": 50000,
      "error_count": 500,
      "total_cpu_ms": 500000.0,
      "total_bytes_sent": 10485760,
      "total_bytes_received": 20971520,
      "total_http_requests": 2000,
      "total_sql_queries": 1000,
      "avg_duration": 125.5
    }
  }
  ```

**GET /api/services/{service}**
- **Description**: Get detailed information about a specific service
- **Authentication**: Required
- **Query Parameters**:
  - `from` (optional): Start time filter
  - `to` (optional): End time filter
- **Response**: Service details with traces

**GET /api/services/metadata**
- **Description**: Get metadata for all services
- **Authentication**: Required
- **Response**:
  ```json
  {
    "services": [
      {
        "service": "api-service",
        "language": "php",
        "language_version": "8.4",
        "framework": "symfony",
        "framework_version": "7.0"
      }
    ]
  }
  ```

#### Service Map

**GET /api/service-map**
- **Description**: Get service dependency map showing relationships between services
- **Authentication**: Required
- **Query Parameters**:
  - `from` (optional): Start time filter
  - `to` (optional): End time filter
- **Response**:
  ```json
  {
    "nodes": [
      {
        "id": "api-service",
        "name": "API Service",
        "service": "api-service"
      }
    ],
    "edges": [
      {
        "from": "api-service",
        "to": "db-service",
        "latency_ms": 10.5,
        "error_rate": 0.01,
        "throughput": 1000
      }
    ]
  }
  ```

**GET /api/service-map/thresholds**
- **Description**: Get threshold configuration for service map
- **Authentication**: Required
- **Response**:
  ```json
  {
    "thresholds": {
      "latency_ms": 100.0,
      "error_rate": 0.05,
      "throughput": 100
    }
  }
  ```

**PUT /api/service-map/thresholds**
- **Description**: Update threshold configuration for service map
- **Authentication**: JWT Bearer token required
- **Request Body**:
  ```json
  {
    "latency_ms": 100.0,
    "error_rate": 0.05,
    "throughput": 100
  }
  ```

#### Metrics

**GET /api/metrics/network**
- **Description**: Get network I/O metrics
- **Authentication**: Required
- **Query Parameters**:
  - `from` (optional): Start time filter
  - `to` (optional): End time filter
  - `service` (optional): Filter by service
- **Response**:
  ```json
  {
    "metrics": [
      {
        "time": "2024-01-01T00:00:00Z",
        "service": "api-service",
        "bytes_sent": 1048576,
        "bytes_received": 2097152
      }
    ]
  }
  ```

**GET /api/metrics/performance**
- **Description**: Get performance metrics including duration, CPU, and throughput
- **Authentication**: Required
- **Query Parameters**:
  - `from` (optional): Start time filter
  - `to` (optional): End time filter
  - `service` (optional): Filter by service
- **Response**:
  ```json
  {
    "metrics": [
      {
        "time": "2024-01-01T00:00:00Z",
        "service": "api-service",
        "avg_duration": 125.5,
        "p95_duration": 250.0,
        "p99_duration": 500.0,
        "throughput": 1000,
        "error_rate": 0.01
      }
    ]
  }
  ```

#### SQL Queries

**GET /api/sql/queries**
- **Description**: Get list of SQL queries with performance metrics
- **Authentication**: Required
- **Query Parameters**:
  - `from` (optional): Start time filter
  - `to` (optional): End time filter
  - `service` (optional): Filter by service
  - `limit` (optional): Maximum number of queries (default: 50)
- **Response**:
  ```json
  {
    "queries": [
      {
        "fingerprint": "SELECT * FROM users WHERE id = ?",
        "service": "api-service",
        "execution_count": 1000,
        "avg_duration": 10.5,
        "p95_duration": 20.0,
        "p99_duration": 50.0,
        "max_duration": 100.0
      }
    ]
  }
  ```

**GET /api/sql/queries/{fingerprint}**
- **Description**: Get detailed information about a specific SQL query by fingerprint
- **Authentication**: Required
- **Response**:
  ```json
  {
    "fingerprint": "SELECT * FROM users WHERE id = ?",
    "service": "api-service",
    "execution_count": 1000,
    "avg_duration": 10.5,
    "p95_duration": 20.0,
    "p99_duration": 50.0,
    "max_duration": 100.0,
    "example_query": "SELECT * FROM users WHERE id = 123",
    "trends": [
      {
        "time": "2024-01-01T00:00:00Z",
        "avg_duration": 10.5,
        "p95_duration": 20.0
      }
    ]
  }
  ```

#### Errors

**GET /api/errors**
- **Description**: Get list of error groups with counts and details
- **Authentication**: Required
- **Query Parameters**:
  - `from` (optional): Start time filter
  - `to` (optional): End time filter
  - `service` (optional): Filter by service
  - `limit` (optional): Maximum number of errors (default: 50)
- **Response**:
  ```json
  {
    "errors": [
      {
        "error_id": "api-service:Division by zero",
        "error_message": "Division by zero",
        "service": "api-service",
        "count": 10,
        "first_seen": "2024-01-01T00:00:00Z",
        "last_seen": "2024-01-01T12:00:00Z"
      }
    ]
  }
  ```

**GET /api/errors/{error_id}**
- **Description**: Get detailed information about a specific error
- **Authentication**: Required
- **Note**: Error ID format is `{service}:{error_name}`
- **Response**:
  ```json
  {
    "error_id": "api-service:Division by zero",
    "error_message": "Division by zero",
    "service": "api-service",
    "count": 10,
    "first_seen": "2024-01-01T00:00:00Z",
    "last_seen": "2024-01-01T12:00:00Z",
    "stack_trace": "...",
    "related_traces": [
      {
        "trace_id": "trace-123",
        "start_ts": "2024-01-01T00:00:00Z"
      }
    ],
    "trends": [
      {
        "time": "2024-01-01T00:00:00Z",
        "count": 5
      }
    ]
  }
  ```

#### Logs

**GET /api/logs**
- **Description**: Get log entries with filtering and pagination
- **Authentication**: Required
- **Query Parameters**:
  - `since` (optional): Get logs since this time
  - `service` (optional): Filter by service
  - `level` (optional): Filter by log level (`ERROR`, `WARN`, `INFO`, `DEBUG`, `CRITICAL`)
  - `cursor` (optional): Pagination cursor (timestamp in milliseconds)
  - `limit` (optional): Maximum number of logs (default: 100, max: 500)
  - `all` (optional): If set, fetch all historical logs
- **Response**:
  ```json
  {
    "logs": [
      {
        "id": "log-123",
        "trace_id": "trace-123",
        "span_id": "span-456",
        "level": "ERROR",
        "message": "Failed to connect to database",
        "service": "api-service",
        "timestamp_ms": 1704067200000,
        "fields": {
          "file": "/app/src/Database.php",
          "line": 50,
          "error_code": "DB_CONNECTION_FAILED"
        }
      }
    ],
    "total": 100,
    "has_more": true,
    "next_cursor": 1704067300000
  }
  ```

#### RUM (Real User Monitoring)

**GET /api/rum**
- **Description**: Get Real User Monitoring events
- **Authentication**: Required
- **Query Parameters**:
  - `from` (optional): Start time filter
  - `to` (optional): End time filter
- **Response**:
  ```json
  {
    "events": [
      {
        "event_id": "event-123",
        "trace_id": "trace-123",
        "session_id": "session-456",
        "event_type": "pageview",
        "timestamp": 1704067200000,
        "url": "https://example.com/page",
        "user_agent": "Mozilla/5.0...",
        "viewport": {
          "width": 1920,
          "height": 1080
        }
      }
    ]
  }
  ```

**GET /api/rum/metrics**
- **Description**: Get Real User Monitoring metrics
- **Authentication**: Required
- **Query Parameters**:
  - `from` (optional): Start time filter
  - `to` (optional): End time filter
- **Response**:
  ```json
  {
    "metrics": [
      {
        "time": "2024-01-01T00:00:00Z",
        "page_views": 1000,
        "unique_visitors": 500,
        "avg_page_load_time": 1.5,
        "error_rate": 0.01
      }
    ]
  }
  ```

#### Key Transactions

**GET /api/key-transactions**
- **Description**: Get all configured key transactions
- **Authentication**: Required
- **Response**:
  ```json
  {
    "transactions": [
      {
        "id": "kt-123",
        "name": "User Login",
        "service": "api-service",
        "pattern": "POST /api/auth/login",
        "created_at": "2024-01-01T00:00:00Z"
      }
    ]
  }
  ```

**POST /api/key-transactions**
- **Description**: Create a new key transaction
- **Authentication**: JWT Bearer token required
- **Request Body**:
  ```json
  {
    "name": "User Login",
    "service": "api-service",
    "pattern": "POST /api/auth/login"
  }
  ```

**PUT /api/key-transactions/{id}**
- **Description**: Update an existing key transaction
- **Authentication**: JWT Bearer token required

**DELETE /api/key-transactions/{id}**
- **Description**: Delete a key transaction
- **Authentication**: JWT Bearer token required

#### SLOs (Service Level Objectives)

**GET /api/slos**
- **Description**: Get all Service Level Objectives
- **Authentication**: Required
- **Response**:
  ```json
  {
    "slos": [
      {
        "id": "slo-123",
        "name": "API Availability",
        "description": "99.9% uptime target",
        "service": "api-service",
        "slo_type": "availability",
        "target_value": 99.9,
        "window_hours": 24,
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z"
      }
    ]
  }
  ```

**POST /api/slos**
- **Description**: Create a new Service Level Objective
- **Authentication**: JWT Bearer token required
- **Request Body**:
  ```json
  {
    "name": "API Availability",
    "description": "99.9% uptime target",
    "service": "api-service",
    "slo_type": "availability",
    "target_value": 99.9,
    "window_hours": 24
  }
  ```

**PUT /api/slos/{id}**
- **Description**: Update an existing SLO
- **Authentication**: JWT Bearer token required

**DELETE /api/slos/{id}**
- **Description**: Delete a SLO
- **Authentication**: JWT Bearer token required

**GET /api/slos/{id}/compliance**
- **Description**: Get compliance metrics for a specific SLO
- **Authentication**: Required
- **Response**:
  ```json
  {
    "metrics": [
      {
        "actual_value": 99.95,
        "compliance_percentage": 100.0,
        "is_breach": false,
        "window_start": "2024-01-01T00:00:00Z",
        "window_end": "2024-01-02T00:00:00Z"
      }
    ]
  }
  ```

#### Dashboards

**GET /api/dashboards**
- **Description**: Get all dashboards
- **Authentication**: Required
- **Response**:
  ```json
  {
    "dashboards": [
      {
        "id": "dash-123",
        "name": "Production Dashboard",
        "description": "Main production monitoring dashboard",
        "widgets": [],
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z"
      }
    ]
  }
  ```

**POST /api/dashboards**
- **Description**: Create a new dashboard
- **Authentication**: JWT Bearer token required
- **Request Body**:
  ```json
  {
    "name": "Production Dashboard",
    "description": "Main production monitoring dashboard",
    "widgets": []
  }
  ```

**PUT /api/dashboards/{id}**
- **Description**: Update an existing dashboard
- **Authentication**: JWT Bearer token required

**DELETE /api/dashboards/{id}**
- **Description**: Delete a dashboard
- **Authentication**: JWT Bearer token required

#### Alerts

**GET /api/alerts**
- **Description**: Get all configured alerts
- **Authentication**: Required
- **Response**:
  ```json
  {
    "alerts": [
      {
        "id": "alert-123",
        "name": "High Error Rate",
        "description": "Alert when error rate exceeds 5%",
        "service": "api-service",
        "condition": "error_rate > 5",
        "threshold": 5.0,
        "enabled": true,
        "created_at": "2024-01-01T00:00:00Z"
      }
    ]
  }
  ```

**POST /api/alerts**
- **Description**: Create a new alert
- **Authentication**: JWT Bearer token required
- **Request Body**:
  ```json
  {
    "name": "High Error Rate",
    "description": "Alert when error rate exceeds 5%",
    "service": "api-service",
    "condition": "error_rate > 5",
    "threshold": 5.0,
    "enabled": true
  }
  ```

**GET /api/alerts/{id}**
- **Description**: Get a specific alert by ID
- **Authentication**: Required

**PUT /api/alerts/{id}**
- **Description**: Update an existing alert
- **Authentication**: JWT Bearer token required

**DELETE /api/alerts/{id}**
- **Description**: Delete an alert
- **Authentication**: JWT Bearer token required

**POST /api/alerts/{id}**
- **Description**: Manually trigger an alert check
- **Authentication**: JWT Bearer token required
- **Response**:
  ```json
  {
    "status": "checking"
  }
  ```

#### Control

**POST /api/control/keep**
- **Description**: Mark a trace to be kept (not sampled out)
- **Authentication**: Required
- **Request Body**:
  ```json
  {
    "trace_id": "trace-123"
  }
  ```

**POST /api/control/sampling**
- **Description**: Set the sampling rate for traces (0.0 to 1.0)
- **Authentication**: JWT Bearer token required
- **Request Body**:
  ```json
  {
    "rate": 0.1
  }
  ```
- **Note**: `rate` is a float between 0.0 (0%) and 1.0 (100%)

**DELETE /api/control/purge**
- **Description**: Delete all traces and data from the system
- **Authentication**: JWT Bearer token required
- **Warning**: This operation is irreversible

#### Export

**GET /api/export/traces**
- **Description**: Export traces in various formats (JSON, CSV, NDJSON)
- **Authentication**: Required
- **Query Parameters**:
  - `format` (optional): Export format (`json`, `csv`, `ndjson`), default: `json`
  - `from` (optional): Start time filter
  - `to` (optional): End time filter
- **Response**: Exported data in the requested format
- **Content-Type**: 
  - `application/json` for JSON format
  - `text/csv` for CSV format
  - `application/x-ndjson` for NDJSON format

#### Anomalies

**GET /api/anomalies**
- **Description**: Get detected anomalies
- **Authentication**: Required
- **Query Parameters**:
  - `service` (optional): Filter by service
  - `severity` (optional): Filter by severity (`low`, `medium`, `high`, `critical`)
- **Response**:
  ```json
  {
    "anomalies": [
      {
        "id": "anom-123",
        "type": "duration",
        "service": "api-service",
        "metric": "avg_duration",
        "value": 500.0,
        "expected": 125.0,
        "score": 0.85,
        "severity": "high",
        "detected_at": "2024-01-01T00:00:00Z",
        "metadata": {}
      }
    ]
  }
  ```

**POST /api/anomalies/analyze**
- **Description**: Trigger anomaly detection analysis
- **Authentication**: JWT Bearer token required
- **Request Body**:
  ```json
  {
    "service": "api-service",
    "time_window": 24
  }
  ```
- **Response**: Analysis results with detected anomalies

#### System

**GET /api/languages**
- **Description**: Get all detected programming languages
- **Authentication**: Required
- **Response**:
  ```json
  {
    "languages": ["php", "python", "go"]
  }
  ```

**GET /api/frameworks**
- **Description**: Get all detected frameworks
- **Authentication**: Required
- **Response**:
  ```json
  {
    "frameworks": ["symfony", "django", "gin"]
  }
  ```

**GET /api/dumps**
- **Description**: Get variable dumps from traces
- **Authentication**: Required
- **Query Parameters**:
  - `limit` (optional): Maximum number of dumps (default: 100)
  - `cursor` (optional): Pagination cursor
- **Response**:
  ```json
  {
    "dumps": [
      {
        "id": "dump-123",
        "trace_id": "trace-123",
        "span_id": "span-456",
        "service": "api-service",
        "span_name": "getUser",
        "timestamp": 1704067200000,
        "file": "/app/src/Repository/UserRepository.php",
        "line": 42,
        "data": {},
        "text": "Variable dump text"
      }
    ],
    "total": 100,
    "has_more": true,
    "next_cursor": 1704067300000
  }
  ```

### Error Responses

All endpoints may return the following HTTP status codes:

- **200 OK**: Request successful
- **201 Created**: Resource created successfully
- **204 No Content**: Request successful, no response body
- **400 Bad Request**: Invalid request parameters or body
- **401 Unauthorized**: Authentication required or invalid
- **403 Forbidden**: Insufficient permissions
- **404 Not Found**: Resource not found
- **405 Method Not Allowed**: HTTP method not supported
- **500 Internal Server Error**: Server error

Error responses follow this format:

```json
{
  "error": "Error message description"
}
```

## Authentication and Authorization

### Overview

The OpenProfilingAgent API supports two authentication methods:

1. **JWT Bearer Token**: For user-based authentication
2. **API Key**: For programmatic access

### JWT Bearer Authentication

JWT (JSON Web Token) authentication is used for user-based access to the API.

#### Obtaining a JWT Token

Use the `/api/auth/login` endpoint to obtain a JWT token:

```bash
curl -X POST http://localhost:8080/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "username": "admin",
    "password": "password123"
  }'
```

Response:
```json
{
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "username": "admin",
  "role": "admin",
  "expires": 1704067200
}
```

#### Using JWT Tokens

Include the JWT token in the `Authorization` header:

```bash
curl -X GET http://localhost:8080/api/traces \
  -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
```

#### Token Expiration

JWT tokens expire after 24 hours. When a token expires, you will receive a `401 Unauthorized` response. Obtain a new token by logging in again.

#### User Registration

New users can be registered via the `/api/auth/register` endpoint:

```bash
curl -X POST http://localhost:8080/api/auth/register \
  -H "Content-Type: application/json" \
  -d '{
    "username": "newuser",
    "email": "user@example.com",
    "password": "securepassword",
    "role": "viewer"
  }'
```

**Roles**:
- `viewer`: Read-only access
- `editor`: Read and write access
- `admin`: Full access including system configuration

### API Key Authentication

API keys provide programmatic access without user credentials. They are scoped to specific organizations and projects.

#### Creating an API Key

API keys can be created via the `/api/api-keys` endpoint (requires JWT authentication):

```bash
curl -X POST http://localhost:8080/api/api-keys \
  -H "Authorization: Bearer {jwt_token}" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Production API Key",
    "org_id": "org-123",
    "project_id": "proj-456"
  }'
```

Response includes the `key_hash` (only shown on creation):
```json
{
  "key_id": "key-123",
  "name": "Production API Key",
  "org_id": "org-123",
  "project_id": "proj-456",
  "key_hash": "abc123def456...",
  "created_at": "2024-01-01T00:00:00Z"
}
```

#### Using API Keys

API keys can be used in two formats:

**Format 1: Organization:Project:KeyHash**
```bash
curl -X GET http://localhost:8080/api/traces \
  -H "Authorization: org-123:proj-456:abc123def456..."
```

**Format 2: DSN (Data Source Name)**
If a project has a DSN configured, you can use it directly:
```bash
curl -X GET http://localhost:8080/api/traces \
  -H "Authorization: https://agent.example.com"
```

The agent will look up the organization and project associated with the DSN.

### Multi-Tenancy

The agent supports multi-tenant data isolation through organizations and projects.

#### Tenant Context Extraction

The agent extracts tenant context from requests in the following order of precedence:

1. **DSN-based authentication**: If `Authorization` header contains a DSN (starts with `http://` or `https://`), the agent queries the projects table to find the associated organization and project.

2. **API Key format**: If `Authorization` header contains `{org_id}:{project_id}:{key_hash}`, the agent verifies the API key and uses the associated organization and project.

3. **Headers**: `X-Organization-ID` and `X-Project-ID` headers
   ```bash
   curl -X GET http://localhost:8080/api/traces \
     -H "X-Organization-ID: org-123" \
     -H "X-Project-ID: proj-456"
   ```

4. **Query Parameters**: `organization_id` and `project_id` query parameters
   ```bash
   curl -X GET "http://localhost:8080/api/traces?organization_id=org-123&project_id=proj-456"
   ```

#### Querying All Tenants

To query across all tenants, use `"all"` as the value:
```bash
curl -X GET "http://localhost:8080/api/traces?organization_id=all"
```

#### Default Tenants

If no tenant context is provided, the agent uses:
- `organization_id`: `"default-org"`
- `project_id`: `"default-project"`

### Role-Based Access Control

The agent implements role-based access control (RBAC) with three roles:

- **viewer** (Level 1): Read-only access to all endpoints
- **editor** (Level 2): Read and write access, can create/update/delete resources
- **admin** (Level 3): Full access including system configuration and control endpoints

Users with higher-level roles have access to all lower-level permissions. For example, an `admin` can perform all operations available to `editor` and `viewer`.

Some endpoints require specific roles:
- Control endpoints (`/api/control/*`) typically require `admin` role
- Write operations (POST, PUT, DELETE) typically require `editor` or `admin` role
- Read operations (GET) are available to all authenticated users

## WebSocket API

### Overview

The OpenProfilingAgent provides a WebSocket endpoint for real-time updates. This enables clients to receive live notifications about traces, metrics, and errors without polling.

### Connection

**Endpoint**: `/ws`

**Protocol**: WebSocket (RFC 6455)

**URL Format**:
- Local: `ws://localhost:8082/ws`
- Production: `wss://agent.example.com/ws` (secure WebSocket)

### Connection Establishment

Clients connect to the WebSocket endpoint using standard WebSocket handshake:

```javascript
const ws = new WebSocket('ws://localhost:8082/ws');

ws.onopen = () => {
  console.log('WebSocket connected');
};

ws.onmessage = (event) => {
  const message = JSON.parse(event.data);
  console.log('Received:', message);
};

ws.onerror = (error) => {
  console.error('WebSocket error:', error);
};

ws.onclose = () => {
  console.log('WebSocket disconnected');
};
```

### Message Format

All messages from the server follow this format:

```json
{
  "channel": "traces",
  "data": {
    "trace_id": "trace-123",
    "service": "api-service"
  },
  "timestamp": 1704067200
}
```

**Fields**:
- `channel` (string): The channel name (`traces`, `metrics`, or `errors`)
- `data` (object): The message payload (structure varies by channel)
- `timestamp` (integer): Unix timestamp in seconds

### Channels

Clients automatically subscribe to all available channels upon connection:

- **traces**: Real-time trace updates
- **metrics**: Performance and system metrics
- **errors**: Error notifications

Currently, clients cannot selectively subscribe to specific channels. All connected clients receive all channel broadcasts.

### Keepalive

The server implements a keepalive mechanism to maintain connections:

- **Ping Interval**: Server sends ping messages every 54 seconds
- **Pong Response**: Clients should respond to ping messages with pong
- **Read Timeout**: 60 seconds (connection closes if no pong received)
- **Write Timeout**: 10 seconds

**Client Implementation** (JavaScript example):

```javascript
ws.on('pong', () => {
  // Pong received, connection is alive
});

// Send pong in response to ping
ws.on('ping', () => {
  ws.pong();
});
```

### Message Broadcasting

The server broadcasts messages to all connected clients. Messages are sent as JSON-encoded text frames.

**Example Trace Update**:
```json
{
  "channel": "traces",
  "data": {
    "trace_id": "trace-123",
    "service": "api-service",
    "name": "GET /api/users",
    "status": "ok",
    "duration_ms": 125.456
  },
  "timestamp": 1704067200
}
```

**Example Metrics Update**:
```json
{
  "channel": "metrics",
  "data": {
    "service": "api-service",
    "avg_duration": 125.5,
    "error_rate": 0.01,
    "throughput": 1000
  },
  "timestamp": 1704067200
}
```

**Example Error Notification**:
```json
{
  "channel": "errors",
  "data": {
    "error_id": "api-service:Division by zero",
    "error_message": "Division by zero",
    "service": "api-service",
    "count": 5
  },
  "timestamp": 1704067200
}
```

### Client Read Pump

Clients should implement a read pump to handle incoming messages:

```javascript
function readPump(ws) {
  ws.on('message', (data) => {
    try {
      const message = JSON.parse(data.toString());
      handleMessage(message);
    } catch (error) {
      console.error('Failed to parse message:', error);
    }
  });
}
```

### Client Write Pump

The server handles message batching. If multiple messages are queued, they are sent in a single write operation separated by newlines:

```
{"channel":"traces","data":{...},"timestamp":1704067200}\n
{"channel":"metrics","data":{...},"timestamp":1704067201}\n
```

### Connection Lifecycle

1. **Connect**: Client establishes WebSocket connection
2. **Register**: Server registers client and subscribes to all channels
3. **Receive**: Client receives broadcasts for all channels
4. **Keepalive**: Server sends pings, client responds with pongs
5. **Disconnect**: Connection closes on error, timeout, or explicit close

### Error Handling

**Connection Errors**:
- Network errors: Client should implement reconnection logic with exponential backoff
- Authentication errors: WebSocket connections do not currently require authentication (may change in future versions)
- Server errors: Server logs errors and may close the connection

**Message Errors**:
- Invalid JSON: Client should log and ignore malformed messages
- Unknown channels: Client should ignore messages from unknown channels

### Example Client Implementation

**JavaScript (Browser)**:
```javascript
class OPAWebSocketClient {
  constructor(url) {
    this.url = url;
    this.ws = null;
    this.reconnectDelay = 1000;
    this.maxReconnectDelay = 30000;
  }

  connect() {
    this.ws = new WebSocket(this.url);

    this.ws.onopen = () => {
      console.log('Connected to OpenProfilingAgent WebSocket');
      this.reconnectDelay = 1000;
    };

    this.ws.onmessage = (event) => {
      const message = JSON.parse(event.data);
      this.handleMessage(message);
    };

    this.ws.onerror = (error) => {
      console.error('WebSocket error:', error);
    };

    this.ws.onclose = () => {
      console.log('WebSocket closed, reconnecting...');
      this.reconnect();
    };

    // Handle ping/pong
    this.ws.on('ping', () => {
      this.ws.pong();
    });
  }

  handleMessage(message) {
    switch (message.channel) {
      case 'traces':
        this.onTraceUpdate(message.data);
        break;
      case 'metrics':
        this.onMetricsUpdate(message.data);
        break;
      case 'errors':
        this.onErrorNotification(message.data);
        break;
    }
  }

  onTraceUpdate(data) {
    console.log('Trace update:', data);
  }

  onMetricsUpdate(data) {
    console.log('Metrics update:', data);
  }

  onErrorNotification(data) {
    console.log('Error notification:', data);
  }

  reconnect() {
    setTimeout(() => {
      this.reconnectDelay = Math.min(this.reconnectDelay * 2, this.maxReconnectDelay);
      this.connect();
    }, this.reconnectDelay);
  }

  close() {
    if (this.ws) {
      this.ws.close();
    }
  }
}

// Usage
const client = new OPAWebSocketClient('ws://localhost:8082/ws');
client.connect();
```

**Python Example**:
```python
import asyncio
import websockets
import json

async def opa_websocket_client():
    uri = "ws://localhost:8082/ws"
    
    async with websockets.connect(uri) as websocket:
        print("Connected to OpenProfilingAgent WebSocket")
        
        while True:
            try:
                message = await websocket.recv()
                data = json.loads(message)
                
                channel = data.get("channel")
                payload = data.get("data")
                
                if channel == "traces":
                    print(f"Trace update: {payload}")
                elif channel == "metrics":
                    print(f"Metrics update: {payload}")
                elif channel == "errors":
                    print(f"Error notification: {payload}")
                    
            except websockets.exceptions.ConnectionClosed:
                print("WebSocket connection closed")
                break
            except Exception as e:
                print(f"Error: {e}")

# Run client
asyncio.run(opa_websocket_client())
```

### Limitations

- **No Authentication**: WebSocket connections currently do not require authentication (may be added in future versions)
- **No Selective Subscription**: Clients receive all channel broadcasts (selective subscription may be added in future versions)
- **No Message Acknowledgment**: Messages are fire-and-forget (no delivery confirmation)
- **No Message History**: Clients only receive messages sent after connection (no replay of historical messages)

### Future Enhancements

Potential future enhancements to the WebSocket API:

- Authentication support (JWT tokens or API keys)
- Selective channel subscription
- Message acknowledgment and delivery confirmation
- Message history replay
- Client-to-server commands (e.g., subscribe/unsubscribe)

## Versioning and Compatibility

### Protocol Version

**Current Version**: 1.0

The protocol version is not explicitly specified in messages. The agent accepts messages conforming to this specification.

### Backward Compatibility

- **Guarantee**: Protocol version 1.0 messages will continue to be accepted in future versions
- **Unknown Fields**: Agent ignores unknown fields (forward compatibility)
- **Missing Optional Fields**: Agent handles missing optional fields gracefully

### Forward Compatibility

- **Unknown Fields**: Clients MAY include additional fields (agent ignores them)
- **Future Versions**: New protocol versions will be documented separately

### Breaking Changes

Breaking changes will be communicated through:
- New protocol version number
- Migration guide
- Deprecation notices (if applicable)

## Troubleshooting

### Connection Issues

**Unix Socket**:
- Verify socket file exists: `ls -l /var/run/opa.sock`
- Check permissions: Socket must be readable/writable
- Verify agent is running: `systemctl status opa-agent` or `docker ps`

**TCP/IP**:
- Verify agent is listening: `netstat -tlnp | grep 9090` or `ss -tlnp | grep 9090`
- Check firewall rules
- Verify DNS resolution: `nslookup agent` or `ping agent`
- Test connection: `telnet agent 9090` or `nc agent 9090`

### Message Rejection

**Invalid JSON**:
- Validate JSON before sending
- Check for unescaped special characters
- Verify UTF-8 encoding

**Missing Required Fields**:
- Review message specification
- Ensure all required fields are present
- Check agent logs for specific validation errors

**Field Type Mismatches**:
- Verify field types match specification
- Check number formats (integers vs floats)
- Ensure timestamps are in milliseconds

### Performance Issues

**High Latency**:
- Use Unix socket for local communication
- Implement connection pooling
- Batch multiple messages

**Message Loss**:
- Implement retry logic
- Monitor connection health
- Check agent queue size

### Debugging

**Enable Agent Debug Logs**:
- Check agent logs: `docker logs opa-agent` or `journalctl -u opa-agent`
- Look for validation errors
- Monitor message reception

**Test Message Sending**:
```bash
# Send test message via Unix socket
echo '{"type":"span","trace_id":"test","span_id":"test","service":"test","name":"test","start_ts":1704067200000,"end_ts":1704067201000,"duration_ms":1000,"status":"ok"}' | nc -U /var/run/opa.sock

# Send test message via TCP
echo '{"type":"span","trace_id":"test","span_id":"test","service":"test","name":"test","start_ts":1704067200000,"end_ts":1704067201000,"duration_ms":1000,"status":"ok"}' | nc localhost 9090
```

## References

- [RFC 7159 - JSON Specification](https://tools.ietf.org/html/rfc7159)
- [RFC 2119 - Key words for use in RFCs](https://tools.ietf.org/html/rfc2119)
- [LZ4 Compression](https://github.com/lz4/lz4)
- [OpenProfilingAgent Technical Documentation](TECHNICAL.md)
- [OpenProfilingAgent Installation Guide](INSTALLATION.md)

---

**Document Status**: Stable  
**Maintained By**: OpenProfilingAgent Team  
**Last Review**: 2025-12-12

