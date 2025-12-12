# OpenProfilingAgent Agent (Go)

This agent listens on a Unix socket or TCP/IP for ND-JSON payloads produced by profiling tools (PHP extension, or any tool implementing the API contract). It buffers traces, writes minimal span metadata to ClickHouse using JSONEachRow, and exposes Prometheus metrics and a small admin API.

## API Contract

**For implementers of profiling tools**: See [docs/AGENT_API_CONTRACT.md](docs/AGENT_API_CONTRACT.md) for the complete API contract specification. This document defines:

- Complete message format specifications (span, error, log messages)
- Transport protocols (Unix socket and TCP/IP)
- Data structure definitions
- Implementation guides and examples
- Validation rules and troubleshooting

The API contract is language-agnostic and enables any developer to implement a profiling tool that communicates with OpenProfilingAgent.

Build:
```
cd agent
go build -o opa-agent
```

Run (local):
```
./opa-agent -socket /var/run/opa.sock -clickhouse http://localhost:8123 -batch 200
```

Configuration via flags:
- -socket: path to unix socket (e.g., `/var/run/opa.sock`)
- -tcp: TCP address to listen on (e.g., `:9090` or `0.0.0.0:9090`). If empty, TCP listener is disabled
- -metrics: Prometheus listen address
- -api: admin HTTP address
- -clickhouse: ClickHouse HTTP endpoint
- -batch: batch size for ClickHouse
- -batch_interval_ms: ClickHouse batch interval in milliseconds
- -sampling_rate: Default sampling rate (0.0 to 1.0)

The agent can listen on both Unix socket and TCP/IP simultaneously. Profiling tools can connect via either transport method. See [docs/AGENT_API_CONTRACT.md](docs/AGENT_API_CONTRACT.md) for detailed transport implementation guides.
