# OpenProfilingAgent Agent (Go)

This agent listens on a Unix socket for ND-JSON payloads produced by the PHP extension. It buffers traces, writes minimal span metadata to ClickHouse using JSONEachRow, and exposes Prometheus metrics and a small admin API.

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
- -socket: path to unix socket
- -metrics: Prometheus listen address
- -api: admin HTTP address
- -clickhouse: ClickHouse HTTP endpoint
- -batch: batch size for ClickHouse
