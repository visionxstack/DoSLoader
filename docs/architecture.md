# Architecture Documentation - DoSLoader

DoSLoader is modeled around a central coordination layout with distributed agents, optimized for zero-overhead HTTP execution.

## System Topology

```
                  ┌──────────────────────────────┐
                  │          CLI Client          │
                  └──────────────┬───────────────┘
                                 │
                                 │ HTTP/2 gRPC API
                                 ▼
                  ┌──────────────────────────────┐
                  │      Central Controller      │
                  └──────────────┬───────────────┘
                                 │
                 ┌───────────────┼───────────────┐
                 │ HTTP/2 Stream │ HTTP/2 Stream │
                 ▼               ▼               ▼
          ┌────────────┐   ┌────────────┐   ┌────────────┐
          │   Worker   │   │   Worker   │   │   Worker   │
          │  (Agent)   │   │  (Agent)   │   │  (Agent)   │
          └─────┬──────┘   └─────┬──────┘   └─────┬──────┘
                │                │                │
                ▼                ▼                ▼
         ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
         │ HTTP Engine │  │ HTTP Engine │  │ HTTP Engine │
         └─────────────┘  └─────────────┘  └─────────────┘
```

---

## Component Analysis

### 1. Central Controller
- **Worker Registry**: Tracks worker registrations, addresses, and states.
- **Heartbeat Monitor**: Monitors heartbeats on a background loop. If a worker fails to send a heartbeat within 6 seconds, it is removed.
- **Job Orchestrator**: Maintains the active test execution state and broadcasts start/pause/resume/stop commands.
- **Metrics Aggregator**: Receives 1-second reports from workers, accumulates request counters, joins latency histogram buckets, and computes cluster-wide latency percentiles (P50, P90, P95, P99) and live RPS.
- **Subscribers Broadcaster**: Streams compiled aggregated stats to CLI dashboard instances.

### 2. Worker Agent
- **Lifecycle Client**: Connects to the Controller via H2C, registers, and spawns the heartbeat ticker and command listening loop.
- **Auto-Reconnect**: Expands connection retry backoff to automatically recover from Controller downtime.
- **Local Engine Coordinator**: Dispatches start, pause, resume, and stop events directly to the scheduler.
- **Metrics Reporter**: Sends snapshots of local load metrics periodically (every 1s) to the Controller.

### 3. gRPC HTTP/2 Cleartext (H2C) Compat Layer
To perform gRPC communication in offline, read-only compiler sandboxes without standard gRPC dependencies, DoSLoader implements a **native gRPC-over-HTTP/2 framing layer**:
- **Framing Protocol**: Implements the standard `[1-byte-flags][4-byte-length][protobuf-payload]` header prefix for both requests and responses.
- **HTTP/2 Transport (H2C)**: Leverages Go's `golang.org/x/net/http2/h2c` to run cleartext HTTP/2 servers and custom `http2.Transport` dials to perform direct H2C calls, providing native gRPC compatibility without requiring SSL/TLS setups.
- **Streaming Support**: Handles Client-Streaming (e.g. `MetricsStream` via `io.Pipe`) and Server-Streaming (e.g. `RegisterWorker` and `StreamAggregatedMetrics`) natively.

### 4. Core HTTP Engine
The load testing engine is decoupled and operates independently from the network coordinator:
- **Client Manager**: Customizes connection pools (`MaxIdleConns`), disables default redirect follows to log 301/302 latencies, resolves hosts via an in-memory DNS cache, and wraps connections in a `countingConn` to count actual raw physical bytes sent and received.
- **Metrics Engine**: Tracks counts via lock-free `sync/atomic` variables. Calculates percentiles using a pre-allocated 52-bucket histogram (1ms to 60s) updated via atomic bucket increments, minimizing lock contention during massive concurrent requests.
- **Virtual User Scheduler**: Spawns concurrent VU goroutines. Adjusts execution load by increasing/decreasing worker goroutine targets based on the ramp-up profile. Manages pause and resume states using a broadcast channel to block VUs without CPU polling.
