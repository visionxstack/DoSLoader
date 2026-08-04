# DoSLoader - Distributed HTTP Load Testing Platform

DoSLoader is a production-quality, high-performance distributed HTTP load testing platform written in Go. It is architected with a decoupled design matching the virtual-user thread isolation and session semantics of Apache JMeter, while supporting horizontal worker scale-out via a custom HTTP/2 Cleartext (H2C) gRPC compatibility layer.

---

## Directory Structure

```
dosloader/
├── cmd/
│   └── dosloader/          # Main CLI entry point
├── configs/                # Example YAML test specs
├── docs/                   # Full reference guides
│   ├── architecture.md    # System topology and gRPC H2C details
│   ├── cli_usage.md       # Detailed command usage & flags reference
│   ├── configuration.md   # YAML schema specification
│   ├── developer.md       # Developer build & offline dependency guide
│   └── api.md             # Custom H2C gRPC API endpoints
├── internal/
│   ├── config/             # YAML configuration parser
│   ├── controller/         # Coordinator & metrics aggregator
│   ├── logger/             # slog structured logger
│   ├── grpc/               # HTTP/2 cleartext gRPC compat layer
│   ├── worker/             # Distributed load worker daemon
│   └── engine/
│       ├── client/         # Client manager, DNS cache, byte counters
│       ├── request/        # Request builder
│       ├── response/       # Parser and TCP connection recycler
│       ├── scheduler/      # Virtual User scheduler with loops control
│       └── metrics/        # Lock-free atomic stats & percentile calculators
└── proto/                  # Protobuf contract definitions
```

---

## Execution Modes

DoSLoader can be executed in two modes depending on your benchmarking scale:

### 1. Local Mode (Single Process)
Perfect for quick, ad-hoc tests directly from your local machine without launching background servers.

```bash
# Format: ./dosloader run [target] [users] [duration] [flags]
./dosloader run https://example.com 100 30s --think-time 100ms
```

### 2. Distributed Mode (Multi-Node Scaling)
For higher capacity tests that exceed a single machine's resources, you can scale out horizontally using multiple workers.

1. **Start the Controller**:
   ```bash
   ./dosloader controller --port 50051
   ```
2. **Start Workers** (on other nodes/terminals):
   ```bash
   ./dosloader worker --controller localhost:50051 --id worker-1
   ```
3. **Trigger the Load Test**:
   ```bash
   ./dosloader test start https://example.com 100 30s --think-time 100ms
   ```
4. **Monitor Performance**:
   ```bash
   ./dosloader metrics --controller localhost:50051
   ```

---

## CLI Flag Reference

The `run` and `test start` subcommands support the following flags:

| Flag | Type | Description |
| :--- | :--- | :--- |
| `--config` | string | Path to a YAML configuration file. |
| `--users` | int | Override/set the number of concurrent Virtual Users (default: `10` for ad-hoc). |
| `--duration` | string | Override/set the test duration (e.g., `30s`, `5m`, `24h`). |
| `--ramp-up` | string | Set the load ramp-up time (e.g., `5s`, `1m`). |
| `--loops` | int | Limit the number of loops/iterations per virtual user thread (stops early when reached). |
| `--think-time`| string | Pacing delay between requests per thread (e.g., `100ms`, `1s`). |
| `--controller`| string | gRPC server endpoint for distributed commands (default: `localhost:50051`). |

---

## Core Engine Architecture (JMeter Principles)

DoSLoader incorporates the performance testing designs used in Apache JMeter:

* **Thread-Level Session Isolation**: Each Virtual User (thread) operates with its own isolated runtime context. It uses a private `http.Client` carrying its own local `cookiejar.Jar`. Cookies, authentication headers, and redirects are isolated per thread, preventing cross-contamination.
* **Shared Connection Pooling**: All Virtual User threads share the same underlying `http.Transport` connection manager. This allows efficient TCP/TLS connection reuse (Keep-Alive) across all virtual users, reducing handshake overhead and maximizing throughput.
* **Pacing and Throughput control**: By adjusting `--think-time`, you can pace threads to target a specific Requests-Per-Second (RPS) rate. For instance, `100 threads` with a `1s think-time` targets exactly `100 RPS` under low latency.

---

## Local Capacity Limitations (High Concurrency Bottlenecks)

When testing with high concurrency (e.g. 500+ threads) from a single computer or domestic network, tests may result in **100% Failures** and slow down your local internet. This is not a software bug, but a result of network bottlenecks:

1. **NAT Table Saturation**: Standard consumer routers have limited memory. Maintaining thousands of simultaneous TCP connections overflows the router's NAT lookup tables, causing the router to drop packets and slow down the local network.
2. **Upstream Bandwidth Exhaustion**: Standard home connections have limited upload speeds. The overhead of thousands of simultaneous TCP/TLS handshakes (cert exchange, key agreements) easily saturates the upload channel.
3. **OS Port Exhaustion**: Operating systems have a limited range of outbound source ports. Attempting to open more connections than available ephemeral ports will cause the OS to immediately reject connection requests.
4. **File Descriptor Limits**: Linux limits open file handles per process (typically `1024`). You must run `ulimit -n 65536` on your machine before running high-concurrency benchmarks.

For high-scale stress testing, always distribute worker nodes across multiple cloud-hosting servers with high network backbones.
