# API Documentation - DoSLoader

DoSLoader uses custom gRPC-over-HTTP/2 cleartext (H2C) endpoints. All requests must be made via `POST` with `Content-Type: application/grpc`.

---

## Worker-facing Services

### `RegisterWorker`
Registers a new worker node and opens a server-streaming connection to receive commands from the Controller.
- **Path**: `/dosloader.WorkerService/RegisterWorker`
- **Request**: `RegisterRequest`
  - `worker_id` (string): Unique identifier for the worker.
  - `hostname` (string): Hostname of the worker system.
- **Response**: Stream of `ControllerCommand`
  - `type` (enum): `START` (0), `PAUSE` (1), `RESUME` (2), `STOP` (3).
  - `config` (TestConfig): Only populated for `START` command. Contains target, users, duration, etc.
  - `timestamp` (int64): Command epoch nanoseconds.

---

### `Heartbeat`
Periodic ping sent by workers to inform the Controller they are alive.
- **Path**: `/dosloader.WorkerService/Heartbeat`
- **Request**: `HeartbeatRequest`
  - `worker_id` (string)
- **Response**: `HeartbeatResponse`
  - `success` (bool)

---

### `MetricsStream`
Client-side stream used by workers to push local 1-second cumulative metrics reports to the Controller.
- **Path**: `/dosloader.WorkerService/MetricsStream`
- **Request**: Stream of `MetricsPayload`
  - `worker_id` (string)
  - `timestamp` (int64)
  - `metrics` (MetricsData):
    - `requests` (int64): Cumulative request count.
    - `successes` (int64): Cumulative success count (status < 400).
    - `failures` (int64): Cumulative failure count (status >= 400 or network errors).
    - `timeouts` (int64): Cumulative timeout count.
    - `bytes_sent` (int64): Cumulative physical bytes sent.
    - `bytes_received` (int64): Cumulative physical bytes received.
    - `running_users` (int64): Active virtual users.
    - `completed_users` (int64): Completed virtual users.
    - `min_latency_ns` (int64): Minimum latency observed.
    - `max_latency_ns` (int64): Maximum latency observed.
    - `total_latency_ns` (int64): Cumulative latency sum.
    - `histogram_buckets` (map<int32, int64>): Latency bucket counts.
- **Response**: `MetricsResponse`
  - `success` (bool)

---

## CLI / GUI Control Services

### `StartTest`
Triggers a new load test.
- **Path**: `/dosloader.ControllerService/StartTest`
- **Request**: `StartTestRequest`
  - `config` (TestConfig)
- **Response**: `StartTestResponse`
  - `success` (bool)
  - `message` (string)

---

### `PauseTest`
Pauses the active load test.
- **Path**: `/dosloader.ControllerService/PauseTest`
- **Request**: `PauseTestRequest`
- **Response**: `PauseTestResponse`
  - `success` (bool)
  - `message` (string)

---

### `ResumeTest`
Resumes the paused load test.
- **Path**: `/dosloader.ControllerService/ResumeTest`
- **Request**: `ResumeTestRequest`
- **Response**: `ResumeTestResponse`
  - `success` (bool)
  - `message` (string)

---

### `StopTest`
Aborts the load test execution immediately.
- **Path**: `/dosloader.ControllerService/StopTest`
- **Request**: `StopTestRequest`
- **Response**: `StopTestResponse`
  - `success` (bool)
  - `message` (string)

---

### `WorkerHealth`
Queries registered workers and their statuses.
- **Path**: `/dosloader.ControllerService/WorkerHealth`
- **Request**: `WorkerHealthRequest`
- **Response**: `WorkerHealthResponse`
  - `workers` (repeated WorkerInfo):
    - `worker_id` (string)
    - `hostname` (string)
    - `status` (string): `"ONLINE"`, `"OFFLINE"`, `"TESTING"`.
    - `last_heartbeat` (int64)

---

### `JobStatus`
Queries the active load test state.
- **Path**: `/dosloader.ControllerService/JobStatus`
- **Request**: `JobStatusRequest`
- **Response**: `JobStatusResponse`
  - `status` (string): `"IDLE"`, `"RUNNING"`, `"PAUSED"`, `"STOPPED"`, `"COMPLETED"`.
  - `config` (TestConfig)
  - `start_time` (int64)

---

### `StreamAggregatedMetrics`
Server-side stream utilized by CLI/GUI to watch live aggregated execution statistics.
- **Path**: `/dosloader.ControllerService/StreamAggregatedMetrics`
- **Request**: `MetricsSubscription`
- **Response**: Stream of `AggregatedMetrics`
  - `timestamp` (int64)
  - `total_requests` (int64)
  - `total_successes` (int64)
  - `total_failures` (int64)
  - `total_timeouts` (int64)
  - `rps` (double)
  - `avg_latency_ms` (double)
  - `min_latency_ms` (double)
  - `max_latency_ms` (double)
  - `p50_ms` (double)
  - `p90_ms` (double)
  - `p95_ms` (double)
  - `p99_ms` (double)
  - `bytes_sent` (int64)
  - `bytes_received` (int64)
  - `active_users` (int32)
