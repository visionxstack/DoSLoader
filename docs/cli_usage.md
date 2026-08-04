# CLI Usage Guide - DoSLoader

DoSLoader is controlled entirely via its terminal-based command interface.

## Command Reference

### `dosloader controller`
Starts the orchestration controller server.
- **Flags**:
  - `--host`: The IP address to bind (default: `0.0.0.0`)
  - `--port`: The port to listen on (default: `50051`)
  - `-v`, `--verbose`: Enable debug logs

**Example**:
```bash
./dosloader controller --port 50051 -v
```

---

### `dosloader worker`
Launches a worker load-agent node.
- **Flags**:
  - `--controller`: Controller host/port address (default: `localhost:50051`)
  - `--id`: Unique worker identifier (default: `hostname-<rand>`)
  - `-v`, `--verbose`: Enable debug logs

**Example**:
```bash
./dosloader worker --controller localhost:50051 --id worker-node-01
```

---

### `dosloader test start [target] [users] [duration]`
Triggers a load test run across all active workers. You can either pass a configuration file or input the target, users, and duration directly as positional arguments.
- **Positional Arguments**:
  - `target` (string, optional): The target domain or fully qualified URL (e.g. `visionkc.com.np:8080`). Defaults to `http://` if no scheme is specified.
  - `users` (integer, optional): The number of concurrent virtual users (default: `10`).
  - `duration` (string, optional): The duration of the test (e.g. `1m`, `30s`, default: `10s`).
- **Flags**:
  - `--config`: Path to the YAML configuration file (optional if `target` is provided)
  - `--ramp-up`: Load ramp-up time (e.g. `5s`)
  - `--loops`: Limit the number of iterations/loops per thread (e.g. `10`)
  - `--think-time`: Pause delay between loop requests per thread (e.g. `100ms`, `1s`)
  - `--controller`: Controller host/port address

**Examples**:
- *Using direct positional arguments*:
  ```bash
  ./dosloader test start visionkc.com.np:8080 100 1m
  ```
- *Using a configuration file*:
  ```bash
  ./dosloader test start --config configs/load_test.yaml
  ```

---

### `dosloader test pause`
Pauses the active load test run. VUs will block immediately after completing their current request.
- **Flags**:
  - `--controller`: Controller host/port address

**Example**:
```bash
./dosloader test pause
```

---

### `dosloader test resume`
Resumes the paused load test run.
- **Flags**:
  - `--controller`: Controller host/port address

**Example**:
```bash
./dosloader test resume
```

---

### `dosloader test stop`
Aborts the active load test run immediately across all worker nodes.
- **Flags**:
  - `--controller`: Controller host/port address

**Example**:
```bash
./dosloader test stop
```

---

### `dosloader workers`
Queries the controller and prints all registered workers and their health status in a formatted table.
- **Flags**:
  - `--controller`: Controller host/port address

**Example**:
```bash
./dosloader workers
```

---

### `dosloader metrics`
Subscribes to the controller's real-time aggregated metrics stream and renders a live, updating terminal dashboard.
- **Flags**:
  - `--controller`: Controller host/port address

**Example**:
```bash
./dosloader metrics
```

---

### `dosloader version`
Prints the application version and compilation metadata.

**Example**:
```bash
./dosloader version
```
