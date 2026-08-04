# Configuration Reference - DoSLoader

DoSLoader test configurations are defined using standard YAML files. Below is the complete parameter specification and validation rules.

## Example Configuration

```yaml
name: API Benchmark Load Test
target: https://api.example.com/v1/users
method: POST
users: 1000
duration: 5m
ramp_up: 30s
think_time: 100ms
timeout: 5s
headers:
  Content-Type: application/json
  Authorization: Bearer mock-token-123
body: '{"name": "test-user", "email": "test@example.com"}'
```

---

## Schema Definition

| Parameter | Type | Required | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| `name` | string | Yes | - | A descriptive name for the load test. |
| `target` | string | Yes | - | The target HTTP/HTTPS URL to test. Must be fully qualified. |
| `method` | string | Yes | - | The HTTP method. Supported: `GET`, `POST`, `PUT`, `PATCH`, `DELETE`, `HEAD`, `OPTIONS`. |
| `users` | integer | Yes | - | The target number of concurrent Virtual Users (VUs) to run. |
| `duration` | string | Yes | - | The total duration of the test. Expressed as a Go duration string (e.g. `60s`, `5m`, `1h`). |
| `ramp_up` | string | No | `0s` | The time over which to scale up the virtual users from 0 to `users`. Expressed as a duration string (e.g. `10s`, `1m`). |
| `think_time` | string | No | `0s` | The delay between requests for each virtual user. Expressed as a duration string (e.g. `100ms`, `1s`). |
| `timeout` | string | No | `30s` | The HTTP request timeout. Expressed as a duration string (e.g. `2s`, `30s`). |
| `headers` | map[string]string | No | - | A key-value map of HTTP headers to attach to each request. |
| `body` | string | No | - | The raw payload body for `POST`, `PUT`, or `PATCH` requests. |

---

## Validation Constraints

When `dosloader test start` is run, the engine validates the configuration file before transmission:
1. **Target**: Must be a valid URL starting with `http://` or `https://`.
2. **Method**: Must match one of the standard HTTP request methods.
3. **Users**: Must be a positive integer (> 0).
4. **Durations**: All duration fields (`duration`, `ramp_up`, `think_time`, `timeout`) must parse correctly using Go's `time.ParseDuration` syntax:
   - Valid units: `ns` (nanoseconds), `us` or `µs` (microseconds), `ms` (milliseconds), `s` (seconds), `m` (minutes), `h` (hours).
   - Compound strings are supported, e.g., `1m30s`.
