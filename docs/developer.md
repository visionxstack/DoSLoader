# Developer Guide - DoSLoader

This guide provides setup and development instructions for engineers working on the DoSLoader platform.

## Development Prerequisites

- **Go**: Version 1.25+ (verified on 1.26+)
- **Protobuf Compiler (`protoc`)**: Version 3+

---

## Workspace Setup

### 1. Compile Protobuf Definitions
If you modify `proto/dosloader.proto`, regenerate the Go structures using the local `protoc-gen-go` plugin:
```bash
protoc --plugin=protoc-gen-go=./bin/protoc-gen-go --go_out=. --go_opt=module=dosloader proto/dosloader.proto
```

### 2. Manage Dependencies Offline
If you add new dependencies, they must be cached. To ensure offline consistency in sandboxed environments, DoSLoader uses Go vendoring:
1. Mock any test-only transitive packages that are not present in your offline cache (e.g., inside `third_party/`).
2. Run `go mod tidy` (using local proxies if necessary).
3. Run `go mod vendor` to update the `vendor/` directory.

---

## Component Customization

### Adding a New gRPC Endpoint
1. Define the request and response messages and service procedure in `proto/dosloader.proto`.
2. Compile the proto file to regenerate the Go structs.
3. Register the endpoint path (e.g., `/dosloader.ControllerService/MyNewMethod`) on the gRPC server:
   - In `internal/controller/controller.go`, add a handler function mapping:
     ```go
     s.Register("/dosloader.ControllerService/MyNewMethod", c.handleMyNewMethod)
     ```
   - In the handler, parse the request using `grpc.ReadFrame(r.Body, &req)` and write the response using `grpc.WriteFrame(w, &resp)`.
4. Call it from the client (CLI or Worker) using `grpc.UnaryCall` or `grpc.NewServerStream` in `internal/grpc/compat.go`.

---

## Testing Guidelines

Write unit tests for any new package or logic. 
Run tests using the local vendor folder:
```bash
go test -mod=vendor -v ./...
```

To run test coverage analysis:
```bash
go test -mod=vendor -coverprofile=coverage.out ./...
go tool cover -func=coverage.out
```
Aim for at least 80% coverage on core packages (`internal/engine/client/`, `internal/engine/metrics/`, `internal/engine/scheduler/`).
