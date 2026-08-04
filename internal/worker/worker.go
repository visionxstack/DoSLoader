package worker

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"dosloader/internal/config"
	"dosloader/internal/engine/metrics"
	"dosloader/internal/engine/scheduler"
	"dosloader/internal/grpc"
	pb "dosloader/internal/grpc/pb"

	"golang.org/x/net/http2"
)

type Worker struct {
	mu            sync.Mutex
	id            string
	controller    string
	httpClient    *http.Client
	sched         *scheduler.Scheduler
	metrics       *metrics.Metrics
	cancelTest    context.CancelFunc
	metricsCancel context.CancelFunc
}

func NewWorker(id, controller string) *Worker {
	// Configure H2C (HTTP/2 Cleartext) Transport
	h2t := &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, network, addr string, cfg *tls.Config) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, network, addr)
		},
	}

	client := &http.Client{
		Transport: h2t,
	}

	return &Worker{
		id:         id,
		controller: controller,
		httpClient: client,
	}
}

// Start launches the worker registration and heartbeat loops. It automatically reconnects.
func (w *Worker) Start(ctx context.Context) {
	slog.Info("Worker starting", "id", w.id, "controller", w.controller)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		err := w.connectAndRun(ctx)
		if err != nil {
			slog.Error("Worker lost connection, retrying in 2 seconds", "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
		}
	}
}

func (w *Worker) connectAndRun(ctx context.Context) error {
	regURL := fmt.Sprintf("http://%s/dosloader.WorkerService/RegisterWorker", w.controller)
	hostname, _ := os.Hostname()

	req := &pb.RegisterRequest{
		WorkerId: w.id,
		Hostname: hostname,
	}

	// Establish server stream for commands
	stream, err := grpc.NewServerStream(ctx, w.httpClient, regURL, req)
	if err != nil {
		return fmt.Errorf("failed to register: %w", err)
	}
	defer stream.Close()

	slog.Info("Registered successfully with controller")

	// Start heartbeat loop
	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	go w.heartbeatLoop(hbCtx)

	// Command listening loop
	for {
		var cmd pb.ControllerCommand
		err := stream.Recv(&cmd)
		if err != nil {
			w.stopCurrentTest() // clean up if controller drops
			return fmt.Errorf("command stream closed: %w", err)
		}

		w.handleCommand(ctx, &cmd)
	}
}

func (w *Worker) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	hbURL := fmt.Sprintf("http://%s/dosloader.WorkerService/Heartbeat", w.controller)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			req := &pb.HeartbeatRequest{
				WorkerId: w.id,
			}
			var resp pb.HeartbeatResponse
			err := grpc.UnaryCall(ctx, w.httpClient, hbURL, req, &resp)
			if err != nil {
				slog.Debug("Failed to send heartbeat to controller", "err", err)
			}
		}
	}
}

func (w *Worker) handleCommand(ctx context.Context, cmd *pb.ControllerCommand) {
	w.mu.Lock()
	defer w.mu.Unlock()

	slog.Info("Received controller command", "type", cmd.Type.String())

	switch cmd.Type {
	case pb.ControllerCommand_START:
		w.stopCurrentTestLocked() // stop previous test if any

		w.metrics = metrics.NewMetrics()
		engineCfg := w.protoToEngineConfig(cmd.Config)

		w.sched = scheduler.NewScheduler(engineCfg, w.metrics)
		err := w.sched.Start()
		if err != nil {
			slog.Error("Failed to start scheduler", "err", err)
			return
		}

		_, testCancel := context.WithCancel(ctx)
		w.cancelTest = testCancel

		metricsCtx, metricsCancel := context.WithCancel(ctx)
		w.metricsCancel = metricsCancel

		// Start background metrics streaming loop
		go w.metricsStreamLoop(metricsCtx, w.metrics)

	case pb.ControllerCommand_PAUSE:
		if w.sched != nil {
			w.sched.Pause()
		}

	case pb.ControllerCommand_RESUME:
		if w.sched != nil {
			w.sched.Resume()
		}

	case pb.ControllerCommand_STOP:
		w.stopCurrentTestLocked()
	}
}

func (w *Worker) stopCurrentTest() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.stopCurrentTestLocked()
}

func (w *Worker) stopCurrentTestLocked() {
	if w.sched != nil {
		w.sched.Stop()
		w.sched = nil
	}
	if w.cancelTest != nil {
		w.cancelTest()
		w.cancelTest = nil
	}
	if w.metricsCancel != nil {
		w.metricsCancel()
		w.metricsCancel = nil
	}
}

func (w *Worker) metricsStreamLoop(ctx context.Context, m *metrics.Metrics) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	streamURL := fmt.Sprintf("http://%s/dosloader.WorkerService/MetricsStream", w.controller)

	stream, err := grpc.NewClientStream(ctx, w.httpClient, streamURL)
	if err != nil {
		slog.Error("Failed to open metrics stream", "err", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			var resp pb.MetricsResponse
			_ = stream.CloseAndRecv(&resp)
			return
		case <-ticker.C:
			snap := m.Snapshot()
			payload := &pb.MetricsPayload{
				WorkerId:  w.id,
				Timestamp: time.Now().Unix(),
				Metrics:   w.mapSnapshotToProto(snap),
			}

			if err := stream.Send(payload); err != nil {
				slog.Error("Failed to send metrics payload", "err", err)
				return
			}
		}
	}
}

func (w *Worker) protoToEngineConfig(p *pb.TestConfig) *config.Config {
	return &config.Config{
		Name:      p.Name,
		Target:    p.Target,
		Method:    p.Method,
		Users:     int(p.Users),
		Duration:  p.Duration,
		RampUp:    p.RampUp,
		ThinkTime: p.ThinkTime,
		Headers:   p.Headers,
		Timeout:   p.Timeout,
		Body:      string(p.Body),
		Loops:     int(p.Loops),
	}
}

func (w *Worker) mapSnapshotToProto(snap *metrics.MetricsSnapshot) *pb.MetricsData {
	buckets := make(map[int32]int64)
	for i, val := range snap.Buckets {
		if val > 0 {
			buckets[int32(i)] = val
		}
	}
	return &pb.MetricsData{
		Requests:         snap.Requests,
		Successes:        snap.Successes,
		Failures:         snap.Failures,
		Timeouts:         snap.Timeouts,
		BytesSent:        snap.BytesSent,
		BytesReceived:    snap.BytesReceived,
		RunningUsers:     snap.RunningUsers,
		CompletedUsers:   snap.CompletedUsers,
		MinLatencyNs:     snap.MinLatency.Nanoseconds(),
		MaxLatencyNs:     snap.MaxLatency.Nanoseconds(),
		TotalLatencyNs:   snap.TotalLatency.Nanoseconds(),
		HistogramBuckets: buckets,
	}
}
