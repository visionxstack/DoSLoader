package controller

import (
	"context"
	"io"
	"log/slog"
	"math"
	"net/http"
	"sync"
	"time"

	"dosloader/internal/engine/metrics"
	"dosloader/internal/grpc"
	pb "dosloader/internal/grpc/pb"
)

type WorkerState struct {
	ID            string
	Hostname      string
	Status        string // "ONLINE", "TESTING", "OFFLINE"
	LastHeartbeat time.Time
	CmdChan       chan *pb.ControllerCommand
	LatestMetrics *pb.MetricsData
}

type Controller struct {
	mu           sync.RWMutex
	workers      map[string]*WorkerState
	jobStatus    string // "IDLE", "RUNNING", "PAUSED", "STOPPED", "COMPLETED"
	jobConfig    *pb.TestConfig
	jobStartTime time.Time

	subs  map[chan *pb.AggregatedMetrics]bool
	subMu sync.Mutex
}

func NewController() *Controller {
	return &Controller{
		workers:   make(map[string]*WorkerState),
		jobStatus: "IDLE",
		subs:      make(map[chan *pb.AggregatedMetrics]bool),
	}
}

// RegisterHandlers maps the gRPC compat HTTP routes to Controller functions.
func (c *Controller) RegisterHandlers(s *grpc.Server) {
	s.Register("/dosloader.WorkerService/RegisterWorker", c.handleRegisterWorker)
	s.Register("/dosloader.WorkerService/Heartbeat", c.handleHeartbeat)
	s.Register("/dosloader.WorkerService/MetricsStream", c.handleMetricsStream)

	s.Register("/dosloader.ControllerService/StartTest", c.handleStartTest)
	s.Register("/dosloader.ControllerService/PauseTest", c.handlePauseTest)
	s.Register("/dosloader.ControllerService/ResumeTest", c.handleResumeTest)
	s.Register("/dosloader.ControllerService/StopTest", c.handleStopTest)
	s.Register("/dosloader.ControllerService/WorkerHealth", c.handleWorkerHealth)
	s.Register("/dosloader.ControllerService/JobStatus", c.handleJobStatus)
	s.Register("/dosloader.ControllerService/StreamAggregatedMetrics", c.handleStreamAggregatedMetrics)
}

// StartBackgroundLoops initiates the heartbeat monitoring and metrics aggregation loops.
func (c *Controller) StartBackgroundLoops(ctx context.Context) {
	go c.heartbeatCheckerLoop(ctx)
	go c.metricsAggregatorLoop(ctx)
}

func (c *Controller) heartbeatCheckerLoop(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.mu.Lock()
			now := time.Now()
			for id, w := range c.workers {
				if now.Sub(w.LastHeartbeat) > 6*time.Second {
					slog.Warn("Worker missed heartbeats, marking offline", "id", id)
					w.Status = "OFFLINE"
					close(w.CmdChan)
					delete(c.workers, id)
				}
			}
			c.mu.Unlock()
		}
	}
}

func (c *Controller) metricsAggregatorLoop(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var prevRequests int64
	var prevTime = time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		c.mu.Lock()
		if len(c.workers) == 0 {
			c.mu.Unlock()
			continue
		}

		var (
			totalRequests  int64
			totalSuccesses int64
			totalFailures  int64
			totalTimeouts  int64
			totalBytesSent int64
			totalBytesRecv int64
			activeUsers    int32
			minLatencyNS   int64 = math.MaxInt64
			maxLatencyNS   int64
			totalLatencyNS int64
		)
		combinedBuckets := make([]int64, 52)

		hasMetrics := false
		for _, w := range c.workers {
			if w.LatestMetrics == nil {
				continue
			}
			hasMetrics = true
			m := w.LatestMetrics
			totalRequests += m.Requests
			totalSuccesses += m.Successes
			totalFailures += m.Failures
			totalTimeouts += m.Timeouts
			totalBytesSent += m.BytesSent
			totalBytesRecv += m.BytesReceived
			activeUsers += int32(m.RunningUsers)

			if m.MinLatencyNs < minLatencyNS && m.MinLatencyNs > 0 {
				minLatencyNS = m.MinLatencyNs
			}
			if m.MaxLatencyNs > maxLatencyNS {
				maxLatencyNS = m.MaxLatencyNs
			}
			totalLatencyNS += m.TotalLatencyNs

			for bIdx, count := range m.HistogramBuckets {
				if int(bIdx) < len(combinedBuckets) {
					combinedBuckets[bIdx] += count
				}
			}
		}

		if !hasMetrics {
			c.mu.Unlock()
			continue
		}

		if minLatencyNS == math.MaxInt64 {
			minLatencyNS = 0
		}

		now := time.Now()
		elapsed := now.Sub(prevTime).Seconds()
		if elapsed <= 0 {
			elapsed = 1
		}

		rps := float64(totalRequests-prevRequests) / elapsed
		if rps < 0 {
			rps = 0
		}
		prevRequests = totalRequests
		prevTime = now

		snap := &metrics.MetricsSnapshot{
			Requests:      totalRequests,
			Successes:     totalSuccesses,
			Failures:      totalFailures,
			Timeouts:      totalTimeouts,
			BytesSent:     totalBytesSent,
			BytesReceived: totalBytesRecv,
			MinLatency:    time.Duration(minLatencyNS),
			MaxLatency:    time.Duration(maxLatencyNS),
			Buckets:       combinedBuckets,
		}

		var avgLatency time.Duration
		if totalRequests > 0 {
			avgLatency = time.Duration(totalLatencyNS / totalRequests)
		}

		p50 := snap.CalculatePercentile(50.0)
		p90 := snap.CalculatePercentile(90.0)
		p95 := snap.CalculatePercentile(95.0)
		p99 := snap.CalculatePercentile(99.0)

		agg := &pb.AggregatedMetrics{
			Timestamp:      now.Unix(),
			TotalRequests:  totalRequests,
			TotalSuccesses: totalSuccesses,
			TotalFailures:  totalFailures,
			TotalTimeouts:  totalTimeouts,
			Rps:            rps,
			AvgLatencyMs:   float64(avgLatency.Microseconds()) / 1000.0,
			MinLatencyMs:   float64(snap.MinLatency.Microseconds()) / 1000.0,
			MaxLatencyMs:   float64(snap.MaxLatency.Microseconds()) / 1000.0,
			P50Ms:          float64(p50.Microseconds()) / 1000.0,
			P90Ms:          float64(p90.Microseconds()) / 1000.0,
			P95Ms:          float64(p95.Microseconds()) / 1000.0,
			P99Ms:          float64(p99.Microseconds()) / 1000.0,
			BytesSent:      totalBytesSent,
			BytesReceived:  totalBytesRecv,
			ActiveUsers:    activeUsers,
		}
		c.mu.Unlock()

		c.subMu.Lock()
		for sub := range c.subs {
			select {
			case sub <- agg:
			default:
			}
		}
		c.subMu.Unlock()
	}
}

func (c *Controller) handleRegisterWorker(w http.ResponseWriter, r *http.Request) {
	var req pb.RegisterRequest
	if err := grpc.ReadFrame(r.Body, &req); err != nil {
		w.Header().Set("grpc-status", "3")
		return
	}

	cmdChan := make(chan *pb.ControllerCommand, 20)

	c.mu.Lock()
	ws := &WorkerState{
		ID:            req.WorkerId,
		Hostname:      req.Hostname,
		Status:        "ONLINE",
		LastHeartbeat: time.Now(),
		CmdChan:       cmdChan,
	}
	c.workers[req.WorkerId] = ws
	c.mu.Unlock()

	slog.Info("Worker connected", "id", req.WorkerId, "hostname", req.Hostname)

	w.Header().Set("Content-Type", "application/grpc")
	w.WriteHeader(http.StatusOK)

	flusher, hasFlush := w.(http.Flusher)
	if hasFlush {
		flusher.Flush()
	}

	// If test is currently active, immediately send START command to new worker
	c.mu.RLock()
	jobStatus := c.jobStatus
	jobConfig := c.jobConfig
	c.mu.RUnlock()

	if jobStatus == "RUNNING" {
		cmd := &pb.ControllerCommand{
			Type:      pb.ControllerCommand_START,
			Config:    jobConfig,
			Timestamp: time.Now().UnixNano(),
		}
		if err := grpc.WriteFrame(w, cmd); err == nil {
			if hasFlush {
				flusher.Flush()
			}
		}
	}

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			c.mu.Lock()
			delete(c.workers, req.WorkerId)
			c.mu.Unlock()
			slog.Info("Worker connection closed", "id", req.WorkerId)
			return
		case cmd, ok := <-cmdChan:
			if !ok {
				return
			}
			if err := grpc.WriteFrame(w, cmd); err != nil {
				c.mu.Lock()
				delete(c.workers, req.WorkerId)
				c.mu.Unlock()
				return
			}
			if hasFlush {
				flusher.Flush()
			}
		}
	}
}

func (c *Controller) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var req pb.HeartbeatRequest
	if err := grpc.ReadFrame(r.Body, &req); err != nil {
		w.Header().Set("grpc-status", "3")
		return
	}

	c.mu.Lock()
	ws, ok := c.workers[req.WorkerId]
	if ok {
		ws.LastHeartbeat = time.Now()
		if ws.Status == "OFFLINE" {
			ws.Status = "ONLINE"
		}
	}
	c.mu.Unlock()

	var resp pb.HeartbeatResponse
	resp.Success = true
	_ = grpc.WriteFrame(w, &resp)
}

func (c *Controller) handleMetricsStream(w http.ResponseWriter, r *http.Request) {
	for {
		var payload pb.MetricsPayload
		err := grpc.ReadFrame(r.Body, &payload)
		if err != nil {
			if err == io.EOF {
				break
			}
			w.Header().Set("grpc-status", "3")
			return
		}

		c.mu.Lock()
		ws, ok := c.workers[payload.WorkerId]
		if ok {
			ws.LatestMetrics = payload.Metrics
			ws.LastHeartbeat = time.Now() // counts as heartbeat
			if c.jobStatus == "RUNNING" {
				ws.Status = "TESTING"
			}
		}
		c.mu.Unlock()
	}

	var resp pb.MetricsResponse
	resp.Success = true
	_ = grpc.WriteFrame(w, &resp)
}

func (c *Controller) handleStartTest(w http.ResponseWriter, r *http.Request) {
	var req pb.StartTestRequest
	if err := grpc.ReadFrame(r.Body, &req); err != nil {
		w.Header().Set("grpc-status", "3")
		return
	}

	c.mu.Lock()
	if len(c.workers) == 0 {
		c.mu.Unlock()
		_ = grpc.WriteFrame(w, &pb.StartTestResponse{Success: false, Message: "No registered workers available"})
		return
	}

	c.jobStatus = "RUNNING"
	c.jobConfig = req.Config
	c.jobStartTime = time.Now()

	cmd := &pb.ControllerCommand{
		Type:      pb.ControllerCommand_START,
		Config:    req.Config,
		Timestamp: time.Now().UnixNano(),
	}

	for _, ws := range c.workers {
		select {
		case ws.CmdChan <- cmd:
			ws.Status = "TESTING"
		default:
		}
	}
	c.mu.Unlock()

	slog.Info("Broadcasted test start", "target", req.Config.Target)
	_ = grpc.WriteFrame(w, &pb.StartTestResponse{Success: true, Message: "Test started successfully"})
}

func (c *Controller) handlePauseTest(w http.ResponseWriter, r *http.Request) {
	var req pb.PauseTestRequest
	if err := grpc.ReadFrame(r.Body, &req); err != nil {
		w.Header().Set("grpc-status", "3")
		return
	}

	c.mu.Lock()
	if c.jobStatus != "RUNNING" {
		c.mu.Unlock()
		_ = grpc.WriteFrame(w, &pb.PauseTestResponse{Success: false, Message: "No test is currently running"})
		return
	}

	c.jobStatus = "PAUSED"
	cmd := &pb.ControllerCommand{
		Type:      pb.ControllerCommand_PAUSE,
		Timestamp: time.Now().UnixNano(),
	}

	for _, ws := range c.workers {
		select {
		case ws.CmdChan <- cmd:
		default:
		}
	}
	c.mu.Unlock()

	slog.Info("Broadcasted test pause")
	_ = grpc.WriteFrame(w, &pb.PauseTestResponse{Success: true, Message: "Test paused successfully"})
}

func (c *Controller) handleResumeTest(w http.ResponseWriter, r *http.Request) {
	var req pb.ResumeTestRequest
	if err := grpc.ReadFrame(r.Body, &req); err != nil {
		w.Header().Set("grpc-status", "3")
		return
	}

	c.mu.Lock()
	if c.jobStatus != "PAUSED" {
		c.mu.Unlock()
		_ = grpc.WriteFrame(w, &pb.ResumeTestResponse{Success: false, Message: "Test is not paused"})
		return
	}

	c.jobStatus = "RUNNING"
	cmd := &pb.ControllerCommand{
		Type:      pb.ControllerCommand_RESUME,
		Timestamp: time.Now().UnixNano(),
	}

	for _, ws := range c.workers {
		select {
		case ws.CmdChan <- cmd:
		default:
		}
	}
	c.mu.Unlock()

	slog.Info("Broadcasted test resume")
	_ = grpc.WriteFrame(w, &pb.ResumeTestResponse{Success: true, Message: "Test resumed successfully"})
}

func (c *Controller) handleStopTest(w http.ResponseWriter, r *http.Request) {
	var req pb.StopTestRequest
	if err := grpc.ReadFrame(r.Body, &req); err != nil {
		w.Header().Set("grpc-status", "3")
		return
	}

	c.mu.Lock()
	if c.jobStatus == "IDLE" || c.jobStatus == "STOPPED" {
		c.mu.Unlock()
		_ = grpc.WriteFrame(w, &pb.StopTestResponse{Success: false, Message: "No test is currently active"})
		return
	}

	c.jobStatus = "STOPPED"
	cmd := &pb.ControllerCommand{
		Type:      pb.ControllerCommand_STOP,
		Timestamp: time.Now().UnixNano(),
	}

	for _, ws := range c.workers {
		select {
		case ws.CmdChan <- cmd:
			ws.Status = "ONLINE"
		default:
		}
	}
	c.mu.Unlock()

	slog.Info("Broadcasted test stop")
	_ = grpc.WriteFrame(w, &pb.StopTestResponse{Success: true, Message: "Test stopped successfully"})
}

func (c *Controller) handleWorkerHealth(w http.ResponseWriter, r *http.Request) {
	var req pb.WorkerHealthRequest
	if err := grpc.ReadFrame(r.Body, &req); err != nil {
		w.Header().Set("grpc-status", "3")
		return
	}

	c.mu.RLock()
	var list []*pb.WorkerInfo
	for _, ws := range c.workers {
		list = append(list, &pb.WorkerInfo{
			WorkerId:      ws.ID,
			Hostname:      ws.Hostname,
			Status:        ws.Status,
			LastHeartbeat: ws.LastHeartbeat.Unix(),
		})
	}
	c.mu.RUnlock()

	_ = grpc.WriteFrame(w, &pb.WorkerHealthResponse{Workers: list})
}

func (c *Controller) handleJobStatus(w http.ResponseWriter, r *http.Request) {
	var req pb.JobStatusRequest
	if err := grpc.ReadFrame(r.Body, &req); err != nil {
		w.Header().Set("grpc-status", "3")
		return
	}

	c.mu.RLock()
	resp := &pb.JobStatusResponse{
		Status:    c.jobStatus,
		Config:    c.jobConfig,
		StartTime: c.jobStartTime.Unix(),
	}
	c.mu.RUnlock()

	_ = grpc.WriteFrame(w, resp)
}

func (c *Controller) handleStreamAggregatedMetrics(w http.ResponseWriter, r *http.Request) {
	var req pb.MetricsSubscription
	if err := grpc.ReadFrame(r.Body, &req); err != nil {
		w.Header().Set("grpc-status", "3")
		return
	}

	ch := make(chan *pb.AggregatedMetrics, 20)
	c.subMu.Lock()
	c.subs[ch] = true
	c.subMu.Unlock()

	defer func() {
		c.subMu.Lock()
		delete(c.subs, ch)
		c.subMu.Unlock()
	}()

	w.Header().Set("Content-Type", "application/grpc")
	w.WriteHeader(http.StatusOK)

	flusher, hasFlush := w.(http.Flusher)
	if hasFlush {
		flusher.Flush()
	}

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case agg := <-ch:
			if err := grpc.WriteFrame(w, agg); err != nil {
				return
			}
			if hasFlush {
				flusher.Flush()
			}
		}
	}
}
