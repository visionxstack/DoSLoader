package scheduler

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/cookiejar"
	"sync"
	"sync/atomic"
	"time"

	"dosloader/internal/config"
	"dosloader/internal/engine/client"
	"dosloader/internal/engine/metrics"
	"dosloader/internal/engine/request"
	"dosloader/internal/engine/response"
)

type State int

const (
	StateIdle State = iota
	StateRunning
	StatePaused
	StateStopped
	StateCompleted
)

func (s State) String() string {
	switch s {
	case StateIdle:
		return "IDLE"
	case StateRunning:
		return "RUNNING"
	case StatePaused:
		return "PAUSED"
	case StateStopped:
		return "STOPPED"
	case StateCompleted:
		return "COMPLETED"
	default:
		return "UNKNOWN"
	}
}

// Scheduler manages the lifecycle and execution of Virtual Users (VUs).
type Scheduler struct {
	mu            sync.RWMutex
	state         State
	config        *config.Config
	metrics       *metrics.Metrics
	clientManager *client.ClientManager
	ctx           context.Context
	cancel        context.CancelFunc
	pauseChan     chan struct{}
	wg            sync.WaitGroup
	targetUsers   int32
	startTime     time.Time
}

// NewScheduler creates an idle scheduler.
func NewScheduler(cfg *config.Config, m *metrics.Metrics) *Scheduler {
	timeout, _ := cfg.ParsedTimeout()
	clientManager := client.NewClientManager(timeout, true) // InsecureSkipVerify true for load testing

	return &Scheduler{
		state:         StateIdle,
		config:        cfg,
		metrics:       m,
		clientManager: clientManager,
	}
}

// Start initiates the load test, launching the adjuster loop.
func (s *Scheduler) Start() error {
	s.mu.Lock()
	if s.state != StateIdle {
		s.mu.Unlock()
		return fmt.Errorf("scheduler is not in idle state: %s", s.state)
	}

	s.state = StateRunning
	s.startTime = time.Now()
	s.pauseChan = make(chan struct{})
	close(s.pauseChan) // start unblocked

	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.mu.Unlock()

	rampUp, _ := s.config.ParsedRampUp()
	duration, _ := s.config.ParsedDuration()
	maxUsers := s.config.Users

	go func() {
		s.adjusterLoop(s.ctx, rampUp, duration, maxUsers)

		s.mu.Lock()
		if s.state == StateRunning || s.state == StatePaused {
			s.state = StateCompleted
		}
		s.mu.Unlock()

		s.Stop()
	}()

	return nil
}

// Pause pauses all active VUs.
func (s *Scheduler) Pause() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != StateRunning {
		return
	}
	s.state = StatePaused
	s.pauseChan = make(chan struct{})
}

// Resume resumes all paused VUs.
func (s *Scheduler) Resume() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != StatePaused {
		return
	}
	s.state = StateRunning
	close(s.pauseChan)
}

// Stop cancels the context and waits for all VUs to exit.
func (s *Scheduler) Stop() {
	s.mu.Lock()
	if s.state == StateStopped {
		s.mu.Unlock()
		return
	}
	if s.state != StateCompleted {
		s.state = StateStopped
	}
	if s.cancel != nil {
		s.cancel()
	}

	// Ensure pauseChan is closed so VUs aren't hung
	select {
	case <-s.pauseChan:
		// already closed
	default:
		close(s.pauseChan)
	}
	s.mu.Unlock()

	s.wg.Wait()
}

// GetState returns the current scheduler state.
func (s *Scheduler) GetState() State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

// GetStartTime returns the test start time.
func (s *Scheduler) GetStartTime() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.startTime
}

func (s *Scheduler) waitIfPaused(ctx context.Context) error {
	s.mu.RLock()
	paused := s.state == StatePaused
	ch := s.pauseChan
	s.mu.RUnlock()

	if paused {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
			// resumed
		}
	}
	return nil
}

func (s *Scheduler) adjusterLoop(ctx context.Context, rampUp, duration time.Duration, maxUsers int) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// Adjust start time if paused to keep actual execution duration correct
	var accumulatedPauseTime time.Duration
	var lastPauseStart time.Time

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		s.mu.RLock()
		state := s.state
		s.mu.RUnlock()

		if state == StatePaused {
			if lastPauseStart.IsZero() {
				lastPauseStart = time.Now()
			}
			continue
		}

		if !lastPauseStart.IsZero() {
			accumulatedPauseTime += time.Since(lastPauseStart)
			lastPauseStart = time.Time{}
		}

		// Early exit if loop limit is reached and all users completed
		if s.config.Loops > 0 {
			completed := atomic.LoadInt64(&s.metrics.CompletedUsers)
			if int(completed) >= maxUsers {
				return
			}
		}

		elapsed := time.Since(s.startTime) - accumulatedPauseTime
		if elapsed >= duration {
			return
		}

		var target int
		if elapsed < rampUp && rampUp > 0 {
			fraction := float64(elapsed) / float64(rampUp)
			target = int(fraction * float64(maxUsers))
			if target < 1 && maxUsers > 0 {
				target = 1
			}
		} else {
			target = maxUsers
		}

		s.adjustWorkers(ctx, target)
	}
}

func (s *Scheduler) adjustWorkers(ctx context.Context, target int) {
	currentMax := int(atomic.LoadInt32(&s.targetUsers))
	if target == currentMax {
		return
	}

	if target > currentMax {
		atomic.StoreInt32(&s.targetUsers, int32(target))
		for id := currentMax; id < target; id++ {
			go s.runVU(ctx, id)
		}
	} else {
		atomic.StoreInt32(&s.targetUsers, int32(target))
	}
}

func (s *Scheduler) runVU(ctx context.Context, id int) {
	s.wg.Add(1)
	defer s.wg.Done()

	atomic.AddInt64(&s.metrics.RunningUsers, 1)
	defer func() {
		atomic.AddInt64(&s.metrics.RunningUsers, -1)
		atomic.AddInt64(&s.metrics.CompletedUsers, 1)
	}()

	thinkTime, _ := s.config.ParsedThinkTime()
	timeout, _ := s.config.ParsedTimeout()

	reqBuilder := request.NewRequestBuilder(s.config.Method, s.config.Target)
	reqBuilder.Headers = s.config.Headers
	if s.config.Body != "" {
		reqBuilder.Body = []byte(s.config.Body)
	}

	// Recreate client session per thread group rules in JMeter (independent cookies & context)
	jar, _ := cookiejar.New(nil)
	vuClient := &http.Client{
		Transport:     s.clientManager.Transport,
		Timeout:       s.clientManager.Client.Timeout,
		CheckRedirect: s.clientManager.Client.CheckRedirect,
		Jar:           jar,
	}

	loops := s.config.Loops
	loopCount := 0

	for {
		if loops > 0 && loopCount >= loops {
			return
		}
		if int32(id) >= atomic.LoadInt32(&s.targetUsers) {
			return
		}

		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := s.waitIfPaused(ctx); err != nil {
			return
		}

		loopCount++

		var bytesSent, bytesRecv int64
		vuCtx := context.WithValue(ctx, client.BytesSentKey, &bytesSent)
		vuCtx = context.WithValue(vuCtx, client.BytesRecvKey, &bytesRecv)
		vuCtx, cancel := context.WithTimeout(vuCtx, timeout)

		req, err := reqBuilder.Build(vuCtx)
		if err != nil {
			cancel()
			s.metrics.Record(false, false, 0, 0, 0)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		startTime := time.Now()
		resp, err := vuClient.Do(req)
		latency := time.Since(startTime)

		if err != nil {
			cancel()
			isTimeout := false
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				isTimeout = true
			} else if ctx.Err() == context.DeadlineExceeded || vuCtx.Err() == context.DeadlineExceeded {
				isTimeout = true
			}
			s.metrics.Record(false, isTimeout, latency, bytesSent, bytesRecv)
		} else {
			respInfo, parseErr := response.ParseResponse(resp, latency, bytesSent, bytesRecv)
			cancel()
			if parseErr != nil {
				s.metrics.Record(false, false, latency, bytesSent, bytesRecv)
			} else {
				success := respInfo.StatusCode < 400
				s.metrics.Record(success, false, respInfo.Latency, respInfo.BytesSent, respInfo.BytesReceived)
			}
		}

		if thinkTime > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(thinkTime):
			}
		}
	}
}
