package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"text/tabwriter"
	"time"

	"dosloader/internal/config"
	"dosloader/internal/controller"
	"dosloader/internal/engine/metrics"
	"dosloader/internal/engine/scheduler"
	"dosloader/internal/grpc"
	pb "dosloader/internal/grpc/pb"
	"dosloader/internal/logger"
	"dosloader/internal/worker"

	"github.com/spf13/cobra"
	"golang.org/x/net/http2"
)

var (
	verbose           bool
	host              string
	port              string
	controllerAddr    string
	workerID          string
	configPath        string
	users             int
	duration          string
	rampUp            string
	loopsOverride     int
	thinkTimeOverride string
)

func main() {
	var rootCmd = &cobra.Command{
		Use:   "dosloader",
		Short: "DoSLoader is a distributed HTTP load testing platform",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			logger.InitLogger(verbose)
		},
	}

	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Enable verbose debug logging")
	rootCmd.PersistentFlags().StringVar(&controllerAddr, "controller", "localhost:50051", "Controller gRPC server address")

	// Controller command
	var controllerCmd = &cobra.Command{
		Use:   "controller",
		Short: "Start the DoSLoader Controller",
		Run: func(cmd *cobra.Command, args []string) {
			addr := net.JoinHostPort(host, port)
			c := controller.NewController()

			grpcServer := grpc.NewServerWrapper()
			c.RegisterHandlers(grpcServer.GrpcServer)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			c.StartBackgroundLoops(ctx)

			// Graceful shutdown handling
			sigChan := make(chan os.Signal, 1)
			signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

			go func() {
				slog.Info("Starting controller gRPC server", "addr", addr)
				if err := grpcServer.Start(addr); err != nil && err != http.ErrServerClosed {
					log.Fatalf("Controller server failed: %v", err)
				}
			}()

			<-sigChan
			slog.Info("Shutting down controller gracefully...")
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer shutdownCancel()

			if err := grpcServer.Shutdown(shutdownCtx); err != nil {
				slog.Error("Failed to shutdown server cleanly", "err", err)
			}
			slog.Info("Controller stopped.")
		},
	}
	controllerCmd.Flags().StringVar(&host, "host", "0.0.0.0", "IP address to bind")
	controllerCmd.Flags().StringVar(&port, "port", "50051", "Port to listen on")
	rootCmd.AddCommand(controllerCmd)

	// Worker command
	var workerCmd = &cobra.Command{
		Use:   "worker",
		Short: "Start a DoSLoader Worker node",
		Run: func(cmd *cobra.Command, args []string) {
			if workerID == "" {
				hostname, _ := os.Hostname()
				if hostname == "" {
					hostname = "worker"
				}
				workerID = fmt.Sprintf("%s-%d", hostname, rand.Intn(10000))
			}

			w := worker.NewWorker(workerID, controllerAddr)

			sigChan := make(chan os.Signal, 1)
			signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				w.Start(ctx)
			}()

			<-sigChan
			slog.Info("Stopping worker gracefully...")
			cancel()
			time.Sleep(1 * time.Second) // wait for cleanup
			slog.Info("Worker stopped.")
		},
	}
	workerCmd.Flags().StringVar(&workerID, "id", "", "Unique worker identifier (defaults to hostname-rand)")
	rootCmd.AddCommand(workerCmd)

	// Test parent command
	var testCmd = &cobra.Command{
		Use:   "test",
		Short: "Manage and run load tests",
	}

	// Test start command
	var testStartCmd = &cobra.Command{
		Use:   "start [target] [users] [duration]",
		Short: "Start a load test",
		Args:  cobra.MaximumNArgs(3),
		Run: func(cmd *cobra.Command, args []string) {
			var cfg *config.Config
			var err error

			if configPath != "" {
				cfg, err = config.LoadConfig(configPath)
				if err != nil {
					log.Fatalf("Failed to load config: %v", err)
				}
			} else {
				if len(args) == 0 {
					log.Fatal("Error: Either --config flag or a [target] positional URL/domain is required.")
				}
				target := args[0]
				// Prepend protocol scheme if not present for convenience
				if !strings.HasPrefix(target, "http://") && !strings.HasPrefix(target, "https://") {
					target = "http://" + target
				}

				usersVal := 10 // Default users for ad-hoc tests
				if len(args) > 1 {
					val, err := strconv.Atoi(args[1])
					if err != nil {
						log.Fatalf("Invalid users count: %v. Must be an integer.", args[1])
					}
					usersVal = val
				}

				durationVal := "10s" // Default duration for ad-hoc tests
				if len(args) > 2 {
					durationVal = args[2]
				}

				cfg = &config.Config{
					Name:      "Ad-hoc Load Test",
					Target:    target,
					Method:    "GET",
					Users:     usersVal,
					Duration:  durationVal,
					RampUp:    "0s",
					ThinkTime: "0s",
					Timeout:   "10s",
				}
			}

			// CLI overrides
			if users > 0 {
				cfg.Users = users
			}
			if duration != "" {
				cfg.Duration = duration
			}
			if rampUp != "" {
				cfg.RampUp = rampUp
			}
			if loopsOverride > 0 {
				cfg.Loops = loopsOverride
			}
			if thinkTimeOverride != "" {
				cfg.ThinkTime = thinkTimeOverride
			}

			// Validate final configuration after overrides
			if err := cfg.Validate(); err != nil {
				log.Fatalf("Configuration validation failed: %v", err)
			}

			// Read body if specified
			var bodyBytes []byte
			if cfg.Body != "" {
				bodyBytes = []byte(cfg.Body)
			}

			// Map to Protobuf
			protoConfig := &pb.TestConfig{
				Name:      cfg.Name,
				Target:    cfg.Target,
				Method:    cfg.Method,
				Users:     int32(cfg.Users),
				Duration:  cfg.Duration,
				RampUp:    cfg.RampUp,
				ThinkTime: cfg.ThinkTime,
				Headers:   cfg.Headers,
				Timeout:   cfg.Timeout,
				Body:      bodyBytes,
				Loops:     int32(cfg.Loops),
			}

			client := getH2CClient()
			startURL := fmt.Sprintf("http://%s/dosloader.ControllerService/StartTest", controllerAddr)

			req := &pb.StartTestRequest{Config: protoConfig}
			var resp pb.StartTestResponse

			err = grpc.UnaryCall(context.Background(), client, startURL, req, &resp)
			if err != nil {
				log.Fatalf("Failed to communicate with controller: %v", err)
			}

			if !resp.Success {
				log.Fatalf("Failed to start test: %s", resp.Message)
			}

			fmt.Printf("✔ Test start command submitted: %s\n", resp.Message)
			fmt.Printf("Run 'dosloader metrics' to watch execution in real-time.\n")
		},
	}
	testStartCmd.Flags().StringVar(&configPath, "config", "", "Path to the load test YAML config file")
	testStartCmd.Flags().IntVar(&users, "users", 0, "Override configured Virtual Users count")
	testStartCmd.Flags().StringVar(&duration, "duration", "", "Override configured execution duration")
	testStartCmd.Flags().StringVar(&rampUp, "ramp-up", "", "Override configured ramp-up time")
	testStartCmd.Flags().IntVar(&loopsOverride, "loops", 0, "Override configured loop count limit per VU")
	testStartCmd.Flags().StringVar(&thinkTimeOverride, "think-time", "", "Override configured think time (e.g. 100ms)")
	testCmd.AddCommand(testStartCmd)

	// Test stop command
	var testStopCmd = &cobra.Command{
		Use:   "stop",
		Short: "Stop the currently running load test",
		Run: func(cmd *cobra.Command, args []string) {
			client := getH2CClient()
			stopURL := fmt.Sprintf("http://%s/dosloader.ControllerService/StopTest", controllerAddr)

			var resp pb.StopTestResponse
			err := grpc.UnaryCall(context.Background(), client, stopURL, &pb.StopTestRequest{}, &resp)
			if err != nil {
				log.Fatalf("Failed to communicate with controller: %v", err)
			}

			if !resp.Success {
				log.Fatalf("Failed to stop test: %s", resp.Message)
			}
			fmt.Printf("✔ Test stop command submitted: %s\n", resp.Message)
		},
	}
	testCmd.AddCommand(testStopCmd)

	// Test pause command
	var testPauseCmd = &cobra.Command{
		Use:   "pause",
		Short: "Pause the currently running load test",
		Run: func(cmd *cobra.Command, args []string) {
			client := getH2CClient()
			pauseURL := fmt.Sprintf("http://%s/dosloader.ControllerService/PauseTest", controllerAddr)

			var resp pb.PauseTestResponse
			err := grpc.UnaryCall(context.Background(), client, pauseURL, &pb.PauseTestRequest{}, &resp)
			if err != nil {
				log.Fatalf("Failed to communicate with controller: %v", err)
			}

			if !resp.Success {
				log.Fatalf("Failed to pause test: %s", resp.Message)
			}
			fmt.Printf("✔ Test pause command submitted: %s\n", resp.Message)
		},
	}
	testCmd.AddCommand(testPauseCmd)

	// Test resume command
	var testResumeCmd = &cobra.Command{
		Use:   "resume",
		Short: "Resume the paused load test",
		Run: func(cmd *cobra.Command, args []string) {
			client := getH2CClient()
			resumeURL := fmt.Sprintf("http://%s/dosloader.ControllerService/ResumeTest", controllerAddr)

			var resp pb.ResumeTestResponse
			err := grpc.UnaryCall(context.Background(), client, resumeURL, &pb.ResumeTestRequest{}, &resp)
			if err != nil {
				log.Fatalf("Failed to communicate with controller: %v", err)
			}

			if !resp.Success {
				log.Fatalf("Failed to resume test: %s", resp.Message)
			}
			fmt.Printf("✔ Test resume command submitted: %s\n", resp.Message)
		},
	}
	testCmd.AddCommand(testResumeCmd)
	rootCmd.AddCommand(testCmd)

	// Workers list command
	var workersCmd = &cobra.Command{
		Use:   "workers",
		Short: "List all registered worker nodes and their status",
		Run: func(cmd *cobra.Command, args []string) {
			client := getH2CClient()
			workersURL := fmt.Sprintf("http://%s/dosloader.ControllerService/WorkerHealth", controllerAddr)

			var resp pb.WorkerHealthResponse
			err := grpc.UnaryCall(context.Background(), client, workersURL, &pb.WorkerHealthRequest{}, &resp)
			if err != nil {
				log.Fatalf("Failed to list workers: %v", err)
			}

			if len(resp.Workers) == 0 {
				fmt.Println("No workers registered.")
				return
			}

			w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', tabwriter.Debug)
			fmt.Fprintln(w, "WORKER ID\tHOSTNAME\tSTATUS\tLAST HEARTBEAT")
			fmt.Fprintln(w, "---------\t--------\t------\t--------------")
			for _, wInfo := range resp.Workers {
				lastHb := time.Unix(wInfo.LastHeartbeat, 0)
				ago := time.Since(lastHb).Round(time.Second)
				fmt.Fprintf(w, "%s\t%s\t%s\t%s ago\n", wInfo.WorkerId, wInfo.Hostname, wInfo.Status, ago)
			}
			w.Flush()
		},
	}
	rootCmd.AddCommand(workersCmd)

	// Metrics display command
	var metricsCmd = &cobra.Command{
		Use:   "metrics",
		Short: "Display real-time aggregated metrics of the active test",
		Run: func(cmd *cobra.Command, args []string) {
			client := getH2CClient()
			metricsURL := fmt.Sprintf("http://%s/dosloader.ControllerService/StreamAggregatedMetrics", controllerAddr)

			stream, err := grpc.NewServerStream(context.Background(), client, metricsURL, &pb.MetricsSubscription{})
			if err != nil {
				log.Fatalf("Failed to subscribe to metrics: %v", err)
			}
			defer stream.Close()

			fmt.Println("Subscribed to live metrics stream. Waiting for data...")

			for {
				var agg pb.AggregatedMetrics
				err := stream.Recv(&agg)
				if err != nil {
					if err == io.EOF {
						fmt.Println("\nMetrics stream ended.")
						break
					}
					log.Fatalf("\nFailed to read metrics payload: %v", err)
				}

				// Clear screen ANSI command
				fmt.Print("\033[H\033[2J")
				fmt.Printf("DoSLoader Live Metrics Dashboard [Time: %s]\n", time.Unix(agg.Timestamp, 0).Format("15:04:05"))
				fmt.Println("==========================================================================")
				fmt.Printf("RPS:              %.2f\n", agg.Rps)
				fmt.Printf("Active VUs:       %d\n", agg.ActiveUsers)
				fmt.Printf("Total Requests:   %d (Success: %d | Fail: %d | Timeout: %d)\n",
					agg.TotalRequests, agg.TotalSuccesses, agg.TotalFailures, agg.TotalTimeouts)
				fmt.Printf("Bandwidth:        Sent: %.2f MB | Recv: %.2f MB\n",
					float64(agg.BytesSent)/(1024*1024), float64(agg.BytesReceived)/(1024*1024))
				fmt.Println("\nLatencies:")
				fmt.Printf("  Min:   %.2f ms  |  Avg:   %.2f ms  |  Max:   %.2f ms\n",
					agg.MinLatencyMs, agg.AvgLatencyMs, agg.MaxLatencyMs)
				fmt.Printf("  P50:   %.2f ms  |  P90:   %.2f ms  |  P95:   %.2f ms  |  P99:   %.2f ms\n",
					agg.P50Ms, agg.P90Ms, agg.P95Ms, agg.P99Ms)
				fmt.Println("==========================================================================")
			}
		},
	}
	rootCmd.AddCommand(metricsCmd)

	// Run command (local mode)
	var runCmd = &cobra.Command{
		Use:   "run [target] [users] [duration]",
		Short: "Run a local load test without distributed workers",
		Args:  cobra.MaximumNArgs(3),
		Run: func(cmd *cobra.Command, args []string) {
			var cfg *config.Config
			var err error

			if configPath != "" {
				cfg, err = config.LoadConfig(configPath)
				if err != nil {
					log.Fatalf("Failed to load config: %v", err)
				}
			} else {
				if len(args) == 0 {
					log.Fatal("Error: Either --config flag or a [target] positional URL/domain is required.")
				}
				target := args[0]
				if !strings.HasPrefix(target, "http://") && !strings.HasPrefix(target, "https://") {
					target = "http://" + target
				}

				usersVal := 10
				if len(args) > 1 {
					val, err := strconv.Atoi(args[1])
					if err != nil {
						log.Fatalf("Invalid users count: %v. Must be an integer.", args[1])
					}
					usersVal = val
				}

				durationVal := "10s"
				if len(args) > 2 {
					durationVal = args[2]
				}

				cfg = &config.Config{
					Name:      "Local Load Test",
					Target:    target,
					Method:    "GET",
					Users:     usersVal,
					Duration:  durationVal,
					RampUp:    "0s",
					ThinkTime: "0s",
					Timeout:   "10s",
				}
			}

			// CLI overrides
			if users > 0 {
				cfg.Users = users
			}
			if duration != "" {
				cfg.Duration = duration
			}
			if rampUp != "" {
				cfg.RampUp = rampUp
			}
			if loopsOverride > 0 {
				cfg.Loops = loopsOverride
			}
			if thinkTimeOverride != "" {
				cfg.ThinkTime = thinkTimeOverride
			}

			if err := cfg.Validate(); err != nil {
				log.Fatalf("Configuration validation failed: %v", err)
			}

			m := metrics.NewMetrics()
			sched := scheduler.NewScheduler(cfg, m)

			slog.Info("Starting local load test...", "target", cfg.Target, "users", cfg.Users, "duration", cfg.Duration)
			if err := sched.Start(); err != nil {
				log.Fatalf("Failed to start load test: %v", err)
			}

			// Live reporting loop
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var prevRequests int64
			var prevTime = time.Now()

			go func() {
				ticker := time.NewTicker(1 * time.Second)
				defer ticker.Stop()

				for {
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
						snap := m.Snapshot()
						now := time.Now()
						elapsed := now.Sub(prevTime).Seconds()
						if elapsed <= 0 {
							elapsed = 1
						}
						rps := float64(snap.Requests-prevRequests) / elapsed
						if rps < 0 {
							rps = 0
						}
						prevRequests = snap.Requests
						prevTime = now

						avgLatency := time.Duration(0)
						if snap.Requests > 0 {
							avgLatency = snap.TotalLatency / time.Duration(snap.Requests)
						}

						fmt.Printf("[%s] VUs: %d | RPS: %.2f | Requests: %d | Success: %d | Failures: %d | Latency: %s\n",
							time.Now().Format("15:04:05"),
							atomic.LoadInt64(&m.RunningUsers),
							rps,
							snap.Requests,
							snap.Successes,
							snap.Failures,
							avgLatency.Round(time.Millisecond),
						)
					}
				}
			}()

			// Wait for scheduler to complete
			for {
				state := sched.GetState()
				if state == scheduler.StateCompleted || state == scheduler.StateStopped {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}

			cancel() // Stop the metrics printing loop
			time.Sleep(100 * time.Millisecond)

			// Print final report
			snap := m.Snapshot()
			avgLatency := time.Duration(0)
			if snap.Requests > 0 {
				avgLatency = snap.TotalLatency / time.Duration(snap.Requests)
			}

			p50 := snap.CalculatePercentile(50)
			p90 := snap.CalculatePercentile(90)
			p95 := snap.CalculatePercentile(95)
			p99 := snap.CalculatePercentile(99)

			successRate := 0.0
			if snap.Requests > 0 {
				successRate = float64(snap.Successes) / float64(snap.Requests) * 100
			}

			fmt.Println("\n==========================================")
			fmt.Println("             TEST COMPLETED")
			fmt.Println("==========================================")
			fmt.Printf("Target:          %s\n", cfg.Target)
			fmt.Printf("Method:          %s\n", cfg.Method)
			fmt.Printf("Duration:        %s\n", cfg.Duration)
			fmt.Printf("VUs:             %d\n", cfg.Users)
			fmt.Printf("Total Requests:  %d\n", snap.Requests)
			fmt.Printf("Successes:       %d\n", snap.Successes)
			fmt.Printf("Failures:        %d (Timeouts: %d)\n", snap.Failures, snap.Timeouts)
			fmt.Printf("Success Rate:    %.2f%%\n", successRate)
			fmt.Printf("Bandwidth Sent:  %.2f MB\n", float64(snap.BytesSent)/(1024*1024))
			fmt.Printf("Bandwidth Recv:  %.2f MB\n", float64(snap.BytesReceived)/(1024*1024))
			fmt.Println("Latencies:")
			fmt.Printf("  Min:   %v\n", snap.MinLatency.Round(time.Millisecond))
			fmt.Printf("  Avg:   %v\n", avgLatency.Round(time.Millisecond))
			fmt.Printf("  Max:   %v\n", snap.MaxLatency.Round(time.Millisecond))
			fmt.Printf("  P50:   %v\n", p50.Round(time.Millisecond))
			fmt.Printf("  P90:   %v\n", p90.Round(time.Millisecond))
			fmt.Printf("  P95:   %v\n", p95.Round(time.Millisecond))
			fmt.Printf("  P99:   %v\n", p99.Round(time.Millisecond))
			fmt.Println("==========================================")
		},
	}
	runCmd.Flags().StringVar(&configPath, "config", "", "Path to the load test YAML config file")
	runCmd.Flags().IntVar(&users, "users", 0, "Override configured Virtual Users count")
	runCmd.Flags().StringVar(&duration, "duration", "", "Override configured execution duration")
	runCmd.Flags().StringVar(&rampUp, "ramp-up", "", "Override configured ramp-up time")
	runCmd.Flags().IntVar(&loopsOverride, "loops", 0, "Override configured loop count limit per VU")
	runCmd.Flags().StringVar(&thinkTimeOverride, "think-time", "", "Override configured think time (e.g. 100ms)")
	rootCmd.AddCommand(runCmd)

	// Version command
	var versionCmd = &cobra.Command{
		Use:   "version",
		Short: "Print the version of DoSLoader",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("DoSLoader v1.0.0 (Go 1.26.5)")
		},
	}
	rootCmd.AddCommand(versionCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func getH2CClient() *http.Client {
	h2t := &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, network, addr string, cfg *tls.Config) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, network, addr)
		},
	}
	return &http.Client{
		Transport: h2t,
	}
}
