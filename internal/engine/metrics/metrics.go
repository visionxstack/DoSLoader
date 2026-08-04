package metrics

import (
	"math"
	"sort"
	"sync/atomic"
	"time"
)

// BucketBoundaries defines the latency thresholds for the histogram.
var BucketBoundaries = []time.Duration{
	1 * time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond, 4 * time.Millisecond, 5 * time.Millisecond,
	6 * time.Millisecond, 7 * time.Millisecond, 8 * time.Millisecond, 9 * time.Millisecond, 10 * time.Millisecond,
	15 * time.Millisecond, 20 * time.Millisecond, 25 * time.Millisecond, 30 * time.Millisecond, 40 * time.Millisecond,
	50 * time.Millisecond, 60 * time.Millisecond, 70 * time.Millisecond, 80 * time.Millisecond, 90 * time.Millisecond,
	100 * time.Millisecond, 120 * time.Millisecond, 150 * time.Millisecond, 180 * time.Millisecond, 200 * time.Millisecond,
	250 * time.Millisecond, 300 * time.Millisecond, 350 * time.Millisecond, 400 * time.Millisecond, 450 * time.Millisecond,
	500 * time.Millisecond, 600 * time.Millisecond, 700 * time.Millisecond, 800 * time.Millisecond, 900 * time.Millisecond,
	1 * time.Second, 1200 * time.Millisecond, 1500 * time.Millisecond, 1800 * time.Millisecond, 2 * time.Second,
	2500 * time.Millisecond, 3 * time.Second, 4 * time.Second, 5 * time.Second, 7500 * time.Millisecond,
	10 * time.Second, 15 * time.Second, 20 * time.Second, 30 * time.Second, 45 * time.Second, 60 * time.Second,
}

// Metrics collects load test execution statistics using atomic operations.
type Metrics struct {
	Requests       int64
	Successes      int64
	Failures       int64
	Timeouts       int64
	BytesSent      int64
	BytesReceived  int64
	RunningUsers   int64
	CompletedUsers int64
	MinLatencyNS   int64
	MaxLatencyNS   int64
	TotalLatencyNS int64
	// Buckets holds count for each boundary + 1 extra for overflow (> 60s)
	Buckets [52]int64
}

// NewMetrics initializes a Metrics engine.
func NewMetrics() *Metrics {
	return &Metrics{
		MinLatencyNS: 0, // 0 indicates unset
		MaxLatencyNS: 0,
	}
}

// Record updates the stats using lock-free operations.
func (m *Metrics) Record(success bool, timeout bool, latency time.Duration, bytesSent int64, bytesRecv int64) {
	atomic.AddInt64(&m.Requests, 1)

	if success {
		atomic.AddInt64(&m.Successes, 1)
	} else {
		atomic.AddInt64(&m.Failures, 1)
	}

	if timeout {
		atomic.AddInt64(&m.Timeouts, 1)
	}

	atomic.AddInt64(&m.BytesSent, bytesSent)
	atomic.AddInt64(&m.BytesReceived, bytesRecv)

	latencyNS := latency.Nanoseconds()
	atomic.AddInt64(&m.TotalLatencyNS, latencyNS)

	// Update Min Latency (using CAS loop)
	for {
		oldMin := atomic.LoadInt64(&m.MinLatencyNS)
		if latencyNS >= oldMin && oldMin != 0 {
			break
		}
		if atomic.CompareAndSwapInt64(&m.MinLatencyNS, oldMin, latencyNS) {
			break
		}
	}

	// Update Max Latency (using CAS loop)
	for {
		oldMax := atomic.LoadInt64(&m.MaxLatencyNS)
		if latencyNS <= oldMax {
			break
		}
		if atomic.CompareAndSwapInt64(&m.MaxLatencyNS, oldMax, latencyNS) {
			break
		}
	}

	// Find the appropriate histogram bucket
	idx := sort.Search(len(BucketBoundaries), func(i int) bool {
		return BucketBoundaries[i] >= latency
	})
	// If idx == len(BucketBoundaries), it falls into the overflow bucket
	if idx < len(m.Buckets) {
		atomic.AddInt64(&m.Buckets[idx], 1)
	}
}

// MetricsSnapshot represents a point-in-time snapshot of the metrics.
type MetricsSnapshot struct {
	Requests       int64
	Successes      int64
	Failures       int64
	Timeouts       int64
	BytesSent      int64
	BytesReceived  int64
	RunningUsers   int64
	CompletedUsers int64
	MinLatency     time.Duration
	MaxLatency     time.Duration
	AvgLatency     time.Duration
	TotalLatency   time.Duration
	Buckets        []int64
}

// Snapshot returns a copy of the current metrics data.
func (m *Metrics) Snapshot() *MetricsSnapshot {
	snap := &MetricsSnapshot{
		Requests:       atomic.LoadInt64(&m.Requests),
		Successes:      atomic.LoadInt64(&m.Successes),
		Failures:       atomic.LoadInt64(&m.Failures),
		Timeouts:       atomic.LoadInt64(&m.Timeouts),
		BytesSent:      atomic.LoadInt64(&m.BytesSent),
		BytesReceived:  atomic.LoadInt64(&m.BytesReceived),
		RunningUsers:   atomic.LoadInt64(&m.RunningUsers),
		CompletedUsers: atomic.LoadInt64(&m.CompletedUsers),
		MinLatency:     time.Duration(atomic.LoadInt64(&m.MinLatencyNS)),
		MaxLatency:     time.Duration(atomic.LoadInt64(&m.MaxLatencyNS)),
	}

	total := atomic.LoadInt64(&m.TotalLatencyNS)
	snap.TotalLatency = time.Duration(total)
	if snap.Requests > 0 {
		snap.AvgLatency = time.Duration(total / snap.Requests)
	}

	snap.Buckets = make([]int64, len(m.Buckets))
	for i := range m.Buckets {
		snap.Buckets[i] = atomic.LoadInt64(&m.Buckets[i])
	}

	return snap
}

// CalculatePercentile interpolates the percentile value from the snapshot buckets.
func (s *MetricsSnapshot) CalculatePercentile(percentile float64) time.Duration {
	if s.Requests == 0 {
		return 0
	}

	targetCount := float64(s.Requests) * (percentile / 100.0)
	var countSum int64

	for idx, count := range s.Buckets {
		countSum += count
		if float64(countSum) >= targetCount {
			// Percentile lies within this bucket
			var lowerBound time.Duration
			if idx > 0 {
				lowerBound = BucketBoundaries[idx-1]
			}

			var upperBound time.Duration
			if idx < len(BucketBoundaries) {
				upperBound = BucketBoundaries[idx]
			} else {
				upperBound = s.MaxLatency
				if upperBound < lowerBound {
					upperBound = lowerBound + 1*time.Second
				}
			}

			// Interpolation
			bucketCount := count
			if bucketCount == 0 {
				return lowerBound
			}

			prevCountSum := countSum - bucketCount
			fraction := (targetCount - float64(prevCountSum)) / float64(bucketCount)
			val := float64(lowerBound) + fraction*float64(upperBound-lowerBound)

			if val < 0 {
				return 0
			}
			if val > float64(math.MaxInt64) {
				return time.Duration(math.MaxInt64)
			}
			return time.Duration(val)
		}
	}

	return s.MaxLatency
}
