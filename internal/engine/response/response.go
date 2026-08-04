package response

import (
	"io"
	"net/http"
	"time"
)

// ResponseInfo contains the collected performance metadata of an HTTP execution.
type ResponseInfo struct {
	StatusCode    int
	Headers       http.Header
	BodySize      int64
	Latency       time.Duration
	BytesSent     int64
	BytesReceived int64
}

// ParseResponse reads the response, discards the body to count size without allocation,
// and collects the statistics. It ensures the response body is closed.
func ParseResponse(resp *http.Response, latency time.Duration, bytesSent, bytesReceived int64) (*ResponseInfo, error) {
	defer resp.Body.Close()

	// Discard body to measure exact size and reuse TCP connection in pool
	bodySize, err := io.Copy(io.Discard, resp.Body)
	if err != nil {
		// Even if reading fails, we return the status code we got
		bodySize = 0
	}

	return &ResponseInfo{
		StatusCode:    resp.StatusCode,
		Headers:       resp.Header,
		BodySize:      bodySize,
		Latency:       latency,
		BytesSent:     bytesSent,
		BytesReceived: bytesReceived,
	}, nil
}
