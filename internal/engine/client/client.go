package client

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/http2"
)

type contextKey string

const (
	BytesSentKey contextKey = "bytes_sent"
	BytesRecvKey contextKey = "bytes_recv"
)

// DNSCache holds resolved IPs to bypass recurrent system lookups.
type DNSCache struct {
	mu    sync.RWMutex
	cache map[string][]net.IP
}

func NewDNSCache() *DNSCache {
	return &DNSCache{
		cache: make(map[string][]net.IP),
	}
}

// Lookup retrieves an IP from cache or performs a net lookup.
func (d *DNSCache) Lookup(ctx context.Context, host string) (string, error) {
	d.mu.RLock()
	ips, ok := d.cache[host]
	d.mu.RUnlock()
	if ok && len(ips) > 0 {
		return ips[0].String(), nil
	}

	resolver := net.DefaultResolver
	resolvedIPs, err := resolver.LookupIP(ctx, "ip", host)
	if err != nil {
		return "", err
	}
	if len(resolvedIPs) == 0 {
		return "", fmt.Errorf("no IP address found for host: %s", host)
	}

	d.mu.Lock()
	d.cache[host] = resolvedIPs
	d.mu.Unlock()

	return resolvedIPs[0].String(), nil
}

// countingConn wraps net.Conn to record physical bytes sent/received.
type countingConn struct {
	net.Conn
	bytesSent *int64
	bytesRecv *int64
}

func (c *countingConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if n > 0 && c.bytesRecv != nil {
		atomic.AddInt64(c.bytesRecv, int64(n))
	}
	return n, err
}

func (c *countingConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	if n > 0 && c.bytesSent != nil {
		atomic.AddInt64(c.bytesSent, int64(n))
	}
	return n, err
}

// ClientManager creates and configures the optimized HTTP clients.
type ClientManager struct {
	dnsCache  *DNSCache
	Client    *http.Client
	Transport *http.Transport
}

func NewClientManager(timeout time.Duration, skipVerify bool) *ClientManager {
	dnsCache := NewDNSCache()
	dialer := &net.Dialer{
		Timeout:   timeout,
		KeepAlive: 30 * time.Second,
	}

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				host = addr
				port = ""
			}

			// Resolve host via custom DNS cache
			ip, err := dnsCache.Lookup(ctx, host)
			if err == nil {
				if port != "" {
					addr = net.JoinHostPort(ip, port)
				} else {
					addr = ip
				}
			}

			conn, err := dialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}

			// Wrap connection with byte counters if pointers are passed in the context
			var sPtr, rPtr *int64
			if val := ctx.Value(BytesSentKey); val != nil {
				if ptr, ok := val.(*int64); ok {
					sPtr = ptr
				}
			}
			if val := ctx.Value(BytesRecvKey); val != nil {
				if ptr, ok := val.(*int64); ok {
					rPtr = ptr
				}
			}

			return &countingConn{
				Conn:      conn,
				bytesSent: sPtr,
				bytesRecv: rPtr,
			}, nil
		},
		MaxIdleConns:          10000,
		MaxIdleConnsPerHost:   10000,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    false,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: skipVerify,
		},
	}

	// Enable HTTP/2 support
	_ = http2.ConfigureTransport(transport)

	client := &http.Client{
		Transport: transport,
		Timeout:   timeout,
		// In load testing, we often want to inspect the redirect itself (301/302 response)
		// rather than following it indefinitely. Disable auto-redirect by default.
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	return &ClientManager{
		dnsCache:  dnsCache,
		Client:    client,
		Transport: transport,
	}
}
