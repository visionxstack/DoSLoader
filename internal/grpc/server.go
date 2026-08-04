package grpc

import (
	"context"
	"net/http"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// ServerWrapper encapsulates the HTTP/2 plain-text (h2c) server.
type ServerWrapper struct {
	HttpServer *http.Server
	GrpcServer *Server
}

// NewServerWrapper initializes a ServerWrapper.
func NewServerWrapper() *ServerWrapper {
	grpcServer := NewServer()
	h2s := &http2.Server{}

	// Wrap our custom gRPC server in h2c to allow cleartext HTTP/2 connections
	h2cHandler := h2c.NewHandler(grpcServer, h2s)

	httpServer := &http.Server{
		Handler:           h2cHandler,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       30 * time.Second,
	}

	return &ServerWrapper{
		HttpServer: httpServer,
		GrpcServer: grpcServer,
	}
}

// Register binds a path to a handler function in our gRPC router.
func (s *ServerWrapper) Register(path string, handler http.HandlerFunc) {
	s.GrpcServer.Register(path, handler)
}

// Start binds to the address and listens for H2C connections.
func (s *ServerWrapper) Start(addr string) error {
	s.HttpServer.Addr = addr
	return s.HttpServer.ListenAndServe()
}

// Shutdown gracefully stops the server.
func (s *ServerWrapper) Shutdown(ctx context.Context) error {
	return s.HttpServer.Shutdown(ctx)
}

type ServerInterface = ServerWrapper
