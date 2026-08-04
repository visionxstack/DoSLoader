package grpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"

	"google.golang.org/protobuf/proto"
)

// WriteFrame encodes and writes a Protobuf message as a gRPC frame.
func WriteFrame(w io.Writer, msg proto.Message) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal proto: %w", err)
	}

	header := make([]byte, 5)
	header[0] = 0 // uncompressed
	binary.BigEndian.PutUint32(header[1:5], uint32(len(data)))

	if _, err := w.Write(header); err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

// ReadFrame reads and decodes a gRPC frame into a Protobuf message.
func ReadFrame(r io.Reader, msg proto.Message) error {
	header := make([]byte, 5)
	if _, err := io.ReadFull(r, header); err != nil {
		return err
	}

	compressed := header[0]
	if compressed != 0 {
		return fmt.Errorf("compressed frames are not supported")
	}

	length := binary.BigEndian.Uint32(header[1:5])
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return err
	}

	return proto.Unmarshal(data, msg)
}

// UnaryCall performs a standard gRPC Unary RPC.
func UnaryCall(ctx context.Context, client *http.Client, url string, reqMsg, respMsg proto.Message) error {
	var buf bytes.Buffer
	if err := WriteFrame(&buf, reqMsg); err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/grpc")
	req.Header.Set("TE", "trailers")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected HTTP status: %d", resp.StatusCode)
	}

	if err := ReadFrame(resp.Body, respMsg); err != nil {
		return err
	}

	return nil
}

// ClientStream represents a client-to-server streaming gRPC connection.
type ClientStream struct {
	pw     *io.PipeWriter
	respCh chan *http.Response
	errCh  chan error
}

// NewClientStream establishes a client-to-server streaming connection.
func NewClientStream(ctx context.Context, client *http.Client, url string) (*ClientStream, error) {
	pr, pw := io.Pipe()

	req, err := http.NewRequestWithContext(ctx, "POST", url, pr)
	if err != nil {
		pr.Close()
		pw.Close()
		return nil, err
	}
	req.Header.Set("Content-Type", "application/grpc")
	req.Header.Set("TE", "trailers")

	respCh := make(chan *http.Response, 1)
	errCh := make(chan error, 1)

	go func() {
		resp, err := client.Do(req)
		if err != nil {
			errCh <- err
			return
		}
		respCh <- resp
	}()

	return &ClientStream{
		pw:     pw,
		respCh: respCh,
		errCh:  errCh,
	}, nil
}

// Send writes a message to the active client stream.
func (cs *ClientStream) Send(msg proto.Message) error {
	select {
	case err := <-cs.errCh:
		return err
	default:
	}
	return WriteFrame(cs.pw, msg)
}

// CloseAndRecv closes the stream and decodes the server's final response.
func (cs *ClientStream) CloseAndRecv(respMsg proto.Message) error {
	_ = cs.pw.Close()

	var resp *http.Response
	select {
	case err := <-cs.errCh:
		return err
	case resp = <-cs.respCh:
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	return ReadFrame(resp.Body, respMsg)
}

// ServerStream represents a server-to-client streaming gRPC connection.
type ServerStream struct {
	resp *http.Response
}

// NewServerStream establishes a server-to-client streaming connection.
func NewServerStream(ctx context.Context, client *http.Client, url string, reqMsg proto.Message) (*ServerStream, error) {
	var buf bytes.Buffer
	if err := WriteFrame(&buf, reqMsg); err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, &buf)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/grpc")
	req.Header.Set("TE", "trailers")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	return &ServerStream{resp: resp}, nil
}

// Recv reads the next message from the server stream.
func (ss *ServerStream) Recv(respMsg proto.Message) error {
	err := ReadFrame(ss.resp.Body, respMsg)
	if err != nil {
		ss.resp.Body.Close()
		return err
	}
	return nil
}

// Close terminates the server stream.
func (ss *ServerStream) Close() error {
	return ss.resp.Body.Close()
}

// Server implements a routing layer for handling gRPC requests over HTTP/2.
type Server struct {
	handlers map[string]http.HandlerFunc
}

func NewServer() *Server {
	return &Server{
		handlers: make(map[string]http.HandlerFunc),
	}
}

// Register registers a path handler.
func (s *Server) Register(path string, handler http.HandlerFunc) {
	s.handlers[path] = handler
}

// ServeHTTP implements http.Handler to dispatch incoming gRPC requests.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "gRPC requires POST method", http.StatusMethodNotAllowed)
		return
	}

	h, ok := s.handlers[r.URL.Path]
	if !ok {
		w.Header().Set("Content-Type", "application/grpc")
		w.Header().Set("grpc-status", "12") // Unimplemented
		w.WriteHeader(http.StatusOK)
		return
	}

	w.Header().Set("Content-Type", "application/grpc")
	w.Header().Set("Trailer", "grpc-status, grpc-message")
	w.Header().Set("grpc-status", "0") // Default to OK, can be overridden by handler

	h(w, r)
}
