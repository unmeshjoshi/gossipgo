package rpc

import (
	"net"
	"net/rpc"
	"sync"
	"log"
)

// Server is a Cockroach-specific RPC server with an embedded go RPC
// server struct.
type Server struct {
	*rpc.Server                          // Embedded RPC server instance
	Addr           net.Addr              // Server address; may change if picking unused port
	listener       net.Listener          // Server listener
	listening      chan struct{}         // signaled when the server is listening
	mu             sync.Mutex            // Mutex protects closed bool & closeCallbacks slice
	closed         bool                  // Set upon invocation of Close()
	closeCallbacks []func(conn net.Conn) // Slice of callbacks to invoke on conn close
}

// NewServer creates a new instance of Server.
func NewServer(addr net.Addr) *Server {
	s := &Server{
		Server:         rpc.NewServer(),
		Addr:           addr,
		listening:      make(chan struct{}, 1),
		closeCallbacks: make([]func(conn net.Conn), 0, 1),
	}
	heartbeat := &HeartbeatService{}
	s.RegisterName("Heartbeat", heartbeat)
	return s
}

// AddCloseCallback adds a callback to the closeCallbacks slice to
// be invoked when a connection is closed.
func (s *Server) AddCloseCallback(cb func(conn net.Conn)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeCallbacks = append(s.closeCallbacks, cb)
}

// ListenAndServe begins listening and serving. This method should be
// invoked by goroutine as it will loop serving incoming client and
// not return until Close() is invoked.
func (s *Server) ListenAndServe() {
	ln, err := net.Listen(s.Addr.Network(), s.Addr.String())
	if err != nil {
		log.Printf("unable to start gossip node: %s", err)
	}
	s.listening <- struct{}{} // signal that we are now listening.
	s.listener = ln
	s.Addr = ln.Addr()

	// Start serving gossip protocol in a loop until listener is closed.
	log.Printf("serving on %+v...", s.Addr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			defer s.mu.Unlock()
			if !s.closed {
				log.Printf("gossip server terminated: %s", err)
			}
			break
		}
		// Serve connection to completion in a goroutine.
		go s.serveConn(conn)
	}
}

// Close closes the listener.
func (s *Server) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.listener.Close()
}

// serveConn synchronously serves a single connection. When the
// connection is closed, the client address is removed from the
// incoming set.
func (s *Server) serveConn(conn net.Conn) {
	s.ServeConn(conn)
	s.mu.Lock()
	for _, cb := range s.closeCallbacks {
		cb(conn)
	}
	s.mu.Unlock()
	conn.Close()
}
