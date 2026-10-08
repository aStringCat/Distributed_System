package cluster

import (
	"errors"
	"net"
	"net/rpc"
	"sync"
)

type rpcTransport struct {
	server   *rpc.Server
	listener net.Listener
	mu       sync.Mutex
	conns    map[net.Conn]struct{}
	wg       sync.WaitGroup
	stopped  chan struct{}
	errors   chan error
}

func listenRPC(addr string) (*rpcTransport, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	t := &rpcTransport{
		server:   rpc.NewServer(),
		listener: listener,
		conns:    make(map[net.Conn]struct{}),
		stopped:  make(chan struct{}),
		errors:   make(chan error, 1),
	}
	go t.accept()
	return t, nil
}

func (t *rpcTransport) accept() {
	defer close(t.stopped)
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				t.errors <- err
			}
			return
		}
		t.mu.Lock()
		t.conns[conn] = struct{}{}
		t.mu.Unlock()
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			t.server.ServeConn(conn)
			_ = conn.Close()
			t.mu.Lock()
			delete(t.conns, conn)
			t.mu.Unlock()
		}()
	}
}

func (t *rpcTransport) close() {
	_ = t.listener.Close()
	<-t.stopped
	t.mu.Lock()
	for conn := range t.conns {
		_ = conn.Close()
	}
	t.mu.Unlock()
	t.wg.Wait()
}
