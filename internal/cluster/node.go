package cluster

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strconv"
	"time"

	"system/internal/raft"
	"system/internal/server"
)

type Config struct {
	ID             int
	Peers          []string
	HTTPAddr       string
	DataDir        string
	StartupTimeout time.Duration
	SnapshotEvery  int
}

func (c Config) validate() error {
	if c.SnapshotEvery < 0 {
		return errors.New("snapshot interval must not be negative")
	}
	if len(c.Peers) == 0 || c.ID < 0 || c.ID >= len(c.Peers) {
		return errors.New("node ID must index the peer list")
	}
	if c.DataDir == "" || c.StartupTimeout <= 0 {
		return errors.New("data directory and positive startup timeout are required")
	}
	seen := make(map[string]bool)
	for _, addr := range append(append([]string(nil), c.Peers...), c.HTTPAddr) {
		host, port, err := net.SplitHostPort(addr)
		number, portErr := strconv.Atoi(port)
		if err != nil || portErr != nil || host == "" || number < 1 || number > 65535 {
			return errors.New("addresses must include a host and nonzero port")
		}
		if seen[addr] {
			return errors.New("duplicate RPC or HTTP address")
		}
		seen[addr] = true
	}
	return nil
}

func Run(ctx context.Context, cfg Config) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return nil
	}
	return runNode(ctx, cfg)
}

func runNode(ctx context.Context, cfg Config) error {
	transport, err := listenRPC(cfg.Peers[cfg.ID])
	if err != nil {
		return err
	}
	defer transport.close()

	startupCtx, cancelStartup := context.WithTimeout(ctx, cfg.StartupTimeout)
	peers, err := connectPeers(startupCtx, cfg.ID, cfg.Peers)
	cancelStartup()
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	defer closePeers(peers)

	node, err := raft.OpenNode(cfg.ID, peers, cfg.DataDir)
	if err != nil {
		return err
	}
	if err := transport.server.RegisterName("Node", node); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return err
	}
	defer listener.Close()

	service := server.NewReplicated(node, cfg.SnapshotEvery)
	httpServer := &http.Server{
		Handler:           server.NewHandler(service),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	runCtx, cancel := context.WithCancel(context.Background())
	serviceDone := make(chan struct{})
	var serviceErr error
	go func() {
		defer close(serviceDone)
		serviceErr = service.Run(runCtx)
	}()
	httpDone := make(chan struct{})
	var httpErr error
	go func() {
		defer close(httpDone)
		httpErr = httpServer.Serve(listener)
	}()
	defer func() {
		_ = httpServer.Close()
		cancel()
		closePeers(peers)
		<-httpDone
		<-serviceDone
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelShutdown()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			return err
		}
		return node.Err()
	case <-serviceDone:
		return serviceErr
	case <-httpDone:
		if errors.Is(httpErr, http.ErrServerClosed) {
			return nil
		}
		return httpErr
	case err := <-transport.errors:
		return err
	}
}

func connectPeers(ctx context.Context, id int, addresses []string) ([]raft.Peer, error) {
	peers := make([]raft.Peer, len(addresses))
	dialer := net.Dialer{Timeout: 200 * time.Millisecond}
	for peer, addr := range addresses {
		if peer == id {
			continue
		}
		for {
			if err := ctx.Err(); err != nil {
				closePeers(peers)
				return nil, err
			}
			conn, err := dialer.DialContext(ctx, "tcp", addr)
			if err == nil {
				_ = conn.Close()
				peers[peer] = newPeerClient(addr)
				break
			}
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				closePeers(peers)
				return nil, ctx.Err()
			case <-timer.C:
			}
		}
	}
	if err := ctx.Err(); err != nil {
		closePeers(peers)
		return nil, err
	}
	return peers, nil
}

func closePeers(peers []raft.Peer) {
	for _, peer := range peers {
		if peer != nil {
			_ = peer.Close()
		}
	}
}
