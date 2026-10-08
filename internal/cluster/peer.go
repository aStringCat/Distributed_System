package cluster

import (
	"context"
	"net"
	"net/rpc"
	"time"
)

type peerClient struct {
	addr   string
	ctx    context.Context
	cancel context.CancelFunc
}

func newPeerClient(addr string) *peerClient {
	ctx, cancel := context.WithCancel(context.Background())
	return &peerClient{addr: addr, ctx: ctx, cancel: cancel}
}

func (p *peerClient) Call(method string, req, res any) error {
	if err := p.ctx.Err(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(p.ctx, 500*time.Millisecond)
	defer cancel()

	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", p.addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		return err
	}
	client := rpc.NewClient(conn)
	defer client.Close()
	err = client.Call(method, req, res)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

func (p *peerClient) Close() error {
	p.cancel()
	return nil
}
