package raft

import (
	"context"
	"math/rand/v2"
	"time"
)

const (
	tickInterval      = 20 * time.Millisecond
	heartbeatInterval = 100 * time.Millisecond
	electionMin       = 450 * time.Millisecond
	electionMax       = 750 * time.Millisecond
)

func (n *Node) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	applyDone := make(chan struct{})
	go func() {
		defer close(applyDone)
		defer close(n.applyCh)
		n.runApplier(ctx)
	}()
	defer func() {
		cancel()
		<-applyDone
	}()

	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return n.Err()
		case <-n.failed:
			return n.Err()
		case <-ticker.C:
			n.tick()
		}
	}
}

func (n *Node) resetElectionTimer() {
	timeout := electionMin + time.Duration(rand.Int64N(int64(electionMax-electionMin)))
	n.electionDeadline = time.Now().Add(timeout)
}

func (n *Node) tick() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.storageErr != nil {
		return
	}
	now := time.Now()
	if n.role == Leader {
		if !now.Before(n.nextHeartbeat) {
			n.broadcast()
		}
	} else if !now.Before(n.electionDeadline) {
		n.startElection()
	}
}
