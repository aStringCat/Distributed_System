package raft

import (
	"bytes"
	"context"
)

type ApplyMessage struct {
	Index    int
	Term     uint64
	Data     string
	Snapshot *Snapshot
}

func (n *Node) Applied() <-chan ApplyMessage {
	return n.applyCh
}

func (n *Node) notifyApply() {
	select {
	case n.applyReady <- struct{}{}:
	default:
	}
}

func (n *Node) runApplier(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		n.mu.Lock()
		if n.storageErr != nil {
			n.mu.Unlock()
			return
		}
		if n.pendingSnapshot == nil && n.lastApplied >= n.commitIndex {
			n.mu.Unlock()
			select {
			case <-n.applyReady:
			case <-n.failed:
				return
			case <-ctx.Done():
				return
			}
			continue
		}

		var msg ApplyMessage
		if n.pendingSnapshot != nil {
			snapshot := *n.pendingSnapshot
			snapshot.Data = bytes.Clone(snapshot.Data)
			msg = ApplyMessage{Index: snapshot.Index, Term: snapshot.Term, Snapshot: &snapshot}
		} else {
			index := max(n.lastApplied+1, n.snapshot.Index+1)
			entry := n.log[n.offset(index)]
			msg = ApplyMessage{Index: index, Term: entry.Term, Data: entry.Data}
		}
		n.mu.Unlock()

		select {
		case n.applyCh <- msg:
			n.mu.Lock()
			n.lastApplied = max(n.lastApplied, msg.Index)
			if msg.Snapshot != nil && n.pendingSnapshot != nil && n.pendingSnapshot.Index <= msg.Index {
				n.pendingSnapshot = nil
			}
			n.mu.Unlock()
		case <-ctx.Done():
			return
		case <-n.failed:
			return
		}
	}
}
