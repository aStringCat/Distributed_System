package raft

import (
	"bytes"
	"slices"
	"time"
)

type LogEntry struct {
	Term uint64
	Data string
}

type AppendEntriesRequest struct {
	Term         uint64
	PrevLogIndex int
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesResponse struct {
	Term    uint64
	Success bool
}

func (n *Node) AppendEntries(req *AppendEntriesRequest, res *AppendEntriesResponse) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	*res = AppendEntriesResponse{Term: n.term}
	if err := n.updateTerm(req.Term); err != nil {
		return err
	}
	res.Term = n.term
	if req.Term < n.term {
		return nil
	}

	n.role = Follower
	n.resetElectionTimer()
	prev := req.PrevLogIndex
	if prev < n.snapshot.Index || prev > n.lastIndex() || n.termAt(prev) != req.PrevLogTerm {
		return nil
	}

	for offset, entry := range req.Entries {
		index := prev + 1 + offset
		if index > n.lastIndex() || n.termAt(index) != entry.Term {
			n.log = append(n.log[:n.offset(index)], req.Entries[offset:]...)
			if err := n.persist(); err != nil {
				return err
			}
			break
		}
	}

	commit := min(req.LeaderCommit, req.PrevLogIndex+len(req.Entries))
	if commit > n.commitIndex {
		n.commitIndex = commit
		n.notifyApply()
	}

	res.Success = true
	return nil
}

func (n *Node) broadcast() {
	n.nextHeartbeat = time.Now().Add(heartbeatInterval)
	for peer := range n.peers {
		if peer == n.id || n.peers[peer] == nil {
			continue
		}
		next := n.nextIndex[peer]
		if next <= n.snapshot.Index {
			snapshot := n.snapshot
			snapshot.Data = bytes.Clone(snapshot.Data)
			go n.sendSnapshot(peer, InstallSnapshotRequest{Term: n.term, Snapshot: snapshot})
			continue
		}
		req := AppendEntriesRequest{
			Term:         n.term,
			PrevLogIndex: next - 1, PrevLogTerm: n.termAt(next - 1),
			Entries: slices.Clone(n.log[n.offset(next):]), LeaderCommit: n.commitIndex,
		}
		go n.replicate(peer, req)
	}
}

func (n *Node) replicate(peer int, req AppendEntriesRequest) {
	var res AppendEntriesResponse
	if err := n.peers[peer].Call("Node.AppendEntries", &req, &res); err != nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.updateTerm(res.Term); err != nil {
		return
	}
	if n.role != Leader || n.term != req.Term {
		return
	}
	if res.Success {
		matched := req.PrevLogIndex + len(req.Entries)
		n.matchIndex[peer] = max(n.matchIndex[peer], matched)
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		n.advanceCommit()
	} else if n.nextIndex[peer] == req.PrevLogIndex+1 {
		n.nextIndex[peer] = max(req.PrevLogIndex, n.matchIndex[peer]+1)
	}
}

func (n *Node) resetReplication() {
	n.nextIndex = make([]int, len(n.peers))
	n.matchIndex = make([]int, len(n.peers))
	for peer := range n.peers {
		n.nextIndex[peer] = n.lastIndex() + 1
	}
	n.matchIndex[n.id] = n.lastIndex()
}
