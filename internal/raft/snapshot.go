package raft

import (
	"bytes"
	"errors"
)

type Snapshot struct {
	Index int    `json:"index"`
	Term  uint64 `json:"term"`
	Data  []byte `json:"data"`
}

type InstallSnapshotRequest struct {
	Term     uint64
	Snapshot Snapshot
}

type InstallSnapshotResponse struct {
	Term uint64
}

func (n *Node) lastIndex() int          { return n.snapshot.Index + len(n.log) - 1 }
func (n *Node) offset(index int) int    { return index - n.snapshot.Index }
func (n *Node) termAt(index int) uint64 { return n.log[n.offset(index)].Term }

func (n *Node) CurrentSnapshot() Snapshot {
	n.mu.Lock()
	defer n.mu.Unlock()
	snapshot := n.snapshot
	snapshot.Data = bytes.Clone(snapshot.Data)
	return snapshot
}

func (n *Node) Compact(index int, data []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.storageErr != nil {
		return n.storageErr
	}
	if index < 0 || index > n.commitIndex || index > n.lastIndex() {
		return errors.New("snapshot index is not committed")
	}
	if index <= n.snapshot.Index {
		return nil
	}
	if len(data) == 0 {
		return errors.New("empty snapshot")
	}

	term := n.termAt(index)

	log := make([]LogEntry, 0, 1+n.lastIndex()-index)
	log = append(log, LogEntry{Term: term})
	log = append(log, n.log[n.offset(index+1):]...)

	n.log = log
	n.snapshot = Snapshot{Index: index, Term: term, Data: bytes.Clone(data)}
	return n.persist()
}

func (n *Node) restoreSnapshot(snapshot Snapshot) error {
	if snapshot.Index < 0 {
		return errors.New("negative snapshot index")
	}
	if snapshot.Index == 0 {
		if snapshot.Term != 0 || len(snapshot.Data) != 0 {
			return errors.New("invalid empty snapshot")
		}
		return nil
	}
	if snapshot.Term == 0 {
		return errors.New("snapshot boundary has no term")
	}
	if len(snapshot.Data) == 0 {
		return errors.New("empty snapshot data")
	}

	n.snapshot = Snapshot{Index: snapshot.Index, Term: snapshot.Term, Data: bytes.Clone(snapshot.Data)}
	n.commitIndex = snapshot.Index
	n.lastApplied = snapshot.Index
	return nil
}

func (n *Node) InstallSnapshot(req *InstallSnapshotRequest, res *InstallSnapshotResponse) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	*res = InstallSnapshotResponse{Term: n.term}
	if err := n.updateTerm(req.Term); err != nil {
		return err
	}
	res.Term = n.term
	if req.Term < n.term {
		return nil
	}
	n.role = Follower
	n.resetElectionTimer()
	snapshot := req.Snapshot
	if snapshot.Index <= n.commitIndex {
		return nil
	}
	if snapshot.Index <= 0 || snapshot.Term == 0 || snapshot.Term > req.Term || len(snapshot.Data) == 0 {
		return errors.New("invalid snapshot metadata")
	}

	var suffix []LogEntry
	if snapshot.Index <= n.lastIndex() && n.termAt(snapshot.Index) == snapshot.Term {
		suffix = n.log[n.offset(snapshot.Index+1):]
	}
	log := make([]LogEntry, 0, 1+len(suffix))
	log = append(log, LogEntry{Term: snapshot.Term})
	log = append(log, suffix...)
	snapshot.Data = bytes.Clone(snapshot.Data)

	n.log = log
	n.snapshot = snapshot
	n.commitIndex = snapshot.Index
	if err := n.persist(); err != nil {
		return err
	}

	pending := snapshot
	n.pendingSnapshot = &pending
	n.notifyApply()
	return nil
}

func (n *Node) sendSnapshot(peer int, req InstallSnapshotRequest) {
	var res InstallSnapshotResponse
	if err := n.peers[peer].Call("Node.InstallSnapshot", &req, &res); err != nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.updateTerm(res.Term); err != nil {
		return
	}
	if n.role != Leader || n.term != req.Term || res.Term != req.Term {
		return
	}

	n.matchIndex[peer] = max(n.matchIndex[peer], req.Snapshot.Index)
	n.nextIndex[peer] = max(n.nextIndex[peer], n.matchIndex[peer]+1)
	n.advanceCommit()
}
