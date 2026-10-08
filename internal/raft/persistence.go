package raft

import (
	"encoding/json"
	"errors"
	"fmt"

	"system/internal/storage"
)

type persistentState struct {
	Version  int        `json:"version"`
	Term     uint64     `json:"term"`
	VotedFor int        `json:"voted_for"`
	Log      []LogEntry `json:"log"`
	Snapshot Snapshot   `json:"snapshot"`
}

func OpenNode(id int, peers []Peer, dir string) (*Node, error) {
	store, err := storage.New(dir)
	if err != nil {
		return nil, err
	}
	data, err := store.Load()
	if err != nil {
		return nil, err
	}
	n := newNode(id, peers)
	n.store = store
	if err := n.restore(data); err != nil {
		return nil, fmt.Errorf("restore raft state: %w", err)
	}
	return n, nil
}

func (n *Node) Err() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.storageErr
}

func (n *Node) persist() error {
	if n.storageErr != nil {
		return n.storageErr
	}
	if err := n.saveState(); err != nil {
		n.storageErr = fmt.Errorf("persist raft state: %w", err)
		n.role = Follower
		close(n.failed)
		return n.storageErr
	}
	return nil
}

func (n *Node) saveState() error {
	state := persistentState{Version: 1, Term: n.term, VotedFor: n.votedFor, Log: n.log, Snapshot: n.snapshot}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return n.store.Save(data)
}

func (n *Node) restore(data []byte) error {
	if data == nil {
		return nil
	}
	var state persistentState
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	if state.Version != 1 {
		return errors.New("unsupported state version")
	}
	if state.VotedFor < -1 || state.VotedFor >= len(n.peers) {
		return errors.New("invalid votedFor")
	}
	if state.Snapshot.Term > state.Term {
		return errors.New("snapshot term exceeds current term")
	}
	if err := n.restoreSnapshot(state.Snapshot); err != nil {
		return err
	}
	if len(state.Log) == 0 || state.Log[0] != (LogEntry{Term: state.Snapshot.Term}) {
		return errors.New("invalid log sentinel")
	}
	for index := 1; index < len(state.Log); index++ {
		term := state.Log[index].Term
		if term == 0 || term > state.Term || term < state.Log[index-1].Term {
			return fmt.Errorf("invalid term at log index %d", index)
		}
	}
	n.term, n.votedFor, n.log = state.Term, state.VotedFor, state.Log
	return nil
}
