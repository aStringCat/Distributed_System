package raft

import (
	"sync"
	"time"

	"system/internal/storage"
)

type Peer interface {
	Call(method string, req, res any) error
	Close() error
}

type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

type VoteRequest struct {
	Term         uint64
	CandidateID  int
	LastLogIndex int
	LastLogTerm  uint64
}

type VoteResponse struct {
	Term    uint64
	Granted bool
}

type Node struct {
	mu              sync.Mutex
	id              int
	peers           []Peer
	role            Role
	term            uint64
	votedFor        int
	log             []LogEntry
	snapshot        Snapshot
	pendingSnapshot *Snapshot

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	applyCh     chan ApplyMessage
	applyReady  chan struct{}
	store       *storage.Store
	storageErr  error
	failed      chan struct{}

	electionDeadline time.Time
	nextHeartbeat    time.Time
}

func newNode(id int, peers []Peer) *Node {
	n := &Node{
		id:         id,
		peers:      append([]Peer(nil), peers...),
		role:       Follower,
		votedFor:   -1,
		log:        []LogEntry{{}},
		applyCh:    make(chan ApplyMessage),
		applyReady: make(chan struct{}, 1),
		failed:     make(chan struct{}),
	}
	n.resetElectionTimer()
	return n
}

func (n *Node) startElection() {
	if n.storageErr != nil {
		return
	}
	n.role = Candidate
	n.votedFor = n.id
	n.term++
	if err := n.persist(); err != nil {
		return
	}
	n.resetElectionTimer()
	votes := 1
	majority := len(n.peers)/2 + 1
	if votes >= majority {
		n.becomeLeader()
		return
	}

	last := n.lastIndex()
	req := VoteRequest{
		Term: n.term, CandidateID: n.id,
		LastLogIndex: last, LastLogTerm: n.termAt(last),
	}
	for peer := range n.peers {
		if peer == n.id || n.peers[peer] == nil {
			continue
		}
		go func(peer int) {
			var res VoteResponse
			if err := n.peers[peer].Call("Node.RequestVote", &req, &res); err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()
			if err := n.updateTerm(res.Term); err != nil {
				return
			}
			if n.role != Candidate || n.term != req.Term || res.Term != req.Term || !res.Granted {
				return
			}
			votes++
			if votes >= majority {
				n.becomeLeader()
			}
		}(peer)
	}
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.resetReplication()
	n.nextHeartbeat = time.Now()
}

func (n *Node) RequestVote(req *VoteRequest, res *VoteResponse) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	*res = VoteResponse{Term: n.term}
	if err := n.updateTerm(req.Term); err != nil {
		return err
	}
	res.Term = n.term
	if req.Term < n.term || (n.votedFor != -1 && n.votedFor != req.CandidateID) {
		return nil
	}
	last := n.lastIndex()
	if n.termAt(last) > req.LastLogTerm || (n.termAt(last) == req.LastLogTerm && last > req.LastLogIndex) {
		return nil
	}
	if n.votedFor != req.CandidateID {
		n.votedFor = req.CandidateID
		if err := n.persist(); err != nil {
			return err
		}
	}
	n.resetElectionTimer()
	res.Granted = true
	return nil
}

func (n *Node) updateTerm(term uint64) error {
	if n.storageErr != nil {
		return n.storageErr
	}
	if term <= n.term {
		return nil
	}
	n.term = term
	n.role = Follower
	n.votedFor = -1
	return n.persist()
}
