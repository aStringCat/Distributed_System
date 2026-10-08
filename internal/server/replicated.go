package server

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"sync"

	"system/internal/kv"
	"system/internal/raft"
)

var ErrInvalidRequestID = errors.New("invalid request ID")
var ErrNotLeader = errors.New("not leader")
var ErrStopped = errors.New("server stopped")

type RequestID struct {
	ClientID string `json:"client_id"`
	Seq      uint64 `json:"seq"`
}

type operation struct {
	ID      string         `json:"id"`
	Request RequestID      `json:"request"`
	Get     *kv.GetRequest `json:"get,omitempty"`
	Put     *kv.PutRequest `json:"put,omitempty"`
}

type result struct {
	get kv.GetResponse
	put kv.PutResponse
}

type Replicated struct {
	node          *raft.Node
	store         *kv.Store
	mu            sync.Mutex
	pending       map[string]chan result
	completed     map[RequestID]result
	done          chan struct{}
	snapshotEvery int
	snapshotIndex int
	appliedIndex  int
}

func NewReplicated(node *raft.Node, snapshotEvery int) *Replicated {
	return &Replicated{
		node:          node,
		snapshotEvery: snapshotEvery,
		store:         kv.NewStore(),
		pending:       make(map[string]chan result),
		completed:     make(map[RequestID]result),
		done:          make(chan struct{}),
	}
}

func (id RequestID) valid() bool {
	return id.ClientID != "" && id.Seq != 0
}

func (s *Replicated) Run(ctx context.Context) error {
	if err := s.restoreSnapshot(s.node.CurrentSnapshot()); err != nil {
		close(s.done)
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	raftDone := make(chan struct{})
	go func() {
		defer close(raftDone)
		_ = s.node.Run(ctx)
	}()
	defer func() {
		cancel()
		<-raftDone
		close(s.done)
	}()

	for {
		select {
		case <-ctx.Done():
			return s.node.Err()
		case msg, ok := <-s.node.Applied():
			if !ok {
				return s.node.Err()
			}
			if err := s.applyMessage(msg); err != nil {
				return err
			}
		}
	}
}

func (s *Replicated) Get(ctx context.Context, id RequestID, req kv.GetRequest) (kv.GetResponse, error) {
	res, err := s.submitOperation(ctx, operation{Request: id, Get: &req})
	return res.get, err
}

func (s *Replicated) Put(ctx context.Context, id RequestID, req kv.PutRequest) (kv.PutResponse, error) {
	res, err := s.submitOperation(ctx, operation{Request: id, Put: &req})
	return res.put, err
}

func (s *Replicated) submitOperation(ctx context.Context, op operation) (result, error) {
	if err := ctx.Err(); err != nil {
		return result{}, err
	}

	select {
	case <-s.done:
		return result{}, s.stoppedError()
	default:
	}

	op.ID = rand.Text()
	data, err := json.Marshal(op)
	if err != nil {
		return result{}, err
	}

	finished := make(chan result, 1)
	s.mu.Lock()
	s.pending[op.ID] = finished
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.pending, op.ID)
		s.mu.Unlock()
	}()

	_, _, ok := s.node.Propose(string(data))
	if !ok {
		if err := s.node.Err(); err != nil {
			return result{}, err
		}
		return result{}, ErrNotLeader
	}

	select {
	case res := <-finished:
		return res, nil
	case <-ctx.Done():
		return result{}, ctx.Err()
	case <-s.done:
		return result{}, s.stoppedError()
	}
}

func (s *Replicated) stoppedError() error {
	if err := s.node.Err(); err != nil {
		return err
	}
	return ErrStopped
}

func (s *Replicated) applyOperation(msg raft.ApplyMessage) error {
	var op operation
	if err := json.Unmarshal([]byte(msg.Data), &op); err != nil {
		return err
	}

	if op.ID == "" {
		return errors.New("missing operation ID")
	}
	if !op.Request.valid() {
		return ErrInvalidRequestID
	}
	if (op.Get == nil) == (op.Put == nil) {
		return errors.New("expected exactly one operation")
	}

	var res result
	if cached, ok := s.completed[op.Request]; ok {
		res = cached
	} else {
		if op.Get != nil {
			res.get = s.store.Get(*op.Get)
		}
		if op.Put != nil {
			res.put = s.store.Put(*op.Put)
		}
		s.completed[op.Request] = res
	}

	s.mu.Lock()
	finished := s.pending[op.ID]
	s.mu.Unlock()
	select {
	case finished <- res:
	default:
	}
	return nil
}
