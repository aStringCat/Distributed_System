package server

import (
	"encoding/json"
	"errors"
	"fmt"

	"system/internal/kv"
	"system/internal/raft"
)

type snapshotResult struct {
	Request RequestID      `json:"request"`
	Get     kv.GetResponse `json:"get"`
	Put     kv.PutResponse `json:"put"`
}

type stateSnapshot struct {
	Version   int                         `json:"version"`
	Index     int                         `json:"index"`
	KV        map[string]kv.SnapshotEntry `json:"kv"`
	Completed []snapshotResult            `json:"completed"`
}

func (s *Replicated) encodeSnapshot() ([]byte, error) {
	state := stateSnapshot{
		Version:   1,
		Index:     s.appliedIndex,
		KV:        s.store.Snapshot(),
		Completed: make([]snapshotResult, 0, len(s.completed)),
	}
	for id, res := range s.completed {
		state.Completed = append(state.Completed, snapshotResult{
			Request: id,
			Get:     res.get,
			Put:     res.put,
		})
	}
	return json.Marshal(state)
}

func (s *Replicated) restoreSnapshot(snapshot raft.Snapshot) error {
	if snapshot.Index <= s.appliedIndex {
		return nil
	}
	if len(snapshot.Data) == 0 {
		return errors.New("empty service snapshot")
	}
	var state stateSnapshot
	if err := json.Unmarshal(snapshot.Data, &state); err != nil {
		return fmt.Errorf("decode service snapshot: %w", err)
	}
	if state.Version != 1 {
		return errors.New("unsupported service snapshot version")
	}
	if state.Index <= 0 || state.Index != snapshot.Index {
		return errors.New("service snapshot index mismatch")
	}

	completed := make(map[RequestID]result, len(state.Completed))
	for _, sr := range state.Completed {
		if !sr.Request.valid() {
			return errors.New("invalid request ID in service snapshot")
		}
		if _, ok := completed[sr.Request]; ok {
			return errors.New("duplicate request ID in service snapshot")
		}
		if (sr.Get.Code == "") == (sr.Put.Code == "") {
			return errors.New("snapshot must contain exactly one response per request")
		}
		code := sr.Get.Code
		if code == "" {
			code = sr.Put.Code
		}
		switch code {
		case kv.OK, kv.NotFound, kv.InvalidArgument, kv.VersionConflict, kv.VersionExhausted:
		default:
			return errors.New("invalid response code in snapshot")
		}
		completed[sr.Request] = result{get: sr.Get, put: sr.Put}
	}
	store := kv.NewStore()
	if err := store.Restore(state.KV); err != nil {
		return fmt.Errorf("restore snapshot kv: %w", err)
	}

	s.store = store
	s.completed = completed
	s.appliedIndex = snapshot.Index
	s.snapshotIndex = snapshot.Index
	return nil
}

func (s *Replicated) applyMessage(msg raft.ApplyMessage) error {
	if msg.Snapshot != nil {
		return s.restoreSnapshot(*msg.Snapshot)
	}
	if msg.Index <= s.appliedIndex {
		return nil
	}
	if msg.Index != s.appliedIndex+1 {
		return errors.New("non-contiguous applied log")
	}
	if err := s.applyOperation(msg); err != nil {
		return err
	}
	s.appliedIndex = msg.Index
	return s.maybeSnapshot()
}

func (s *Replicated) maybeSnapshot() error {
	if s.snapshotEvery == 0 || s.appliedIndex-s.snapshotIndex < s.snapshotEvery {
		return nil
	}
	data, err := s.encodeSnapshot()
	if err != nil {
		return err
	}
	if err := s.node.Compact(s.appliedIndex, data); err != nil {
		return err
	}
	s.snapshotIndex = s.appliedIndex
	return nil
}
