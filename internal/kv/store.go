package kv

import "sync"

type Store struct {
	mu   sync.Mutex
	data map[string]SnapshotEntry
}

func NewStore() *Store {
	return &Store{data: make(map[string]SnapshotEntry)}
}

func (s *Store) Get(req GetRequest) GetResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	if req.Key == "" {
		return GetResponse{Code: InvalidArgument}
	}
	e, ok := s.data[req.Key]
	if !ok {
		return GetResponse{Code: NotFound}
	}
	return GetResponse{Code: OK, Value: e.Value, Version: e.Version}
}

func (s *Store) Put(req PutRequest) PutResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	if req.Key == "" {
		return PutResponse{Code: InvalidArgument}
	}
	e, ok := s.data[req.Key]
	if !ok && req.Version != 0 {
		return PutResponse{Code: NotFound}
	}
	if req.Version != e.Version {
		return PutResponse{Code: VersionConflict, Version: e.Version}
	}
	if e.Version == 1<<64-1 {
		return PutResponse{Code: VersionExhausted}
	}
	s.data[req.Key] = SnapshotEntry{Value: req.Value, Version: req.Version + 1}
	return PutResponse{Code: OK, Version: req.Version + 1}
}
