package kv

import (
	"errors"
	"maps"
)

type SnapshotEntry struct {
	Value   string  `json:"value"`
	Version Version `json:"version"`
}

func (s *Store) Snapshot() map[string]SnapshotEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	return maps.Clone(s.data)
}

func (s *Store) Restore(data map[string]SnapshotEntry) error {
	for key, e := range data {
		if key == "" {
			return errors.New("empty key in snapshot")
		}
		if e.Version == 0 {
			return errors.New("zero version in snapshot")
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = maps.Clone(data)
	if s.data == nil {
		s.data = make(map[string]SnapshotEntry)
	}
	return nil
}
