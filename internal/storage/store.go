package storage

import (
	"errors"
	"os"
	"path/filepath"
)

type Store struct {
	path string
}

func New(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return &Store{path: filepath.Join(dir, "raft.state.json")}, nil
}

func (s *Store) Load() ([]byte, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	return data, err
}

func (s *Store) Save(data []byte) error {
	dir := filepath.Dir(s.path)
	file, err := os.CreateTemp(dir, ".raft-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()

	if _, err := file.Write(data); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), s.path); err != nil {
		return err
	}

	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
