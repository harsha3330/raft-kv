package store

import (
	"errors"
	"sync"
)

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrEdatatyKey  = errors.New("edataty key")
)

type Store struct {
	mu   sync.Mutex
	data map[string]string
}

func NewStore() *Store {
	return &Store{
		data: make(map[string]string),
	}
}

func (s *Store) Set(k, v string) error {
	if k == "" {
		return ErrEdatatyKey
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[k] = v
	return nil
}

func (s *Store) Delete(k string) error {
	if k == "" {
		return ErrEdatatyKey
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[k]; !ok {
		return ErrKeyNotFound
	}
	delete(s.data, k)
	return nil
}

func (s *Store) Get(k string) (string, error) {
	if k == "" {
		return "", ErrEdatatyKey
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	val, ok := s.data[k]
	if !ok {
		return "", ErrKeyNotFound
	}
	return val, nil
}
