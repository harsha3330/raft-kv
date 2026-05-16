package wal

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
)

type Operation string

const (
	OpSet    Operation = "SET"
	OpDelete Operation = "DELETE"
)

type Command struct {
	Op  Operation `json:"op"`
	Key string    `json:"key"`
	Val string    `json:"val"`
}

type CommitLog struct {
	mu   sync.Mutex
	File *os.File
}

func NewWal(path string) (*CommitLog, error) {
	file, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}

	return &CommitLog{
		File: file,
	}, nil
}

func (c *CommitLog) Append(cmd Command) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	b, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	b = append(b, '\n')

	if _, err := c.File.Write(b); err != nil {
		return err
	}
	return c.File.Sync()
}

func (c *CommitLog) Replay(handler func(Command)) error {
	scanner := bufio.NewScanner(c.File)
	for scanner.Scan() {
		line := scanner.Bytes()
		var cmd Command
		err := json.Unmarshal(line, &cmd)
		if err != nil {
			return err
		}
		handler(cmd)
	}
	return scanner.Err()
}
