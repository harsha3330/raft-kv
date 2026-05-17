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
	Index int       `json:"index"`
	Term  int       `json:"term"`
	Op    Operation `json:"op"`
	Key   string    `json:"key"`
	Val   string    `json:"val"`
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

func (c *CommitLog) Replay(handler func(Command)) (int, error) {
	scanner := bufio.NewScanner(c.File)
	var lastIndex int
	for scanner.Scan() {
		line := scanner.Bytes()
		var cmd Command
		err := json.Unmarshal(line, &cmd)
		if err != nil {
			return 0, err
		}
		handler(cmd)
		lastIndex = cmd.Index
	}
	return lastIndex, scanner.Err()
}

func (c *CommitLog) ReadFrom(index int) ([]Command, error) {
	file, err := os.Open(c.File.Name())
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var entries []Command
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Bytes()
		var cmd Command
		err := json.Unmarshal(line, &cmd)
		if err != nil {
			return nil, err
		}
		if cmd.Index >= index {
			entries = append(entries, cmd)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return entries, nil
}
