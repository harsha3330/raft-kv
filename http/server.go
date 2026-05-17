package httpd

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/harsha3330/raft-kv/store"
	"github.com/harsha3330/raft-kv/wal"
)

type Node struct {
	Id       string
	Addr     string
	Peers    []string
	IsLeader bool
	Term     int
}

type Server struct {
	store     *store.Store
	commitLog *wal.CommitLog
	mux       *http.ServeMux
	addr      string
	logPath   string
	node      Node
	logger    *slog.Logger
	lastIndex int
}

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		rw := &responseWriter{
			ResponseWriter: w,
			statusCode:     http.StatusOK,
		}

		next.ServeHTTP(rw, r)

		log.Printf(
			"%s %s %d %s",
			r.Method,
			r.URL.Path,
			rw.statusCode,
			time.Since(start),
		)
	})
}

func (s *Server) Handler() http.Handler {
	return LoggingMiddleware(s.mux)
}

func NewServer(node Node, logPath string) (*Server, error) {
	st := store.NewStore()

	log, err := wal.NewWal(logPath)
	if err != nil {
		return nil, err
	}

	idx, err := log.Replay(func(cmd wal.Command) {
		switch cmd.Op {
		case wal.OpSet:
			st.Set(cmd.Key, cmd.Val)
		case wal.OpDelete:
			st.Delete(cmd.Key)
		}
	})
	if err != nil {
		return nil, err
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	s := &Server{
		store:     st,
		commitLog: log,
		logPath:   logPath,
		addr:      node.Addr,
		node:      node,
		mux:       http.NewServeMux(),
		logger:    logger,
		lastIndex: idx,
	}
	s.routes()
	return s, nil
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /key/{key}", s.GetKey)
	s.mux.HandleFunc("POST /key", s.SetKey)
	s.mux.HandleFunc("DELETE /key/{key}", s.DeleteKey)
	s.mux.HandleFunc("POST /replicate", s.ReplicateKey)
	s.mux.HandleFunc("GET /logs/from/{index}", s.GetLogsFrom)
	s.mux.HandleFunc("GET /logs", s.GetLastLogIndex)
	s.mux.HandleFunc("GET /health", s.HealthCheck)
}

func (s *Server) Start() error {
	s.logger.Info("server starting", "addr", s.addr)

	httpServer := http.Server{
		Addr:    s.addr,
		Handler: s.Handler(),
	}

	return httpServer.ListenAndServe()
}

type KeyResponse struct {
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
	Error string `json:"error,omitempty"`
}

type SetKeyRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	err := json.NewEncoder(w).Encode(data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Server) GetKey(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	val, err := s.store.Get(key)
	if err != nil {

		if errors.Is(err, store.ErrKeyNotFound) {
			writeJSON(w, http.StatusNotFound, KeyResponse{
				Key:   key,
				Error: err.Error(),
			})
			return
		}

		writeJSON(w, http.StatusInternalServerError, KeyResponse{
			Key:   key,
			Error: err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, KeyResponse{
		Key:   key,
		Value: val,
	})
}

func (s *Server) SetKey(w http.ResponseWriter, r *http.Request) {
	var req SetKeyRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	cmd := wal.Command{
		Op:    wal.OpSet,
		Key:   req.Key,
		Val:   req.Value,
		Term:  s.node.Term,
		Index: s.lastIndex + 1,
	}

	err = s.commitLog.Append(cmd)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.lastIndex++

	for _, peer := range s.node.Peers {
		err := s.replicateToPeer(peer, cmd)
		if err != nil {
			s.logger.Error("replication failed, syncing follower", "peer", peer, "err", err)
			err = s.syncFollower(peer)
			if err != nil {
				s.logger.Error("follower sync failed", "peer", peer, "err", err)
			}
			continue
		}
		s.logger.Info("replication successful", "peer", peer)
	}

	err = s.store.Set(req.Key, req.Value)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{
		"message": "key stored",
	})
}

func (s *Server) ReplicateKey(w http.ResponseWriter, r *http.Request) {
	var cmd wal.Command

	err := json.NewDecoder(r.Body).Decode(&cmd)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if cmd.Index != s.lastIndex+1 {
		http.Error(w, "invalid log index", http.StatusConflict)
		return
	}

	err = s.commitLog.Append(cmd)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.lastIndex++

	switch cmd.Op {
	case wal.OpSet:
		err = s.store.Set(cmd.Key, cmd.Val)
	case wal.OpDelete:
		err = s.store.Delete(cmd.Key)
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{
		"message": "key stored",
	})
}

func (s *Server) DeleteKey(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	cmd := wal.Command{
		Op:    wal.OpDelete,
		Key:   key,
		Term:  s.node.Term,
		Index: s.lastIndex + 1,
	}

	err := s.commitLog.Append(cmd)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.lastIndex++

	for _, peer := range s.node.Peers {
		err := s.replicateToPeer(peer, cmd)
		if err != nil {
			s.logger.Error("replication failed, syncing follower", "peer", peer, "err", err)
			err = s.syncFollower(peer)
			if err != nil {
				s.logger.Error("follower sync failed", "peer", peer, "err", err)
			}
			continue
		}
		s.logger.Info("replication successful", "peer", peer)
	}

	err = s.store.Delete(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"message": "key deleted",
	})
}

func (s *Server) HealthCheck(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
	})
}

func (s *Server) GetLastLogIndex(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]int{
		"last_index": s.lastIndex,
	})
}

func (s *Server) GetLogsFrom(w http.ResponseWriter, r *http.Request) {
	indexStr := r.PathValue("index")

	index, err := strconv.Atoi(indexStr)
	if err != nil {
		http.Error(w, "invalid index", http.StatusBadRequest)
		return
	}

	entries, err := s.commitLog.ReadFrom(index)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, entries)
}

func (s *Server) replicateToPeer(peer string, cmd wal.Command) error {
	url := fmt.Sprintf("%s/replicate", peer)
	payload, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	resp, err := http.Post(
		url,
		"application/json",
		bytes.NewBuffer(payload),
	)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("replication failed with status %d", resp.StatusCode)
	}
	return nil
}

type LogStatusResponse struct {
	LastIndex int `json:"last_index"`
}

func (s *Server) syncFollower(peer string) error {
	url := fmt.Sprintf("%s/logs", peer)

	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var status LogStatusResponse

	err = json.NewDecoder(resp.Body).Decode(&status)
	if err != nil {
		return err
	}

	entries, err := s.commitLog.ReadFrom(status.LastIndex + 1)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		err := s.replicateToPeer(peer, entry)
		if err != nil {
			return err
		}
	}

	return nil
}
