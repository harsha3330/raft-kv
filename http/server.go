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
	"time"

	"github.com/harsha3330/raft-kv/store"
	"github.com/harsha3330/raft-kv/wal"
)

type Node struct {
	Id       string
	Addr     string
	Peers    []string
	IsLeader bool
}

type Server struct {
	store     *store.Store
	commitLog *wal.CommitLog
	mux       *http.ServeMux
	addr      string
	logPath   string
	node      Node
	logger    *slog.Logger
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

	err = log.Replay(func(cmd wal.Command) {
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
	}
	s.Handler()
	s.routes()

	return s, nil
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /key/{key}", s.GetKey)
	s.mux.HandleFunc("POST /key", s.SetKey)
	s.mux.HandleFunc("DELETE /key/{key}", s.DeleteKey)
	s.mux.HandleFunc("POST /replicate", s.ReplicateKey)
	s.mux.HandleFunc("GET /health", s.HealthCheck)
}

func (s *Server) Start() error {
	s.logger.Info("server starting", "addr", s.addr)

	httpServer := http.Server{
		Addr:    s.addr,
		Handler: s.mux,
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
		Op:  wal.OpSet,
		Key: req.Key,
		Val: req.Value,
	}

	err = s.commitLog.Append(cmd)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	for _, peer := range s.node.Peers {
		url := fmt.Sprintf("%s/replicate", peer)
		payload, _ := json.Marshal(req)
		_, err := http.Post(url, "application/json", bytes.NewBuffer(payload))
		if err != nil {
			s.logger.Error("Error Replicating request for peer", "addr", peer, "err", err.Error())
		} else {
			s.logger.Info("Replication done for peer", "addr", peer)
		}
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
	var req SetKeyRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	cmd := wal.Command{
		Op:  wal.OpSet,
		Key: req.Key,
		Val: req.Value,
	}

	err = s.commitLog.Append(cmd)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
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

func (s *Server) DeleteKey(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	cmd := wal.Command{
		Op:  wal.OpDelete,
		Key: key,
	}

	err := s.commitLog.Append(cmd)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
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
