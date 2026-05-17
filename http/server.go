package httpd

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"

	"github.com/harsha3330/raft-kv/store"
	"github.com/harsha3330/raft-kv/wal"
)

type Server struct {
	store     *store.Store
	commitLog *wal.CommitLog
	mux       *http.ServeMux
	addr      string
	logPath   string
	nodeID    string
	logger    *slog.Logger
}

func NewServer(addr string, logPath string, nodeID string) (*Server, error) {
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
		addr:      addr,
		nodeID:    nodeID,
		mux:       http.NewServeMux(),
		logger:    logger,
	}

	s.routes()

	return s, nil
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /key/{key}", s.GetKey)
	s.mux.HandleFunc("POST /key", s.SetKey)
	s.mux.HandleFunc("DELETE /key/{key}", s.DeleteKey)
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
