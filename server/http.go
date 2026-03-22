package server

import (
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"

	"tinykv/engine"
)

type Handler struct {
	db *engine.DB
}

func NewHandler(db *engine.DB) http.Handler {
	handler := &Handler{db: db}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", handler.handleHealth)
	mux.HandleFunc("/kv", handler.handleKV)
	mux.HandleFunc("/kv/", handler.handleKV)
	return mux
}

func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (h *Handler) handleKV(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut, http.MethodGet, http.MethodDelete:
	default:
		writeMethodNotAllowed(w)
		return
	}

	key, err := keyFromRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPut:
		h.handlePut(w, r, key)
	case http.MethodGet:
		h.handleGet(w, key)
	case http.MethodDelete:
		h.handleDelete(w, key)
	}
}

func (h *Handler) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	defer r.Body.Close()

	value, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read request body failed", http.StatusBadRequest)
		return
	}
	if err := h.db.Put(key, value); err != nil {
		writeDBError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) handleGet(w http.ResponseWriter, key string) {
	value, err := h.db.Get(key)
	if err != nil {
		writeDBError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(value)
}

func (h *Handler) handleDelete(w http.ResponseWriter, key string) {
	if err := h.db.Delete(key); err != nil {
		writeDBError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func keyFromRequest(r *http.Request) (string, error) {
	path := r.URL.EscapedPath()
	if path == "/kv" || path == "/kv/" {
		return "", engine.ErrEmptyKey
	}
	if !strings.HasPrefix(path, "/kv/") {
		return "", engine.ErrEmptyKey
	}

	key, err := url.PathUnescape(strings.TrimPrefix(path, "/kv/"))
	if err != nil {
		return "", err
	}
	if key == "" {
		return "", engine.ErrEmptyKey
	}
	return key, nil
}

func writeMethodNotAllowed(w http.ResponseWriter) {
	w.Header().Set("Allow", "DELETE, GET, PUT")
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}

func writeDBError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, engine.ErrEmptyKey):
		http.Error(w, err.Error(), http.StatusBadRequest)
	case errors.Is(err, engine.ErrKeyNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
	default:
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}
}
