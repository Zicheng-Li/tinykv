package server

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"

	"tinykv/cluster"
	"tinykv/engine"
)

type Options struct {
	Cluster    *cluster.Membership
	PeerClient cluster.PeerClient
}

type Handler struct {
	db         *engine.DB
	cluster    *cluster.Membership
	peerClient cluster.PeerClient
}

func NewHandler(db *engine.DB, opts Options) http.Handler {
	handler := &Handler{
		db:         db,
		cluster:    opts.Cluster,
		peerClient: opts.PeerClient,
	}
	if handler.cluster != nil && handler.peerClient == nil {
		handler.peerClient = cluster.NewHTTPClient(nil)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", handler.handleHealth)
	mux.HandleFunc("/kv", handler.handleKV)
	mux.HandleFunc("/kv/", handler.handleKV)
	mux.HandleFunc("/cluster/membership", handler.handleMembership)
	mux.HandleFunc("/cluster/route/", handler.handleRoute)
	return mux
}

func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, "GET")
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (h *Handler) handleMembership(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, "GET")
		return
	}

	summary := cluster.Summary{Enabled: false}
	if h.cluster != nil {
		summary = h.cluster.Summary()
	}
	writeJSON(w, summary)
}

func (h *Handler) handleRoute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, "GET")
		return
	}
	if h.cluster == nil {
		http.Error(w, "cluster mode is not enabled", http.StatusNotFound)
		return
	}

	key, err := keyFromPath(r, "/cluster/route/")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, h.cluster.RouteForKey(key))
}

func (h *Handler) handleKV(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut, http.MethodGet, http.MethodDelete:
	default:
		writeMethodNotAllowed(w, "DELETE, GET, PUT")
		return
	}

	key, err := keyFromPath(r, "/kv/")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPut:
		h.handlePut(w, r, key)
	case http.MethodGet:
		h.handleGet(w, r, key)
	case http.MethodDelete:
		h.handleDelete(w, r, key)
	}
}

func (h *Handler) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	defer r.Body.Close()

	value, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read request body failed", http.StatusBadRequest)
		return
	}

	if h.cluster == nil {
		h.applyLocalPut(w, key, value)
		return
	}

	route := h.cluster.RouteForKey(key)
	if isReplicationRequest(r) {
		if !route.HasNode(h.cluster.Self().ID) {
			http.Error(w, "node is not a replica for key", http.StatusForbidden)
			return
		}
		h.applyLocalPut(w, key, value)
		return
	}

	if h.cluster.SelfRole(key) != cluster.RoleLeader {
		h.proxyToLeader(w, r, route.Leader, http.MethodPut, key, value)
		return
	}

	if err := h.db.Put(key, value); err != nil {
		writeDBError(w, err)
		return
	}
	if err := h.replicateToFollowers(r, route, http.MethodPut, key, value); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	if h.cluster == nil {
		h.applyLocalGet(w, key)
		return
	}

	role := h.cluster.SelfRole(key)
	switch role {
	case cluster.RoleLeader, cluster.RoleFollower:
		value, err := h.db.Get(key)
		if err == nil {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(value)
			return
		}
		if role == cluster.RoleFollower && errors.Is(err, engine.ErrKeyNotFound) {
			route := h.cluster.RouteForKey(key)
			h.proxyToLeader(w, r, route.Leader, http.MethodGet, key, nil)
			return
		}
		writeDBError(w, err)
	default:
		route := h.cluster.RouteForKey(key)
		h.proxyToLeader(w, r, route.Leader, http.MethodGet, key, nil)
	}
}

func (h *Handler) handleDelete(w http.ResponseWriter, r *http.Request, key string) {
	if h.cluster == nil {
		h.applyLocalDelete(w, key, false)
		return
	}

	route := h.cluster.RouteForKey(key)
	if isReplicationRequest(r) {
		if !route.HasNode(h.cluster.Self().ID) {
			http.Error(w, "node is not a replica for key", http.StatusForbidden)
			return
		}
		h.applyLocalDelete(w, key, true)
		return
	}

	if h.cluster.SelfRole(key) != cluster.RoleLeader {
		h.proxyToLeader(w, r, route.Leader, http.MethodDelete, key, nil)
		return
	}

	if err := h.db.Delete(key); err != nil {
		writeDBError(w, err)
		return
	}
	if err := h.replicateToFollowers(r, route, http.MethodDelete, key, nil); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) applyLocalPut(w http.ResponseWriter, key string, value []byte) {
	if err := h.db.Put(key, value); err != nil {
		writeDBError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) applyLocalGet(w http.ResponseWriter, key string) {
	value, err := h.db.Get(key)
	if err != nil {
		writeDBError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(value)
}

func (h *Handler) applyLocalDelete(w http.ResponseWriter, key string, ignoreMissing bool) {
	err := h.db.Delete(key)
	if ignoreMissing && errors.Is(err, engine.ErrKeyNotFound) {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err != nil {
		writeDBError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) replicateToFollowers(r *http.Request, route cluster.Route, method, key string, body []byte) error {
	for _, follower := range route.Followers() {
		resp, err := h.peerClient.Send(r.Context(), follower, method, key, body, true)
		if err != nil {
			log.Printf("replicate %s for key %q to %s failed: %v", method, key, follower.ID, err)
			return err
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			log.Printf("replicate %s for key %q to %s returned %d", method, key, follower.ID, resp.StatusCode)
			return errors.New("replication to follower failed")
		}
	}
	return nil
}

func (h *Handler) proxyToLeader(w http.ResponseWriter, r *http.Request, leader cluster.Node, method, key string, body []byte) {
	resp, err := h.peerClient.Send(r.Context(), leader, method, key, body, false)
	if err != nil {
		http.Error(w, "forward request to leader failed", http.StatusBadGateway)
		return
	}

	if contentType := resp.Header.Get("Content-Type"); contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	w.WriteHeader(resp.StatusCode)
	if len(resp.Body) > 0 {
		_, _ = w.Write(resp.Body)
	}
}

func keyFromPath(r *http.Request, prefix string) (string, error) {
	path := r.URL.EscapedPath()
	if path == strings.TrimSuffix(prefix, "/") || path == prefix {
		return "", engine.ErrEmptyKey
	}
	if !strings.HasPrefix(path, prefix) {
		return "", engine.ErrEmptyKey
	}

	key, err := url.PathUnescape(strings.TrimPrefix(path, prefix))
	if err != nil {
		return "", err
	}
	if key == "" {
		return "", engine.ErrEmptyKey
	}
	return key, nil
}

func isReplicationRequest(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get(cluster.HeaderReplication), "true")
}

func writeMethodNotAllowed(w http.ResponseWriter, allow string) {
	w.Header().Set("Allow", allow)
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

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(v); err != nil {
		http.Error(w, "encode json failed", http.StatusInternalServerError)
	}
}
