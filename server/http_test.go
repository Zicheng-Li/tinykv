package server

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"tinykv/engine"
)

func TestKVHandlerLifecycle(t *testing.T) {
	handler := newTestHandler(t)

	putReq := httptest.NewRequest(http.MethodPut, "/kv/name", bytes.NewBufferString("tinykv"))
	putResp := httptest.NewRecorder()
	handler.ServeHTTP(putResp, putReq)
	if putResp.Code != http.StatusNoContent {
		t.Fatalf("unexpected PUT status: %d", putResp.Code)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/kv/name", nil)
	getResp := httptest.NewRecorder()
	handler.ServeHTTP(getResp, getReq)
	if getResp.Code != http.StatusOK {
		t.Fatalf("unexpected GET status: %d", getResp.Code)
	}
	body, err := io.ReadAll(getResp.Body)
	if err != nil {
		t.Fatalf("read GET body: %v", err)
	}
	if string(body) != "tinykv" {
		t.Fatalf("unexpected GET body: %q", string(body))
	}

	delReq := httptest.NewRequest(http.MethodDelete, "/kv/name", nil)
	delResp := httptest.NewRecorder()
	handler.ServeHTTP(delResp, delReq)
	if delResp.Code != http.StatusNoContent {
		t.Fatalf("unexpected DELETE status: %d", delResp.Code)
	}

	missReq := httptest.NewRequest(http.MethodGet, "/kv/name", nil)
	missResp := httptest.NewRecorder()
	handler.ServeHTTP(missResp, missReq)
	if missResp.Code != http.StatusNotFound {
		t.Fatalf("unexpected status after delete: %d", missResp.Code)
	}
}

func TestKVHandlerEncodedKey(t *testing.T) {
	handler := newTestHandler(t)

	putReq := httptest.NewRequest(http.MethodPut, "/kv/user%2F1", bytes.NewBufferString("value"))
	putResp := httptest.NewRecorder()
	handler.ServeHTTP(putResp, putReq)
	if putResp.Code != http.StatusNoContent {
		t.Fatalf("unexpected PUT status: %d", putResp.Code)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/kv/user%2F1", nil)
	getResp := httptest.NewRecorder()
	handler.ServeHTTP(getResp, getReq)
	if getResp.Code != http.StatusOK {
		t.Fatalf("unexpected GET status: %d", getResp.Code)
	}
	if getResp.Body.String() != "value" {
		t.Fatalf("unexpected GET body: %q", getResp.Body.String())
	}
}

func TestKVHandlerBadRequest(t *testing.T) {
	handler := newTestHandler(t)

	req := httptest.NewRequest(http.MethodPut, "/kv", bytes.NewBuffer(nil))
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", resp.Code)
	}
}

func TestHealthz(t *testing.T) {
	handler := newTestHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", resp.Code)
	}
	if resp.Body.String() != "ok" {
		t.Fatalf("unexpected body: %q", resp.Body.String())
	}
}

func newTestHandler(t *testing.T) http.Handler {
	t.Helper()

	path := filepath.Join(t.TempDir(), "tinykv-http.data")
	db, err := engine.Open(engine.DefaultOptions(path))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	return NewHandler(db)
}
