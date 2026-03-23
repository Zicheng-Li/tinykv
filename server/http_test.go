package server

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"tinykv/cluster"
	"tinykv/engine"
)

func TestStandaloneKVHandlerLifecycle(t *testing.T) {
	handler := newStandaloneHandler(t)

	putResp, _ := performHandlerRequest(t, handler, http.MethodPut, "/kv/name", "tinykv")
	if putResp.Code != http.StatusNoContent {
		t.Fatalf("unexpected PUT status: %d", putResp.Code)
	}

	getResp, getBody := performHandlerRequest(t, handler, http.MethodGet, "/kv/name", "")
	if getResp.Code != http.StatusOK {
		t.Fatalf("unexpected GET status: %d", getResp.Code)
	}
	if string(getBody) != "tinykv" {
		t.Fatalf("unexpected GET body: %q", string(getBody))
	}

	delResp, _ := performHandlerRequest(t, handler, http.MethodDelete, "/kv/name", "")
	if delResp.Code != http.StatusNoContent {
		t.Fatalf("unexpected DELETE status: %d", delResp.Code)
	}
}

func TestStandaloneEncodedKey(t *testing.T) {
	handler := newStandaloneHandler(t)

	putResp, _ := performHandlerRequest(t, handler, http.MethodPut, "/kv/user%2F1", "value")
	if putResp.Code != http.StatusNoContent {
		t.Fatalf("unexpected PUT status: %d", putResp.Code)
	}

	getResp, getBody := performHandlerRequest(t, handler, http.MethodGet, "/kv/user%2F1", "")
	if getResp.Code != http.StatusOK {
		t.Fatalf("unexpected GET status: %d", getResp.Code)
	}
	if string(getBody) != "value" {
		t.Fatalf("unexpected GET body: %q", string(getBody))
	}
}

func TestKVHandlerBadRequest(t *testing.T) {
	handler := newStandaloneHandler(t)

	resp, _ := performHandlerRequest(t, handler, http.MethodPut, "/kv", "")
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", resp.Code)
	}
}

func TestHealthz(t *testing.T) {
	handler := newStandaloneHandler(t)

	resp, body := performHandlerRequest(t, handler, http.MethodGet, "/healthz", "")
	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", resp.Code)
	}
	if string(body) != "ok" {
		t.Fatalf("unexpected body: %q", string(body))
	}
}

func TestSnapshotAndRestoreEndpoints(t *testing.T) {
	handler := newStandaloneHandler(t)

	putResp, _ := performHandlerRequest(t, handler, http.MethodPut, "/kv/name", "tinykv")
	if putResp.Code != http.StatusNoContent {
		t.Fatalf("unexpected PUT status: %d", putResp.Code)
	}
	putResp, _ = performHandlerRequest(t, handler, http.MethodPut, "/kv/color", "blue")
	if putResp.Code != http.StatusNoContent {
		t.Fatalf("unexpected PUT status: %d", putResp.Code)
	}

	snapshotResp, snapshotBody := performHandlerRequestBytes(t, handler, http.MethodGet, "/snapshot", nil)
	if snapshotResp.Code != http.StatusOK {
		t.Fatalf("unexpected snapshot status: %d", snapshotResp.Code)
	}
	if !strings.Contains(snapshotResp.Header().Get("Content-Type"), "application/octet-stream") {
		t.Fatalf("unexpected snapshot content type: %q", snapshotResp.Header().Get("Content-Type"))
	}

	putResp, _ = performHandlerRequest(t, handler, http.MethodPut, "/kv/name", "changed")
	if putResp.Code != http.StatusNoContent {
		t.Fatalf("unexpected overwrite status: %d", putResp.Code)
	}
	delResp, _ := performHandlerRequest(t, handler, http.MethodDelete, "/kv/color", "")
	if delResp.Code != http.StatusNoContent {
		t.Fatalf("unexpected delete status: %d", delResp.Code)
	}

	restoreResp, _ := performHandlerRequestBytes(t, handler, http.MethodPost, "/restore", snapshotBody)
	if restoreResp.Code != http.StatusNoContent {
		t.Fatalf("unexpected restore status: %d", restoreResp.Code)
	}

	getResp, getBody := performHandlerRequest(t, handler, http.MethodGet, "/kv/name", "")
	if getResp.Code != http.StatusOK {
		t.Fatalf("unexpected restored GET status: %d", getResp.Code)
	}
	if string(getBody) != "tinykv" {
		t.Fatalf("unexpected restored value: %q", string(getBody))
	}

	getResp, getBody = performHandlerRequest(t, handler, http.MethodGet, "/kv/color", "")
	if getResp.Code != http.StatusOK {
		t.Fatalf("unexpected restored color GET status: %d", getResp.Code)
	}
	if string(getBody) != "blue" {
		t.Fatalf("unexpected restored color value: %q", string(getBody))
	}
}

func TestRestoreEndpointRejectsInvalidSnapshot(t *testing.T) {
	handler := newStandaloneHandler(t)

	resp, body := performHandlerRequest(t, handler, http.MethodPost, "/restore", "not-a-snapshot")
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", resp.Code)
	}
	if !strings.Contains(string(body), "invalid snapshot") {
		t.Fatalf("unexpected error body: %q", string(body))
	}
}

func TestClusterProxyWriteToLeader(t *testing.T) {
	fixture := newClusterFixture(t, 1)

	resp, _ := performHTTP(t, http.MethodPut, fixture.url("node-b")+"/kv/name", "tinykv")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("unexpected PUT status: %d", resp.StatusCode)
	}

	v, err := fixture.dbs["node-a"].Get("name")
	if err != nil {
		t.Fatalf("leader should store value: %v", err)
	}
	if string(v) != "tinykv" {
		t.Fatalf("unexpected leader value: %q", string(v))
	}

	_, err = fixture.dbs["node-b"].Get("name")
	if !errors.Is(err, engine.ErrKeyNotFound) {
		t.Fatalf("non-replica node should not store value, got: %v", err)
	}

	getResp, getBody := performHTTP(t, http.MethodGet, fixture.url("node-b")+"/kv/name", "")
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected proxied GET status: %d", getResp.StatusCode)
	}
	if string(getBody) != "tinykv" {
		t.Fatalf("unexpected proxied GET body: %q", string(getBody))
	}
}

func TestClusterReplicatesFollowers(t *testing.T) {
	fixture := newClusterFixture(t, 2)

	putResp, _ := performHTTP(t, http.MethodPut, fixture.url("node-a")+"/kv/name", "tinykv")
	if putResp.StatusCode != http.StatusNoContent {
		t.Fatalf("unexpected PUT status: %d", putResp.StatusCode)
	}

	followerValue, err := fixture.dbs["node-b"].Get("name")
	if err != nil {
		t.Fatalf("follower should have replicated value: %v", err)
	}
	if string(followerValue) != "tinykv" {
		t.Fatalf("unexpected follower value: %q", string(followerValue))
	}

	delResp, _ := performHTTP(t, http.MethodDelete, fixture.url("node-b")+"/kv/name", "")
	if delResp.StatusCode != http.StatusNoContent {
		t.Fatalf("unexpected DELETE status: %d", delResp.StatusCode)
	}

	_, err = fixture.dbs["node-a"].Get("name")
	if !errors.Is(err, engine.ErrKeyNotFound) {
		t.Fatalf("leader value should be deleted, got: %v", err)
	}
	_, err = fixture.dbs["node-b"].Get("name")
	if !errors.Is(err, engine.ErrKeyNotFound) {
		t.Fatalf("follower value should be deleted, got: %v", err)
	}
}

func TestClusterDebugEndpoints(t *testing.T) {
	fixture := newClusterFixture(t, 2)

	membershipResp, membershipBody := performHTTP(t, http.MethodGet, fixture.url("node-a")+"/cluster/membership", "")
	if membershipResp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected membership status: %d", membershipResp.StatusCode)
	}
	if !strings.Contains(string(membershipBody), "\"enabled\": true") {
		t.Fatalf("membership response should indicate cluster mode: %s", string(membershipBody))
	}

	routeResp, routeBody := performHTTP(t, http.MethodGet, fixture.url("node-a")+"/cluster/route/name", "")
	if routeResp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected route status: %d", routeResp.StatusCode)
	}
	if !strings.Contains(string(routeBody), "\"leader\"") {
		t.Fatalf("route response should include leader info: %s", string(routeBody))
	}
}

func newStandaloneHandler(t *testing.T) http.Handler {
	t.Helper()

	path := filepath.Join(t.TempDir(), "tinykv-http.data")
	db, err := engine.Open(engine.DefaultOptions(path))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	return NewHandler(db, Options{})
}

type clusterFixture struct {
	servers map[string]*httptest.Server
	dbs     map[string]*engine.DB
}

func newClusterFixture(t *testing.T, replicationFactor int) *clusterFixture {
	t.Helper()

	muxes := map[string]*http.ServeMux{
		"node-a": http.NewServeMux(),
		"node-b": http.NewServeMux(),
	}
	servers := map[string]*httptest.Server{
		"node-a": httptest.NewServer(muxes["node-a"]),
		"node-b": httptest.NewServer(muxes["node-b"]),
	}
	t.Cleanup(func() {
		for _, server := range servers {
			server.Close()
		}
	})

	nodes := []cluster.Node{
		{ID: "node-a", Address: servers["node-a"].URL},
		{ID: "node-b", Address: servers["node-b"].URL},
	}

	dbs := make(map[string]*engine.DB, len(nodes))
	t.Cleanup(func() {
		for _, db := range dbs {
			_ = db.Close()
		}
	})

	for _, node := range nodes {
		path := filepath.Join(t.TempDir(), node.ID+".data")
		db, err := engine.Open(engine.DefaultOptions(path))
		if err != nil {
			t.Fatalf("open db for %s: %v", node.ID, err)
		}
		dbs[node.ID] = db

		membership, err := cluster.NewMembership(cluster.Config{
			SelfID:            node.ID,
			Nodes:             nodes,
			ShardCount:        1,
			ReplicationFactor: replicationFactor,
		})
		if err != nil {
			t.Fatalf("new membership for %s: %v", node.ID, err)
		}

		muxes[node.ID].Handle("/", NewHandler(db, Options{
			Cluster:    membership,
			PeerClient: cluster.NewHTTPClient(nil),
		}))
	}

	return &clusterFixture{
		servers: servers,
		dbs:     dbs,
	}
}

func (f *clusterFixture) url(nodeID string) string {
	return f.servers[nodeID].URL
}

func performHandlerRequest(t *testing.T, handler http.Handler, method, path, body string) (*httptest.ResponseRecorder, []byte) {
	t.Helper()
	return performHandlerRequestBytes(t, handler, method, path, []byte(body))
}

func performHandlerRequestBytes(t *testing.T, handler http.Handler, method, path string, body []byte) (*httptest.ResponseRecorder, []byte) {
	t.Helper()

	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read handler body: %v", err)
	}
	return resp, payload
}

func performHTTP(t *testing.T, method, target, body string) (*http.Response, []byte) {
	t.Helper()

	req, err := http.NewRequest(method, target, bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("new http request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do http request: %v", err)
	}
	t.Cleanup(func() {
		_ = resp.Body.Close()
	})

	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read http body: %v", err)
	}
	return resp, payload
}
