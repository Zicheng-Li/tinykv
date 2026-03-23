package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const HeaderReplication = "X-TinyKV-Replication"

type PeerResponse struct {
	StatusCode int
	Body       []byte
	Header     http.Header
}

type PeerClient interface {
	Send(ctx context.Context, node Node, method, key string, body []byte, internal bool) (PeerResponse, error)
}

type HTTPClient struct {
	client *http.Client
}

func NewHTTPClient(client *http.Client) *HTTPClient {
	if client == nil {
		client = &http.Client{Timeout: 3 * time.Second}
	}
	return &HTTPClient{client: client}
}

func (c *HTTPClient) Send(ctx context.Context, node Node, method, key string, body []byte, internal bool) (PeerResponse, error) {
	target := strings.TrimRight(node.Address, "/") + "/kv/" + url.PathEscape(key)
	req, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(body))
	if err != nil {
		return PeerResponse{}, fmt.Errorf("build peer request: %w", err)
	}
	if internal {
		req.Header.Set(HeaderReplication, "true")
	}
	if method == http.MethodPut {
		req.Header.Set("Content-Type", "application/octet-stream")
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return PeerResponse{}, fmt.Errorf("send peer request to %s: %w", node.ID, err)
	}
	defer resp.Body.Close()

	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return PeerResponse{}, fmt.Errorf("read peer response from %s: %w", node.ID, err)
	}

	return PeerResponse{
		StatusCode: resp.StatusCode,
		Body:       payload,
		Header:     resp.Header.Clone(),
	}, nil
}
