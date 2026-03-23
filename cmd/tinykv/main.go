package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"tinykv/cluster"
	"tinykv/engine"
	"tinykv/server"
)

func main() {
	addr := flag.String("addr", ":8080", "http listen address")
	path := flag.String("data", filepath.Join(".", "tinykv.data"), "path to the TinyKV data file")
	cacheCap := flag.Int("cache", 1024, "LRU cache capacity")
	syncOnWrite := flag.Bool("sync-on-write", false, "sync WAL on every write")

	nodeID := flag.String("node-id", "", "local node id for clustered mode")
	peers := flag.String("peers", "", "cluster nodes in the form id=http://host:port,id2=http://host:port")
	shards := flag.Int("shards", 16, "number of logical shards in clustered mode")
	replicas := flag.Int("replicas", 1, "replication factor in clustered mode")

	flag.Parse()

	opts := engine.DefaultOptions(*path)
	opts.CacheCapacity = *cacheCap
	opts.SyncOnWrite = *syncOnWrite

	db, err := engine.Open(opts)
	if err != nil {
		log.Fatalf("open db failed: %v", err)
	}
	defer db.Close()

	handlerOpts := server.Options{}
	if strings.TrimSpace(*peers) != "" {
		nodes, err := parseClusterNodes(*peers)
		if err != nil {
			log.Fatalf("parse peers failed: %v", err)
		}
		membership, err := cluster.NewMembership(cluster.Config{
			SelfID:            strings.TrimSpace(*nodeID),
			Nodes:             nodes,
			ShardCount:        *shards,
			ReplicationFactor: *replicas,
		})
		if err != nil {
			log.Fatalf("build cluster membership failed: %v", err)
		}
		handlerOpts.Cluster = membership
		handlerOpts.PeerClient = cluster.NewHTTPClient(&http.Client{Timeout: 3 * time.Second})

		summary := membership.Summary()
		log.Printf(
			"cluster mode enabled: node=%s shards=%d replicas=%d members=%d",
			summary.Self.ID,
			summary.ShardCount,
			summary.ReplicationFactor,
			len(summary.Nodes),
		)
	}

	httpServer := &http.Server{
		Addr:              *addr,
		Handler:           server.NewHandler(db, handlerOpts),
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("tinykv listening on %s", *addr)

	shutdownCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		<-shutdownCtx.Done()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := httpServer.Shutdown(ctx); err != nil {
			log.Printf("http shutdown failed: %v", err)
		}
	}()

	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("http server failed: %v", err)
	}
}

func parseClusterNodes(raw string) ([]cluster.Node, error) {
	parts := strings.Split(raw, ",")
	nodes := make([]cluster.Node, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		pair := strings.SplitN(part, "=", 2)
		if len(pair) != 2 {
			return nil, fmt.Errorf("invalid peer %q", part)
		}

		node := cluster.Node{
			ID:      strings.TrimSpace(pair[0]),
			Address: strings.TrimSpace(pair[1]),
		}
		nodes = append(nodes, node)
	}

	if len(nodes) == 0 {
		return nil, fmt.Errorf("at least one peer is required")
	}
	return nodes, nil
}
