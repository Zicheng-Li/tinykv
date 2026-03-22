package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"tinykv/engine"
	"tinykv/server"
)

func main() {
	addr := flag.String("addr", ":8080", "http listen address")
	path := flag.String("data", filepath.Join(".", "tinykv.data"), "path to the TinyKV data file")
	cacheCap := flag.Int("cache", 1024, "LRU cache capacity")
	syncOnWrite := flag.Bool("sync-on-write", false, "sync WAL on every write")
	flag.Parse()

	opts := engine.DefaultOptions(*path)
	opts.CacheCapacity = *cacheCap
	opts.SyncOnWrite = *syncOnWrite

	db, err := engine.Open(opts)
	if err != nil {
		log.Fatalf("open db failed: %v", err)
	}
	defer db.Close()

	httpServer := &http.Server{
		Addr:              *addr,
		Handler:           server.NewHandler(db),
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
