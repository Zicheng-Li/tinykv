package main

import (
	"fmt"
	"os"
	"path/filepath"

	"tinykv/engine"
)

func main() {
	path := filepath.Join(".", "tinykv.data")
	opts := engine.DefaultOptions(path)
	opts.CacheCapacity = 1024

	db, err := engine.Open(opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open db failed: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := db.Put("hello", []byte("world")); err != nil {
		fmt.Fprintf(os.Stderr, "put failed: %v\n", err)
		os.Exit(1)
	}
	val, err := db.Get("hello")
	if err != nil {
		fmt.Fprintf(os.Stderr, "get failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("GET hello => %s\n", string(val))

	if err := db.Delete("hello"); err != nil {
		fmt.Fprintf(os.Stderr, "delete failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("DELETE hello success")
}

