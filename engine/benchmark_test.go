package engine

import (
	"fmt"
	"path/filepath"
	"testing"
)

func BenchmarkPut(b *testing.B) {
	db, cleanup := openBenchDB(b)
	defer cleanup()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("k-%d", i)
		if err := db.Put(key, []byte("value")); err != nil {
			b.Fatalf("put: %v", err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	db, cleanup := openBenchDB(b)
	defer cleanup()

	const preload = 10000
	for i := 0; i < preload; i++ {
		key := fmt.Sprintf("k-%d", i)
		if err := db.Put(key, []byte("value")); err != nil {
			b.Fatalf("preload put: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("k-%d", i%preload)
		if _, err := db.Get(key); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}

func BenchmarkMixedReadWrite80_20(b *testing.B) {
	db, cleanup := openBenchDB(b)
	defer cleanup()

	const preload = 5000
	for i := 0; i < preload; i++ {
		key := fmt.Sprintf("k-%d", i)
		if err := db.Put(key, []byte("value")); err != nil {
			b.Fatalf("preload put: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%5 == 0 {
			key := fmt.Sprintf("k-new-%d", i)
			if err := db.Put(key, []byte("value")); err != nil {
				b.Fatalf("mixed put: %v", err)
			}
			continue
		}
		key := fmt.Sprintf("k-%d", i%preload)
		if _, err := db.Get(key); err != nil {
			b.Fatalf("mixed get: %v", err)
		}
	}
}

func openBenchDB(b *testing.B) (*DB, func()) {
	b.Helper()
	path := filepath.Join(b.TempDir(), "bench.log")
	opts := DefaultOptions(path)
	opts.CacheCapacity = 2048

	db, err := Open(opts)
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	return db, func() {
		_ = db.Close()
	}
}

