package engine

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestPutGetDelete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.log")
	db, err := Open(DefaultOptions(path))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	if err := db.Put("name", []byte("tinykv")); err != nil {
		t.Fatalf("put: %v", err)
	}
	v, err := db.Get("name")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(v) != "tinykv" {
		t.Fatalf("unexpected value: %q", string(v))
	}

	if err := db.Delete("name"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	_, err = db.Get("name")
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("want ErrKeyNotFound, got: %v", err)
	}
}

func TestRecoveryAfterReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.log")

	db, err := Open(DefaultOptions(path))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := db.Put("k1", []byte("v1")); err != nil {
		t.Fatalf("put k1: %v", err)
	}
	if err := db.Put("k2", []byte("v2")); err != nil {
		t.Fatalf("put k2: %v", err)
	}
	if err := db.Delete("k1"); err != nil {
		t.Fatalf("delete k1: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	db2, err := Open(DefaultOptions(path))
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer db2.Close()

	_, err = db2.Get("k1")
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("k1 should be deleted, got: %v", err)
	}
	v, err := db2.Get("k2")
	if err != nil {
		t.Fatalf("get k2: %v", err)
	}
	if string(v) != "v2" {
		t.Fatalf("unexpected k2 value: %q", string(v))
	}
}

func TestCompact(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.log")
	db, err := Open(DefaultOptions(path))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	if err := db.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put("a", []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put("b", []byte("3")); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete("b"); err != nil {
		t.Fatal(err)
	}

	before, err := db.logFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	after, err := db.logFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() >= before.Size() {
		t.Fatalf("expected compacted file smaller, before=%d after=%d", before.Size(), after.Size())
	}

	v, err := db.Get("a")
	if err != nil {
		t.Fatalf("get a: %v", err)
	}
	if string(v) != "2" {
		t.Fatalf("unexpected value for a: %q", string(v))
	}
	_, err = db.Get("b")
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("b should not exist after compact, got: %v", err)
	}
}

func TestAppendAfterReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.log")

	db, err := Open(DefaultOptions(path))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := db.Put("k1", []byte("v1")); err != nil {
		t.Fatalf("put k1: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	db, err = Open(DefaultOptions(path))
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer db.Close()

	if err := db.Put("k2", []byte("v2")); err != nil {
		t.Fatalf("put k2 after reopen: %v", err)
	}

	v, err := db.Get("k1")
	if err != nil {
		t.Fatalf("get k1 after reopen append: %v", err)
	}
	if string(v) != "v1" {
		t.Fatalf("unexpected k1 value: %q", string(v))
	}

	v, err = db.Get("k2")
	if err != nil {
		t.Fatalf("get k2 after reopen append: %v", err)
	}
	if string(v) != "v2" {
		t.Fatalf("unexpected k2 value: %q", string(v))
	}
}

func TestAppendAfterCompact(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.log")

	db, err := Open(DefaultOptions(path))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	if err := db.Put("a", []byte("1")); err != nil {
		t.Fatalf("put a: %v", err)
	}
	if err := db.Put("b", []byte("2")); err != nil {
		t.Fatalf("put b: %v", err)
	}
	if err := db.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if err := db.Put("c", []byte("3")); err != nil {
		t.Fatalf("put c after compact: %v", err)
	}

	for key, want := range map[string]string{"a": "1", "b": "2", "c": "3"} {
		v, err := db.Get(key)
		if err != nil {
			t.Fatalf("get %s: %v", key, err)
		}
		if string(v) != want {
			t.Fatalf("unexpected %s value: %q", key, string(v))
		}
	}
}

func TestOpenTruncatesPartialRecordTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.log")

	db, err := Open(DefaultOptions(path))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := db.Put("stable", []byte("value")); err != nil {
		t.Fatalf("put stable: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat before partial append: %v", err)
	}
	validSize := info.Size()

	partial := encodeRecord(logRecord{
		Type:  recordTypePut,
		Key:   []byte("partial"),
		Value: []byte("tail"),
	})
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open file for partial append: %v", err)
	}
	if _, err := f.Write(partial[:len(partial)/2]); err != nil {
		_ = f.Close()
		t.Fatalf("append partial record: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close file after partial append: %v", err)
	}

	db, err = Open(DefaultOptions(path))
	if err != nil {
		t.Fatalf("reopen db with partial tail: %v", err)
	}
	defer db.Close()

	info, err = os.Stat(path)
	if err != nil {
		t.Fatalf("stat after reopen: %v", err)
	}
	if info.Size() != validSize {
		t.Fatalf("expected truncated size %d, got %d", validSize, info.Size())
	}

	v, err := db.Get("stable")
	if err != nil {
		t.Fatalf("get stable after truncation: %v", err)
	}
	if string(v) != "value" {
		t.Fatalf("unexpected stable value: %q", string(v))
	}
}

func TestConcurrentCacheHits(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.log")
	opts := DefaultOptions(path)
	opts.CacheCapacity = 16

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	if err := db.Put("hot", []byte("value")); err != nil {
		t.Fatalf("put hot: %v", err)
	}
	if _, err := db.Get("hot"); err != nil {
		t.Fatalf("warm cache: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				v, err := db.Get("hot")
				if err != nil {
					t.Errorf("concurrent get: %v", err)
					return
				}
				if string(v) != "value" {
					t.Errorf("unexpected concurrent value: %q", string(v))
					return
				}
			}
		}()
	}
	wg.Wait()
}
