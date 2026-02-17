package engine

import (
	"errors"
	"path/filepath"
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

