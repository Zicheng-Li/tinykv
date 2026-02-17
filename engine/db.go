package engine

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

var (
	ErrKeyNotFound    = errors.New("key not found")
	ErrEmptyKey       = errors.New("empty key")
	ErrCorruptLogData = errors.New("corrupt log data")
)

type Options struct {
	Path          string
	SyncOnWrite   bool
	CacheCapacity int
}

func DefaultOptions(path string) Options {
	return Options{
		Path:          path,
		SyncOnWrite:   false,
		CacheCapacity: 0,
	}
}

type indexEntry struct {
	offset int64
}

type DB struct {
	mu          sync.RWMutex
	logPath     string
	logFile     *os.File
	writeOffset int64
	index       map[string]indexEntry
	cache       *lruCache
	cacheCap    int
	syncOnWrite bool
}

func Open(opts Options) (*DB, error) {
	if opts.Path == "" {
		return nil, errors.New("path is required")
	}
	if err := os.MkdirAll(filepath.Dir(opts.Path), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir: %w", err)
	}
	f, err := os.OpenFile(opts.Path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}

	db := &DB{
		logPath:     opts.Path,
		logFile:     f,
		index:       make(map[string]indexEntry),
		cache:       newLRU(opts.CacheCapacity),
		cacheCap:    opts.CacheCapacity,
		syncOnWrite: opts.SyncOnWrite,
	}
	if err := db.rebuildIndex(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return db, nil
}

func (db *DB) Put(key string, value []byte) error {
	if key == "" {
		return ErrEmptyKey
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	rec := logRecord{
		Type:  recordTypePut,
		Key:   []byte(key),
		Value: cloneBytes(value),
	}
	offset, err := db.appendRecord(rec)
	if err != nil {
		return err
	}
	db.index[key] = indexEntry{offset: offset}
	db.cache.put(key, value)
	return nil
}

func (db *DB) Get(key string) ([]byte, error) {
	if key == "" {
		return nil, ErrEmptyKey
	}

	db.mu.RLock()
	if v, ok := db.cache.get(key); ok {
		db.mu.RUnlock()
		return v, nil
	}
	entry, ok := db.index[key]
	if !ok {
		db.mu.RUnlock()
		return nil, ErrKeyNotFound
	}
	rec, _, err := db.readRecordAt(entry.offset)
	db.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	if rec.Type != recordTypePut {
		return nil, ErrKeyNotFound
	}

	db.mu.Lock()
	db.cache.put(key, rec.Value)
	db.mu.Unlock()
	return cloneBytes(rec.Value), nil
}

func (db *DB) Delete(key string) error {
	if key == "" {
		return ErrEmptyKey
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if _, ok := db.index[key]; !ok {
		return ErrKeyNotFound
	}
	rec := logRecord{
		Type: recordTypeDel,
		Key:  []byte(key),
	}
	if _, err := db.appendRecord(rec); err != nil {
		return err
	}
	delete(db.index, key)
	db.cache.remove(key)
	return nil
}

func (db *DB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.logFile.Sync()
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.logFile.Close()
}

// Compact rewrites the current live keys to a new log file and atomically swaps it in.
func (db *DB) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	tmpPath := db.logPath + ".compact.tmp"
	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open compact tmp file: %w", err)
	}
	defer func() {
		_ = tmpFile.Close()
	}()

	newIndex := make(map[string]indexEntry, len(db.index))
	var newOffset int64
	for key, entry := range db.index {
		rec, _, err := db.readRecordAt(entry.offset)
		if err != nil {
			return fmt.Errorf("read old record for key %q: %w", key, err)
		}
		encoded := encodeRecord(rec)
		n, err := tmpFile.Write(encoded)
		if err != nil {
			return fmt.Errorf("write compact record: %w", err)
		}
		if n != len(encoded) {
			return io.ErrShortWrite
		}
		newIndex[key] = indexEntry{offset: newOffset}
		newOffset += int64(n)
	}
	if err := tmpFile.Sync(); err != nil {
		return fmt.Errorf("sync compact tmp file: %w", err)
	}

	if err := db.logFile.Close(); err != nil {
		return fmt.Errorf("close old log file: %w", err)
	}
	if err := os.Rename(tmpPath, db.logPath); err != nil {
		return fmt.Errorf("rename compact file: %w", err)
	}

	newLogFile, err := os.OpenFile(db.logPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("reopen log file: %w", err)
	}
	db.logFile = newLogFile
	db.index = newIndex
	db.writeOffset = newOffset
	db.cache = newLRU(db.cacheCap)
	return nil
}

func (db *DB) appendRecord(rec logRecord) (int64, error) {
	offset := db.writeOffset
	encoded := encodeRecord(rec)
	n, err := db.logFile.Write(encoded)
	if err != nil {
		return 0, fmt.Errorf("append record: %w", err)
	}
	if n != len(encoded) {
		return 0, io.ErrShortWrite
	}
	db.writeOffset += int64(n)
	if db.syncOnWrite {
		if err := db.logFile.Sync(); err != nil {
			return 0, fmt.Errorf("sync log: %w", err)
		}
	}
	return offset, nil
}

func (db *DB) readRecordAt(offset int64) (logRecord, int64, error) {
	header := make([]byte, recordHeaderSize)
	n, err := db.logFile.ReadAt(header, offset)
	if err != nil {
		return logRecord{}, 0, err
	}
	if n != recordHeaderSize {
		return logRecord{}, 0, io.ErrUnexpectedEOF
	}
	storedCRC, typ, keyLen, valLen := decodeHeader(header)
	size := int64(recordHeaderSize + keyLen + valLen)

	body := make([]byte, int(keyLen)+int(valLen))
	n, err = db.logFile.ReadAt(body, offset+recordHeaderSize)
	if err != nil {
		return logRecord{}, 0, err
	}
	if n != len(body) {
		return logRecord{}, 0, io.ErrUnexpectedEOF
	}

	checksumBuf := make([]byte, 1+4+4+len(body))
	checksumBuf[0] = typ
	copy(checksumBuf[1:5], header[5:9])
	copy(checksumBuf[5:9], header[9:13])
	copy(checksumBuf[9:], body)
	actualCRC := crc32.ChecksumIEEE(checksumBuf)
	if storedCRC != actualCRC {
		return logRecord{}, 0, ErrCorruptLogData
	}

	rec := logRecord{
		Type:  typ,
		Key:   cloneBytes(body[:int(keyLen)]),
		Value: cloneBytes(body[int(keyLen):]),
	}
	return rec, size, nil
}

func (db *DB) rebuildIndex() error {
	var offset int64
	for {
		rec, size, err := db.readRecordAt(offset)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			db.writeOffset = offset
			return nil
		}
		if err != nil {
			return fmt.Errorf("replay at offset %d: %w", offset, err)
		}
		key := string(rec.Key)
		switch rec.Type {
		case recordTypePut:
			db.index[key] = indexEntry{offset: offset}
		case recordTypeDel:
			delete(db.index, key)
		default:
			return fmt.Errorf("unknown record type %d at offset %d", rec.Type, offset)
		}
		offset += size
	}
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
