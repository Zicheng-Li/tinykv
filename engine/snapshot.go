package engine

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
)

var ErrInvalidSnapshot = errors.New("invalid snapshot")

const snapshotMagic = "TKVSNP01"

func (db *DB) Snapshot(w io.Writer) error {
	db.mu.RLock()
	records, err := db.liveRecordsLocked()
	db.mu.RUnlock()
	if err != nil {
		return err
	}
	return writeSnapshot(w, records)
}

func (db *DB) Restore(r io.Reader) error {
	records, err := readSnapshot(r)
	if err != nil {
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	return db.restoreLocked(records)
}

func (db *DB) liveRecordsLocked() ([]logRecord, error) {
	keys := make([]string, 0, len(db.index))
	for key := range db.index {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	records := make([]logRecord, 0, len(keys))
	for _, key := range keys {
		entry := db.index[key]
		rec, _, err := db.readRecordAt(entry.offset)
		if err != nil {
			return nil, fmt.Errorf("read live record for key %q: %w", key, err)
		}
		if rec.Type != recordTypePut {
			return nil, fmt.Errorf("unexpected live record type %d for key %q", rec.Type, key)
		}
		records = append(records, rec)
	}
	return records, nil
}

func (db *DB) restoreLocked(records []logRecord) error {
	tmpPath := db.logPath + ".restore.tmp"
	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open restore tmp file: %w", err)
	}

	cleanupTmp := true
	defer func() {
		_ = tmpFile.Close()
		if cleanupTmp {
			_ = os.Remove(tmpPath)
		}
	}()

	newIndex := make(map[string]indexEntry, len(records))
	var newOffset int64
	for i, rec := range records {
		if rec.Type != recordTypePut {
			return fmt.Errorf("%w: restore record %d is not a put", ErrInvalidSnapshot, i)
		}

		encoded := encodeRecord(rec)
		if err := writeFull(tmpFile, encoded); err != nil {
			return fmt.Errorf("write restore record %d: %w", i, err)
		}
		newIndex[string(rec.Key)] = indexEntry{offset: newOffset}
		newOffset += int64(len(encoded))
	}

	if err := tmpFile.Sync(); err != nil {
		return fmt.Errorf("sync restore tmp file: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("close restore tmp file: %w", err)
	}

	if err := db.logFile.Close(); err != nil {
		return fmt.Errorf("close old log file: %w", err)
	}
	if err := os.Rename(tmpPath, db.logPath); err != nil {
		return fmt.Errorf("rename restore file: %w", err)
	}

	newLogFile, err := os.OpenFile(db.logPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("reopen log file after restore: %w", err)
	}

	db.logFile = newLogFile
	db.index = newIndex
	db.writeOffset = newOffset
	db.cache = newLRU(db.cacheCap)
	cleanupTmp = false
	return nil
}

func writeSnapshot(w io.Writer, records []logRecord) error {
	header := make([]byte, len(snapshotMagic)+8)
	copy(header, snapshotMagic)
	binary.LittleEndian.PutUint64(header[len(snapshotMagic):], uint64(len(records)))
	if err := writeFull(w, header); err != nil {
		return fmt.Errorf("write snapshot header: %w", err)
	}

	for i, rec := range records {
		if rec.Type != recordTypePut {
			return fmt.Errorf("%w: snapshot record %d is not a put", ErrInvalidSnapshot, i)
		}
		if err := writeFull(w, encodeRecord(rec)); err != nil {
			return fmt.Errorf("write snapshot record %d: %w", i, err)
		}
	}
	return nil
}

func readSnapshot(r io.Reader) ([]logRecord, error) {
	header := make([]byte, len(snapshotMagic)+8)
	if _, err := io.ReadFull(r, header); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("%w: truncated snapshot header", ErrInvalidSnapshot)
		}
		return nil, fmt.Errorf("read snapshot header: %w", err)
	}
	if string(header[:len(snapshotMagic)]) != snapshotMagic {
		return nil, fmt.Errorf("%w: bad snapshot magic", ErrInvalidSnapshot)
	}

	count := binary.LittleEndian.Uint64(header[len(snapshotMagic):])
	maxInt := int(^uint(0) >> 1)
	if count > uint64(maxInt) {
		return nil, fmt.Errorf("%w: snapshot contains too many records", ErrInvalidSnapshot)
	}

	records := make([]logRecord, 0, int(count))
	for i := uint64(0); i < count; i++ {
		rec, err := readRecordFromReader(r)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, ErrCorruptLogData) {
				return nil, fmt.Errorf("%w: invalid record %d", ErrInvalidSnapshot, i)
			}
			return nil, fmt.Errorf("read snapshot record %d: %w", i, err)
		}
		if rec.Type != recordTypePut {
			return nil, fmt.Errorf("%w: snapshot record %d is not a put", ErrInvalidSnapshot, i)
		}
		records = append(records, rec)
	}

	var trailing [1]byte
	n, err := r.Read(trailing[:])
	switch {
	case n == 0 && errors.Is(err, io.EOF):
		return records, nil
	case n == 0 && err == nil:
		return nil, fmt.Errorf("%w: trailing snapshot data", ErrInvalidSnapshot)
	case err != nil && !errors.Is(err, io.EOF):
		return nil, fmt.Errorf("verify snapshot trailer: %w", err)
	default:
		return nil, fmt.Errorf("%w: trailing snapshot data", ErrInvalidSnapshot)
	}
}

func readRecordFromReader(r io.Reader) (logRecord, error) {
	header := make([]byte, recordHeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return logRecord{}, err
	}

	storedCRC, typ, keyLen, valLen := decodeHeader(header)
	body := make([]byte, int(keyLen)+int(valLen))
	if _, err := io.ReadFull(r, body); err != nil {
		return logRecord{}, err
	}

	checksumBuf := make([]byte, 1+4+4+len(body))
	checksumBuf[0] = typ
	copy(checksumBuf[1:5], header[5:9])
	copy(checksumBuf[5:9], header[9:13])
	copy(checksumBuf[9:], body)
	if storedCRC != crc32.ChecksumIEEE(checksumBuf) {
		return logRecord{}, ErrCorruptLogData
	}

	return logRecord{
		Type:  typ,
		Key:   cloneBytes(body[:int(keyLen)]),
		Value: cloneBytes(body[int(keyLen):]),
	}, nil
}

func writeFull(w io.Writer, buf []byte) error {
	n, err := w.Write(buf)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return io.ErrShortWrite
	}
	return nil
}
