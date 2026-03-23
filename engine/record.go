package engine

import (
	"encoding/binary"
	"hash/crc32"
)

const (
	recordTypePut byte = 1
	recordTypeDel byte = 2
	recordHeaderSize = 13 // crc32(4) + type(1) + keyLen(4) + valLen(4)
)

type logRecord struct {
	Type  byte
	Key   []byte
	Value []byte
}

func encodeRecord(r logRecord) []byte {
	keyLen := uint32(len(r.Key))
	valLen := uint32(len(r.Value))
	total := recordHeaderSize + int(keyLen) + int(valLen)
	buf := make([]byte, total)

	buf[4] = r.Type
	binary.LittleEndian.PutUint32(buf[5:9], keyLen)
	binary.LittleEndian.PutUint32(buf[9:13], valLen)
	copy(buf[13:13+keyLen], r.Key)
	copy(buf[13+keyLen:], r.Value)

	crc := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], crc)
	return buf
}

func decodeHeader(buf []byte) (crc uint32, typ byte, keyLen uint32, valLen uint32) {
	crc = binary.LittleEndian.Uint32(buf[0:4])
	typ = buf[4]
	keyLen = binary.LittleEndian.Uint32(buf[5:9])
	valLen = binary.LittleEndian.Uint32(buf[9:13])
	return
}

