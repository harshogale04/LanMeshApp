package core

import (
	"encoding/binary"
	"hash/fnv"
	"os"
)

func dirMkdirAll(dir string) error {
	return os.MkdirAll(dir, 0755)
}

// MakeTimestampKey constructs a 16-byte composite key for BoltDB:
// [8 bytes: BigEndian UnixMs timestamp] + [8 bytes: FNV-1a hash of message ID].
// This guarantees lexicographical ordering by time in BoltDB B-Trees,
// enabling O(log N) cursor seeks without simple sequence collisions.
func MakeTimestampKey(tsMs int64, id string) []byte {
	key := make([]byte, 16)
	binary.BigEndian.PutUint64(key[:8], uint64(tsMs))
	h := fnv.New64a()
	h.Write([]byte(id))
	binary.BigEndian.PutUint64(key[8:], h.Sum64())
	return key
}

// MakeSeekKey constructs an 8-byte or 16-byte prefix key for BoltDB cursor seeks.
func MakeSeekKey(tsMs int64) []byte {
	key := make([]byte, 16)
	binary.BigEndian.PutUint64(key[:8], uint64(tsMs))
	return key
}
