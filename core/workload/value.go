// Package workload turns a declarative domain.Workload into a deterministic,
// lock-free stream of concrete domain.Operation values. Each load goroutine gets
// its own Generator with an independent RNG stream, so the hot path never locks.
package workload

import (
	"encoding/binary"
	"math/rand/v2"
)

// valueSalt fixes the second PCG word so Value depends only on (key, size).
// (digits of pi in hex; any constant works as long as it never changes).
const valueSalt uint64 = 0x243F6A8885A308D3

// Value returns deterministic payload bytes of exactly length size for key.
//
// Because the bytes are a pure function of (key, size), reconciliation can
// recompute the expected checksum for a key without the harness ever storing
// the payloads it wrote. size <= 0 yields nil.
func Value(key int64, size int) []byte {
	if size <= 0 {
		return nil
	}
	buf := make([]byte, size)
	src := rand.NewPCG(uint64(key), valueSalt)
	i := 0
	for ; i+8 <= size; i += 8 {
		binary.LittleEndian.PutUint64(buf[i:], src.Uint64())
	}
	if i < size {
		var tail [8]byte
		binary.LittleEndian.PutUint64(tail[:], src.Uint64())
		copy(buf[i:], tail[:])
	}
	return buf
}
