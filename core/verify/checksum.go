// Package verify implements proofload's reconciliation correctness model: an
// append-only expectation log the load engine writes (one record per committed
// write) and a checker that reads every expected key back from the target after
// a run to detect data loss, corruption, and cluster divergence.
package verify

import "hash/fnv"

// Checksum is the canonical content hash the recorder and the verifier agree
// on. It is FNV-1a 64-bit. The runner that appends WriteRecords MUST hash each
// committed value with this exact function so recorded and read-back values are
// directly comparable. A nil or empty slice hashes to the FNV-1a offset basis.
func Checksum(b []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(b) // hash.Hash.Write never returns an error.
	return h.Sum64()
}
