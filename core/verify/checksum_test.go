package verify_test

import (
	"hash/fnv"
	"testing"

	"github.com/proofload/proofload/core/verify"
)

func TestChecksum(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
	}{
		{"nil", nil},
		{"empty", []byte{}},
		{"short", []byte("v-1")},
		{"binary", []byte{0x00, 0xff, 0x10, 0x00}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := fnv.New64a()
			_, _ = h.Write(tt.in)
			want := h.Sum64()
			if got := verify.Checksum(tt.in); got != want {
				t.Fatalf("Checksum(%q) = %d, want %d (FNV-1a/64)", tt.in, got, want)
			}
		})
	}
}

func TestChecksumDistinguishesContent(t *testing.T) {
	if verify.Checksum([]byte("alpha")) == verify.Checksum([]byte("beta")) {
		t.Fatal("distinct content produced identical checksums")
	}
}
