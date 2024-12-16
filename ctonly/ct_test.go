package ctonly

import (
	"crypto/sha256"
	"testing"

	"github.com/google/go-cmp/cmp"
	"golang.org/x/crypto/cryptobyte"
)

func TestLeafDataRoundTrip(t *testing.T) {
	keyHash := sha256.Sum256([]byte("I'm a key hash"))
	for _, test := range []struct {
		name string
		e    Entry
		idx  uint64
	}{
		{
			name: "cert works",
			e: Entry{
				Timestamp:         1234,
				IsPrecert:         false,
				Certificate:       []byte("I'm a certificate"),
				FingerprintsChain: [][32]byte{sha256.Sum256([]byte("yo"))},
			},
			idx: 456214,
		},
		{
			name: "precert works",
			e: Entry{
				Timestamp:         1234,
				IsPrecert:         true,
				Certificate:       []byte("Precert TBS"),
				Precertificate:    []byte("I'm a precert"),
				IssuerKeyHash:     keyHash[:],
				FingerprintsChain: [][32]byte{sha256.Sum256([]byte("yo"))},
			},
			idx: 9548,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			b := cryptobyte.String(test.e.LeafData(test.idx))
			gotE, gotIdx, err := readLeafData(&b)
			if err != nil {
				t.Fatalf("ParseLeafData: %v", err)
			}
			if gotIdx != test.idx {
				t.Errorf("got index %d, want %d", gotIdx, test.idx)
			}
			if diff := cmp.Diff(*gotE, test.e); diff != "" {
				t.Fatalf("Got diff: %v", diff)
			}
		})
	}

}
