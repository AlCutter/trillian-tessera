// Copyright 2024 The Tessera authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//
// Original source: https://github.com/FiloSottile/sunlight/blob/main/tile.go
//
// # Copyright 2023 The Sunlight Authors
//
// Permission to use, copy, modify, and/or distribute this software for any
// purpose with or without fee is hereby granted, provided that the above
// copyright notice and this permission notice appear in all copies.
//
// THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
// WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
// ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
// WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
// ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
// OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

// Package ctonly has support for CT Tiles API.
//
// This code should not be reused outside of CT.
// Most of this code came from Filippo's Sunlight implementation of https://c2sp.org/ct-static-api.
package ctonly

import (
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/transparency-dev/merkle/rfc6962"
	"golang.org/x/crypto/cryptobyte"
)

// Entry represents a CT log entry.
type Entry struct {
	Timestamp uint64
	IsPrecert bool
	// Certificate holds different things depending on whether the entry represents a Certificate or a Precertificate submission:
	//   - IsPrecert == false: the bytes here are the x509 certificate submitted for logging.
	//   - IsPrecert == true: the bytes here are the TBS certificate extracted from the submitted precert.
	Certificate []byte
	// Precertificate holds the precertificate to be logged, only used when IsPrecert is true.
	Precertificate    []byte
	IssuerKeyHash     []byte
	FingerprintsChain [][32]byte
}

// IntegratedEntry represents an Entry in a Static CT Entry Bundle.
// This is an Entry plus its location in the log.
type IntegratedEntry struct {
	Entry Entry
	Index uint64
}

// ParseEntryBundle decodes the entries stored in a Static CT Entry Bundle.
func ParseEntryBundle(bs []byte) ([]IntegratedEntry, error) {
	b := cryptobyte.String(bs)
	r := make([]IntegratedEntry, 0)
	for i := 0; len(b) > 0; i++ {
		e, idx, err := readLeafData(&b)
		if err != nil {
			return nil, fmt.Errorf("failed to read entry %d of bundle: %v", i, err)
		}
		r = append(r, IntegratedEntry{
			Entry: *e,
			Index: idx,
		})
	}
	return r, nil
}

// EntryBundleHashes calculates the Merkle leaf hashes for all entries in the provided Static CT Bundle.
func EntryBundleHashes(bs []byte) ([][]byte, error) {
	b := cryptobyte.String(bs)
	r := make([][]byte, 0)
	for i := 0; len(b) > 0; i++ {
		e, idx, err := readLeafData(&b)
		if err != nil {
			return nil, fmt.Errorf("failed to read entry %d of bundle: %v", i, err)
		}
		r = append(r, e.MerkleLeafHash(idx))
	}
	return r, nil

}

func readLeafData(b *cryptobyte.String) (*Entry, uint64, error) {
	r := &Entry{}

	if !b.ReadUint64(&r.Timestamp) {
		return nil, 0, fmt.Errorf("failed to parse timestamp")
	}

	var leafType uint16
	if !b.ReadUint16(&leafType) {
		return nil, 0, fmt.Errorf("failed to parse leaftype")
	}
	switch leafType {
	case 0:
		r.IsPrecert = false
		certBytes := cryptobyte.String{}
		if !b.ReadUint24LengthPrefixed(&certBytes) {
			return nil, 0, fmt.Errorf("failed to parse certificate")
		}
		r.Certificate = certBytes
	case 1:
		r.IsPrecert = true
		if !b.ReadBytes(&r.IssuerKeyHash, sha256.Size) {
			return nil, 0, fmt.Errorf("failed to read issuer key hash")
		}
		tbsBytes := cryptobyte.String{}
		if !b.ReadUint24LengthPrefixed(&tbsBytes) {
			return nil, 0, fmt.Errorf("failed to parse TBS")
		}
		r.Certificate = tbsBytes
	default:
		return nil, 0, fmt.Errorf("unknown leaf type %d", leafType)
	}

	extB := cryptobyte.String{}
	if !b.ReadUint16LengthPrefixed(&extB) {
		return nil, 0, fmt.Errorf("failed to read extension bytes")
	}
	idx := &extensions{}
	if err := idx.Unmarshal(extB); err != nil {
		return nil, 0, fmt.Errorf("failed to read index: %v", err)
	}

	if r.IsPrecert {
		precert := cryptobyte.String{}
		if !b.ReadUint24LengthPrefixed(&precert) {
			return nil, 0, fmt.Errorf("failed to read precert bytes")
		}
		r.Precertificate = precert
	}

	fpChain := cryptobyte.String{}
	if !b.ReadUint16LengthPrefixed(&fpChain) {
		return nil, 0, fmt.Errorf("failed to read fingerprint chain bytes")
	}
	if l := len(fpChain); l%sha256.Size != 0 {
		return nil, 0, fmt.Errorf("unexpected length of fingerprint chain %d", l)
	}
	for ; len(fpChain) > 0; fpChain = fpChain[32:] {
		r.FingerprintsChain = append(r.FingerprintsChain, [32]byte(fpChain[:32]))
	}

	return r, idx.LeafIndex, nil
}

// LeafData returns the data which should be added to an entry bundle for this entry.
//
// Note that this will include data which IS NOT directly committed to by the entry's
// MerkleLeafHash.
func (c Entry) LeafData(idx uint64) []byte {
	b := cryptobyte.NewBuilder([]byte{})
	b.AddUint64(uint64(c.Timestamp))
	if !c.IsPrecert {
		b.AddUint16(0 /* entry_type = x509_entry */)
		b.AddUint24LengthPrefixed(func(b *cryptobyte.Builder) {
			b.AddBytes(c.Certificate)
		})
	} else {
		b.AddUint16(1 /* entry_type = precert_entry */)
		b.AddBytes(c.IssuerKeyHash[:])
		b.AddUint24LengthPrefixed(func(b *cryptobyte.Builder) {
			// Note that this is really the TBS extracted from the submitted precertificate.
			b.AddBytes(c.Certificate)
		})
	}
	addExtensions(b, idx)
	if c.IsPrecert {
		b.AddUint24LengthPrefixed(func(b *cryptobyte.Builder) {
			b.AddBytes(c.Precertificate)
		})
	}
	b.AddUint16LengthPrefixed(func(b *cryptobyte.Builder) {
		for _, f := range c.FingerprintsChain {
			b.AddBytes(f[:])
		}
	})
	return b.BytesOrPanic()
}

// MerkleTreeLeaf returns a RFC 6962 MerkleTreeLeaf.
//
// Note that we embed an SCT extension which captures the index of the entry in the log according to
// the mechanism specified in https://c2sp.org/ct-static-api.
func (e *Entry) MerkleTreeLeaf(idx uint64) []byte {
	b := &cryptobyte.Builder{}
	b.AddUint8(0 /* version = v1 */)
	b.AddUint8(0 /* leaf_type = timestamped_entry */)
	b.AddUint64(uint64(e.Timestamp))
	if !e.IsPrecert {
		b.AddUint16(0 /* entry_type = x509_entry */)
		b.AddUint24LengthPrefixed(func(b *cryptobyte.Builder) {
			b.AddBytes(e.Certificate)
		})
	} else {
		b.AddUint16(1 /* entry_type = precert_entry */)
		b.AddBytes(e.IssuerKeyHash[:])
		b.AddUint24LengthPrefixed(func(b *cryptobyte.Builder) {
			// Note that this is really the TBS extracted from the submitted precertificate.
			b.AddBytes(e.Certificate)
		})
	}
	addExtensions(b, idx)
	return b.BytesOrPanic()
}

// MerkleLeafHash returns the RFC6962 leaf hash for this entry.
//
// Note that we embed an SCT extension which captures the index of the entry in the log according to
// the mechanism specified in https://c2sp.org/ct-static-api.
func (c Entry) MerkleLeafHash(leafIndex uint64) []byte {
	return rfc6962.DefaultHasher.HashLeaf(c.MerkleTreeLeaf(leafIndex))
}

func (c Entry) Identity() []byte {
	var r [sha256.Size]byte
	if c.IsPrecert {
		r = sha256.Sum256(c.Precertificate)
	} else {
		r = sha256.Sum256(c.Certificate)
	}
	return r[:]
}

func addExtensions(b *cryptobyte.Builder, leafIndex uint64) {
	b.AddUint16LengthPrefixed(func(b *cryptobyte.Builder) {
		ext, err := extensions{LeafIndex: leafIndex}.Marshal()
		if err != nil {
			b.SetError(err)
			return
		}
		b.AddBytes(ext)
	})
}

// extensions is the CTExtensions field of SignedCertificateTimestamp and
// TimestampedEntry, according to c2sp.org/static-ct-api.
type extensions struct {
	LeafIndex uint64
}

func (c *extensions) Unmarshal(bs []byte) error {
	b := cryptobyte.String(bs)
	var eType uint8
	if !b.ReadUint8(&eType) {
		return fmt.Errorf("failed to read extension type")
	}
	if eType != 0 {
		return fmt.Errorf("unknown extension type %d", eType)
	}
	idxS := cryptobyte.String{}
	if !b.ReadUint16LengthPrefixed(&idxS) {
		return fmt.Errorf("failed to read index bytes")
	}
	if l := len(idxS); l != 5 {
		return fmt.Errorf("unexpected number of index bytes %d, wanted %d", l, 5)
	}
	c.LeafIndex = uint64(idxS[0])<<32 | uint64(idxS[1])<<24 | uint64(idxS[2])<<16 | uint64(idxS[3])<<8 | uint64(idxS[4])
	return nil
}

func (c extensions) Marshal() ([]byte, error) {
	// enum {
	//     leaf_index(0), (255)
	// } ExtensionType;
	//
	// struct {
	//     ExtensionType extension_type;
	//     opaque extension_data<0..2^16-1>;
	// } Extension;
	//
	// Extension CTExtensions<0..2^16-1>;
	//
	// uint8 uint40[5];
	// uint40 LeafIndex;

	b := &cryptobyte.Builder{}
	b.AddUint8(0 /* extension_type = leaf_index */)
	b.AddUint16LengthPrefixed(func(b *cryptobyte.Builder) {
		if c.LeafIndex >= 1<<40 {
			b.SetError(errors.New("leaf_index out of range"))
			return
		}
		addUint40(b, uint64(c.LeafIndex))
	})
	return b.Bytes()
}

// addUint40 appends a big-endian, 40-bit value to the byte string.
func addUint40(b *cryptobyte.Builder, v uint64) {
	b.AddBytes([]byte{byte(v >> 32), byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)})
}
