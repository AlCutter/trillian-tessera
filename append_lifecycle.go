// Copyright 2024 The Tessera authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tessera

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	f_log "github.com/transparency-dev/formats/log"
	"github.com/transparency-dev/trillian-tessera/api/layout"
	"golang.org/x/mod/sumdb/note"
	"k8s.io/klog/v2"
)

const (
	// DefaultBatchMaxSize is used by storage implementations if no WithBatching option is provided when instantiating it.
	DefaultBatchMaxSize = 256
	// DefaultBatchMaxAge is used by storage implementations if no WithBatching option is provided when instantiating it.
	DefaultBatchMaxAge = 250 * time.Millisecond
	// DefaultCheckpointInterval is used by storage implementations if no WithCheckpointInterval option is provided when instantiating it.
	DefaultCheckpointInterval = 10 * time.Second
	// DefaultPushbackMaxOutstanding is used by storage implementations if no WithPushback option is provided when instantiating it.
	DefaultPushbackMaxOutstanding = 4096
)

// Add adds a new entry to be sequenced.
// This method quickly returns an IndexFuture, which will return the index assigned
// to the new leaf. Until this is returned, the leaf is not durably added to the log,
// and terminating the process may lead to this leaf being lost.
// Once the future resolves and returns an index, the leaf is durably sequenced and will
// be preserved even in the process terminates.
//
// Once a leaf is sequenced, it will be integrated into the tree soon (generally single digit
// seconds). Until it is integrated, clients of the log will not be able to verifiably access
// this value. Personalities that require blocking until the leaf is integrated can use the
// IntegrationAwaiter to wrap the call to this method.
type AddFn func(ctx context.Context, entry *Entry) IndexFuture

// IndexFuture is the signature of a function which can return an assigned index or error.
//
// Implementations of this func are likely to be "futures", or a promise to return this data at
// some point in the future, and as such will block when called if the data isn't yet available.
type IndexFuture func() (uint64, error)

// Appender allows personalities access to the lifecycle methods associated with logs
// in sequencing mode. This only has a single method, but other methods are likely to be added
// such as a Shutdown method for #341.
type Appender struct {
	Add AddFn
	// TODO(#341): add this method and implement it in all drivers
	// Shutdown func(ctx context.Context)
}

// NewAppender returns an Appender, which allows a personality to incrementally append new
// leaves to the log and to read from it.
//
// decorators provides a list of optional constructor functions that will return decorators
// that wrap the base appender. This can be used to provide deduplication. Decorators will be
// called in-order, and the last in the chain will be the base appender.
func NewAppender(ctx context.Context, d Driver, opts *AppendOptions) (*Appender, LogReader, error) {
	type appendLifecycle interface {
		Appender(context.Context, *AppendOptions) (*Appender, LogReader, error)
	}
	lc, ok := d.(appendLifecycle)
	if !ok {
		return nil, nil, fmt.Errorf("driver %T does not implement Appender lifecycle", d)
	}
	a, r, err := lc.Appender(ctx, opts)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to init appender lifecycle: %v", err)
	}
	for i := len(opts.addDecorators) - 1; i >= 0; i-- {
		a.Add = opts.addDecorators[i](a.Add)
	}
	for _, f := range opts.followers {
		go f(ctx, r)
	}
	return a, r, nil
}

func (o *AppendOptions) WithAntispam(inMemEntries uint, as Antispam) *AppendOptions {
	o.addDecorators = append(o.addDecorators, newInMemoryDedupe(inMemEntries))
	if as != nil {
		o.addDecorators = append(o.addDecorators, as.Decorator())
		o.followers = append(o.followers, func(ctx context.Context, lr LogReader) {
			as.Populate(ctx, lr, defaultIDHasher)
		})
	}
	return o
}

func NewAppendOptions() *AppendOptions {
	return &AppendOptions{
		batchMaxSize:           DefaultBatchMaxSize,
		batchMaxAge:            DefaultBatchMaxAge,
		entriesPath:            layout.EntriesPath,
		checkpointInterval:     DefaultCheckpointInterval,
		addDecorators:          make([]func(AddFn) AddFn, 0),
		pushbackMaxOutstanding: DefaultPushbackMaxOutstanding,
	}
}

// AppendOptions holds settings for all storage implementations.
type AppendOptions struct {
	// newCP knows how to format and sign checkpoints.
	newCP func(size uint64, hash []byte) ([]byte, error)

	batchMaxAge  time.Duration
	batchMaxSize uint

	pushbackMaxOutstanding uint

	// EntriesPath knows how to format entry bundle paths.
	entriesPath func(n uint64, p uint8) string

	checkpointInterval time.Duration
	witnesses          WitnessGroup

	addDecorators []func(AddFn) AddFn
	followers     []func(context.Context, LogReader)
}

func (o AppendOptions) NewCP() func(uint64, []byte) ([]byte, error) {
	return o.newCP
}

func (o AppendOptions) BatchMaxAge() time.Duration {
	return o.batchMaxAge
}

func (o AppendOptions) BatchMaxSize() uint {
	return o.batchMaxSize
}

func (o AppendOptions) PushbackMaxOutstanding() uint {
	return o.pushbackMaxOutstanding
}

func (o AppendOptions) EntriesPath() func(uint64, uint8) string {
	return o.entriesPath
}

func (o AppendOptions) CheckpointInterval() time.Duration {
	return o.checkpointInterval
}

func (o AppendOptions) Witnesses() WitnessGroup {
	return o.witnesses
}

func (o AppendOptions) AddDecorators() []func(AddFn) AddFn {
	return o.addDecorators
}

func (o AppendOptions) Followers() []func(context.Context, LogReader) {
	return o.followers
}

// WithCheckpointSigner is an option for setting the note signer and verifier to use when creating and parsing checkpoints.
// This option is mandatory for creating logs where the checkpoint is signed locally, e.g. in
// the Appender mode. This does not need to be provided where the storage will be used to mirror
// other logs.
//
// A primary signer must be provided:
// - the primary signer is the "canonical" signing identity which should be used when creating new checkpoints.
//
// Zero or more dditional signers may also be provided.
// This enables cases like:
//   - a rolling key rotation, where checkpoints are signed by both the old and new keys for some period of time,
//   - using different signature schemes for different audiences, etc.
//
// When providing additional signers, their names MUST be identical to the primary signer name, and this name will be used
// as the checkpoint Origin line.
//
// Checkpoints signed by these signer(s) will be standard checkpoints as defined by https://c2sp.org/tlog-checkpoint.
func (o *AppendOptions) WithCheckpointSigner(s note.Signer, additionalSigners ...note.Signer) *AppendOptions {
	origin := s.Name()
	for _, signer := range additionalSigners {
		if origin != signer.Name() {
			klog.Exitf("WithCheckpointSigner: additional signer name (%q) does not match primary signer name (%q)", signer.Name(), origin)
		}
	}
	o.newCP = func(size uint64, hash []byte) ([]byte, error) {
		// If we're signing a zero-sized tree, the tlog-checkpoint spec says (via RFC6962) that
		// the root must be SHA256 of the empty string, so we'll enforce that here:
		if size == 0 {
			emptyRoot := sha256.Sum256([]byte{})
			hash = emptyRoot[:]
		}
		cpRaw := f_log.Checkpoint{
			Origin: origin,
			Size:   size,
			Hash:   hash,
		}.Marshal()

		n, err := note.Sign(&note.Note{Text: string(cpRaw)}, append([]note.Signer{s}, additionalSigners...)...)
		if err != nil {
			return nil, fmt.Errorf("note.Sign: %w", err)
		}
		return n, nil
	}
	return o
}

// WithBatching configures the batching behaviour of leaves being sequenced.
// A batch will be allowed to grow in memory until either:
//   - the number of entries in the batch reach maxSize
//   - the first entry in the batch has reached maxAge
//
// At this point the batch will be sent to the sequencer.
//
// Configuring these parameters allows the personality to tune to get the desired
// balance of sequencing latency with cost. In general, larger batches allow for
// lower cost of operation, where more frequent batches reduce the amount of time
// required for entries to be included in the log.
//
// If this option isn't provided, storage implementations with use the DefaultBatchMaxSize and DefaultBatchMaxAge consts above.
func (o *AppendOptions) WithBatching(maxSize uint, maxAge time.Duration) *AppendOptions {
	o.batchMaxSize = maxSize
	o.batchMaxAge = maxAge
	return o
}

// WithPushback allows configuration of when the storage should start pushing back on add requests.
//
// maxOutstanding is the number of "in-flight" add requests - i.e. the number of entries with sequence numbers
// assigned, but which are not yet integrated into the log.
func (o *AppendOptions) WithPushback(maxOutstanding uint) *AppendOptions {
	o.pushbackMaxOutstanding = maxOutstanding
	return o
}

// WithCheckpointInterval configures the frequency at which Tessera will attempt to create & publish
// a new checkpoint.
//
// Well behaved clients of the log will only "see" newly sequenced entries once a new checkpoint is published,
// so it's important to set that value such that it works well with your ecosystem.
//
// Regularly publishing new checkpoints:
//   - helps show that the log is "live", even if no entries are being added.
//   - enables clients of the log to reason about how frequently they need to have their
//     view of the log refreshed, which in turn helps reduce work/load across the ecosystem.
//
// Note that this option probably only makes sense for long-lived applications (e.g. HTTP servers).
//
// If this option isn't provided, storage implementations will use the DefaultCheckpointInterval const above.
func (o *AppendOptions) WithCheckpointInterval(interval time.Duration) *AppendOptions {
	o.checkpointInterval = interval
	return o
}

// WithWitnesses configures the set of witnesses that Tessera will contact in order to counter-sign
// a checkpoint before publishing it. A request will be sent to every witness referenced by the group
// using the URLs method. The checkpoint will be accepted for publishing when a sufficient number of
// witnesses to Satisfy the group have responded.
//
// If this method is not called, then the default empty WitnessGroup will be used, which contacts zero
// witnesses and requires zero witnesses in order to publish.
func (o *AppendOptions) WithWitnesses(witnesses WitnessGroup) *AppendOptions {
	o.witnesses = witnesses
	return o
}
