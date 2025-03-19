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
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/transparency-dev/trillian-tessera/api/layout"
	"github.com/transparency-dev/trillian-tessera/client"
	"golang.org/x/sync/errgroup"
	"k8s.io/klog/v2"
)

type MigrationWriter interface {
	// SetEntryBundle stores the provided serialised entry bundle at the location implied by the provided
	// entry bundle index and partial size.
	//
	// Bundles may be set in any order (not just consecutively), and the implementation should integrate
	// them into the local tree in the most efficient way possible.
	//
	// Writes should be idempotent; repeated calls to set the same bundle with the same data should not
	// return an error.
	SetEntryBundle(ctx context.Context, idx uint64, partial uint8, bundle []byte) error
	// AwaitIntegration should block until the local integrated tree has grown to the provided size,
	// and should return the locally calculated root hash derived from the integration of the contents of
	// entry bundles set using SetEntryBundle above.
	AwaitIntegration(ctx context.Context, size uint64) ([]byte, error)
	// IntegratedSize returns the current size of the locally integrated log.
	IntegratedSize(ctx context.Context) (uint64, error)
}

// UnbundlerFunc is a function which knows how to turn a serialised entry bundle into a slice of
// []byte representing each of the entries within the bundle.
type UnbundlerFunc func(entryBundle []byte) ([][]byte, error)

// NewMigrationTarget returns a MigrationTarget, which allows a personality to "import" a C2SP
// tlog-tiles or static-ct compliant log into a Tessera instance.
func NewMigrationTarget(ctx context.Context, d Driver, opts *MigrationOptions) (*MigrationTarget, error) {
	type migrateLifecycle interface {
		MigrationWriter(context.Context, *MigrationOptions) (MigrationWriter, LogReader, error)
	}
	lc, ok := d.(migrateLifecycle)
	if !ok {
		return nil, fmt.Errorf("driver %T does not implement MigrationTarget lifecycle", d)
	}
	mw, r, err := lc.MigrationWriter(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to init MigrationTarget lifecycle: %v", err)
	}
	for _, f := range opts.followers {
		go f(ctx, r)
	}
	return &MigrationTarget{
		writer: mw,
	}, nil
}

func NewMigrationOptions() *MigrationOptions {
	return &MigrationOptions{
		entriesPath:      layout.EntriesPath,
		bundleIDHasher:   defaultIDHasher,
		bundleLeafHasher: defaultMerkleLeafHasher,
	}
}

// MigrationOptions holds migration lifecycle settings for all storage implementations.
type MigrationOptions struct {
	// entriesPath knows how to format entry bundle paths.
	entriesPath func(n uint64, p uint8) string
	// bundleIDHasher knows how to create antispam leaf identities for entries in a serialised bundle.
	bundleIDHasher func([]byte) ([][]byte, error)
	// bundleLeafHasher knows how to create Merkle leaf hashes for the entries in a serialised bundle.
	bundleLeafHasher func([]byte) ([][]byte, error)
	followers        []func(context.Context, LogReader)
}

func (o MigrationOptions) EntriesPath() func(uint64, uint8) string {
	return o.entriesPath
}

func (o MigrationOptions) Followers() []func(context.Context, LogReader) {
	return o.followers
}

func (o *MigrationOptions) LeafHasher() func([]byte) ([][]byte, error) {
	return o.bundleLeafHasher
}

// WithAntispam configures the migration target to *populate* the provided antispam storage using
// the data being migrated into the target tree.
//
// Note that since the tree is being _migrated_, the resulting target tree must match the structure
// of the source tree and so no attempt is made to reject/deduplicate entries.
func (o *MigrationOptions) WithAntispam(as Antispam) *MigrationOptions {
	if as != nil {
		o.followers = append(o.followers, func(ctx context.Context, lr LogReader) {
			as.Populate(ctx, lr, o.bundleIDHasher)
		})
	}
	return o
}

// MigrationTarget handles the process of migrating/importing a source log into a Tessera instance.
type MigrationTarget struct {
	writer MigrationWriter
}

// Migrate performs the work of importing a source log into the local Tessera instance.
//
// Any entry bundles implied by the provided source log size which are not already present in the local log
// will be fetched using the provided getEntries function, and stored by the underlying driver.
// A background process will continuously attempt to integrate these bundles into the local tree.
//
// An error will be returned if there is an unrecoverable problem encountered during the migration
// process, or if, once all entries have been copied and integrated into the local tree, the local
// root hash does not match the provided sourceRoot.
func (mt *MigrationTarget) Migrate(ctx context.Context, numWorkers uint, sourceSize uint64, sourceRoot []byte, getEntries client.EntryBundleFetcherFunc) error {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	c := newCopier(numWorkers, mt.writer.SetEntryBundle, getEntries)

	fromSize, err := mt.writer.IntegratedSize(ctx)
	if err != nil {
		return fmt.Errorf("fetching integrated size failed: %v", err)
	}

	// Print stats
	go func() {
		bundlesToCopy := (sourceSize / layout.EntryBundleWidth)
		if bundlesToCopy == 0 {
			return
		}
		for {
			select {
			case <-cctx.Done():
				return
			case <-time.After(time.Second):
			}
			bn := c.BundlesCopied()
			bnp := float64(bn*100) / float64(bundlesToCopy)
			s, err := mt.writer.IntegratedSize(ctx)
			if err != nil {
				klog.Warningf("Size: %v", err)
			}
			intp := float64(s*100) / float64(sourceSize)
			klog.Infof("integration: %d (%.2f%%)  bundles: %d (%.2f%%)", s, intp, bn, bnp)
		}
	}()

	// go integrate
	errG := errgroup.Group{}
	errG.Go(func() error {
		return c.Copy(cctx, fromSize, sourceSize)
	})

	var calculatedRoot []byte
	errG.Go(func() error {
		r, err := mt.writer.AwaitIntegration(cctx, sourceSize)
		if err != nil {
			return fmt.Errorf("awaiting integration failed: %v", err)
		}
		calculatedRoot = r
		return nil
	})

	if err := errG.Wait(); err != nil {
		return fmt.Errorf("migrate failed: %v", err)
	}

	if !bytes.Equal(calculatedRoot, sourceRoot) {
		return fmt.Errorf("migration completed, but local root hash %x != source root hash %x", calculatedRoot, sourceRoot)
	}

	klog.Infof("Migration successful.")
	return nil
}
