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

package migrate

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/transparency-dev/trillian-tessera/api/layout"
	"github.com/transparency-dev/trillian-tessera/client"
	"golang.org/x/sync/errgroup"
	"k8s.io/klog/v2"
)

type migrate struct {
	storage    MigrationStorage
	getEntries client.EntryBundleFetcherFunc

	sourceSize uint64
	sourceRoot []byte

	todo chan span

	tilesToMigrate   uint64
	bundlesToMigrate uint64
	tilesMigrated    atomic.Uint64
	bundlesMigrated  atomic.Uint64
}

// span represents the number of entry bundles
type span struct {
	start uint64
	N     uint64
}

type MigrationStorage interface {
	SetEntryBundle(ctx context.Context, index uint64, partial uint8, bundle []byte) error
	GetState(ctx context.Context) (uint64, []byte, error)
}

func Migrate(ctx context.Context, stateDB string, sourceSize uint64, sourceRoot []byte, getEntries client.EntryBundleFetcherFunc, storage MigrationStorage) error {
	// TODO store state & resume
	m := &migrate{
		storage:    storage,
		sourceSize: sourceSize,
		sourceRoot: sourceRoot,
		getEntries: getEntries,
		todo:       make(chan span, 100),
	}

	// init
	// figure out what needs copying
	targetSize, targetRoot, err := m.storage.GetState(ctx)
	if err != nil {
		return fmt.Errorf("GetState: %v", err)
	}
	if targetSize > sourceSize {
		return fmt.Errorf("Target size %d > source size %d", targetSize, sourceSize)
	}
	if targetSize == sourceSize {
		if !bytes.Equal(targetRoot, sourceRoot) {
			return fmt.Errorf("Target root %x != source root %x", targetRoot, sourceRoot)
		}
		return nil
	}
	go m.populateSpans(targetSize, sourceSize)

	// Print stats
	go func() {
		for {
			time.Sleep(time.Second)
			bn := m.bundlesMigrated.Load()
			bnp := float64(bn*100) / float64(m.bundlesToMigrate)
			s, _, err := m.storage.GetState(ctx)
			if err != nil {
				klog.Warningf("GetState: %v", err)
			}
			intp := float64(s*100) / float64(sourceSize)
			klog.Infof("integration: %d (%.2f%%)  bundles: %d (%.2f%%)", s, intp, bn, bnp)
		}
	}()

	// Do the copying
	eg := errgroup.Group{}
	for i := 0; i < 300; i++ {
		eg.Go(func() error {
			return m.migrateRange(ctx)

		})
	}
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("migrate failed to copy resources: %v", err)
	}
	// TODO, wait for integrate

	for {
		time.Sleep(time.Second)
		is, ir, err := m.storage.GetState(ctx)
		if err != nil {
			klog.Warningf("GetState: %v", err)
			continue
		}
		if is == sourceSize {
			klog.Infof("Integration complete:\bsource size: %d, source root: %x\ntarget size %d, target root %x", sourceSize, sourceRoot, is, ir)
			if !bytes.Equal(sourceRoot, ir) {
				klog.Errorf("Source root and target root do not match!")
			}
			break
		}
	}
	return nil
}

// TODO: handle resuming from a partially migrated tree
func (m *migrate) populateSpans(from, treeSize uint64) {
	klog.Infof("Spans for entry range [%d, %d)", from, treeSize)
	defer close(m.todo)
	defer klog.Infof("total bundles to fetch %d", m.bundlesToMigrate)

	if from%layout.EntryBundleWidth != 0 {
		m.bundlesToMigrate = 1
	}
	m.bundlesToMigrate += (treeSize - from) / layout.EntryBundleWidth
	if treeSize%layout.EntryBundleWidth != 0 {
		m.bundlesToMigrate++
	}

	idx := from / layout.EntryBundleWidth
	if p := from % layout.EntryBundleWidth; p != 0 {
		if from+layout.EntryBundleWidth > treeSize {
			s := treeSize % layout.EntryBundleWidth
			m.todo <- span{start: idx, N: s}
			return
		}
		m.todo <- span{start: idx, N: layout.EntryBundleWidth}
		idx++
	}

	for idx < treeSize/layout.EntryBundleWidth {
		m.todo <- span{start: idx, N: layout.EntryBundleWidth}
		idx++
	}
	if p := treeSize % layout.EntryBundleWidth; p != 0 {
		m.todo <- span{start: idx, N: p}
	}
}

func (m *migrate) migrateRange(ctx context.Context) error {
	for s := range m.todo {
		retry.Do(func() error {
			if s.N == layout.TileWidth {
				s.N = 0
			}
			d, err := m.getEntries(ctx, s.start, uint8(s.N))
			if err != nil {
				return fmt.Errorf("failed to fetch entrybundle %d (p=%d): %v", s.start, s.N, err)
			}
			if err := m.storage.SetEntryBundle(ctx, s.start, uint8(s.N), d); err != nil {
				return fmt.Errorf("failed to store entrybundle %d (p=%d): %v", s.start, s.N, err)
			}
			m.bundlesMigrated.Add(1)
			return nil
		},
			retry.Attempts(10),
			retry.DelayType(retry.BackOffDelay))
	}
	return nil
}
