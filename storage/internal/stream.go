// Copyright 2025 The Tessera Authors. All Rights Reserved.
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

package storage

import (
	"context"
	"time"

	tessera "github.com/transparency-dev/trillian-tessera"
	"github.com/transparency-dev/trillian-tessera/api/layout"
	"k8s.io/klog/v2"
)

// GetBundleFn is a function which knows how to fetch a single entry bundle from the specified address.
type GetBundleFn func(ctx context.Context, bundleIdx uint64, partial uint8) ([]byte, error)

// GetTreeSizeFn is a function which knows how to return a tree size.
type GetTreeSizeFn func(ctx context.Context) (uint64, error)

// StreamAdaptor uses the provided function to produce a stream of entry bundles accesible via the returned functions.
//
// Entry bundles are retuned strictly in order via consecutive calls to the returned next func.
// If the adaptor encounters an error while reading an entry bundle, the encountered error will be returned by the corresponding call to next,
// and the stream will be stopped - further calls to next will continue to return errors.
//
// When the caller has finished consuming entry bundles (either because of an error being returned via next, or having consumed all the bundles it needs),
// it MUST call the returned cancel function to release resources.
//
// This adaptor is optimised for the case where calling getBundle has some appreciable latency, and works
// around that by maintaining a read-ahead cache of subsequent bundles which is populated a number of parallel
// requests to getBundle. The request parallelism is set by the value of the numWorkers paramemter, which can be tuned
// to balance throughput against consumption of resources, but such balancing needs to be mindful of the nature of the
// source infrastructure, and how concurrent requests affect performance (e.g. GCS buckets vs. files on a single disk).
func StreamAdaptor(ctx context.Context, numWorkers uint, getSize GetTreeSizeFn, getBundle GetBundleFn, fromEntry uint64) (next func() (ri layout.RangeInfo, bundle []byte, err error), cancel func()) {
	ctx, span := tracer.Start(ctx, "tessera.storage.StreamAdaptor")
	defer span.End()

	// bundleOrErr represents a fetched entry bundle and its params, or an error if we couldn't fetch it for
	// some reason.
	type bundleOrErr struct {
		ri  layout.RangeInfo
		b   []byte
		err error
	}

	// bundles will be filled with futures for in-order entry bundles by the worker
	// go routines below.
	// This channel will be drained by the loop at the bottom of this func which
	// yields the bundles to the caller.
	bundles := make(chan func() bundleOrErr, numWorkers)
	exit := make(chan struct{})

	// Fetch entry bundle resources in parallel.
	// We use a limited number of tokens here to prevent this from
	// consuming an unbounded amount of resources.
	go func() {
		ctx, span := tracer.Start(ctx, "tessera.storage.StreamAdaptorWorker")
		defer span.End()

		defer close(bundles)

		// We'll limit ourselves to numWorkers worth of on-going work using these tokens:
		tokens := make(chan struct{}, numWorkers)
		for range numWorkers {
			tokens <- struct{}{}
		}

		// We'll keep looping around until told to exit.
		for {
			// Check afresh what size the tree is so we can keep streaming entries as the tree grows.
			treeSize, err := getSize(ctx)
			if err != nil {
				klog.Warningf("StreamAdaptor: failed to get current tree size: %v", err)
				continue
			}
			klog.V(1).Infof("StreamAdaptor: streaming from %d to %d", fromEntry, treeSize)

			// For each bundle, pop a future into the bundles channel and kick off an async request
			// to resolve it.
		rangeLoop:
			for ri := range layout.Range(fromEntry, treeSize, treeSize) {
				select {
				case <-exit:
					break rangeLoop
				case <-tokens:
					// We'll return a token below, once the bundle is fetched _and_ is being yielded.
				}

				c := make(chan bundleOrErr, 1)
				go func(ri layout.RangeInfo) {
					b, err := getBundle(ctx, ri.Index, ri.Partial)
					c <- bundleOrErr{ri: ri, b: b, err: err}
				}(ri)

				f := func() bundleOrErr {
					b := <-c
					// We're about to yield a value, so we can now return the token and unblock another fetch.
					tokens <- struct{}{}
					return b
				}

				bundles <- f
			}

			// Next loop, carry on from where we got to.
			fromEntry = treeSize

			select {
			case <-exit:
				klog.Infof("StreamAdaptor: exiting")
				return
			case <-time.After(time.Second):
				// We've caught up with and hit the end of the tree, so wait a bit before looping to avoid busy waiting.
				// TODO(al): could consider a shallow channel of sizes here.
			}
		}
	}()

	cancel = func() {
		close(exit)
	}

	var streamErr error
	next = func() (layout.RangeInfo, []byte, error) {
		if streamErr != nil {
			return layout.RangeInfo{}, nil, streamErr
		}

		f, ok := <-bundles
		if !ok {
			streamErr = tessera.ErrNoMoreEntries
			return layout.RangeInfo{}, nil, streamErr
		}
		b := f()
		if b.err != nil {
			streamErr = b.err
		}
		return b.ri, b.b, b.err
	}
	return next, cancel
}
