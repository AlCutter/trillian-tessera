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

// migrate-posix-ct is a command-line tool for migrating data from a tlog-tiles
// compliant log, into a Tessera log instance.
package main

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	tessera "github.com/transparency-dev/trillian-tessera"
	"github.com/transparency-dev/trillian-tessera/api/layout"
	"github.com/transparency-dev/trillian-tessera/client"
	"github.com/transparency-dev/trillian-tessera/storage/posix"
	"k8s.io/klog/v2"
)

var (
	storageDir = flag.String("storage_dir", "", "Root directory to store log data.")
	sourceURL  = flag.String("source_url", "", "Base URL for the source log.")
	numWorkers = flag.Int("num_workers", 30, "Number of migration worker goroutines.")
)

func main() {
	klog.InitFlags(nil)
	flag.Parse()
	ctx := context.Background()

	srcURL, err := url.Parse(*sourceURL)
	if err != nil {
		klog.Exitf("Invalid --source_url %q: %v", *sourceURL, err)
	}
	src, err := client.NewHTTPFetcher(srcURL, nil)
	if err != nil {
		klog.Exitf("Failed to create HTTP fetcher: %v", err)
	}
	sourceCP, err := src.ReadCheckpoint(ctx)
	if err != nil {
		klog.Exitf("fetch initial source checkpoint: %v", err)
	}
	bits := strings.Split(string(sourceCP), "\n")
	sourceSize, err := strconv.ParseUint(bits[1], 10, 64)
	if err != nil {
		klog.Exitf("invalid CP size %q: %v", bits[1], err)
	}
	sourceRoot, err := base64.StdEncoding.DecodeString(bits[2])
	if err != nil {
		klog.Exitf("invalid checkpoint roothash %q: %v", bits[2], err)
	}

	driver, err := posix.New(ctx, *storageDir)
	if err != nil {
		klog.Exitf("Failed to create new POSIX storage driver: %v", err)
	}
	// Create our Tessera migration target instance
	opts := tessera.NewMigrationOptions().WithCTLayout()
	st, err := tessera.NewMigrationTarget(ctx, driver, opts)
	if err != nil {
		klog.Exitf("Failed to create new POSIX storage: %v", err)
	}

	readEntryBundle := readCTEntryBundle(*sourceURL)
	if err := tessera.Migrate(context.Background(), *numWorkers, sourceSize, sourceRoot, readEntryBundle, st); err != nil {
		klog.Exitf("Migrate failed: %v", err)
	}
}

func readCTEntryBundle(srcURL string) func(ctx context.Context, i uint64, p uint8) ([]byte, error) {
	return func(ctx context.Context, i uint64, p uint8) ([]byte, error) {
		up := strings.Replace(layout.EntriesPath(i, p), "entries", "data", 1)
		reqURL, err := url.JoinPath(srcURL, up)
		if err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
		if err != nil {
			return nil, err
		}
		rsp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer rsp.Body.Close()
		if rsp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("GET %q: %v", req.URL.Path, rsp.Status)
		}
		return io.ReadAll(rsp.Body)
	}
}
