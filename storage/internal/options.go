// Copyright 2024 Google LLC. All Rights Reserved.
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
	"fmt"

	"github.com/transparency-dev/merkle/rfc6962"
	tessera "github.com/transparency-dev/trillian-tessera"
	"github.com/transparency-dev/trillian-tessera/api"
	"github.com/transparency-dev/trillian-tessera/api/layout"
	"github.com/transparency-dev/trillian-tessera/internal/options"
)

// ResolveStorageOptions turns a variadic array of storage options into a StorageOptions instance.
func ResolveStorageOptions(opts ...func(*options.StorageOptions)) *options.StorageOptions {
	defaults := &options.StorageOptions{
		BatchMaxSize:       tessera.DefaultBatchMaxSize,
		BatchMaxAge:        tessera.DefaultBatchMaxAge,
		EntriesPath:        layout.EntriesPath,
		BundleHashes:       tlogTilesBundleHashes,
		CheckpointInterval: tessera.DefaultCheckpointInterval,
	}
	for _, opt := range opts {
		opt(defaults)
	}
	return defaults
}

func tlogTilesBundleHashes(bs []byte) ([][]byte, error) {
	eb := &api.EntryBundle{}
	if err := eb.UnmarshalText(bs); err != nil {
		return nil, fmt.Errorf("invalid bundle: %v", err)
	}
	r := make([][]byte, len(eb.Entries))
	for i := range eb.Entries {
		r[i] = rfc6962.DefaultHasher.HashLeaf(eb.Entries[i])
	}
	return r, nil
}
